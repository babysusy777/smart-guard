#!/usr/bin/env python3

import asyncio
import json
import time
import aiocoap
from influxdb_client import InfluxDBClient
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

TOKEN  = "-dDRkdv-5FuOWXeJSpnVr_gjHKrhGw2CyYp-MezlwAyIU5cwKbaQKyPV5--0hRj0kZHJ8W_cXymV6JaB_vBo6g=="
ORG    = "myorg"
BUCKET = "iot_health"
URL    = "http://localhost:8086"

influx_client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)
query_api = influx_client.query_api()

#rate adaptation parameters
DEFAULT_RATE_SECONDS = 30
MAX_RATE_SECONDS = 240
WINDOW_SECONDS = 5 * 60 #5 minutes
EXPECTED_RATIO_THRESHOLD = 0.7 #below 70% of expected heartbeats -> congestion
POLL_INTERVAL_SECONDS = 60 #how often we check for congestion

ALARM_SPEEDUP_FACTOR = 60

CONFIG_PATH = "/config"
COAP_PORT = 5683

#node_id -> {"ip": str, "rate": int}
#made at startup via a GET /config to each registered node
node_state = {}

#returns {node_id: ip_address} for every node (patients and caregivers)
def get_registered_nodes():
    flux_query = f'''
    from(bucket: "{BUCKET}")
      |> range(start: -24h)
      |> filter(fn: (r) => r._measurement == "registration")
      |> filter(fn: (r) => r._field == "ip_address")
      |> group(columns: ["node_id"])
      |> last()
    '''
    tables = query_api.query(flux_query, org=ORG)

    result = {}
    for table in tables:
        for record in table.records:
            result[record.values.get("node_id")] = record.get_value()
    return result


#used by failure_detection to understand actual rate
def write_config_rate(node_id, rate):
    point = Point("config").tag("node_id", node_id).field("rate", rate)
    write_api.write(bucket=BUCKET, org=ORG, record=point)


#counts how many heartbeat were written for this node_id in the last WINDOW_SECONDS
def count_heartbeats(node_id):
    flux_query = f'''
    from(bucket: "{BUCKET}")
      |> range(start: -{WINDOW_SECONDS}s)
      |> filter(fn: (r) => r._measurement == "heartbeat")
      |> filter(fn: (r) => r._field == "state")
      |> filter(fn: (r) => r.node_id == "{node_id}")
      |> count()
    '''
    tables = query_api.query(flux_query, org=ORG)

    for table in tables:
        for record in table.records:
            return record.get_value()
    return 0

#adaptation for active alarms
def get_active_alarms():
    flux_query = f'''
    from(bucket: "{BUCKET}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "alarm")
      |> filter(fn: (r) => r._field == "event")
      |> group(columns: ["node_id"])
      |> last()
    '''
    tables = query_api.query(flux_query, org=ORG)
    active = set()
    for table in tables:
        for record in table.records:
            if record.get_value() == "FALL":
                active.add(record.values.get("node_id"))
    return active


#GET /config on a node, used at startup to discover its current rate
async def coap_get_config(ip_address):
    protocol = await aiocoap.Context.create_client_context()
    request = aiocoap.Message(
        code=aiocoap.GET,
        uri=f'coap://[{ip_address}]{CONFIG_PATH}'
    )
    try:
        response = await protocol.request(request).response
        data = json.loads(response.payload.decode())
        return data.get("rate", DEFAULT_RATE_SECONDS)
    except Exception as e:
        print(f"[CoAP_actuator] GET /config failed for {ip_address}: {e}")
        return DEFAULT_RATE_SECONDS
    finally:
        await protocol.shutdown()


#PUT /config on a node to change its publish rate
async def coap_put_config(ip_address, new_rate):
    protocol = await aiocoap.Context.create_client_context()
    request = aiocoap.Message(
        code=aiocoap.PUT,
        uri=f'coap://[{ip_address}]{CONFIG_PATH}',
        payload=f"rate={new_rate}".encode()
    )
    try:
        response = await protocol.request(request).response
        print(f"[CoAP_actuator] PUT /config -> {ip_address}: rate={new_rate} "
              f"(response code={response.code})")
    except Exception as e:
        print(f"[CoAP_actuator] PUT /config failed for {ip_address}: {e}")
    finally:
        await protocol.shutdown()


#for every node calls coap_get_config
async def initialize_node_state():
    nodes = get_registered_nodes()
    now = time.time()
    for node_id, ip_address in nodes.items():
        rate = await coap_get_config(ip_address)
        node_state[node_id] = {"ip": ip_address, "rate": rate, "discovered_at": now}
        write_config_rate(node_id, rate)
        print(f"[CoAP_actuator] Discovered {node_id} ({ip_address}): rate={rate}s")


#for every node, counts heartbeats received in the last window and compares to the expected ones
#if the ratio is below threshold, doubles the rate and PUT /config to the node
async def check_congestion_once():
    nodes = get_registered_nodes()
    for node_id, ip_address in nodes.items():
        if node_id not in node_state:
            rate = await coap_get_config(ip_address)
            node_state[node_id] = {"ip": ip_address, "rate": rate, "discovered_at": time.time()}
            write_config_rate(node_id, rate) 

    for node_id, state in node_state.items():
        active_alarms = get_active_alarms()
        elapsed_since_discovery = time.time() - state["discovered_at"]

        effective_rate = state["rate"]
        if node_id in active_alarms:
            effective_rate = max(state["rate"] / ALARM_SPEEDUP_FACTOR, 0.5)
        
        #do not control a node until he sent at least 2 heartbits
        if elapsed_since_discovery < effective_rate * 2:
            continue

        effective_window = min(elapsed_since_discovery, WINDOW_SECONDS)
        expected = effective_window / effective_rate
        received = count_heartbeats(node_id)
        ratio = received / expected if expected > 0 else 1.0

        if ratio < EXPECTED_RATIO_THRESHOLD:
            new_rate = min(state["rate"] * 2, MAX_RATE_SECONDS)
            if new_rate != state["rate"]:
                print(f"[CoAP_actuator] Congestion detected for {node_id}: "
                      f"received {received}/{expected:.0f} heartbeats "
                      f"(ratio={ratio:.2f}). Raising rate {state['rate']}s -> {new_rate}s")
                await coap_put_config(state["ip"], new_rate)
                state["rate"] = new_rate
                write_config_rate(node_id, new_rate) 
            else:
                #no congestions are present, so if the rate is over the default we reduce it 
                if state["rate"] > DEFAULT_RATE_SECONDS:
                    new_rate = max(state["rate"] // 2, DEFAULT_RATE_SECONDS)
                    print(f"[CoAP_actuator] No congestion for {node_id} "
                        f"(ratio={ratio:.2f}). Lowering rate {state['rate']}s -> {new_rate}s")
                    await coap_put_config(state["ip"], new_rate)
                    state["rate"] = new_rate
                    write_config_rate(node_id, new_rate) 


async def main():
    print("CoAP Actuator (rate adaptation) starting...")
    await initialize_node_state()

    print(f"CoAP Actuator running, checking for congestion every "
          f"{POLL_INTERVAL_SECONDS}s")
    while True:
        try:
            await check_congestion_once()
        except Exception as e:
            print(f"[CoAP_actuator] error during check: {e}")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    finally:
        influx_client.close()