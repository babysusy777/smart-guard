#!/usr/bin/env python3

import time
from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

TOKEN  = "-dDRkdv-5FuOWXeJSpnVr_gjHKrhGw2CyYp-MezlwAyIU5cwKbaQKyPV5--0hRj0kZHJ8W_cXymV6JaB_vBo6g=="
ORG    = "myorg"
BUCKET = "iot_health"
URL    = "http://localhost:8086"

influx_client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)
query_api = influx_client.query_api()

POLL_INTERVAL_SECONDS = 10

#failure detection thresholds
ALARM_SPEEDUP_FACTOR = 60 
MISSED_NORMAL = 3
MISSED_ALARM = 6
FALLBACK_RATE_SECONDS = 30

#returns {node_id: last_heartbeat_datetime} for every node that has published at least one heartbeat in the last hour
def get_last_heartbeats():
    flux_query = f'''
    from(bucket: "{BUCKET}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "heartbeat")
      |> filter(fn: (r) => r._field == "state")
      |> group(columns: ["node_id"])
      |> last()
    '''
    tables = query_api.query(flux_query, org=ORG)

    result = {}
    for table in tables:
        for record in table.records:
            result[record.values.get("node_id")] = record.get_time()
    return result


#Returns the set of node_id whose most recent alarm event is "FALL" not yet followed by a "RESOLVED"
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


def write_failure_status(node_id, severity):
    point = (
        Point("failure_status")
        .tag("node_id", node_id)
        .field("severity", severity)
    )
    write_api.write(bucket=BUCKET, org=ORG, record=point)


#to understand the actual rate
def get_configured_rates():
    flux_query = f'''
    from(bucket: "{BUCKET}")
      |> range(start: -24h)
      |> filter(fn: (r) => r._measurement == "config")
      |> filter(fn: (r) => r._field == "rate")
      |> group(columns: ["node_id"])
      |> last()
    '''
    tables = query_api.query(flux_query, org=ORG)
    result = {}
    for table in tables:
        for record in table.records:
            result[record.values.get("node_id")] = record.get_value()
    return result


def poll_once():
    last_heartbeats = get_last_heartbeats()
    active_alarms = get_active_alarms()
    configured_rates = get_configured_rates()
    now = datetime.now(timezone.utc)

    for node_id, last_seen in last_heartbeats.items():
        elapsed = (now - last_seen).total_seconds()
        has_active_alarm = node_id in active_alarms
        rate = configured_rates.get(node_id, FALLBACK_RATE_SECONDS)   # <-- mancava

        if has_active_alarm:
            alarm_interval = max(rate / ALARM_SPEEDUP_FACTOR, 0.5)
            threshold = MISSED_ALARM * alarm_interval
        else:
            threshold = MISSED_NORMAL * rate

        severity = "CRITICAL" if elapsed > threshold else "NORMAL"
        write_failure_status(node_id, severity)

        if severity == "CRITICAL":
            print(f"[FailureDetector] {node_id}: CRITICAL "
                  f"(no heartbeat for {elapsed:.1f}s, threshold={threshold:.1f}s, "
                  f"active_alarm={has_active_alarm}, rate={rate}s)")


def main():
    print(f"Failure Detector running (polling every {POLL_INTERVAL_SECONDS}s)")
    while True:
        try:
            poll_once()
        except Exception as e:
            print(f"[FailureDetector] error during poll: {e}")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == '__main__':
    try:
        main()
    finally:
        influx_client.close()