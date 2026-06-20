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

#failure detection thresholds
#different thresholds: if the node has an active FALL alarm (heartbeat rate decrease)
NORMAL_THRESHOLD_SECONDS = 90  #about 3 missed heartbeats at the 30s rate
ALARM_THRESHOLD_SECONDS = 3  #about 6 missed heartbeats at the 0.5s rate

POLL_INTERVAL_SECONDS = 10

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


def poll_once():
    last_heartbeats = get_last_heartbeats()
    active_alarms = get_active_alarms()
    now = datetime.now(timezone.utc)

    for node_id, last_seen in last_heartbeats.items():
        elapsed = (now - last_seen).total_seconds()
        has_active_alarm = node_id in active_alarms
        threshold = ALARM_THRESHOLD_SECONDS if has_active_alarm else NORMAL_THRESHOLD_SECONDS

        severity = "CRITICAL" if elapsed > threshold else "NORMAL"
        write_failure_status(node_id, severity)

        if severity == "CRITICAL":
            print(f"[FailureDetector] {node_id}: CRITICAL "
                  f"(no heartbeat for {elapsed:.1f}s, threshold={threshold}s, "
                  f"active_alarm={has_active_alarm})")


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