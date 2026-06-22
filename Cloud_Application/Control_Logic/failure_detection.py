#!/usr/bin/env python3

import time
from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json
import paho.mqtt.client as mqtt

TOKEN  = "-dDRkdv-5FuOWXeJSpnVr_gjHKrhGw2CyYp-MezlwAyIU5cwKbaQKyPV5--0hRj0kZHJ8W_cXymV6JaB_vBo6g=="
ORG    = "myorg"
BUCKET = "iot_health"
URL    = "http://localhost:8086"

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_CLIENT_ID = "failure_detector"

influx_client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)
query_api = influx_client.query_api()

mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID)
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
mqtt_client.loop_start()


POLL_INTERVAL_SECONDS = 10

#failure detection thresholds
ALARM_SPEEDUP_FACTOR = 10 
MISSED_NORMAL = 3
MISSED_ALARM = 6
FALLBACK_PATIENT_RATE_SECONDS = 30
FALLBACK_CAREGIVER_RATE_SECONDS = 60

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

def get_last_failure_statuses():
    flux_query = f'''
    from(bucket: "{BUCKET}")
      |> range(start: -24h)
      |> filter(fn: (r) => r._measurement == "failure_status")
      |> filter(fn: (r) => r._field == "severity")
      |> group(columns: ["node_id"])
      |> last()
    '''
    tables = query_api.query(flux_query, org=ORG)

    result = {}
    for table in tables:
        for record in table.records:
            node_id = record.values.get("node_id")
            if node_id is not None:
                result[node_id] = record.get_value()

    return result

def write_recovery_event(node_id, elapsed, threshold, has_active_alarm, rate):
    point = (
        Point("recovery_event")
        .tag("node_id", node_id)
        .field("event", "RECOVERED")
        .field("previous_severity", "CRITICAL")
        .field("reason", "heartbeat_restored")
        .field("elapsed_seconds", float(elapsed))
        .field("threshold_seconds", float(threshold))
        .field("active_alarm", bool(has_active_alarm))
        .field("configured_rate_seconds", float(rate))
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

def get_effective_rate(node_id, node_type, configured_rates):
    if node_type == "caregiver":
        fallback_rate = FALLBACK_CAREGIVER_RATE_SECONDS
    else:
        fallback_rate = FALLBACK_PATIENT_RATE_SECONDS

    raw_rate = configured_rates.get(node_id, fallback_rate)

    try:
        rate = float(raw_rate)
    except (TypeError, ValueError):
        rate = float(fallback_rate)

    if rate <= 0:
        rate = float(fallback_rate)

    return rate


def poll_once():
    last_heartbeats = get_last_heartbeats()
    active_alarms = get_active_alarms()
    configured_rates = get_configured_rates()
    last_failure_statuses = get_last_failure_statuses()
    node_types = get_node_types()

    patient_ids = [
        node_id for node_id, node_type in node_types.items()
        if node_type == "patient"
    ]

    now = datetime.now(timezone.utc)

    for node_id, last_seen in last_heartbeats.items():
        elapsed = (now - last_seen).total_seconds()
        has_active_alarm = node_id in active_alarms
        node_type = node_types.get(node_id, "unknown")

        rate = get_effective_rate(node_id, node_type, configured_rates)   

        if has_active_alarm:
            alarm_interval = max(rate / ALARM_SPEEDUP_FACTOR, 2.0)
            threshold = max(MISSED_ALARM * alarm_interval, 15.0)
        else:
            threshold = MISSED_NORMAL * rate
        
        previous_severity = last_failure_statuses.get(node_id)
        severity = "CRITICAL" if elapsed > threshold else "NORMAL"
        write_failure_status(node_id, severity)

        if severity == "CRITICAL":
            print(f"[FailureDetector] {node_id}: CRITICAL "
                  f"(type={node_type}, no heartbeat for {elapsed:.1f}s, "
                  f"threshold={threshold:.1f}s, active_alarm={has_active_alarm}, "
                  f"rate={rate}s)")

            if previous_severity != "CRITICAL":
                if node_type == "patient":
                    notify_caregiver_patient_critical(patient_id=node_id, elapsed=elapsed, threshold=threshold)
                elif node_type == "caregiver":
                    notify_patients_caregiver_critical(patient_ids=patient_ids, caregiver_id=node_id)
                else:
                    print(f"[FailureDetector] Unknown node type for {node_id}. "
                          f"No external notification sent.")  
        elif previous_severity == "CRITICAL" and severity == "NORMAL":
            write_recovery_event(node_id=node_id, elapsed=elapsed, threshold=threshold, has_active_alarm=has_active_alarm, rate=rate)

            print(f"[FailureDetector] {node_id}: RECOVERED "
                  f"(type={node_type}, heartbeat restored, elapsed={elapsed:.1f}s, "
                  f"threshold={threshold:.1f}s, active_alarm={has_active_alarm}, "
                  f"rate={rate}s)")

            if node_type == "patient":
                notify_caregiver_patient_recovered(patient_id=node_id, elapsed=elapsed, threshold=threshold)

            elif node_type == "caregiver":
                notify_patients_caregiver_recovered(patient_ids=patient_ids, caregiver_id=node_id)

            else:
                print(f"[FailureDetector] Unknown node type for {node_id}. "
                      f"No recovery notification sent.")


def publish_mqtt_event(topic, payload):
    message = json.dumps(payload)
    result = mqtt_client.publish(topic, message, qos=1, retain=False)

    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        print(f"[FailureDetector] MQTT publish failed on {topic}: rc={result.rc}")
    else:
        print(f"[FailureDetector] MQTT published on {topic}: {message}")

def get_node_types():
    flux_query = f'''
    from(bucket: "{BUCKET}")
      |> range(start: -24h)
      |> filter(fn: (r) => r._measurement == "heartbeat")
      |> filter(fn: (r) => r._field == "type")
      |> group(columns: ["node_id"])
      |> last()
    '''
    tables = query_api.query(flux_query, org=ORG)

    result = {}
    for table in tables:
        for record in table.records:
            result[record.values.get("node_id")] = record.get_value()
    return result

def notify_caregiver_patient_critical(patient_id, elapsed, threshold):
    topic = f"alarm/{patient_id}"

    payload = {
        "node_id": patient_id,
        "event": "NODE_CRITICAL",
        "reason": "heartbeat_missing",
        "elapsed_seconds": round(float(elapsed), 2),
        "threshold_seconds": round(float(threshold), 2)
    }

    publish_mqtt_event(topic, payload)


def notify_caregiver_patient_recovered(patient_id, elapsed, threshold):
    topic = f"alarm/{patient_id}"

    payload = {
        "node_id": patient_id,
        "event": "NODE_RECOVERED",
        "reason": "heartbeat_restored",
        "elapsed_seconds": round(float(elapsed), 2),
        "threshold_seconds": round(float(threshold), 2)
    }

    publish_mqtt_event(topic, payload)

def notify_patients_caregiver_critical(patient_ids, caregiver_id):
    for patient_id in patient_ids:
        topic = f"alarm/{patient_id}/ack"

        payload = {
            "node_id": patient_id,
            "event": "CAREGIVER_CRITICAL",
            "caregiver_id": caregiver_id,
            "action": "ENABLE_LOCAL_SOUND_ON_FALL"
        }

        publish_mqtt_event(topic, payload)


def notify_patients_caregiver_recovered(patient_ids, caregiver_id):
    for patient_id in patient_ids:
        topic = f"alarm/{patient_id}/ack"

        payload = {
            "node_id": patient_id,
            "event": "CAREGIVER_RECOVERED",
            "caregiver_id": caregiver_id,
            "action": "DISABLE_LOCAL_SOUND_ON_FALL"
        }

        publish_mqtt_event(topic, payload)


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
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        influx_client.close()