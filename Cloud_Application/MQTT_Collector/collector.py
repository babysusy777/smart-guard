#!/usr/bin/env python3

import json
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


TOKEN  = "-dDRkdv-5FuOWXeJSpnVr_gjHKrhGw2CyYp-MezlwAyIU5cwKbaQKyPV5--0hRj0kZHJ8W_cXymV6JaB_vBo6g=="
ORG    = "myorg"
BUCKET = "iot_health"
URL    = "http://localhost:8086"

influx_client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

#MQTT broker configuration
MQTT_BROKER = "localhost"
MQTT_PORT = 1883

#wildcard topics: '+' matches exactly one topic level, so this subscribes to every patient's heartbeat/alarm without knowing their node_id in advance
HEARTBEAT_TOPIC = "health/+/heartbeat"
ALARM_TOPIC = "alarm/+"
BATTERY_TOPIC = "battery/+"



#writes a heartbeat point: a periodic status report from a patient/caregiver node
#the Cloud App uses the timestamp of the most recent report for a given node_id to detect failures
def write_heartbeat(node_id, state, node_type):
    point = (
        Point("heartbeat")
        .tag("node_id", node_id)
        .field("state", state)
        .field("type", node_type)
    )
    write_api.write(bucket=BUCKET, org=ORG, record=point)


#writes an alarm point: a FALL or RESOLVED event from a patient node
#the Cloud App control logic reads these points to know which alarms are currently active
def write_alarm(node_id, event):
    point = (
        Point("alarm")
        .tag("node_id", node_id)
        .field("event", event)
    )
    write_api.write(bucket=BUCKET, org=ORG, record=point)

def write_notification(node_id, event, payload):
    point = (
        Point("notification")
        .tag("node_id", node_id)
        .field("event", event)
    )

    if "reason" in payload:
        point = point.field("reason", str(payload["reason"]))

    if "battery" in payload:
        point = point.field("battery", int(payload["battery"]))

    if "new_rate" in payload:
        point = point.field("new_rate", int(payload["new_rate"]))

    if "requires_ack" in payload:
        point = point.field("requires_ack", bool(payload["requires_ack"]))

    write_api.write(bucket=BUCKET, org=ORG, record=point)

def write_battery(node_id, node_type, battery, new_rate=None):
    point = (
        Point("battery")
        .tag("node_id", node_id)
        .field("type", node_type)
        .field("battery", int(battery))
    )

    if new_rate is not None:
        point = point.field("new_rate", int(new_rate))

    write_api.write(bucket=BUCKET, org=ORG, record=point)

def write_config_rate(node_id, rate, reason):
    point = (
        Point("config")
        .tag("node_id", node_id)
        .field("rate", int(rate))
        .field("reason", reason)
    )

    write_api.write(bucket=BUCKET, org=ORG, record=point)

def notify_caregiver_patient_battery(client, patient_id, battery, new_rate=None):
    topic = f"alarm/{patient_id}"

    payload = {
        "node_id": patient_id,
        "event": "BATTERY_LOW",
        "battery": int(battery),
        "requires_ack": False,
        "source": "collector"
    }

    if new_rate is not None:
        payload["new_rate"] = int(new_rate)

    result = client.publish(topic, json.dumps(payload), qos=1)

    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        print(f"[Caregiver notification] patient={patient_id} BATTERY_LOW battery={battery}%")
    else:
        print(f"[Collector] MQTT publish failed on {topic}: rc={result.rc}")

#called when the client connects to the broker: handles the first connection and any future reconnections
def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected to MQTT broker (reason_code={reason_code})")
    client.subscribe(HEARTBEAT_TOPIC)
    client.subscribe(ALARM_TOPIC)
    client.subscribe(BATTERY_TOPIC)
    print(f"Subscribed to '{HEARTBEAT_TOPIC}', '{ALARM_TOPIC}' and '{BATTERY_TOPIC}'")


#called for every message on a subscribed topic 
#we look at the topic to decide if it's a heartbeat or an alarm, then parse the JSON payload and write the point to InfluxDB
def on_message(client, userdata, msg):
    topic_parts = msg.topic.split("/")

    try:
        payload = json.loads(msg.payload.decode())
    except (json.JSONDecodeError, UnicodeDecodeError):
        print(f"Ignoring malformed payload on topic {msg.topic}")
        return

    node_id = payload.get("node_id")
    if not node_id:
        print(f"Ignoring payload without node_id on topic {msg.topic}")
        return

    if topic_parts[0] == "health" and topic_parts[-1] == "heartbeat":
        state = payload.get("state", "NORMAL")
        node_type = payload.get("type", "unknown")

        write_heartbeat(node_id, state, node_type)

        print(f"[Heartbeat] node_id={node_id} type={node_type} state={state}")

    elif topic_parts[0] == "alarm":
        event = payload.get("event")

        if not event:
            print(f"Ignoring alarm payload without event on topic {msg.topic}")
            return

        if event in ("FALL", "RESOLVED"):
            write_alarm(node_id, event)
            print(f"[Alarm] node_id={node_id} event={event}")

        elif event in ("BATTERY_LOW", "NODE_CRITICAL", "NODE_RECOVERED"):

            if event == "BATTERY_LOW" and payload.get("source") == "collector":
                return

            write_notification(node_id, event, payload)
            print(f"[Notification] node_id={node_id} event={event}")

        else:
            write_notification(node_id, event, payload)
            print(f"[Notification] node_id={node_id} unknown_event={event}")

    elif topic_parts[0] == "battery":
        event = payload.get("event")
        node_type = payload.get("type", "unknown")
        battery = payload.get("battery")
        new_rate = payload.get("new_rate")

        if event != "BATTERY_LOW":
            print(f"Ignoring unknown battery event on topic {msg.topic}: {event}")
            return

        if battery is None:
            print(f"Ignoring BATTERY_LOW without battery level on topic {msg.topic}")
            return

        write_battery(node_id, node_type, battery, new_rate)

        if new_rate is not None:
            write_config_rate(
                node_id=node_id,
                rate=new_rate,
                reason="low_battery"
            )

        print(
            f"[Battery] node_id={node_id} type={node_type} "
            f"battery={battery}% new_rate={new_rate}"
        )

        if node_type == "patient":
            notify_caregiver_patient_battery(client, node_id, battery, new_rate)


def main():
    #callback_api_version is required since paho-mqtt 2.0
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)

    print("MQTT Collector running, waiting for messages...")
    client.loop_forever()


if __name__ == '__main__':
    try:
        main()
    finally:
        influx_client.close()