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


#writes a heartbeat point: a periodic status report from a patient node
#the Cloud App uses the timestamp of the most recent report for a given node_id to detect failures
def write_heartbeat(node_id, state):
    point = (
        Point("heartbeat")
        .tag("node_id", node_id)
        .field("state", state)
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


#called when the client connects to the broker: handles the first connection and any future reconnections
def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected to MQTT broker (reason_code={reason_code})")
    client.subscribe(HEARTBEAT_TOPIC)
    client.subscribe(ALARM_TOPIC)
    print(f"Subscribed to '{HEARTBEAT_TOPIC}' and '{ALARM_TOPIC}'")


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
        write_heartbeat(node_id, state)
        print(f"[Heartbeat] node_id={node_id} state={state}")

    elif topic_parts[0] == "alarm":
        event = payload.get("event")
        if event:
            write_alarm(node_id, event)
            print(f"[Alarm] node_id={node_id} event={event}")


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