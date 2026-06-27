#!/usr/bin/env python3

import asyncio
import json
import aiocoap
import aiocoap.resource as resource
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

TOKEN  = "-dDRkdv-5FuOWXeJSpnVr_gjHKrhGw2CyYp-MezlwAyIU5cwKbaQKyPV5--0hRj0kZHJ8W_cXymV6JaB_vBo6g=="
ORG    = "myorg"
BUCKET = "iot_health"
URL    = "http://localhost:8086"

influx_client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)


#writes a "registration" point: a (re)registration event for a node
#each call creates a new point with the current timestamp
def write_registration(node_id, node_type, ip_address):
    point = (
        Point("registration")
        .tag("node_id", node_id)
        .tag("type", node_type)
        .field("ip_address", ip_address)
    )
    write_api.write(bucket=BUCKET, org=ORG, record=point)


#resource CoAP "/registration"
#receives a POST from the nodes (patient or caregiver) at startup, with a JSON payload like: {"node_id":"a1b2c3","type":"patient"}
class RegistrationResource(resource.Resource):
    async def render_post(self, request):
        try:
            payload = json.loads(request.payload.decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            return aiocoap.Message(
                code=aiocoap.BAD_REQUEST,
                payload=b'{"error":"invalid JSON"}'
            )

        node_id = payload.get("node_id")
        node_type = payload.get("type")

        if not node_id or not node_type:
            return aiocoap.Message(
                code=aiocoap.BAD_REQUEST,
                payload=b'{"error":"missing node_id or type"}'
            )

        ip_address = str(request.remote.sockaddr[0])

        write_registration(node_id, node_type, ip_address)

        print(f"[Registration] node_id={node_id} type={node_type} ip={ip_address}")

        response_payload = json.dumps({"status": "registered", "node_id": node_id}).encode()
        return aiocoap.Message(code=aiocoap.CHANGED, payload=response_payload)


async def main():
    root = resource.Site()
    root.add_resource(['.well-known', 'core'],
                      resource.WKCResource(root.get_resources_as_linkheader))
    root.add_resource(['registration'], RegistrationResource())

    await aiocoap.Context.create_server_context(root)
    print('CoAP Registration Server running on port 5683 (resource: /registration)')

    await asyncio.get_running_loop().create_future()  #run forever


if __name__ == '__main__':
    try:
        asyncio.run(main())
    finally:
        influx_client.close()