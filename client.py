# make a python script to send json temperature data to mqtt
# client every second running on localhost and default port
import asyncio
import gzip
from datetime import datetime
from uuid import uuid4

import bson
import paho.mqtt.client as mqtt_client

# broker address
broker = 'localhost'
port = 1883
topic = "Bridge/Cam1/image"
listen_topic = "home/sensor1/temperature"

# generate client ID with pub prefix randomly
client_id = f'mqtt-{uuid4()}'


def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe(listen_topic)


def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


async def publish():
    """
    Publish fake sensor data to MQTT topic
    :param topic: MQTT topic
    :param n: number of messages to send
    :param t: time interval between messages
    :param compress: compress payload
    :param verbose: print messages
    :param debug: print debug messages
    :return:
    """
    with open('./test.jpg', 'rb') as f:
        img_data = f.read()

    payload = bson.dumps({
        'temperature': 25,
        'humidity': 50,
        'location': 'home',
        'sensor_id': 'sensor1',
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'image': img_data
    })

    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect('localhost', 1883, 60)
    # Serialize payload to JSON string
    # Publish message to MQTT broker
    compressed_data = gzip.compress(payload)
    client.publish(topic, compressed_data, qos=1)
    # Schedule task in 1 second
    client.on_subscribe = on_subscribe
    await asyncio.sleep(1)
    for i in range(10):
        client.publish(topic, payload, qos=1)
        await asyncio.sleep(1)
    client.loop_forever()
# Run event loop until KeyboardInterrupt

asyncio.run(publish())