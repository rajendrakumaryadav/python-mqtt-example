import gzip
import io
import logging
from uuid import uuid4

# third party imports
import bson
import numpy as np
from PIL import Image
from paho.mqtt import client as mqtt_client

logging.basicConfig(level=logging.DEBUG)

# broker address
broker = 'localhost'
port = 1883
topic = "Bridge/Cam1/image"

# generate client ID with pub prefix randomly
client_id = f'mqtt-{uuid4()}'
# define global is_connected
is_connected = False


def get_info_from_topic(topic):
    # Split topic into parts
    topic_parts = topic.split('/')
    # Extract information from topic
    return {
        'bridge_id': topic_parts[0],
        'camera_id': topic_parts[1],
        'image_type': topic_parts[2]
    }


def print_payload_size(payload):
    # Print payload size in kb
    return f"{round(len(payload) / 1024, 2)} KB"


# Callback fires when connected to MQTT broker.
def on_connect(client, userdata, flags, rc):
    global is_connected  # Use global variable
    is_connected = True  # Signal connection
    print(f'Connected (RC {rc}), subscribing to {topic}, client ID: {client_id}')
    # Subscribe (or renew if reconnect).
    client.subscribe(topic)


def on_message(client, userdata, msg):
    print(get_info_from_topic(msg.topic))
    if is_connected:
        try:
            print(f"Payload size compressed: {print_payload_size(msg.payload)}")
            original_data = gzip.decompress(msg.payload)
            bson_data = bson.loads(original_data)
            print(f"Payload size original: {print_payload_size(msg.payload)}")
            client.publish("home/sensor1/temperature", '{"ack": "received"}', qos=1)
        except gzip.BadGzipFile as e:
            print(f'Invalid GZIP data received from {msg.topic}')
            return 1
        # handle invalid BSON data
        except bson.InvalidBSON as e:
            print(f'Invalid BSON data received from {msg.topic}')
            return 1

        # Save image file in bson_data['image']
        with open('./received.jpg', 'wb') as f:
            img = Image.open(io.BytesIO(bson_data['image']), mode='r')
            # Image to tensor supported by pytorch
            np_img = np.asarray(img)
            print(np_img.shape)

        print(f'Received `{bson_data["temperature"]}` from {msg.topic}')
        print(f'Received `{bson_data["sensor_id"]}` from {msg.topic}')
        print(f"Received `{bson_data['location']}` from {msg.topic}")


try:
    # Create MQTT client and connect to localhost, i.e. the MQTT broker itself.
    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker, port)
    client.username_pw_set("admin", "admin")
    client.loop_forever()
except KeyboardInterrupt as key:
    print("Keyboard Interrupt")
    client.disconnect()
    client.loop_stop()
