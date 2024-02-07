import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
import time

client_id = "testclient2"
mqttv = mqtt.MQTTv5
messages = []
host = 'becker20.local'
port = 1883
pub_topic = "test/property"


def on_publish(client, userdata, mid):
    print("published")


def on_connect(client, userdata, flags, reasonCode, properties=None):
    print('Connected ', flags)
    print('Connected properties', properties)
    print('Connected ', reasonCode)


def on_message(client, userdata, message):
    msg = str(message.payload.decode("utf-8"))
    messages.append(msg)
    print("correlation=", message.properties)
    print('RECV Topic = ', message.topic)
    print('RECV MSG =', msg)
    print("properties received= ", message.properties)
    user_properties = message.properties.UserProperty
    print("user properties received= ", user_properties)


def on_disconnect(client, userdata, rc):
    print('Received Disconnect ', rc)


def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print('SUBSCRIBED')


def on_unsubscribe(client, userdata, mid, properties, reasonCodes):
    print('UNSUBSCRIBED')


print("creating client")

client_sub = mqtt.Client("subclient", protocol=mqttv)
client_pub = mqtt.Client("pubclient", protocol=mqttv)

client_sub.on_connect = on_connect
client_pub.on_connect = on_connect
client_sub.on_message = on_message
client_sub.on_disconnect = on_disconnect
client_sub.on_subscribe = on_subscribe
client_pub.on_publish = on_publish

client_sub.connect(host)
client_sub.loop_start()
client_sub.subscribe('test/#', qos=0)
client_pub.connect(host)

# while(not client_sub.is_connected() and not client_sub.is_connected())
print("waiting for connection")
time.sleep(5)
print("connected")
print("sending message user properties set")
properties = Properties(PacketTypes.PUBLISH)
count = "1"
properties.UserProperty = [("filename", "test.txt"), ("count", count),
                           ("becker", "was here")]
while True:
    client_pub.publish("test/mqtt", "test message", properties=properties)
    time.sleep(5)
