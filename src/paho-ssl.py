import ssl
import paho.mqtt.client as mqtt
import os
from logger import Logger

BROKER_URL = os.getenv('BROKER_URL')
BROKER_PORT = int(os.getenv('BROKER_PORT', 1883))
BROKER_CERT_CA_FILE = os.getenv('BROKER_CERT_CA_FILE')
BROKER_CERT_FILE = os.getenv('BROKER_CERT_FILE')
BROKER_CERT_KEY_FILE = os.getenv('BROKER_CERT_KEY_FILE')

# Set up global logger
logger = Logger(__name__)


def on_connect(client, userdata, flags, rc):
    logger.info("Connected with result code "+str(rc))
    client.subscribe("some/topic")


def on_message(client, userdata, msg):
    logger.debug(msg.topic+" "+str(msg.payload))


# Create an MQTT client instance
client = mqtt.Client()

# Set the username and password if needed
# client.username_pw_set("username", "password")

# Set the callback methods
client.on_connect = on_connect
client.on_message = on_message

# check if the BROKER_CERT_CA_FILE and BROKER_CERT_FILE
# exists if it does, proceed calling the client.tls_set
if BROKER_CERT_CA_FILE is not None \
        and BROKER_CERT_FILE is not None \
        and BROKER_CERT_KEY_FILE is not None \
        and os.path.isfile(BROKER_CERT_CA_FILE) \
        and os.path.isfile(BROKER_CERT_FILE) \
        and os.path.isfile(BROKER_CERT_KEY_FILE):
    logger.debug("Using TLS")
    client.tls_set(
        ca_certs=BROKER_CERT_CA_FILE,
        certfile=BROKER_CERT_FILE,
        keyfile=BROKER_CERT_KEY_FILE,
        cert_reqs=ssl.CERT_REQUIRED,
        tls_version=ssl.PROTOCOL_TLSv1_2,
        ciphers=None,
    )
    client.tls_insecure_set(True)

broker_address = BROKER_URL
broker_port = BROKER_PORT
logger.debug("Attempting connection")
client.connect(broker_address, broker_port)
logger.info(client.is_connected())

# Start the loop and wait for messages
client.loop_forever()
