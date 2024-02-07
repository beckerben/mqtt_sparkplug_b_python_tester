import time
import json
from paho.mqtt.client import Client
from typing import Callable, List
from enum import Enum


class MqttMessageType(Enum):

    NODE_BIRTH = ('NBIRTH', 0, False)
    NODE_DEATH = ('NDEATH', 0, False)
    NODE_COMMAND = ('NCMD', 0, False)
    NODE_DATA = ('NDATA', 0, False)
    DEVICE_BIRTH = ('DBIRTH', 0, False)
    DEVICE_DEATH = ('DDEATH', 0, False)
    DEVICE_COMMAND = ('DCMD', 0, False)
    DEVICE_DATA = ('DDATA', 0, False)

    def __init__(self, topic_verb: str, qos: int = 0, retain: bool = False):
        self.topic_verb = topic_verb
        self.qos = qos
        self.retain = retain


class DictionaryEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__dict__


class MqttService:
    def __init__(self,
                 module: str,
                 broker_url: str,
                 port: int,
                 username: str,
                 password: str,
                 topic_namespace: str):
        self.module = module
        self.group_id = 'DEV'
        self.edge_node_id = 'TEST'
        self.topic_namespace = topic_namespace
        self.broker_url = broker_url
        self.port = port
        self.username = username
        self.password = password
        self.command_handlers: dict = {}
        self._create_client()

    def _create_client(self):
        self.client = Client()
        self.client.on_connect = self.on_mqtt_connect
        self.client.on_message = self.on_mqtt_message
        self.__set_last_will()

    def start(self) -> None:
        self._authenticate_if_needed()
        self.client.connect(self.broker_url, self.port)
        # Short delay to let on_connect handler fire
        time.sleep(.1)
        self.client.loop_start()

    def _authenticate_if_needed(self):
        if self.username and self.username != 'anonymous' and \
            len(self.username) > 0:
            self.client.username_pw_set(self.username, self.password)

    def join_paths(self, paths: List[str]):
        nodes = []
        for path in paths:
            node = path.strip('/')
            nodes.append(node)
        return '/'.join(nodes)

    def get_topic_root(self, message_type: MqttMessageType) -> str:
        return self.join_paths([self.topic_namespace,
                                self.group_id,
                                message_type.topic_verb,
                                self.edge_node_id])

    def on_mqtt_connect(self, client: Client, userdata, flags, rc):
        print('Connected with result code ' + str(rc))
        self.__publish_birth_certificate(client)

    def __create_birth_certificate(self) -> str:
        try:
            birth_certificate: dict = {
                "online": True,
                "timestamp": time.time(),
                "entityTypes": self.module.entity_types,
                "entitySets": self.module.entity_sets,
                "commands": self.module.commands,
                "events": self.module.events
            }
            return json.dumps(birth_certificate, cls=DictionaryEncoder)
        except Exception as e:
            print('Error creating birth certificate: ' + str(e))
            return "{\"online\": True}"

    def __create_death_certificate(self) -> str:
        try:
            death_certificate: dict = {
                "online": False
            }
            return json.dumps(death_certificate, cls=DictionaryEncoder)
        except Exception as e:
            print('Error creating death certificate: ' + str(e))
            return "{\"online\": False}"

    def __publish_birth_certificate(self, client: Client):
        try:
            birth_payload = self.__create_birth_certificate()
            birth_topic = self.get_topic_root(MqttMessageType.NODE_BIRTH)
            print('Publishing birth certificate to ' + birth_topic)
            client.publish(birth_topic, birth_payload, qos=0, retain=False)
        except Exception as e:
            print('Error publishing birth certificate: ' + str(e))

    def __set_last_will(self):
        print('Setting last will')
        payload = self.__create_death_certificate()
        self.client.will_set(self.get_topic_root(MqttMessageType.NODE_DEATH),
                             payload, qos=0, retain=False)

    def add_command_handler(self,
                            command_name: str,
                            handler: Callable[[str], str]) -> None:
        try:
            print("Attempting to add command handler for command: "
                  + command_name)
            self.command_handlers[command_name] = handler
            self.client.subscribe(self.join_paths(
                [self.get_topic_root(
                    MqttMessageType.NODE_COMMAND),
                    command_name, '+', 'REQ']))
        except Exception as e:
            print('Error adding command handler: ' + str(e))

    def handle_command(self, command_name: str,
                       request_id: str, payload: str) -> None:
        if command_name in self.command_handlers:
            result = self.command_handlers[command_name](payload)
            response_topic = self.join_paths(
                [self.get_topic_root(MqttMessageType.NODE_COMMAND),
                 command_name, request_id, 'RES'])
            self.client.publish(response_topic, result, qos=0, retain=False)
        else:
            raise (Exception('No handler found for command ' + command_name))

    def on_mqtt_message(self, client, userdata, msg):
        # Handle any requests for open tasks
        if msg.topic.startswith(
            self.get_topic_root(MqttMessageType.NODE_COMMAND)) \
                and msg.topic.endswith('REQ'):
            try:
                command_name = msg.topic.split('/')[4]
                request_id = msg.topic.split('/')[5]
                print("Attempting to invoke handler for command "
                      + command_name + " with request id " + request_id)
                payload = msg.payload.decode('utf-8')
                self.handle_command(command_name, request_id, payload)
            except Exception as e:
                print('Error handling command: ' + str(e))

    def publish_event_data(self, payload: str,
                           qos: int = 0,
                           retain: bool = False) -> None:
        topic = self.get_topic_root(MqttMessageType.NODE_DATA)
        self.client.publish(topic, payload, qos, retain)
