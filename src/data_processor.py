import logging
import queue
import threading

from src.aligner import Aligner
from src.deserialiser import Deserialiser
from src.kafka_interface import KafkaInterface
from src.zmq_interface import ZmqInterface


logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(self, kafka_consumer, zmq_push_socket, zmq_pull_socket, serialised_messages_queue=None, deserialised_messages_queue=None):
        # Here, we are assuming you are passing real instances or stubs/spies for testing
        self.kafka_consumer = kafka_consumer
        self.zmq_push_socket = zmq_push_socket
        self.zmq_pull_socket = zmq_pull_socket

        if not serialised_messages_queue:
            serialised_messages_queue = queue.Queue()
        if not deserialised_messages_queue:
            deserialised_messages_queue = queue.Queue()

        self.serialised_messages_queue = serialised_messages_queue
        self.deserialised_messages_queue = deserialised_messages_queue

        self.kafka_interface = KafkaInterface(self.kafka_consumer, self.serialised_messages_queue)
        self.zmq_interface = ZmqInterface(self.zmq_push_socket, self.zmq_pull_socket)
        self.deserialiser = Deserialiser(self.serialised_messages_queue, self.deserialised_messages_queue)
        self.aligner = Aligner(self.deserialised_messages_queue, self.zmq_interface)

        self.zmq_interface.set_kafka_interface(self.kafka_interface)
        self.zmq_interface.set_deserialiser(self.deserialiser)
        self.zmq_interface.set_aligner(self.aligner)

    def start(self):
        logger.info("Starting data processor")
        # self.kafka_interface.start()
        self.deserialiser.start()
        self.aligner.start()
        self.zmq_interface.start()

    def stop(self):
        logger.info("Stopping data processor")
        self.kafka_interface.stop()
        self.kafka_interface.insert_sentinel()
        self.deserialiser.stop()
        self.aligner.stop()
        self.zmq_interface.stop()
