import logging
import queue

from src.aligner import Aligner
from src.deserialiser import Deserialiser
from src.kafka_interface import KafkaInterface
from src.zmq_interface import ZmqInterface

logger = logging.getLogger(__name__)


class DataProcessor:
    """
    DataProcessor coordinates the data flow and processing between different interfaces.

    It integrates Kafka and ZMQ for messaging, manages queues for serialized and deserialized
    messages, and controls the data alignment process. The processor handles the starting and
    stopping of all associated components to ensure a smooth data processing pipeline.

    Attributes:
        kafka_consumer (Consumer): Kafka consumer instance for consuming messages.
        zmq_push_socket (zmq.Socket): ZMQ PUSH socket for sending messages.
        zmq_pull_socket (zmq.Socket): ZMQ PULL socket for receiving messages.
        serialised_messages_queue (queue.Queue): Queue to hold serialized messages.
        deserialised_messages_queue (queue.Queue): Queue to hold deserialized messages.
        kafka_interface (KafkaInterface): Interface to interact with Kafka.
        zmq_interface (ZmqInterface): Interface to handle ZMQ communication.
        deserialiser (Deserialiser): Component to deserialize incoming messages.
        aligner (Aligner): Component to align data based on timestamps.
    """

    def __init__(
        self,
        kafka_consumer,
        zmq_push_socket,
        zmq_pull_socket,
        serialised_messages_queue=None,
        deserialised_messages_queue=None,
    ):
        """
        Initializes the DataProcessor with Kafka consumer, ZMQ sockets, and message queues.

        Args:
            kafka_consumer: Kafka consumer instance for consuming messages.
            zmq_push_socket: ZMQ PUSH socket for sending messages.
            zmq_pull_socket: ZMQ PULL socket for receiving messages.
            serialised_messages_queue (optional): Queue for serialized messages.
            deserialised_messages_queue (optional): Queue for deserialized messages.
        """
        self.kafka_consumer = kafka_consumer
        self.zmq_push_socket = zmq_push_socket
        self.zmq_pull_socket = zmq_pull_socket

        if not serialised_messages_queue:
            serialised_messages_queue = queue.Queue()
        if not deserialised_messages_queue:
            deserialised_messages_queue = queue.Queue()

        self.serialised_messages_queue = serialised_messages_queue
        self.deserialised_messages_queue = deserialised_messages_queue

        self.kafka_interface = KafkaInterface(
            self.kafka_consumer, self.serialised_messages_queue
        )
        self.zmq_interface = ZmqInterface(self.zmq_push_socket, self.zmq_pull_socket)
        self.deserialiser = Deserialiser(
            self.serialised_messages_queue, self.deserialised_messages_queue
        )
        self.aligner = Aligner(self.deserialised_messages_queue, self.zmq_interface)

        self.zmq_interface.set_kafka_interface(self.kafka_interface)
        self.zmq_interface.set_deserialiser(self.deserialiser)
        self.zmq_interface.set_aligner(self.aligner)

    def start(self) -> None:
        """
        Starts all components of the data processing pipeline.
        This includes the deserializer, aligner, and ZMQ interface.
        The Kafka interface is started separately when receiving start
        messages.
        """
        logger.info("Starting data processor")
        self.deserialiser.start()
        self.aligner.start()
        self.zmq_interface.start()

    def stop(self) -> None:
        """
        Stops all components of the data processing pipeline.
        This is done in a safe and orderly manner to ensure that all resources are properly released.
        """
        logger.info("Stopping data processor")
        self.kafka_interface.stop()
        self.kafka_interface.insert_sentinel()
        self.deserialiser.stop()
        self.aligner.stop()
        self.zmq_interface.stop()
