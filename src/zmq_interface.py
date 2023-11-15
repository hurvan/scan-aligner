import json
import logging
import threading
import time
from typing import Any, Callable

import zmq

logger = logging.getLogger(__name__)


class ZmqInterface:
    """
    ZmqInterface handles communication between the system and ZeroMQ sockets.

    It allows for sending messages through a PUSH socket and receiving messages through a PULL socket.
    This class can also be linked to a KafkaInterface, Deserialiser, and Aligner to notify them about
    specific commands received via ZeroMQ.

    Attributes:
        _push_socket (zmq.Socket): ZMQ PUSH socket for sending messages.
        _pull_socket (zmq.Socket): ZMQ PULL socket for receiving messages.
        _callback (Callable): Callback function invoked upon message reception.
        _kafka_interface (KafkaInterface): Interface to interact with Kafka.
        _deserialiser (Deserialiser): Interface to handle message deserialization.
        _aligner (Aligner): Interface to align incoming data.
        running (threading.Event): Event to control the running state of the interface.
        thread (threading.Thread or None): Thread that listens for incoming messages.
    """

    def __init__(
        self,
        push_socket: zmq.Socket,
        pull_socket: zmq.Socket,
        callback: Callable[[str], Any] = None,
    ) -> None:
        """
        Initializes the ZmqInterface with the necessary ZMQ sockets and an optional callback function.

        Args:
            push_socket (zmq.Socket): ZMQ PUSH socket for sending messages.
            pull_socket (zmq.Socket): ZMQ PULL socket for receiving messages.
            callback (Callable[[str], Any], optional): Callback function invoked on message reception.
        """
        self._push_socket = push_socket
        self._pull_socket = pull_socket
        if callback:
            self._callback = callback
        else:
            self._callback = self._default_callback
        self._kafka_interface = None
        self._deserialiser = None
        self._aligner = None

        self.running = threading.Event()
        self.thread = None

    def start(self) -> None:
        """Start the listener thread if it is not already running."""
        if self.thread and self.thread.is_alive():
            logger.info(f"{self.__class__.__name__} already running")
            return

        self.thread = threading.Thread(target=self.listen)
        self.running.set()
        self.thread.start()

    def stop(self) -> None:
        """Stop the listener thread."""
        self.running.clear()
        if self.thread and self.thread.is_alive():
            self.thread.join()
        self.thread = None

    def shutdown(self) -> None:
        """Stop the listener thread and close the ZMQ sockets."""
        self.running.clear()
        if self.thread and self.thread.is_alive():
            self.thread.join()
        self._push_socket.close()
        self._pull_socket.close()

    def listen(self) -> None:
        """
        Main listening loop that waits for incoming messages on the pull socket.
        Calls the specified callback function upon message reception.
        """
        while self.running.is_set():
            try:
                logger.info("listener waiting for message")
                message = self._pull_socket.recv_string()
                logger.info("listener received message")
                if not message:
                    time.sleep(0.05)
                    continue
                if self._callback:
                    logger.info(f"listener received message: {message}")
                    self._callback(message)
            except zmq.ZMQError as e:
                logger.error(f"ZMQ Error: {e}")
                break

    def set_kafka_interface(self, kafka_interface) -> None:
        """Set the Kafka interface for communication with Kafka."""
        self._kafka_interface = kafka_interface

    def set_deserialiser(self, deserialiser) -> None:
        """Set the deserialiser for handling message deserialization."""
        self._deserialiser = deserialiser

    def set_aligner(self, aligner) -> None:
        """Set the aligner for aligning incoming data."""
        self._aligner = aligner

    def _default_callback(self, message: str) -> None:
        """
        Default callback for handling received messages by parsing and notifying relevant interfaces.

        Args:
            message (str): The received message as a JSON string.
        """
        if not self._kafka_interface:
            return
        message_dict = json.loads(message)
        if not message_dict:
            return

        if "command" in message_dict.keys():
            if message_dict["command"] == "STOP":
                logger.info("Received stop command")
                self._kafka_interface.notify_of_stop()
                self._deserialiser.notify_of_stop()
                self._aligner.notify_of_stop()
                return

        topic_list = [dev["topic"] for dev in message_dict.values()]

        if not topic_list:
            return

        self._kafka_interface.notify_of_update(topic_list)
        if self._deserialiser:
            self._deserialiser.notify_of_start(message_dict)
        if self._aligner:
            self._aligner.notify_of_start()

    def send_json(self, data: Any) -> None:
        """
        Send data as a JSON string over the push socket.

        Args:
            data (Any): Data to be serialized to JSON and sent.
        """
        self._push_socket.send_json(data)
