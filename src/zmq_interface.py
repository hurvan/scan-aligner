import json
import logging
import queue
import threading
import time

import zmq


logger = logging.getLogger(__name__)


class ZmqInterface:
    def __init__(self, push_socket, pull_socket, callback=None):
        """
        :param push_socket: Injected ZMQ PUSH socket for sending messages
        :param pull_socket: Injected ZMQ PULL socket for receiving messages
        :param callback: Optional callback function to be invoked on message reception
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

    def start(self):
        if self.thread and self.thread.is_alive():
            logger.info(f"{self.__class__.__name__} already running")
            return

        self.thread = threading.Thread(target=self.listen)
        self.running.set()
        self.thread.start()

    def stop(self):
        self.running.clear()
        if self.thread and self.thread.is_alive():
            self.thread.join()
        self.thread = None

    def shutdown(self):
        self.running.clear()
        if self.thread and self.thread.is_alive():
            self.thread.join()
        self._push_socket.close()
        self._pull_socket.close()

    def listen(self):
        """Starts listening for incoming messages."""
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

    def set_kafka_interface(self, kafka_interface):
        self._kafka_interface = kafka_interface

    def set_deserialiser(self, deserialiser):
        self._deserialiser = deserialiser

    def set_aligner(self, aligner):
        self._aligner = aligner

    def _default_callback(self, message):
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

    def send_json(self, data):
        """
        Send the data as JSON string
        :param data: JSON serializable data
        """
        self._push_socket.send_json(data)
