import json
from unittest.mock import MagicMock

import pytest
import queue
import time
from src.zmq_interface import ZmqInterface


class PushSocketStub:
    def __init__(self):
        self.sent_messages = []

    def send_json(self, data):
        self.sent_messages.append(data)

    def close(self):
        pass


class PullSocketStub:
    def __init__(self):
        self.received_messages = queue.Queue()

    def recv_string(self):
        try:
            return self.received_messages.get_nowait()
        except queue.Empty:
            return None

    def close(self):
        pass


class TestZmqInterface:

    @pytest.fixture
    def push_socket_stub(self):
        return PushSocketStub()

    @pytest.fixture
    def pull_socket_stub(self):
        return PullSocketStub()

    def get_all_messages_from_queue(self, message_queue):
        results = []
        while True:
            try:
                results.append(message_queue.get_nowait())
            except queue.Empty:
                break
        return results

    def test_zmq_send_json(self, push_socket_stub, pull_socket_stub):
        interface = ZmqInterface(push_socket_stub, pull_socket_stub)
        test_message = {"message": "Hello, world!"}
        interface.send_json(test_message)
        assert push_socket_stub.sent_messages == [test_message]

    def test_zmq_start_twice(self, push_socket_stub, pull_socket_stub):
        interface = ZmqInterface(push_socket_stub, pull_socket_stub)
        interface.start()
        time.sleep(0.1)  # Allow some time for the process to start
        interface.start()  # Call start again

        # No specific assertion is needed here as we are testing for the absence of exceptions
        # Cleanup
        interface.stop()

    def test_zmq_stop_twice(self, push_socket_stub, pull_socket_stub):
        interface = ZmqInterface(push_socket_stub, pull_socket_stub)
        interface.start()
        time.sleep(0.1)  # Allow some time for the process to start
        interface.stop()
        # Call stop again and check for exceptions
        try:
            interface.stop()
            exception_raised = False
        except Exception as e:
            exception_raised = True

        assert not exception_raised, "Exception was raised on second stop call"

    def test_zmq_listens_for_messages(self, push_socket_stub, pull_socket_stub):
        test_messages = ["message1", "message2", "message3"]
        for message in test_messages:
            pull_socket_stub.received_messages.put(message)

        received_messages = []

        def callback(msg):
            received_messages.append(msg)

        interface = ZmqInterface(push_socket_stub, pull_socket_stub, callback=callback)

        interface.start()
        time.sleep(0.1)
        interface.stop()

        assert received_messages == test_messages

    def test_can_send_device_information(self, push_socket_stub, pull_socket_stub):
        test_message = json.dumps(
            {
                "device:motor_1": {
                    "topic": "motor_data",
                    "source": "motor"
                },
                "detector:detector_1": {
                    "topic": "detector_data",
                    "source": "detector"
                },
            }
        )

        expected_topics = ["motor_data", "detector_data"]

        kafka_interface_mock = MagicMock()
        kafka_interface_mock.notify_of_update = MagicMock()

        deserialiser_mock = MagicMock()
        deserialiser_mock.notify_of_start = MagicMock()

        aligner_mock = MagicMock()
        aligner_mock.notify_of_start = MagicMock()

        interface = ZmqInterface(push_socket_stub, pull_socket_stub)
        interface.set_kafka_interface(kafka_interface_mock)
        interface.set_deserialiser(deserialiser_mock)
        interface.set_aligner(aligner_mock)

        interface.start()
        time.sleep(0.1)
        pull_socket_stub.received_messages.put(test_message)
        time.sleep(0.1)
        interface.stop()

        kafka_interface_mock.notify_of_update.assert_called_once_with(expected_topics)
        deserialiser_mock.notify_of_start.assert_called_once_with(json.loads(test_message))
        aligner_mock.notify_of_start.assert_called_once()





