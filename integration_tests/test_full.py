import json
import time

import numpy as np
import zmq

import pytest

from integration_tests.kafka_commands import generate_and_produce_data


class TestAlignmentEngine:
    @pytest.fixture(autouse=True)
    def prepare(self):
        time.sleep(1)
        print("Add some initial topic data")
        generate_and_produce_data(start_offset=0)
        time.sleep(1)
        yield

    def send_start_message_to_pull_socket(self):
        """
        Sends a start message to a ZMQ pull socket.
        """
        # Create a ZMQ context
        context = zmq.Context()

        # Create a PUSH socket to send the start message
        push_socket = context.socket(zmq.PUSH)
        push_socket.connect("tcp://localhost:5556")

        # Define the start message
        start_message = {
            "device:motor_1": {
                "topic": "motor_data",
                "source": "motor"
            },
            "detector:detector_1": {
                "topic": "detector_data",
                "source": "detector"
            }
        }

        # Send the message as JSON
        push_socket.send_string(json.dumps(start_message))

        # Clean up: close the socket
        push_socket.close()

    def test_basic_operation(
            self, alignment_engine
    ):
        print("test_basic_operation")
        time.sleep(1)
        self.send_start_message_to_pull_socket()
        time.sleep(1)

        generate_and_produce_data(start_offset=10)

        time.sleep(1)

        all_data = []

        context = zmq.Context()
        pull_socket = context.socket(zmq.PULL)
        pull_socket.connect("tcp://localhost:5555")
        poller = zmq.Poller()
        poller.register(pull_socket, zmq.POLLIN)
        timeout = 1000
        for i in range(22):
            socks = dict(poller.poll(timeout))
            if socks.get(pull_socket) == zmq.POLLIN:
                data = pull_socket.recv_json()
                print("Received Data:", data)
                all_data.append(data)
            else:
                print("No data received within the timeout period.")
        pull_socket.close()

        # assert len(all_data) == 20

        expected_f144_values = np.linspace(9, 18.5, 20).tolist()
        expected_ev44_values = ([i for i in range(11, 21)] * 2)
        expected_ev44_values.sort()

        for i, dat in enumerate(all_data):
            assert expected_f144_values[i] == dat["detector:detector_1"]["device:motor_1"]
            assert expected_ev44_values[i] == dat["detector:detector_1"]["value"]
