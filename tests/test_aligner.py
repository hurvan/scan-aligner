import time

import numpy as np
import pytest
import queue

from src.aligner import Aligner
from src.deserialiser import Deserialiser  # Adjust the import based on your module's name
from tests.doubles.generators import generate_simple_fake_deserialised_f144_data, \
    generate_simple_fake_deserialised_ev44_events, generate_linear_fake_deserialised_f144_data, \
    generate_linear_fake_deserialised_ev44_events
from tests.doubles.zmq_spy import ZmqInterfaceSpy


class TestDeserialiser:
    @pytest.fixture
    def input_queue(self):
        return queue.Queue()

    def fill_input_queue_normal_data(self, input_queue):
        f144_data = generate_simple_fake_deserialised_f144_data(10)
        ev44_data = generate_simple_fake_deserialised_ev44_events(10)
        for dat in f144_data:
            input_queue.put(("device:motor_1", *dat))
        for dat in ev44_data:
            input_queue.put(("detector:detector_1", *dat))
        input_queue.put(Aligner.SENTINEL)  # Add sentinel at the end
        return input_queue

    def fill_input_queue_linear_data(self, input_queue):
        f144_data = generate_linear_fake_deserialised_f144_data(10)
        ev44_data = generate_linear_fake_deserialised_ev44_events(10)
        for dat_1, dat_2 in zip(ev44_data, f144_data):
            input_queue.put(("detector:detector_1", *dat_1))
            input_queue.put(("device:motor_1", *dat_2))
        input_queue.put(Aligner.SENTINEL)  # Add sentinel at the end
        return input_queue

    def get_all_messages_from_queue(self, message_queue):
        # Check the deserialised_messages_queue until it's empty
        results = []
        while True:
            try:
                results.append(message_queue.get_nowait())
            except queue.Empty:
                break
        return results

    def test_successful_publish(self, input_queue):
        full_input_queue = self.fill_input_queue_normal_data(input_queue)
        zmq_spy = ZmqInterfaceSpy()
        aligner = Aligner(full_input_queue, zmq_spy)

        aligner.notify_of_start()

        aligner.run()

        assert len(zmq_spy.sent_data) != 0

    def test_aligner_start_twice(self, input_queue):
        full_input_queue = self.fill_input_queue_normal_data(input_queue)
        zmq_spy = ZmqInterfaceSpy()
        aligner = Aligner(full_input_queue, zmq_spy)

        aligner.notify_of_start()
        time.sleep(0.1)  # Allow some time for the process to start
        aligner.notify_of_start()  # Call start again

        # No specific assertion is needed here as we are testing for the absence of exceptions
        # Cleanup
        aligner.notify_of_stop()

    def test_aligner_stop_twice(self, input_queue):
        full_input_queue = self.fill_input_queue_normal_data(input_queue)
        zmq_spy = ZmqInterfaceSpy()
        aligner = Aligner(full_input_queue, zmq_spy)

        aligner.notify_of_start()
        time.sleep(0.1)  # Allow some time for the process to start
        aligner.notify_of_stop()
        # Call stop again and check for exceptions
        try:
            aligner.notify_of_stop()
            exception_raised = False
        except Exception as e:
            exception_raised = True

        assert not exception_raised, "Exception was raised on second stop call"

    # Remember to add this test method to the TestDeserialiser class

    def test_simple_successful_alignment(self, input_queue):
        full_input_queue = self.fill_input_queue_normal_data(input_queue)
        zmq_spy = ZmqInterfaceSpy()
        aligner = Aligner(full_input_queue, zmq_spy)

        aligner.notify_of_start()

        time.sleep(0.1)
        aligner.run()
        time.sleep(0.1)

        final_data = zmq_spy.sent_data[-1]

        assert final_data["detector:detector_1"]["value"] == 4
        assert final_data["detector:detector_1"]["device:motor_1"] == 9
        assert len(final_data.keys()) == 1

    def test_successful_linear_alignment(self, input_queue):
        full_input_queue = self.fill_input_queue_linear_data(input_queue)
        zmq_spy = ZmqInterfaceSpy()
        aligner = Aligner(full_input_queue, zmq_spy)

        aligner.notify_of_start()
        time.sleep(0.05)

        aligner.run()

        time.sleep(0.05)

        final_data = zmq_spy.sent_data

        expected_f144_values = np.linspace(-0.5, 8.5, 19).tolist()
        expected_f144_values[0] = 0
        expected_f144_values[1] = 0
        expected_ev44_values = ([i for i in range(1, 11)] * 2)
        expected_ev44_values.pop(0)
        expected_ev44_values.sort()

        for i, dat in enumerate(final_data):
            assert expected_f144_values[i] == dat["detector:detector_1"]["device:motor_1"]
            assert expected_ev44_values[i] == dat["detector:detector_1"]["value"]
