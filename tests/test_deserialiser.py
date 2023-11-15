import time

import pytest
import queue

from src.deserialiser import Deserialiser  # Adjust the import based on your module's name
from tests.doubles.generators import generate_simple_fake_f144_data, generate_simple_fake_ev44_events

from tests.doubles.consumer import Message


CONFIG_MESSAGE ={
    "device:motor_1": {
        "topic": "motor_data",
        "source": "f144_source_1"
    },
    "detector:detector_1": {
        "topic": "detector_data",
        "source": "ev44_source_1"
    }
}


class TestDeserialiser:
    @pytest.fixture
    def input_queue(self):
        return queue.Queue()

    @pytest.fixture
    def deserialised_messages_queue(self):
        return queue.Queue()

    def fill_input_queue_normal_data(self, input_queue):
        f144_data = generate_simple_fake_f144_data(10, source_name="f144_source_1")
        ev44_data = generate_simple_fake_ev44_events(10, source_name="ev44_source_1")
        p0_ctr = 0
        p1_ctr = 0
        for i, dat in enumerate(f144_data):
            if i % 2 == 0:
                input_queue.put(Message(topic="f144_source_1", partition=0, offset=p0_ctr, key=None, value=dat))
                p0_ctr += 1
            else:
                input_queue.put(Message(topic="f144_source_1", partition=1, offset=p1_ctr, key=None, value=dat))
                p1_ctr += 1
        p0_ctr = 0
        p1_ctr = 0
        for i, dat in enumerate(ev44_data):
            if i % 2 == 0:
                input_queue.put(Message(topic="ev44_source_1", partition=0, offset=p0_ctr, key=None, value=dat))
                p0_ctr += 1
            else:
                input_queue.put(Message(topic="ev44_source_1", partition=1, offset=p1_ctr, key=None, value=dat))
                p1_ctr += 1
        input_queue.put(Deserialiser.SENTINEL)  # Add sentinel at the end
        return input_queue

    def fill_input_queue_data_with_trash(self, input_queue):
        f144_data = generate_simple_fake_f144_data(10, source_name="f144_source_1")
        ev44_data = generate_simple_fake_ev44_events(10, source_name="ev44_source_1")
        ev44_data_trash = generate_simple_fake_ev44_events(10, source_name="trash")
        for i, dat in enumerate(f144_data):
            input_queue.put(Message(topic="f144_source_1", partition=0, offset=i, key=None, value=dat))
        p0_ctr = 0
        p1_ctr = 0
        for i, dat in enumerate(ev44_data):
            if i % 2 == 0:
                input_queue.put(Message(topic="ev44_source_1", partition=0, offset=p0_ctr, key=None, value=dat))
                p0_ctr += 1
            else:
                input_queue.put(Message(topic="ev44_source_1", partition=1, offset=p1_ctr, key=None, value=dat))
                p1_ctr += 1
        p0_ctr = 0
        p1_ctr = 0
        for i, dat in enumerate(ev44_data_trash):
            if i % 2 == 0:
                input_queue.put(Message(topic="trash", partition=0, offset=p0_ctr, key=None, value=dat))
                p0_ctr += 1
            else:
                input_queue.put(Message(topic="trash", partition=1, offset=p1_ctr, key=None, value=dat))
                p1_ctr += 1
        input_queue.put(Deserialiser.SENTINEL)  # Add sentinel at the end
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

    def test_successful_deserialisation(self, input_queue, deserialised_messages_queue):
        full_input_queue = self.fill_input_queue_normal_data(input_queue)

        deserialiser = Deserialiser(full_input_queue, deserialised_messages_queue)

        deserialiser.notify_of_start(CONFIG_MESSAGE)

        time.sleep(0.05)
        # Run the deserialiser logic
        deserialiser.run()

        time.sleep(0.05)

        # Check the deserialised_messages_queue
        results = self.get_all_messages_from_queue(deserialised_messages_queue)
        assert len(results) == 19  # 18 messages (2 discarded ev44 + 1 sentinel)
        assert results[0][1] == "f144_source_1"
        assert results[0][0] == "device:motor_1"
        assert results[-2][1] == "ev44_source_1"
        assert results[-2][0] == "detector:detector_1"

    def test_successful_deserialisation_with_trash(self, input_queue, deserialised_messages_queue):
        full_input_queue = self.fill_input_queue_data_with_trash(input_queue)

        allowed_sources = {"f144_source_1", "ev44_source_1"}

        deserialiser = Deserialiser(full_input_queue, deserialised_messages_queue, allowed_sources)

        deserialiser.notify_of_start(CONFIG_MESSAGE)

        time.sleep(0.05)
        # Run the deserialiser logic
        deserialiser.run()

        time.sleep(0.05)

        # Check the deserialised_messages_queue
        results = self.get_all_messages_from_queue(deserialised_messages_queue)
        assert len(results) == 19  # 18 messages (2 discarded ev44 + 1 sentinel)
        assert results[0][1] == "f144_source_1"
        assert results[0][0] == "device:motor_1"
        assert results[-2][1] == "ev44_source_1"
        assert results[-2][0] == "detector:detector_1"




