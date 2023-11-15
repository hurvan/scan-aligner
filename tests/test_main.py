import queue
import time

import numpy as np

from src.data_processor import DataProcessor
from tests.doubles.consumer import ConsumerStub
from tests.doubles.generators import generate_linear_fake_f144_data, generate_linear_fake_ev44_events, ev44_generator, f144_generator
from tests.test_zmq_interface import PushSocketStub, PullSocketStub

import json


class TestDataProcessor:
    def fill_kafka_stub_topics_single_part(self, consumer_stub, start_offset=0):

        f144_data = generate_linear_fake_f144_data(10, source_name="f144_source_1", start_offset=start_offset)
        ev44_data = generate_linear_fake_ev44_events(10, source_name="ev44_source_1", start_offset=start_offset)

        data_topic = "motor_data"
        event_topic = "detector_data"

        for i in range(10):
            consumer_stub.add_message(event_topic, 0, i+start_offset, None, ev44_data[i])
            consumer_stub.add_message(data_topic, 0, i+start_offset, None, f144_data[i])

    def fill_kafka_stub_topics_multi_part(self, consumer_stub, start_offset=0):
        f144_data = generate_linear_fake_f144_data(10, source_name="f144_source_1", start_offset=start_offset)
        ev44_data = generate_linear_fake_ev44_events(10, source_name="ev44_source_1", start_offset=start_offset)

        data_topic = "motor_data"
        event_topic = "detector_data"

        p0_ctr = start_offset
        p1_ctr = start_offset
        for i, (f_dat, e_dat) in enumerate(zip(f144_data, ev44_data)):
            if i % 2 == 0:
                consumer_stub.add_message(data_topic, 0, p0_ctr, None, f_dat)
                consumer_stub.add_message(event_topic, 0, p0_ctr, None, e_dat)
                p0_ctr += 1
            else:
                consumer_stub.add_message(data_topic, 1, p1_ctr, None, f_dat)
                consumer_stub.add_message(event_topic, 1, p1_ctr, None, e_dat)
                p1_ctr += 1

    # def fill_kafka_stub_topics_with_stagnant_data(self, consumer_stub, start_offset=0):
    #     motor_val = 15
    #     detector_val = 18
    #
    #     f144_data = [f144_generator(motor_val, i+start_offset, source_name="f144_source_1") for i in range(10)]
    #     ev44_data = [ev44_generator(detector_val, i*2+start_offset, source_name="ev44_source_1") for i in range(4)]
    #
    #     p0_ctr = 0
    #     p1_ctr = 0
    #     for i, dat in enumerate(f144_data):
    #         if i % 2 == 0:
    #             consumer_stub.add_message("f144_source_1", 0, p0_ctr, None, dat)
    #             p0_ctr += 1
    #         else:
    #             consumer_stub.add_message("f144_source_1", 1, p1_ctr, None, dat)
    #             p1_ctr += 1
    #     p0_ctr = 0
    #     p1_ctr = 0
    #     for i, dat in enumerate(ev44_data):
    #         if i % 2 == 0:
    #             consumer_stub.add_message("ev44_source_1", 0, p0_ctr, None, dat)
    #             p0_ctr += 1
    #         else:
    #             consumer_stub.add_message("ev44_source_1", 1, p1_ctr, None, dat)
    #             p1_ctr += 1

    def fill_kafka_stub_topics_with_basic_stagnant_single_part(self, consumer_stub, start_offset=0):
        # Old constant data for 5 points, then a linear increase for 5 points

        motor_vals = [4 for i in range(5)]
        motor_ts = [i for i in range(5)]

        detector_vals = [5 for i in range(5)]
        detector_ts = [i for i in range(5)]

        f144_data = [f144_generator(motor_vals[i], motor_ts[i]+start_offset, source_name="f144_source_1") for i in range(5)]
        ev44_data = [ev44_generator(detector_vals[i], detector_ts[i]+start_offset, source_name="ev44_source_1") for i in range(5)]

        data_topic = "motor_data"
        event_topic = "detector_data"

        for i in range(5):
            consumer_stub.add_message(event_topic, 0, i+start_offset, None, ev44_data[i])
            consumer_stub.add_message(data_topic, 0, i+start_offset, None, f144_data[i])

    def fill_kafka_stub_topics_with_basic_linear_single_part(self, consumer_stub, start_offset=0):
        # Old constant data for 5 points, then a linear increase for 5 points

        motor_vals = [i for i in range(5)]
        motor_ts = [i + 10 for i in range(5)]

        detector_vals = [i + 1 for i in range(5)]
        detector_ts = [i + 10 for i in range(5)]

        f144_data = [f144_generator(motor_vals[i], motor_ts[i] + start_offset, source_name="f144_source_1") for i in
                     range(5)]
        ev44_data = [ev44_generator(detector_vals[i], detector_ts[i] + start_offset, source_name="ev44_source_1") for i
                     in range(5)]

        data_topic = "motor_data"
        event_topic = "detector_data"

        for i in range(5):
            consumer_stub.add_message(event_topic, 0, i + start_offset, None, ev44_data[i])
            consumer_stub.add_message(data_topic, 0, i + start_offset, None, f144_data[i])





    def get_all_messages_from_queue(self, message_queue):
        # Check the deserialised_messages_queue until it's empty
        results = []
        while True:
            try:
                results.append(message_queue.get_nowait())
            except queue.Empty:
                break
        return results

    def send_start_message(self, zmq_interface):
        test_message = json.dumps(
            {
                "device:motor_1": {
                    "topic": "motor_data",
                    "source": "f144_source_1"
                },
                "detector:detector_1": {
                    "topic": "detector_data",
                    "source": "ev44_source_1"
                },
            }
        )
        zmq_interface._pull_socket.received_messages.put(test_message)

    def test_end_to_end_data_flow_single_part(self):
        # Set up the test
        consumer_stub = ConsumerStub({})
        serialised_messages_queue = queue.Queue()
        deserialize_messages_queue = queue.Queue()

        zmq_push_socket_stub = PushSocketStub()
        zmq_pull_socket_stub = PullSocketStub()

        data_processor = DataProcessor(
            consumer_stub, zmq_push_socket_stub, zmq_pull_socket_stub, serialised_messages_queue, deserialize_messages_queue
        )

        time.sleep(0.1)
        data_processor.start()

        time.sleep(0.1)

        self.send_start_message(data_processor.zmq_interface)

        time.sleep(0.1)

        self.fill_kafka_stub_topics_single_part(consumer_stub, start_offset=0)

        time.sleep(0.2)

        data_processor.stop()
        data_processor.zmq_interface.shutdown()

        assert len(zmq_push_socket_stub.sent_messages) == 9

        expected_f144_values = np.linspace(0, 8, 9).tolist()
        expected_ev44_values = [i for i in range(2, 11)]
        expected_ev44_values.sort()

        for i, dat in enumerate(zmq_push_socket_stub.sent_messages):
            assert expected_f144_values[i] == dat["detector:detector_1"]["device:motor_1"]
            assert expected_ev44_values[i] == dat["detector:detector_1"]["value"]

    def test_correct_start_offset(self):
        # Set up the test
        consumer_stub = ConsumerStub({'auto.offset.reset': 'latest'})
        serialised_messages_queue = queue.Queue()
        deserialize_messages_queue = queue.Queue()

        zmq_push_socket_stub = PushSocketStub()
        zmq_pull_socket_stub = PullSocketStub()

        data_processor = DataProcessor(
            consumer_stub, zmq_push_socket_stub, zmq_pull_socket_stub, serialised_messages_queue,
            deserialize_messages_queue
        )

        self.fill_kafka_stub_topics_single_part(consumer_stub, start_offset=0)
        time.sleep(0.1)
        data_processor.start()

        time.sleep(0.1)

        self.send_start_message(data_processor.zmq_interface)

        time.sleep(0.1)

        self.fill_kafka_stub_topics_single_part(consumer_stub, start_offset=10)

        time.sleep(0.2)

        data_processor.stop()
        data_processor.zmq_interface.shutdown()

        assert len(zmq_push_socket_stub.sent_messages) == 10

        expected_f144_values = np.linspace(9, 18, 10).tolist()
        expected_ev44_values = [i for i in range(11, 21)]
        expected_ev44_values.sort()

        for i, dat in enumerate(zmq_push_socket_stub.sent_messages):
            assert expected_f144_values[i] == dat["detector:detector_1"]["device:motor_1"]
            assert expected_ev44_values[i] == dat["detector:detector_1"]["value"]


    # def test_can_discard_old_start_data_for_new_scan(self):
    #     consumer_stub = ConsumerStub({'auto.offset.reset': 'latest'})
    #     serialised_messages_queue = queue.Queue()
    #     deserialize_messages_queue = queue.Queue()
    #
    #     zmq_push_socket_stub = PushSocketStub()
    #     zmq_pull_socket_stub = PullSocketStub()
    #
    #     data_processor = DataProcessor(
    #         consumer_stub, zmq_push_socket_stub, zmq_pull_socket_stub, serialised_messages_queue,
    #         deserialize_messages_queue
    #     )
    #
    #     time.sleep(0.1)
    #     data_processor.start()
    #
    #     self.fill_kafka_stub_topics_with_basic_stagnant_single_part(consumer_stub, start_offset=0)
    #
    #     time.sleep(0.1)
    #
    #     self.send_start_message(data_processor.zmq_interface)
    #     time.sleep(0.1)
    #     self.fill_kafka_stub_topics_with_basic_linear_single_part(consumer_stub, start_offset=5)
    #
    #     time.sleep(0.2)
    #
    #     data_processor.stop()
    #     data_processor.zmq_interface.shutdown()
    #
    #     for dat in zmq_push_socket_stub.sent_messages:
    #         print(f"motor: {dat['detector:detector_1']['device:motor_1']}, detector: {dat['detector:detector_1']['value']}")
    #
    #     assert len(zmq_push_socket_stub.sent_messages) == 5
    #
    #     expected_f144_values = np.linspace(0, 4, 5).tolist() * 2
    #     expected_f144_values.sort()
    #     expected_ev44_values = np.linspace(1, 5, 5).tolist() * 2
    #     expected_ev44_values.sort()
    #
    #     for i, dat in enumerate(zmq_push_socket_stub.sent_messages):
    #         assert expected_f144_values[i] == dat["detector:detector_1"]["device:motor_1"]
    #         assert expected_ev44_values[i] == dat["detector:detector_1"]["value"]

