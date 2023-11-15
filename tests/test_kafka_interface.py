import pytest
import queue
import time
from src.kafka_interface import KafkaInterface
from tests.doubles.consumer import ConsumerStub, Message, TopicPartition


class TestKafkaInterface:

    @pytest.fixture
    def serialised_messages_queue(self):
        return queue.Queue()

    @pytest.fixture
    def consumer_stub(self):
        return ConsumerStub({'auto.offset.reset': 'earliest'})

    def get_all_messages_from_queue(self, message_queue):
        results = []
        while True:
            try:
                results.append(message_queue.get_nowait())
            except queue.Empty:
                break
        return results

    def test_kafka_start_and_stop(self, consumer_stub, serialised_messages_queue):
        kafka_interface = KafkaInterface(consumer_stub, serialised_messages_queue)
        kafka_interface.start()
        time.sleep(0.1)
        kafka_interface.stop()

        assert not kafka_interface.thread.is_alive()  # Ensure thread is stopped

    def test_kafka_start_twice(self, consumer_stub, serialised_messages_queue):
        kafka_interface = KafkaInterface(consumer_stub, serialised_messages_queue)
        kafka_interface.start()
        time.sleep(0.1)  # Allow some time for the thread to start
        kafka_interface.start()  # Call start again

        assert kafka_interface.thread.is_alive()  # Ensure thread is running

        kafka_interface.stop()  # Cleanup

    def test_kafka_stop_twice(self, consumer_stub, serialised_messages_queue):
        kafka_interface = KafkaInterface(consumer_stub, serialised_messages_queue)
        kafka_interface.start()
        time.sleep(0.1)  # Allow some time for the thread to start
        kafka_interface.stop()
        assert not kafka_interface.thread.is_alive()  # Ensure thread is stopped

        # Call stop again and check for exceptions
        try:
            kafka_interface.stop()
            exception_raised = False
        except Exception as e:
            exception_raised = True

        assert not exception_raised, "Exception was raised on second stop call"

    def test_kafka_consumes_messages(self, consumer_stub, serialised_messages_queue):
        test_messages = [Message("topic1", 0, i, None, msg) for i, msg in enumerate(["message1", "message2", "message3"])]


        kafka_interface = KafkaInterface(consumer_stub, serialised_messages_queue)
        kafka_interface.notify_of_update(["topic1"])

        for i, message in enumerate(test_messages):
            consumer_stub.add_message("topic1", 0, i, None, message.value())

        time.sleep(0.1)  # Allow some time for messages to be consumed
        kafka_interface.stop()
        kafka_interface.insert_sentinel()

        results = self.get_all_messages_from_queue(serialised_messages_queue)
        assert len(results) == len(test_messages) + 1  # +1 for sentinel
        assert [res.value() for res in results[0:3]] == [msg.value() for msg in test_messages]

    def test_kafka_subscribes_to_new_topics(self, consumer_stub, serialised_messages_queue):
        kafka_interface = KafkaInterface(consumer_stub, serialised_messages_queue)

        kafka_interface.start()

        initial_topics = ["topic1", "topic2"]
        kafka_interface.notify_of_update(initial_topics)
        metadata = consumer_stub.list_topics()
        available_topics = set(metadata.topics.keys())
        assert available_topics == set(initial_topics)

        new_topics = ["topic3", "topic4"]
        kafka_interface.notify_of_update(new_topics)
        metadata = consumer_stub.list_topics()
        available_topics = set(metadata.topics.keys())
        assert available_topics == set(new_topics)

        kafka_interface.stop()

    def test_kafka_sets_correct_offset_all_partitions(self, consumer_stub, serialised_messages_queue):
        num_messages = 5
        num_partitions = 3

        for partition in range(num_partitions):
            for offset in range(num_messages):
                message = f"message-{partition}-{offset}"
                consumer_stub.add_message("topic1", partition, offset, None, message)

        kafka_interface = KafkaInterface(consumer_stub, serialised_messages_queue)
        kafka_interface.notify_of_update(["topic1"])

        time.sleep(0.05)  # Allow some time for messages to be consumed

        kafka_interface.stop()

        # Collect consumed messages
        consumed_messages = [msg.value() for msg in self.get_all_messages_from_queue(serialised_messages_queue)]

        # Prepare expected messages (last two from each partition)
        expected_messages = [f"message-{partition}-{offset}" for partition in range(num_partitions) for offset in
                             range(num_messages - 1, num_messages)]

        assert sorted(consumed_messages) == sorted(
            expected_messages), "Consumed messages do not match expected last two messages of each partition"

    def test_kafka_handles_empty_topic(self, consumer_stub, serialised_messages_queue):
        kafka_interface = KafkaInterface(consumer_stub, serialised_messages_queue)
        empty_topic = "empty_topic"
        kafka_interface.notify_of_update([empty_topic])
        time.sleep(0.1)  # Allow some time for offsets to be set
        kafka_interface.stop()

        # Check that no exception is raised and a valid offset is set
        try:
            tp = TopicPartition(empty_topic, 0)
            _, high_watermark = consumer_stub.get_watermark_offsets(tp)
            assert high_watermark == 0
            exception_raised = False
        except Exception as e:
            exception_raised = True

        assert not exception_raised, "Exception was raised for empty topic"

