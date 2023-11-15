import logging
import queue
import threading
from typing import List

from confluent_kafka import Consumer, TopicPartition

from src.deserialiser import Deserialiser

logger = logging.getLogger(__name__)


class KafkaInterface:
    """
    Interface for Kafka to handle message consumption and offset commits.

    Attributes:
        consumer (Consumer): The Kafka consumer instance.
        serialized_messages_queue (queue.Queue): Queue to store serialized messages.
        running (threading.Event): Event to manage the running state of the interface.
        thread (threading.Thread or None): Thread running the message consumption loop.
        commit_interval (int): Number of messages after which to commit offsets.
        messages_since_last_commit (int): Counter for messages received since the last commit.
    """

    def __init__(self, consumer: Consumer, serialized_messages_queue: queue.Queue):
        """
        Initialize the KafkaInterface with a Kafka consumer and a queue for serialized messages.

        Args:
            consumer (Consumer): The Kafka consumer instance.
            serialized_messages_queue (queue.Queue): Queue to store serialized messages.
        """
        self.consumer = consumer
        self.serialized_messages_queue = serialized_messages_queue
        self.running = threading.Event()
        self.thread = None
        self.commit_interval = 5  # Commit after every 5 messages
        self.messages_since_last_commit = 0  # Track messages since the last commit
        logger.info(f"KafkaInterface initialized with consumer: {consumer}")

    def start(self) -> None:
        """
        Start the Kafka consumer thread to poll for messages.
        """
        if self.thread and self.thread.is_alive():
            logger.info(
                "Attempted to start KafkaInterface, but thread is already running."
            )
            return  # Already running, do not start another thread
        self.running.set()
        self.thread = threading.Thread(target=self._consume_messages)
        self.thread.start()
        logger.info("KafkaInterface consumer thread started.")

    def stop(self) -> None:
        """
        Stop the Kafka consumer thread and commit any outstanding message offsets.
        """
        logger.info("Stopping KafkaInterface...")
        self.running.clear()
        if self.thread:
            self.thread.join()
        if self.messages_since_last_commit > 0:
            self.commit_offsets()
        self.consumer.unassign()
        # self.consumer.close()
        logger.info("KafkaInterface stopped and consumer closed.")

    def commit_offsets(self) -> None:
        """
        Commit the offsets of the consumed messages to Kafka.
        """
        logger.info("Committing offsets...")
        try:
            self.consumer.commit(asynchronous=False)
        except Exception as e:
            logger.error(f"Failed to commit offsets: {e}")
        self.messages_since_last_commit = 0

    def _consume_messages(self) -> None:
        """
        Poll messages from Kafka and put them into the serialized messages queue.
        Commits offsets at intervals specified by `commit_interval`.
        """
        logger.info("Consumer message polling started.")
        try:
            while self.running.is_set():
                message = self.consumer.poll(0.01)
                if message is None:
                    continue
                if message.error():
                    logger.error(f"Consumer error: {message.error()}")
                    continue

                # self.serialized_messages_queue.put(message.value())
                self.serialized_messages_queue.put(message)
                self.messages_since_last_commit += 1

                if self.messages_since_last_commit >= self.commit_interval:
                    self.commit_offsets()

        finally:
            # Ensure the last offsets are committed on shutdown
            if self.messages_since_last_commit > 0:
                self.commit_offsets()

    def set_consumer_to_offset(self, topics: List[str], offset_from_end: int) -> None:
        """
        Set the Kafka consumer to start consuming from a specific offset from the end.

        Args:
            topics (List[str]): List of topic names to set the offset for.
            offset_from_end (int): The offset from the end of the log to start consuming.
        """
        logger.info(
            f"Setting consumer to offset for topics: {topics}, offset from end: {offset_from_end}"
        )
        topic_partitions = []
        for topic in topics:
            topic_metadata = self.consumer.list_topics(topic).topics[topic]
            for partition in topic_metadata.partitions:
                tp = TopicPartition(topic, partition)
                _, high_watermark = self.consumer.get_watermark_offsets(tp)
                tp.offset = max(0, high_watermark - offset_from_end)
                topic_partitions.append(tp)
                logger.info(
                    f"TopicPartition set: {tp.topic}-{tp.partition} to offset {tp.offset}"
                )
        self.consumer.assign(topic_partitions)

    def notify_of_update(self, topics: List[str]) -> None:
        """
        Handle updates to the list of topics the consumer is subscribed to.

        Args:
            topics (List[str]): List of topic names to update the consumer with.
        """
        logger.info(f"Notify of update called with topics: {topics}")
        self.stop()
        self.set_consumer_to_offset(topics, 1)  # Set to second to last message
        self.start()
        logger.info(f"Consumer updated and restarted for topics: {topics}")

    def notify_of_stop(self) -> None:
        """
        Notify the Kafka interface to stop consuming messages and insert a sentinel value
        into the queue to signal downstream components.
        """
        logger.info("Notifying of stop...")
        self.stop()
        self.insert_sentinel()

    def insert_sentinel(self) -> None:
        """
        Insert a sentinel value into the serialized messages queue to indicate the stop of message consumption.
        """
        self.serialized_messages_queue.put(Deserialiser.SENTINEL)
