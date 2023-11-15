from confluent_kafka import Consumer, TopicPartition
import threading
import logging
import queue
from typing import List

from src.deserialiser import Deserialiser

logger = logging.getLogger(__name__)



class KafkaInterface:
    def __init__(self, consumer: Consumer, serialized_messages_queue: queue.Queue):
        self.consumer = consumer
        self.serialized_messages_queue = serialized_messages_queue
        self.running = threading.Event()
        self.thread = None
        self.commit_interval = 5  # Commit after every 5 messages
        self.messages_since_last_commit = 0  # Track messages since the last commit
        logger.info(f"KafkaInterface initialized with consumer: {consumer}")

    def start(self):
        if self.thread and self.thread.is_alive():
            logger.info("Attempted to start KafkaInterface, but thread is already running.")
            return  # Already running, do not start another thread
        self.running.set()
        self.thread = threading.Thread(target=self._consume_messages)
        self.thread.start()
        logger.info("KafkaInterface consumer thread started.")

    def stop(self):
        logger.info("Stopping KafkaInterface...")
        self.running.clear()
        if self.thread:
            self.thread.join()
        if self.messages_since_last_commit > 0:
            self.commit_offsets()
        self.consumer.unassign()
        # self.consumer.close()
        logger.info("KafkaInterface stopped and consumer closed.")

    def commit_offsets(self):
        logger.info("Committing offsets...")
        try:
            self.consumer.commit(asynchronous=False)
        except Exception as e:
            logger.error(f"Failed to commit offsets: {e}")
        self.messages_since_last_commit = 0

    def _consume_messages(self):
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

    def set_consumer_to_offset(self, topics: List[str], offset_from_end: int):
        logger.info(f"Setting consumer to offset for topics: {topics}, offset from end: {offset_from_end}")
        topic_partitions = []
        for topic in topics:
            topic_metadata = self.consumer.list_topics(topic).topics[topic]
            for partition in topic_metadata.partitions:
                tp = TopicPartition(topic, partition)
                _, high_watermark = self.consumer.get_watermark_offsets(tp)
                tp.offset = max(0, high_watermark - offset_from_end)
                topic_partitions.append(tp)
                logger.info(f"TopicPartition set: {tp.topic}-{tp.partition} to offset {tp.offset}")
        self.consumer.assign(topic_partitions)

    def notify_of_update(self, topics: List[str]):
        logger.info(f"Notify of update called with topics: {topics}")
        self.stop()
        self.set_consumer_to_offset(topics, 1)  # Set to second to last message
        self.start()
        logger.info(f"Consumer updated and restarted for topics: {topics}")

    def notify_of_stop(self):
        logger.info("Notifying of stop...")
        self.stop()
        self.insert_sentinel()

    def insert_sentinel(self):
        self.serialized_messages_queue.put(Deserialiser.SENTINEL)






# class KafkaInterface:
#     def __init__(self, consumer: Consumer, serialized_messages_queue: queue.Queue):
#         self.consumer = consumer
#         self.serialized_messages_queue = serialized_messages_queue
#         self.running = threading.Event()
#         self.thread = None
#         logger.info(f"KafkaInterface initialized with consumer: {consumer}")
#
#     def start(self):
#         if self.thread and self.thread.is_alive():
#             logger.info("Attempted to start KafkaInterface, but thread is already running.")
#             return  # Already running, do not start another thread
#         self.running.set()
#         self.thread = threading.Thread(target=self._consume_messages)
#         self.thread.start()
#         logger.info("KafkaInterface consumer thread started.")
#
#     def stop(self):
#         self.running.clear()
#         if self.thread:
#             self.thread.join()
#         self.consumer.unsubscribe()
#         self.consumer.unassign()
#         logger.info("KafkaInterface stopped and consumer closed.")
#
#     def insert_sentinel(self):
#         self.serialized_messages_queue.put(Deserialiser.SENTINEL)
#
#     def _consume_messages(self, timeout=0.05):
#         logger.info("Consumer message polling started.")
#         counter = 0
#         while self.running.is_set():
#             message = self.consumer.poll(timeout)
#             if message:
#                 # logger.info(f"Message received: {message.value()}")
#                 self.serialized_messages_queue.put(message.value())
#                 counter += 1
#
#     def set_consumer_to_offset(self, topics: List[str], offset_from_end: int):
#         logger.info(f"Setting consumer to offset for topics: {topics}, offset from end: {offset_from_end}")
#         topic_partitions = []
#         for topic in topics:
#             topic_metadata = self.consumer.list_topics(topic).topics[topic]
#             for partition in topic_metadata.partitions:
#                 tp = TopicPartition(topic, partition)
#                 _, high_watermark = self.consumer.get_watermark_offsets(tp)
#                 tp.offset = max(0, high_watermark - offset_from_end)
#                 topic_partitions.append(tp)
#                 logger.info(f"TopicPartition set: {tp.topic}-{tp.partition} to offset {tp.offset}")
#         self.consumer.assign(topic_partitions)
#         logger.info("Consumer assigned to topic partitions.")
#
#         for tp in topic_partitions:
#             current_position = self.consumer.position([tp])
#             logger.info(f"Current position for {tp.topic}-{tp.partition} is {current_position}")
#
#     def notify_of_update(self, topics: List[str]):
#         logger.info(f"Notify of update called with topics: {topics}")
#         self.stop()
#         self.consumer.subscribe(topics)
#         self.set_consumer_to_offset(topics, 2)  # Set to second to last message
#         self.start()
#         logger.info(f"Consumer updated and restarted for topics: {topics}")
#
#     def notify_of_stop(self):
#         self.stop()
#         self.insert_sentinel()
