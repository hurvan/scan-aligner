import heapq


class Message:
    def __init__(self, topic, partition, offset, key, value, error=None):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value
        self._error = error

    def value(self):
        return self._value

    def offset(self):
        return self._offset

    def error(self):
        return self._error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def key(self):
        return self._key


class ExceptionMessage(Message):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def value(self):
        raise Exception("Value method called on ExceptionMessage")

    def offset(self):
        raise Exception("Offset method called on ExceptionMessage")

    def error(self):
        raise Exception("Error method called on ExceptionMessage")


class TopicPartition:
    def __init__(self, topic, partition, offset=0):
        self.topic = topic
        self.partition = partition
        self.offset = offset


class PartitionMetadata:
    def __init__(self, partition_id):
        self.partition_id = partition_id


class TopicMetadata:
    def __init__(self, topic_name, num_partitions=1):
        self.topic_name = topic_name
        self.partitions = {partition_id: PartitionMetadata(partition_id) for partition_id in range(num_partitions)}


class Metadata:
    def __init__(self):
        self.topics = {}

    def add_topic(self, topic_name, num_partitions=1):
        self.topics[topic_name] = TopicMetadata(topic_name, num_partitions)


class ConsumerStub:
    def __init__(self, config={'auto.offset.reset': 'earliest'}):
        self.config = config
        self.messages = {}
        self.current_offsets = {}
        self.topic_partitions = {}
        self._metadata = Metadata()
        self.message_queue = []
        self.message_id = 0

    def add_message(self, topic, partition, offset, key, value, error=None):
        if topic not in self.messages:
            self.messages[topic] = {}
            self.current_offsets[topic] = {}
        if partition not in self.messages[topic]:
            self.messages[topic][partition] = []
            self.current_offsets[topic][partition] = 0
        message = Message(topic, partition, offset, key, value, error)
        self.messages[topic][partition].append(message)
        heapq.heappush(self.message_queue, (offset, self.message_id, message))
        self.message_id += 1

    def add_exception_message(self, topic, partition, offset, key, value, error=None):
        if topic not in self.messages:
            self.messages[topic] = {}
            self.current_offsets[topic] = {}
        if partition not in self.messages[topic]:
            self.messages[topic][partition] = []
            self.current_offsets[topic][partition] = 0
        message = ExceptionMessage(topic, partition, offset, key, value, error)
        self.messages[topic][partition].append(message)
        heapq.heappush(self.message_queue, (offset, self.message_id, message))
        self.message_id += 1

    def poll(self, timeout=None):
        if not self.message_queue:
            return None

        _, _, msg = heapq.heappop(self.message_queue)

        if self.current_offsets[msg.topic()][msg.partition()] == msg.offset():
            self.current_offsets[msg.topic()][msg.partition()] += 1
            return msg

        return None

    def close(self):
        self.unsubscribe()
        self.unassign()
        self.messages = {}
        self.current_offsets = {}
        self._metadata = Metadata()

    def unsubscribe(self):
        self.topic_partitions = {}
        self.current_offsets = {}
        self._metadata = Metadata()

    def unassign(self):
        self.topic_partitions = {}
        self.current_offsets = {}
        self._metadata = Metadata()

    def assignment(self):
        assigned_partitions = []
        for partitions in self.topic_partitions.values():
            assigned_partitions.extend(partitions)
        return assigned_partitions

    def commit(self, asynchronous=False):
        pass

    def subscribe(self, topics):
        for topic in topics:
            if topic not in self._metadata.topics:
                num_partitions = len(self.messages.get(topic, {}))
                self._metadata.add_topic(topic, num_partitions)

            # Initialize offsets for each partition of the topic
            if topic not in self.current_offsets:
                self.current_offsets[topic] = {}
            for partition in range(num_partitions):
                if partition not in self.current_offsets[topic]:
                    if self.config['auto.offset.reset'] == 'latest':
                        self.current_offsets[topic][partition] = len(self.messages[topic].get(partition, []))
                    else:
                        self.current_offsets[topic][partition] = 0

    def list_topics(self, topic=None):
        if topic is not None and topic not in self._metadata.topics:
            num_partitions = len(self.messages.get(topic, {}))
            self._metadata.add_topic(topic, num_partitions)

        return self._metadata

    def assign(self, topic_partitions):
        for tp in topic_partitions:
            topic = tp.topic
            partition = tp.partition
            offset = tp.offset
            if topic not in self._metadata.topics:
                num_partitions = len(self.messages.get(topic, {}))
                self._metadata.add_topic(topic, num_partitions)

            if topic not in self.current_offsets:
                self.current_offsets[topic] = {}
            self.current_offsets[topic][partition] = offset

            if topic not in self.topic_partitions:
                self.topic_partitions[topic] = []
            self.topic_partitions[topic] = [tp for tp in self.topic_partitions[topic] if tp.partition != partition] + [
                tp]

    def get_watermark_offsets(self, tp):
        topic, partition = tp.topic, tp.partition
        if topic in self.messages and partition in self.messages[topic]:
            high_watermark = len(self.messages[topic][partition])
            return 0, high_watermark
        return 0, 0
