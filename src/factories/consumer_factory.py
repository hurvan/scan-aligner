from confluent_kafka import Consumer, KafkaException


class ConsumerFactory:
    def __init__(self):
        pass

    def create_consumer(self, config):
        try:
            return Consumer(config)
        except KafkaException as e:
            print(f"Failed to create consumer with config: {config}")
            raise e
