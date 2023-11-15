from confluent_kafka import Consumer, KafkaException
from streaming_data_types import deserialise_f144


def consume_print_messages():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',  # replace with your server address
        'group.id': 'my_group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe(['motor_data'])  # replace with your topic name
    try:
        while True:
            msg = consumer.poll(0.05)
            if msg is None:
                continue
            # data = deserialise_f144(msg.value())
            data = msg.value()
            print(f"Received data: {data}")
    except KeyboardInterrupt:
        print('Stopped by user')
    finally:
        consumer.close()


if __name__ == '__main__':
    consume_print_messages()
