import time

from confluent_kafka import Producer

from tests.doubles.generators import generate_linear_fake_ev44_events, generate_linear_fake_f144_data


def generate_and_produce_data(start_offset=0):
    producer = Producer({"bootstrap.servers": 'localhost:9092', "message.max.bytes": 100_000_000})

    f144_data = generate_linear_fake_f144_data(10, source_name="motor", start_offset=start_offset)
    ev44_data = generate_linear_fake_ev44_events(10, source_name="detector", start_offset=start_offset)

    data_topic = "motor_data"
    event_topic = "detector_data"

    for i in range(10):
        try:
            producer.produce(event_topic, ev44_data[i])
            producer.flush()
            time.sleep(0.05)
            producer.produce(data_topic, f144_data[i])
            producer.flush()
            time.sleep(0.05)
        except Exception as e:
            print(e)

        time.sleep(0.05)


if __name__ == "__main__":
    generate_and_produce_data()
