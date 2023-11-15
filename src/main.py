import argparse
import queue
import time
import logging
import uuid

import zmq

from src.data_processor import DataProcessor
from src.factories.consumer_factory import ConsumerFactory
from src.sasl_utils import add_sasl_commandline_options, generate_kafka_security_config

logger = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(description='Fill this out later.')

    parser.add_argument('-b', '--broker',
                        required=True,
                        help='The kafka broker to be used.')

    parser.add_argument('-p', '--push_address',
                        required=True,
                        help='The address to push results to.')

    parser.add_argument('-u', '--pull_address',
                        required=True,
                        help='The address to pull device info from.')

    parser.add_argument('-l', '--log_level',
                        default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help='Set the logging level.')

    add_sasl_commandline_options(parser)

    return parser.parse_args()


def configure_logging(level):
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")
    logging.basicConfig(level=numeric_level)


def create_zmq_sockets(push_address, pull_address):
    context = zmq.Context()
    push_socket = context.socket(zmq.PUSH)
    pull_socket = context.socket(zmq.PULL)
    push_socket.bind(push_address)
    pull_socket.bind(pull_address)
    return push_socket, pull_socket


def main():
    args = parse_arguments()

    configure_logging(args.log_level)

    kafka_security_config = generate_kafka_security_config(
        args.security_protocol,
        args.sasl_mechanism,
        args.sasl_username,
        args.sasl_password,
        args.ssl_cafile,
    )

    consumer_config = {
        "bootstrap.servers": args.broker,
        "group.id": str(uuid.uuid4()),
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    }
    consumer_config.update(kafka_security_config)

    consumer = ConsumerFactory().create_consumer(consumer_config)
    push_socket, pull_socket = create_zmq_sockets(
        args.push_address,
        args.pull_address,
    )

    serialised_messages_queue = queue.Queue()
    deserialize_messages_queue = queue.Queue()

    data_processor = DataProcessor(
        consumer,
        push_socket,
        pull_socket,
        serialised_messages_queue,
        deserialize_messages_queue,
    )

    data_processor.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        data_processor.stop()


if __name__ == "__main__":
    main()
