import logging
import queue
import threading
import time
from typing import List

import numpy as np
from streaming_data_types import DESERIALISERS
from streaming_data_types.utils import get_schema

from src.aligner import Aligner


logger = logging.getLogger(__name__)


def handle_f144_data(data):
    return data.source_name, data.value, data.timestamp_unix_ns


# def handle_ev44_data(data):
#     return data.source_name, len(data.time_of_flight), data.reference_time[0] + np.min(data.time_of_flight)

def handle_ev44_data(data):
    if len(data.time_of_flight) <= 1:
        return data.source_name, 0, None if len(data.time_of_flight) == 0 else data.reference_time[0] + \
                                                                               data.time_of_flight[0]
    time_of_flight_s = data.time_of_flight / 1e9
    min_time_s = np.min(time_of_flight_s)
    max_time_s = np.max(time_of_flight_s)
    time_delta_s = max_time_s - min_time_s
    rate = len(data.time_of_flight) / time_delta_s if time_delta_s != 0 else 0

    return data.source_name, rate, data.reference_time[0] + np.min(data.time_of_flight)


SCHEMA_HANDLERS = {
    "f144": handle_f144_data,
    "ev44": handle_ev44_data,
}


class Deserialiser:
    SENTINEL = object()

    def __init__(self, input_queue: queue.Queue, deserialised_messages_queue: queue.Queue, allowed_sources=None):
        self.input_queue = input_queue
        self.deserialised_messages_queue = deserialised_messages_queue
        self.allowed_sources = allowed_sources if allowed_sources else set()
        self._config = {}
        self._source_to_name_map = {}
        self._first_event_data_tracker = {}

        self.running = threading.Event()
        self.thread = None

    def start(self):
        if self.thread and self.thread.is_alive():
            logger.info(f"{self.__class__.__name__} already running")
            return

        self.thread = threading.Thread(target=self.run)
        self.running.set()
        self.thread.start()

    def stop(self):
        self.running.clear()
        if self.thread and self.thread.is_alive():
            self.thread.join()
        self.thread = None
        self._first_event_data_tracker = {}

    def notify_of_start(self, config_message):
        self._config = config_message
        self.allowed_sources = [dev["source"] for dev in self._config.values()]
        self._source_to_name_map = {dev["source"]: name for name, dev in self._config.items()}
        if not self.running.is_set():
            self.start()

    def notify_of_stop(self):
        self.stop()

    def run(self):
        logger.info(f"{self.__class__.__name__} started")
        while self.running.is_set():
            if not self._config or not self._source_to_name_map:
                time.sleep(0.001)
                continue
            try:
                raw_message = self.input_queue.get(timeout=0.001)
            except queue.Empty:
                continue
            if raw_message is self.SENTINEL:
                logger.info(f"{self.__class__.__name__} received sentinel, sending sentinel to aligner")
                self.deserialised_messages_queue.put(Aligner.SENTINEL)
                self.running.clear()
                break

            msg = raw_message.value()
            tp_str = f"{raw_message.topic()}-{raw_message.partition()}"

            schema = get_schema(msg)

            if schema == "ev44":
                if tp_str not in self._first_event_data_tracker.keys():
                    # Skip first detector data on each partition because we don't want "last data"
                    self._first_event_data_tracker[tp_str] = True
                    logger.info(f"Skipping ev44 on tp: {tp_str}")
                    continue

            deserialiser = DESERIALISERS.get(schema, None)
            handler = SCHEMA_HANDLERS.get(schema, None)
            if deserialiser is None or handler is None:
                continue
            deserialised_msg = deserialiser(msg)
            source_name = getattr(deserialised_msg, 'source_name', None)

            if source_name and (source_name in self.allowed_sources or not self.allowed_sources):
                source_name, value, timestamp = handler(deserialised_msg)
                name = self._source_to_name_map.get(source_name, None)
                logger.info(f"{self.__class__.__name__} name: {name}, deserialise message: {source_name}, of type {schema}")
                self.deserialised_messages_queue.put((name, source_name, value, timestamp))
        logger.info(f"{self.__class__.__name__} stopped")
