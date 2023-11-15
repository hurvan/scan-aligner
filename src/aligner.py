import logging
import queue
import threading

import numpy as np

from src.zmq_interface import ZmqInterface
from src.utils import interpolate_to_common_timestamps, interpolate_to_common_timestamps2

logger = logging.getLogger(__name__)


class Aligner:
    SENTINEL = object()

    def __init__(self, input_queue: queue.Queue, zmq_interface: ZmqInterface):
        self.input_queue = input_queue
        self.zmq_interface = zmq_interface
        self.data_buffer = dict()

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
        self.data_buffer = dict()
        self.thread = None

    def notify_of_start(self):
        if not self.running.is_set():
            self.start()

    def notify_of_stop(self):
        self.stop()

    def run(self):
        logger.info(f"{self.__class__.__name__} started")

        # first_det_data_flags = {}

        while self.running.is_set():
            try:
                data = self.input_queue.get(timeout=0.001)
            except queue.Empty:
                continue

            if data is self.SENTINEL:
                logger.info("Received sentinel, clearing data buffer")
                self.data_buffer = dict()
                self.running.clear()
                break

            name, source_name, value, timestamp = data
            data_type = name.split(":")[0]

            if name not in self.data_buffer.keys():
                self.data_buffer[name] = {}
                self.data_buffer[name]["source_name"] = source_name
                self.data_buffer[name]["values"] = []
                self.data_buffer[name]["timestamps"] = []

            if timestamp not in self.data_buffer[name]["timestamps"]:
                self.data_buffer[name]["values"].append(value)
                self.data_buffer[name]["timestamps"].append(timestamp)

            if data_type == "detector":
                try:
                    aligned_data = self.align_data()
                except Exception as e:
                    logger.error(f"Error aligning data: {e}")
                    continue
                if aligned_data:
                    logger.info(f"Sending aligned data: {aligned_data}")
                    self.zmq_interface.send_json(aligned_data)
        logger.info(f"{self.__class__.__name__} stopped")

    def align_data(self):
        if not self.data_buffer:
            return None

        devices = {}
        detectors = {}
        for key, data in self.data_buffer.items():
            data_type = key.split(":")[0]
            if data_type == "device":
                devices[key] = (data["timestamps"], data["values"])
            elif data_type == "detector":
                detectors[key] = (data["timestamps"], data["values"])

        try:
            final_data = interpolate_to_common_timestamps2(devices, detectors)
        except AssertionError:
            logger.error("AssertionError interpolating data")
            return None

        aligned_data = {}
        for det, data in final_data.items():
            aligned_data[det] = {}
            for dev, values in data.items():
                if dev == "values":
                    aligned_data[det]["value"] = float(data["values"][-1])
                else:
                    aligned_data[det][dev] = float(values[-1])

        return aligned_data
