import logging
import queue
import threading
from typing import Union

import numpy as np

from src.utils import (interpolate_to_common_timestamps,
                       interpolate_to_common_timestamps2)
from src.zmq_interface import ZmqInterface

logger = logging.getLogger(__name__)


class Aligner:
    """
    Aligner manages the alignment of data packets based on their timestamps.

    It operates as a separate thread that continuously processes incoming data from an input queue,
    aligns this data according to its timestamps, and sends the aligned data to the next stage via
    a ZMQ interface. The Aligner is designed to work with data that contains timestamps and values,
    typically from various detectors or devices.

    Attributes:
        input_queue (queue.Queue): Queue from which the Aligner receives data packets.
        zmq_interface (ZmqInterface): Interface to send aligned data forward.
        data_buffer (dict): Temporary storage for unaligned data packets.
        running (threading.Event): Event to control the running state of the thread.
        thread (threading.Thread or None): Thread that runs the main processing loop.
        SENTINEL (object): Sentinel object used to signal the stopping of the thread.
    """

    SENTINEL = object()

    def __init__(self, input_queue: queue.Queue, zmq_interface: ZmqInterface):
        """
        Initializes an Aligner instance with an input queue and a ZMQ interface.

        Args:
            input_queue (queue.Queue): Queue for receiving data packets to align.
            zmq_interface (ZmqInterface): Interface for sending out aligned data.
        """
        self.input_queue = input_queue
        self.zmq_interface = zmq_interface
        self.data_buffer = dict()

        self.running = threading.Event()
        self.thread = None

    def start(self) -> None:
        """Start the aligner's processing thread if it is not already running."""
        if self.thread and self.thread.is_alive():
            logger.info(f"{self.__class__.__name__} already running")
            return
        self.thread = threading.Thread(target=self.run)
        self.running.set()
        self.thread.start()

    def stop(self) -> None:
        """Stop the aligner's processing thread and clear the data buffer."""
        self.running.clear()
        if self.thread and self.thread.is_alive():
            self.thread.join()
        self.data_buffer = dict()
        self.thread = None

    def notify_of_start(self) -> None:
        """Notify the aligner that it should start if it's not already running."""
        if not self.running.is_set():
            self.start()

    def notify_of_stop(self) -> None:
        """Notify the aligner that it should stop."""
        self.stop()

    def run(self) -> None:
        """
        The main loop of the aligner that processes incoming data packets.
        It checks for the sentinel object to stop the loop, aligns data, and sends it via ZeroMQ.
        """
        logger.info(f"{self.__class__.__name__} started")
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

    def align_data(self) -> Union[dict, None]:
        """
        Align the data in the buffer based on timestamps and return it.

        :return: A dictionary of aligned data or None if there is nothing to align.
        """
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
