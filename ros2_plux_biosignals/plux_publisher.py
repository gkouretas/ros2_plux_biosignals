#!/usr/bin/python3
"""
ROS node for publishing plux data. Utilizes plux API. See `__init__.py` for configuring the path to the thirdparty SW.
"""
from __future__ import annotations

import rclpy
import rclpy.clock
import rclpy.logging
import rclpy.publisher

import plux # handled by __init__.py
import threading
import traceback
import time
import sys

from plux_configs import *
from plux_processing import *
from plux_typedefs import *
from idl_definitions.msg import PluxMsg
from idl_definitions.msg import PluxSensor as PluxSensor_
from python_utils.ros2_utils.comms.node_manager import get_node, get_realtime_qos_profile

import rclpy.timer
from std_msgs.msg import Header

from typing import Callable
from collections import deque

_RECONNECTION_SLEEP = 1.0

PLUX_SENSORS_PROCESSING_FUNCTIONS: dict[PluxSensor, Callable[[int], float]] = \
    {
        PluxSensor.EMG: emg_in_mV,
        PluxSensor.EDA: eda_in_μsiemens,
        PluxSensor.ECG: ecg_in_mV
    }

class MyPluxDevice(plux.SignalsDev):
    @staticmethod
    def connect(address: str, publisher: rclpy.publisher.Publisher) -> MyPluxDevice:
        while True:
            try:
                device = MyPluxDevice(address)
                device.set_publisher(publisher)
                device.start(PLUX_SAMPLING_FREQUENCY, PLUX_DEVICE_CHANNELS, PLUX_RESOLUTION_BITS)

                return device
            except RuntimeError as e:
                get_node(PLUX_ROS_NODE).get_logger().info(f"Waiting for connection [err: {e}]...")
                time.sleep(_RECONNECTION_SLEEP)

    def __init__(self, address: str):
        plux.SignalsDev.__init__(address)

        self._node = get_node(PLUX_ROS_NODE)
        self._table: dict[PluxSensor, list[int]] = {}
        self._plux_publisher = None
        
        self._setup_sensor_table()
        self._frame = -1
        self._queue = deque[PluxMsg]()
        self._thread = threading.Thread(target=self.loop)
        self._lock = threading.Lock()
        self._timer: rclpy.timer.Timer = None
        self._signal = threading.Event()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass   

    def run(self):
        self._thread.start()
        self._signal.wait()

        self._timer = self._node.create_timer(
            (1.0/PLUX_SAMPLING_FREQUENCY) / 2.0,
            callback=self.process_data
        )

    def process_data(self):
        if not self._thread.is_alive():
            self._node.get_logger().error("Process thread not alive, canceling publisher")
            self._timer.cancel()

        data = None
        try:
            with self._lock:
                if len(self._queue) > 0:
                    data = self._queue.popleft()
                    self._plux_publisher.publish(data)
                    data = None
        except RuntimeError as e:
            self._node.get_logger().error(f"Error publishing: {e}")
            
            # Re-insert data into the queue if it was not able to be published...
            if data is not None: self._queue.appendleft(data)

    def set_publisher(self, publisher: rclpy.publisher.Publisher):
        self._plux_publisher = publisher
        
    def _setup_sensor_table(self):
        for port, sensor in enumerate(self.getSensors().values()):
            sensor_type = PluxSensor(sensor.clas)
            if not sensor_type in PLUX_SENSORS_PROCESSING_FUNCTIONS.keys():
                self._node.get_logger().warning(f"{sensor_type.name} not supported")
            else:
                self._node.get_logger().info(f"Sensor {sensor_type} at port index {port}")
                if sensor_type not in self._table.keys(): self._table[sensor_type] = []
                self._table[sensor_type].append(port)

    def onRawFrame(self, nSeq, data):
        plux_msg = PluxMsg()
        plux_msg.world_timestamp = time.time()

        if nSeq == 0:
            self._node.get_logger().info(f"First frame received")
        
        if self._frame + 1 != nSeq: 
            self._node.get_logger().warning(f"Publisher frame skip {self._frame} {nSeq}")
            
        self._frame = nSeq

        plux_msg.frame = nSeq
        plux_msg.source_timestamp = nSeq / PLUX_SAMPLING_FREQUENCY
        
        for sensor_type, channel_indices in self._table.items():
            plux_msg.__setattr__(sensor_type.name.lower(), [
                PluxSensor_(
                    port=channel_index,
                    value=PLUX_SENSORS_PROCESSING_FUNCTIONS[sensor_type](data[channel_index])
                ) for channel_index in channel_indices])

        self._signal.set()
        with self._lock:
            self._queue.append(plux_msg)

        return False

class MyPluxThread(threading.Thread):
    def __init__(self, log_publisher: rclpy.publisher.Publisher, **kwargs):
        super().__init__(**kwargs)


        self._signal = threading.Event()
        self._node = get_node(PLUX_ROS_NODE)
        self._publisher = self._node.create_publisher(PluxMsg, PLUX_ROS_TOPIC_NAME, get_realtime_qos_profile())
        
        self.publisher = log_publisher

        self.plux_device = self._initialize_plux_device()
        
        self._node.get_logger().info(f"Plux device started : info {self.plux_device.getProperties()}")
        self._node.get_logger().info(f"Battery = {self.plux_device.getBattery()}")
        self.plux_device.setTimeout(-1) # never timeout

        self.publisher.publish(
            Header(frame_id = "Plux Info: Plux Started")
        )

        self._signal.set()
        self._node.create_timer(
            1.0/PLUX_SAMPLING_FREQUENCY,
            self.plux_device.loop
        )


    def _initialize_plux_device(self):
        while True:
            try:
                device = MyPluxDevice(PLUX_DEVICE_MAC_ADDRESS)
                device.set_publisher(self._publisher)
                device.start(PLUX_SAMPLING_FREQUENCY, PLUX_DEVICE_CHANNELS, PLUX_RESOLUTION_BITS)

                return device
            except RuntimeError as e:
                self._node.get_logger().info(f"Waiting for connection [err: {e}]...")
                time.sleep(_RECONNECTION_SLEEP)

    def run(self):
        return
        self._signal.wait()
        while self._signal.is_set():
            try:
                self.plux_device.loop()
            except RuntimeError as e:
                self._node.get_logger().warning(f"Lost connection to plux [err: {e}], attempting to reconnect...")

                # self.plux_device = self._initialize_plux_device()

                # self._node.get_logger().info("Re-established connection")

    def signal_handler(self, sig, frame):
        self.publisher.publish(
            Header(
                frame_id=f"Plux Info: Plux Stopped by PI"
            )
        )

        self._signal.clear()
        self.join(timeout=2)
        
        sys.exit(0)


