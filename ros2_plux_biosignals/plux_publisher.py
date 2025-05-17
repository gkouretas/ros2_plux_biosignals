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
from python_utils.ros2_utils.comms.node_manager import get_node, get_realtime_qos_profile

from std_msgs.msg import Header

from typing import Callable

_RECONNECTION_SLEEP = 5.0

PLUX_SENSORS_PROCESSING_FUNCTIONS: dict[PluxSensor, Callable[[int], float]] = \
    {
        PluxSensor.EMG: emg_in_mV,
        PluxSensor.EDA: eda_in_μsiemens,
        PluxSensor.ECG: ecg_in_mV
    }

class MyPluxDevice(plux.SignalsDev):
    def __init__(self, address: str):
        plux.SignalsDev.__init__(address)

        self._node = get_node(PLUX_ROS_NODE)
        self.table: list[PluxSensor] = []
        self.plux_publisher = None
        
        self._setup_sensor_table()
        self._frame = -1

    def set_publisher(self, publisher: rclpy.publisher.Publisher):
        self.plux_publisher = publisher
        
    def _setup_sensor_table(self):
        for sensor in self.getSensors().values():
            sensor_type = PluxSensor(sensor.clas)
            if not sensor_type in PLUX_SENSORS_PROCESSING_FUNCTIONS.keys():
                self._node.get_logger().warning(f"{sensor_type.name} not supported")
            else:
                self.table.append(sensor_type)

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
        
        for channel_index, sensor_type in enumerate(self.table):
            plux_msg.__setattr__(
                sensor_type.name.lower(), 
                PLUX_SENSORS_PROCESSING_FUNCTIONS[sensor_type](data[channel_index])
            )

        self.plux_publisher.publish(plux_msg)

        return False

class MyPluxThread(threading.Thread):
    def __init__(self, log_publisher: rclpy.publisher.Publisher, **kwargs):
        super().__init__(**kwargs)


        self._signal = threading.Event()
        self._node = get_node(PLUX_ROS_NODE)
        self._publisher = self._node.create_publisher(PluxMsg, PLUX_ROS_TOPIC_NAME, get_realtime_qos_profile())

        self.publisher = log_publisher

        self.plux_device = self._initialize_plux_device()
        
        self._node.get_logger().info(f"Plux device started : version {plux.version}")
        self.publisher.publish(
            Header(frame_id = "Plux Info: Plux Started")
        )

        self._signal.set()

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
        self._signal.wait()
        while self._signal.is_set():
            try:
                self.plux_device.loop()
            except RuntimeError:
                self._node.get_logger().warning("Lost connection to plux, attempting to reconnect...")

                self.plux_device = self._initialize_plux_device()

                self._node.get_logger().info("Re-established connection")

    def signal_handler(self, sig, frame):
        self.publisher.publish(
            Header(
                frame_id=f"Plux Info: Plux Stopped by PI"
            )
        )

        self._signal.clear()
        self.join(timeout=2)
        
        sys.exit(0)


