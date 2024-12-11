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

log_publisher = None

PLUX_SENSORS_PROCESSING_FUNCTIONS: dict[PluxSensor, Callable[[int], float]] = \
    {
        PluxSensor.EMG: emg_in_mV,
        PluxSensor.EDA: eda_in_μsiemens,
        PluxSensor.ECG: ecg_in_mV
    }

class MyPluxDevice(plux.SignalsDev):
    def __init__(self, address: str):
        plux.SignalsDev.__init__(address)

        self.table: list[PluxSensor] = []
        self.plux_publisher = get_node(PLUX_ROS_NODE).create_publisher(PluxMsg, PLUX_ROS_TOPIC_NAME, get_realtime_qos_profile())
        self._setup_sensor_table()
        self._frame = -1

    def _setup_sensor_table(self):
        for sensor in self.getSensors().values():
            sensor_type = PluxSensor(sensor.clas)
            if not sensor_type in PLUX_SENSORS_PROCESSING_FUNCTIONS.keys():
                print(f"{sensor_type.name} not supported")
            else:
                self.table.append(sensor_type)

    def onRawFrame(self, nSeq, data):
        plux_msg = PluxMsg()
        plux_msg.world_timestamp = time.time()

        if nSeq == 0:
            print(f"First frame received")
        
        if self._frame + 1 != nSeq: 
            print(f"Publisher frame skip {self._frame} {nSeq}")
            
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
        threading.Thread.__init__(self, **kwargs)

        self.publisher = log_publisher
        self.plux_device = MyPluxDevice(PLUX_DEVICE_MAC_ADDRESS)

        self.plux_device.start(PLUX_SAMPLING_FREQUENCY, PLUX_DEVICE_CHANNELS, PLUX_RESOLUTION_BITS)
        
        self.publisher.publish(
            Header(frame_id = "Plux Info: Plux Started")
        )

        get_node(PLUX_ROS_NODE).get_logger().info("Plux device started")

    def run(self):
        try:
            self.plux_device.loop()
        except Exception as e:
            self.publisher.publish(
                Header(frame_id=f"Plux Error: Plux Stopped due to error: {e}")
            )

            traceback.print_exc()

            time.sleep(2)

    def signal_handler(self, sig, frame):
        print("You pressed Ctrl+C!")
        self.publisher.publish(
            Header(
                frame_id=f"Plux Info: Plux Stopped by PI"
            )
        )

        time.sleep(2)
        sys.exit(0)


