#!/usr/bin/python3
"""
Script for executing a publisher node
"""
from __future__ import annotations

import rclpy
import signal

# Project based / ROS imports
from ros2_plux_biosignals.plux_publisher import MyPluxThread, MyPluxDevice
from ros2_plux_biosignals.plux_configs import *
from python_utils.ros2_utils.comms.node_manager import create_simple_node, get_realtime_qos_profile
from std_msgs.msg import Header
from idl_definitions.msg import PluxMsg

def main() -> None:
    rclpy.init()
    node = create_simple_node(PLUX_ROS_NODE)
    log_publisher = node.create_publisher(Header, PLUX_ROS_DEBUG_PUBLISHER, 0)
    # plux_thread = MyPluxThread(log_publisher)
    # signal.signal(signal.SIGINT, plux_thread.signal_handler)
    # plux_thread.start()
    device = MyPluxDevice.connect(PLUX_DEVICE_MAC_ADDRESS, node.create_publisher(PluxMsg, PLUX_ROS_TOPIC_NAME, get_realtime_qos_profile()))
    device.run()
    rclpy.spin(node)

if __name__ == "__main__":
    main()