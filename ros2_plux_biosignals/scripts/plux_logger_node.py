#!/usr/bin/python3
"""
Script for executing a logger node
"""
from __future__ import annotations

import rclpy

# Project based / ROS imports
from ros2_plux_biosignals.plux_configs import *
from ros2_plux_biosignals.plux_logger import ROSPluxCSVLogger
from idl_definitions.msg import PluxMsg
from python_utils.ros2_utils.comms.node_manager import get_node

def main() -> None:
    ROSPluxCSVLogger(log_name = PluxMsg.__name__)
    rclpy.spin(node = get_node(PLUX_ROS_NODE))

if __name__ == "__main__":
    main()