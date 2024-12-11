#!/usr/bin/python3
"""
Script for executing a publisher node
"""
from __future__ import annotations

import signal

# Project based / ROS imports
from ros2_plux_biosignals.plux_publisher import MyPluxThread
from ros2_plux_biosignals.plux_configs import *
from python_utils.ros2_utils.comms.node_manager import get_node
from std_msgs.msg import Header

def main() -> None:
    log_publisher = get_node(PLUX_ROS_NODE).create_publisher(Header, PLUX_ROS_DEBUG_PUBLISHER, 0)
    plux_thread = MyPluxThread(log_publisher)
    signal.signal(signal.SIGINT, plux_thread.signal_handler)
    plux_thread.start()

if __name__ == "__main__":
    main()