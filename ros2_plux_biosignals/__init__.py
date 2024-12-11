"""Initializes plux ROS driver"""
import rclpy
import platform
import os
import sys

# TODO: workspace path should not be hard-coded... Is this even needed???
sys.path.append("~/ros2_ws")
sys.path.append(os.path.dirname(__file__))

from plux_configs import *
from python_utils.ros2_utils.comms.node_manager import create_simple_node

# Import plux API
PLUX_API_PATH = "thirdparty/plux/python-samples/PLUX-API-Python3"

osDic = {
    "Darwin": f"MacOS/Intel{''.join(platform.python_version().split('.')[:2])}",
    "Linux": "Linux64",
    "Windows": f"Win{platform.architecture()[0][:2]}_{''.join(platform.python_version().split('.')[:2])}",
}

if platform.mac_ver()[0] != "":
    import subprocess
    from os import linesep

    p = subprocess.Popen("sw_vers", stdout=subprocess.PIPE)
    result = p.communicate()[0].decode("utf-8").split(str("\t"))[2].split(linesep)[0]
    if result.startswith("12."):
        osDic["Darwin"] = "MacOS/Intel310"
        if (
            int(platform.python_version().split(".")[0]) <= 3
            and int(platform.python_version().split(".")[1]) < 10
        ):
            raise FileNotFoundError("Unable to find plux version")
            
PLUX_PATH = f"{PLUX_API_PATH}/{osDic[platform.system()]}"
sys.path.append(PLUX_PATH)

# Create module node
rclpy.init()
create_simple_node(PLUX_ROS_NODE)