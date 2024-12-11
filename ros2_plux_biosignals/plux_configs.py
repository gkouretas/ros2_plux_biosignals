"""Plux device constants"""
PLUX_DEVICE_MAC_ADDRESS = "00:07:80:0F:30:ED"
PLUX_DEVICE_CHANNELS = 0x0F # because our's is a 4 channel device
PLUX_SAMPLING_FREQUENCY = 1000
PLUX_RESOLUTION_BITS = 16
PLUX_ROS_NODE = "bioplux"
PLUX_ROS_TOPIC_NAME = "telemetry"
PLUX_ROS_DEBUG_PUBLISHER = "bioplux_info"
LOG_ROS_TOPIC = "PluxMsg"
