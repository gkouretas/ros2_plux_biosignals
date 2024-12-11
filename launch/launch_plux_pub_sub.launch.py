from ros2_plux_biosignals.plux_configs import PLUX_ROS_NODE
from launch import LaunchDescription
from launch_ros.actions import Node

def generate_launch_description():
    return LaunchDescription([
        Node(
            package='ros2_plux_biosignals',
            executable='pub',
            name=PLUX_ROS_NODE,
            output='screen'
        ),
        Node(
            package='ros2_plux_biosignals',
            executable='logger',
            name=PLUX_ROS_NODE,
            output='screen'
        )
    ])
