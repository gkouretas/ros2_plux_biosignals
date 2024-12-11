# ROS2 Plux Biosignals
Real-time acquisition + analysis of biosignal data

## Connection
Pin: `1234`

## Execute ROS node
The file `launch_plux_pub_sub.launch.py` can be used to launch a general pub/sub node for data logging with the following command:
```cmd
colcon build --symlink-install && source install/setup.bash && ros2 launch ros2_plux_biosignals launch_plux_pub_sub.launch.py
```