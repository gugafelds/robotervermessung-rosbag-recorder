#!/bin/bash

bash /home/$USER/robotervermessung-rosbag-recorder/install/setup.bash

# Set the PYTHONPATH
export PYTHONPATH=$PYTHONPATH:/home/$USER/robotervermessung-rosbag-recorder/src/data_preparation/data_preparation

# Run the Python GUI script
ros2 run data_preparation rosbag_gui
