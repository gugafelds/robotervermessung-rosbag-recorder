import sys
if sys.prefix == '/usr':
    sys.real_prefix = sys.prefix
    sys.prefix = sys.exec_prefix = '/home/noel/robotervermessung-rosbag-recorder/src/data_preparation/install/data_preparation'
