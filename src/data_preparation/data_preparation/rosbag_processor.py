import rclpy
from rclpy.node import Node
import pandas as pd
import rosbag2_py
from rosidl_runtime_py.utilities import get_message
from rclpy.serialization import deserialize_message

from geometry_msgs.msg import Point, Pose, Quaternion, PoseStamped, AccelStamped, TwistStamped
from std_msgs.msg import Float64
from sensor_msgs.msg import JointState
import numpy as np
from pathlib import Path
import os

class RosbagProcessor(Node):
    def __init__(self):
        super().__init__('rosbag_processor')
        self.declare_parameter('bags_directory', str(Path.home()) + '/robotervermessung-rosbag-viz/data/rosbag_data')
        self.declare_parameter('topics', [
            '/vrpn_mocap/abb4400_tcp/pose',
            '/vrpn_mocap/abb4400_tcp/twist',
            '/vrpn_mocap/abb4400_tcp/accel',
            '/socket_data/position',
            '/socket_data/orientation',
            '/socket_data/tcp_speed',
            '/socket_data/joint_states',
            '/socket_data/achieved_position',
        ])        
        self.declare_parameter('merged_output_directory', str(Path.home()) + '/robotervermessung-rosbag-viz/data/csv_data/')
        self.bags_directory = self.get_parameter('bags_directory').get_parameter_value().string_value
        self.topics = self.get_parameter('topics').get_parameter_value().string_array_value
        self.merged_output_directory = self.get_parameter('merged_output_directory').get_parameter_value().string_value
        self.process_all_bags()

    def process_all_bags(self):
        for bag_directory in Path(self.bags_directory).rglob('*.db3'):
            self.bag_file = str(bag_directory).split("data/")[2].split("/")[0]
            merged_filename = os.path.join(self.merged_output_directory, f'{self.bag_file}_final.csv')
            
            # Check if merged CSV file already exists, skip processing if it does
            if os.path.exists(merged_filename):
                self.get_logger().info(f'Merged CSV file {merged_filename} already exists. Skipping processing.')
                continue
            
            self.process_bag(str(bag_directory))
    def process_bag(self, bag_directory):
        storage_options = rosbag2_py.StorageOptions(uri=bag_directory, storage_id='sqlite3')
        converter_options = rosbag2_py.ConverterOptions(input_serialization_format='cdr', output_serialization_format='cdr')
        reader = rosbag2_py.SequentialReader()
        reader.open(storage_options, converter_options)

        topic_type_map = {topic.name: topic.type for topic in reader.get_all_topics_and_types()}
        topic_data = {topic: [] for topic in self.topics}

        while reader.has_next():
            try:
                (topic, data, t) = reader.read_next()
                if topic in self.topics:
                    msg_type_str = topic_type_map[topic]
                    msg_type = get_message(msg_type_str)
                    msg = deserialize_message(data, msg_type)
                    topic_data[topic].append((t, msg))
            except Exception as e:
                self.get_logger().error(f'Error reading message from bag: {e}')
                break

        self.save_to_individual_csv(topic_data)

    def calculate_magnitude(self, x, y, z):
        return np.sqrt(x**2 + y**2 + z**2)

    def save_to_individual_csv(self, topic_data):
        for topic, data in topic_data.items():
            if data:
                parsed_data = []
                columns = []
                for t, msg in data:
                    if isinstance(msg, Point) and topic == "/socket_data/position":
                        parsed_data.append([t, msg.x, msg.y, msg.z])
                        columns = ['timestamp', 'ps_x', 'ps_y', 'ps_z']
                    
                    elif isinstance(msg, Pose) and topic == "/socket_data/achieved_position":
                        parsed_data.append([t, msg.position.x, msg.position.y, msg.position.z, msg.orientation.w, msg.orientation.x, msg.orientation.y, msg.orientation.z])
                        columns = ['timestamp', 'ap_x', 'ap_y', 'ap_z', 'aq_w','aq_x', 'aq_y', 'aq_z']
                    elif isinstance(msg, Quaternion):
                        parsed_data.append([
                            t,
                            msg.x,
                            msg.y,
                            msg.z,
                            msg.w
                        ])
                        columns = ['timestamp', 'os_x', 'os_y', 'os_z', 'os_w']
                    elif isinstance(msg, Float64):
                        parsed_data.append([
                            t,
                            msg.data
                        ])
                        columns = ['timestamp', 'tcp_speeds']
                    elif isinstance(msg, JointState):
                        joint_names = msg.name
                        joint_positions = list(msg.position)

                        # Flatten the joint_positions if it contains nested lists
                        joint_positions_flat = []
                        for item in joint_positions:
                            if isinstance(item, list):
                                joint_positions_flat.extend(item)
                            else:
                                joint_positions_flat.append(item)

                        parsed_data.append([t] + joint_positions_flat[:6])  # Ensure only the first 6 positions are used
                        columns = ['timestamp'] + [f'joint_{i+1}' for i in range(6)]

                        if len(parsed_data) == 13:
                            print("Debug Info: JointState Parsed Data with 13 columns")
                            print(parsed_data)
                            print("Columns: ", columns)
                        
                            print(parsed_data[0])
                            print(parsed_data[1])

                    elif isinstance(msg, PoseStamped):
                        parsed_data.append([
                            t,
                            msg.header.stamp.sec,
                            msg.header.stamp.nanosec,
                            msg.pose.position.x*1000,
                            msg.pose.position.y*1000,
                            msg.pose.position.z*1000,
                            msg.pose.orientation.x,
                            msg.pose.orientation.y,
                            msg.pose.orientation.z,
                            msg.pose.orientation.w
                        ])
                        columns = [
                            'timestamp', 'sec', 'nanosec',
                            'pv_x', 'pv_y', 'pv_z', 'ov_x', 'ov_y', 'ov_z', 'ov_w'
                        ]

                    elif isinstance(msg, TwistStamped):
                        linear_velocity = self.calculate_magnitude(msg.twist.linear.x, msg.twist.linear.y, msg.twist.linear.z)
                        angular_velocity = self.calculate_magnitude(msg.twist.angular.x, msg.twist.angular.y, msg.twist.angular.z)
                        parsed_data.append([
                            t,
                            msg.header.stamp.sec,
                            msg.header.stamp.nanosec,
                            msg.twist.linear.x*1000,
                            msg.twist.linear.y*1000,
                            msg.twist.linear.z*1000,
                            linear_velocity*1000,
                            msg.twist.angular.x,
                            msg.twist.angular.y,
                            msg.twist.angular.z,
                            angular_velocity
                        ])
                        columns = [
                            'timestamp', 'sec', 'nanosec',
                            'tcp_speedv_x', 'tcp_speedv_y', 'tcp_speedv_z', 'tcp_speedv', 'tcp_angularv_x', 'tcp_angularv_y', 'tcp_angularv_z', 'tcp_angularv'
                        ]
                    elif isinstance(msg, AccelStamped):
                        linear_acceleration = self.calculate_magnitude(msg.accel.linear.x, msg.accel.linear.y, msg.accel.linear.z)
                        angular_acceleration = self.calculate_magnitude(msg.accel.angular.x, msg.accel.angular.y, msg.accel.angular.z)
                        parsed_data.append([
                            t,
                            msg.header.stamp.sec,
                            msg.header.stamp.nanosec,
                            msg.accel.linear.x,
                            msg.accel.linear.y,
                            msg.accel.linear.z,
                            linear_acceleration,
                            msg.accel.angular.x,
                            msg.accel.angular.y,
                            msg.accel.angular.z,
                            angular_acceleration
                        ])
                        columns = [
                            'timestamp', 'sec', 'nanosec',
                            'tcp_accelv__x', 'tcp_accelv__y', 'tcp_accelv__z', 'tcp_accelv',
                            'tcp_accelv_angular_x', 'tcp_accelv_angular_y', 'tcp_accelv_angular_z', 'tcp_accelv_angular'
                        ]

                if parsed_data and columns:
                    df = pd.DataFrame(parsed_data, columns=columns)
                    csv_file = os.path.join(self.merged_output_directory, self.bag_file + f'{topic.replace("/", "_")}.csv')
                    print(csv_file)
                    df.to_csv(csv_file, index=False)
                    self.get_logger().info(f'Saved topic {topic} data to {csv_file}.')

        self.merge_csv_files()

    def merge_csv_files(self):
        mocap_topics = ['/vrpn_mocap/abb4400_tcp/pose', '/vrpn_mocap/abb4400_tcp/twist', '/vrpn_mocap/abb4400_tcp/accel']
        websocket_topics = ['/socket_data/position', '/socket_data/orientation', '/socket_data/tcp_speed', '/socket_data/joint_states', '/socket_data/achieved_position']
        both_topics = mocap_topics + websocket_topics

        # Separate merge logic for mocap and websocket data
        self.merge_individual_csv_files(mocap_topics, "mocap")
        self.merge_individual_csv_files(websocket_topics, "websocket")
        self.merge_individual_csv_files(both_topics, "mocap+websocket")

    def merge_individual_csv_files(self, topics, data_type):
        # Collect all CSV files for the specified topics
        all_csv_files = []
        for topic in topics:
            csv_file_path = os.path.join(self.merged_output_directory, self.bag_file + f'{topic.replace("/", "_")}.csv')
            if os.path.exists(csv_file_path):
                all_csv_files.append(csv_file_path)

        # Load all dataframes from the collected CSV files
        dataframes = []
        for file in all_csv_files:
            df = pd.read_csv(file)
            df['source'] = os.path.basename(file)  # Add a column to indicate the source file
            dataframes.append(df)

        if dataframes:
            merged_df = pd.concat(dataframes, ignore_index=True).sort_values(by=['timestamp']).reset_index(drop=True)

            # Remove the 'source' column
            merged_df.drop(columns=['source'], inplace=True)

            # Ensure that the columns that don't exist in certain dataframes are filled with NaN
            if data_type == "mocap":
                all_columns = [
                    'timestamp', 'sec', 'nanosec',
                    'pv_x', 'pv_y', 'pv_z', 'ov_x', 'ov_y', 'ov_z', 'ov_w',
                    'tcp_speedv_x', 'tcp_speedv_y', 'tcp_speedv_z', 'tcp_speedv', 'tcp_angularv_x', 'tcp_angularv_y', 'tcp_angularv_z', 'tcp_angularv',
                    'tcp_accelv__x', 'tcp_accelv__y', 'tcp_accelv__z', 'tcp_accelv',
                    'tcp_accelv_angular_x', 'tcp_accelv_angular_y', 'tcp_accelv_angular_z', 'tcp_accelv_angular'
                ]
            elif data_type ==  "websocket":
                all_columns = [
                    'timestamp', 'ps_x', 'ps_y', 'ps_z',
                    'os_x', 'os_y', 'os_z', 'os_w',
                    'tcp_speeds'
                ] + [f'joint_{i+1}' for i in range(6)] + ['ap_x', 'ap_y', 'ap_z']  # Assuming 6 joints
            elif data_type == "mocap+websocket":
                all_columns = [
                    'timestamp', 'sec', 'nanosec',
                                        'pv_x', 'pv_y', 'pv_z', 'ov_x', 'ov_y', 'ov_z', 'ov_w',
                    'tcp_speedv_x', 'tcp_speedv_y', 'tcp_speedv_z', 'tcp_speedv', 'tcp_angularv_x', 'tcp_angularv_y', 'tcp_angularv_z', 'tcp_angularv',
                    'tcp_accelv__x', 'tcp_accelv__y', 'tcp_accelv__z', 'tcp_accelv',
                    'tcp_accelv_angular_x', 'tcp_accelv_angular_y', 'tcp_accelv_angular_z', 'tcp_accelv_angular', 
                    'ps_x', 'ps_y', 'ps_z',
                    'os_x', 'os_y', 'os_z', 'os_w',
                    'tcp_speeds'] + [f'joint_{i+1}' for i in range(6)
                ] + ['ap_x', 'ap_y', 'ap_z', 'aq_x','aq_y', 'aq_z', 'aq_w']

            for col in all_columns:
                if col not in merged_df.columns:
                    merged_df[col] = np.nan

            # Reorder columns
            merged_df = merged_df[all_columns]

            # Save the merged CSV file
            merged_filename = os.path.join(self.merged_output_directory, f'{self.bag_file}_final.csv')
            merged_df.to_csv(merged_filename, index=False)
            self.get_logger().info(f'Merged individual {data_type} CSV files into {merged_filename}.')
        else:
            self.get_logger().info(f'No {data_type} CSV files found to merge.')

def main(args=None):
    rclpy.init(args=args)
    rosbag_processor = RosbagProcessor()
    rclpy.spin(rosbag_processor)
    rosbag_processor.destroy_node()
    rclpy.shutdown()

if __name__ == '__main__':
    main()