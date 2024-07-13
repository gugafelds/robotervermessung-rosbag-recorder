import re
import threading
import subprocess
import os
from pathlib import Path
from datetime import datetime
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
from tkinter.constants import BOTH, LEFT, RIGHT, BOTTOM
import rclpy
from rclpy.node import Node
from std_msgs.msg import Empty
from rosbag_processor import RosbagProcessor
from random_point_gen import generate_trajectory_and_save
from connect_socket import ConnectSocket
from ftp_download import sendRandomTrajectoriesFTP, changeFTPValue, getFTPTestFile

class RosbagGUI(Node):
    def __init__(self):
        super().__init__('rosbag_gui')
        self.bags_directory = str(Path.home()) + '/robotervermessung-rosbag-viz/data/rosbag_data/'
        self.logs_directory = str(Path.home()) + '/robotervermessung-rosbag-viz/data/ftp_data/'
        self.trajectories_directory = str(Path.home()) + '/robotervermessung-rosbag-viz/data/random_trajectories/'

        self.publisher_process = self.create_publisher(Empty, '/activate_rosbag_processor', 10)
        self.record_process = None
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        self.init_ui()

    def init_ui(self):
        self.window = tk.Tk()
        self.window.title('Robotervermessung')
        self.window.geometry('700x500')  # Adjust the size as needed

        self.style = ttk.Style()

        # Configure overall application style
        self.style.configure('.', font=('Ubuntu', 10))

        # Configure LabelFrames
        self.style.configure('TLabelFrame', font=('Ubuntu', 14, 'bold'), foreground='black')

        # Configure Buttons
        self.style.configure('TButton', font=('Ubuntu Light', 10), foreground='black', background='SkyBlue3', padding=5)
        self.style.map('TButton', foreground = [('active', '!disabled', 'black')],
                     background = [('active', 'SkyBlue2')])
        # Configure Entries
        self.style.configure('TEntry', font=('Ubuntu Light', 10), foreground='black', padding=5)


        self.setup_left_frame()
        self.setup_right_frame()

        self.window.mainloop()


    def setup_left_frame(self):
        left_frame = ttk.Frame(self.window)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10, pady=10)

        # PC Connection Section
        pc_frame = ttk.LabelFrame(left_frame, text='PC Connection')
        pc_frame.pack(pady=10, padx=10, fill=tk.BOTH)

        self.pc_ip_entry = ttk.Entry(pc_frame, width=25, style='TEntry')  
        self.pc_ip_entry.insert(0, '134.147.234.125')
        self.pc_ip_entry.grid(row=0, column=1, padx=5, pady=5)
        ttk.Label(pc_frame, text='PC IP:').grid(row=0, column=0, padx=5, pady=5, sticky='e')

        self.pc_port_entry = ttk.Entry(pc_frame, width=15, style='TEntry')
        self.pc_port_entry.insert(0, '2004')
        self.pc_port_entry.grid(row=1, column=1, padx=5, pady=5)
        ttk.Label(pc_frame, text='Port:').grid(row=1, column=0, padx=5, pady=5, sticky='e')

        ttk.Button(pc_frame, text='Connect Websocket', command=self.connect_websocket, style='TButton').grid(row=2, columnspan=2, pady=5)

        # MoCap Connection Section
        mocap_frame = ttk.LabelFrame(left_frame, text='MoCap Connection')
        mocap_frame.pack(pady=10, padx=10, fill=tk.BOTH)

        self.mocap_ip_entry = ttk.Entry(mocap_frame, width=25, style='TEntry')  
        self.mocap_ip_entry.insert(0, '192.168.178.30')
        self.mocap_ip_entry.grid(row=0, column=1, padx=5, pady=5)
        ttk.Label(mocap_frame, text='MoCap IP:').grid(row=0, column=0, padx=5, pady=5, sticky='e')

        self.mocap_port_entry = ttk.Entry(mocap_frame, width=15, style='TEntry')  
        self.mocap_port_entry.insert(0, '3883')
        self.mocap_port_entry.grid(row=1, column=1, padx=5, pady=5)
        ttk.Label(mocap_frame, text='Port:').grid(row=1, column=0, padx=5, pady=5, sticky='e')

        ttk.Button(mocap_frame, text='Connect MoCap', command=self.connect_mocap, style='TButton').grid(row=2, columnspan=2, pady=5)

        # Recording Section
        record_frame = ttk.LabelFrame(left_frame, text="Record Data")
        record_frame.pack(pady=10, padx=10, fill=tk.BOTH)

        self.recording_btn = ttk.Button(record_frame, text='Start Recording', command=self.start_stop_record, style='TButton')
        self.recording_btn.pack(pady=5)

        ttk.Button(record_frame, text='Generate CSV from ROSBAG', command=self.activate_rosbag_processor).pack(pady=10)

    def setup_right_frame(self):
        right_frame = ttk.Frame(self.window)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=10, pady=10)

        random_traj_frame = ttk.LabelFrame(right_frame, text='Random Trajectory Generation')
        random_traj_frame.pack(pady=10, padx=10, fill=tk.BOTH)

        ttk.Label(random_traj_frame, text='Reorientation XY:').grid(row=0, column=0, padx=5, pady=5, sticky='e')
        self.reorientation_xy_entry = ttk.Entry(random_traj_frame, width=15, style='TEntry')  
        self.reorientation_xy_entry.insert(0, '20')
        self.reorientation_xy_entry.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(random_traj_frame, text='Reorientation Z:').grid(row=1, column=0, padx=5, pady=5, sticky='e')
        self.reorientation_z_entry = ttk.Entry(random_traj_frame, width=15, style='TEntry')  
        self.reorientation_z_entry.insert(0, '60')
        self.reorientation_z_entry.grid(row=1, column=1, padx=5, pady=5)

        ttk.Label(random_traj_frame, text='Min. Distance (mm):').grid(row=2, column=0, padx=5, pady=5, sticky='e')
        self.min_distance_entry = ttk.Entry(random_traj_frame, width=15, style='TEntry')  
        self.min_distance_entry.insert(0, '150')
        self.min_distance_entry.grid(row=2, column=1, padx=5, pady=5)

        ttk.Label(random_traj_frame, text='Points:').grid(row=3, column=0, padx=5, pady=5, sticky='e')
        self.points_entry = ttk.Entry(random_traj_frame, width=15, style='TEntry')  
        self.points_entry.insert(0, '50')
        self.points_entry.grid(row=3, column=1, padx=5, pady=5)

        ttk.Button(random_traj_frame, text='Generate Random Trajectory', command=self.generate_random_trajectories, width=30, style='TButton').grid(row=4, column=0, columnspan=2, pady=10)
        ttk.Button(random_traj_frame, text='Send Random Trajectory', command=self.send_random_trajectories, width=30, style='TButton').grid(row=5, column=0, columnspan=2, pady=10)

        # Status frame
        status_frame = ttk.LabelFrame(right_frame, text='Status')
        status_frame.pack(pady=5, padx=5, fill=tk.BOTH)

        self.status_label = ttk.Label(status_frame, text='', wraplength=250)
        self.status_label.pack(pady=2)


    def connect_websocket(self):
        host = self.pc_ip_entry.get()
        port = self.pc_port_entry.get()

        if self.validate_ip(host) and self.validate_port(port):
            port = int(port)
            self.connect_socket = ConnectSocket(host, port)
            server_thread = threading.Thread(target=self.connect_socket.run_server)
            server_thread.daemon = True
            server_thread.start()
            self.status_label.config(text='Server started.')
            self.get_logger().info('Server started.')
        else:
            messagebox.showerror("Invalid Input", "Please enter a valid IP address and a four-digit port number.")
            self.status_label.config(text='Invalid IP address or port.')

    def connect_mocap(self):
        host = self.mocap_ip_entry.get()
        port = self.mocap_port_entry.get()

        if self.validate_ip(host) and self.validate_port(port):
            port = int(port)
            launch_command = [
                'ros2', 'launch', 'vrpn_mocap', 'client.launch.yaml',
                f'server:={host}', f'port:={port}'
            ]
            subprocess.Popen(launch_command)
            self.status_label.config(text=f'MoCap connected to {host}:{port}')
            self.get_logger().info(f'MoCap connected to {host}:{port}')
        else:
            messagebox.showerror("Invalid Input", "Please enter a valid IP address and a four-digit port number.")
            self.status_label.config(text='Invalid IP address or port.')

    def start_stop_record(self):
        if self.record_process is None:
            self.trajectory = simpledialog.askstring("Input", "Enter trajectory description:")
            if self.trajectory is None:
                self.status_label.config(text='Recording canceled.')
                return

            self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            bag_file = Path(self.bags_directory) / f'record_{self.timestamp}_{self.trajectory}'
            self.record_process = subprocess.Popen(['ros2', 'bag', 'record', '-o', str(bag_file),
                                                    '/vrpn_mocap/abb4400_tcp/pose', '/vrpn_mocap/abb4400_tcp/twist',
                                                    '/vrpn_mocap/abb4400_tcp/accel',
                                                    'socket_data/position', 'socket_data/orientation',
                                                    'socket_data/tcp_speed', 'socket_data/joint_states',
                                                    'socket_data/achieved_position'])

            self.recording_btn.config(text='Stop Recording', style='Green.TButton')
            self.status_label.config(text=f'Recording started: {bag_file}')
            self.get_logger().info(f'Recording started: {bag_file}')

            input_time = simpledialog.askfloat("Input", "Enter recording duration (in seconds):")
            timer = threading.Timer(float(input_time), self.stop_recording)
            timer.start()
            changeFTPValue("1")
        else:
            self.stop_recording()

    def stop_recording(self):
        if self.record_process:
            self.record_process.terminate()
            self.record_process.wait()
            self.record_process = None
            self.recording_btn.config(text='Start Recording', style='Red.TButton')
            self.status_label.config(text='Recording stopped.')
            changeFTPValue("0")
            getFTPTestFile(self.logs_directory)
            my_file = Path(self.logs_directory) / "ProgramExecution"
            if my_file.is_file():
                renamed_file = f'record_{self.timestamp}_{self.trajectory}_rapid_log'
                os.rename(my_file, os.path.join(self.logs_directory, renamed_file))
                self.get_logger().info(f'Renamed and saved: {renamed_file}')
            self.get_logger().info('Recording stopped.')

    def activate_rosbag_processor(self):
        rosbag_processor = RosbagProcessor()
        self.status_label.config(text='Processing completed.')
        self.get_logger().info('Processing completed.')

    def generate_random_trajectories(self):
        reorientation_xy = float(self.reorientation_xy_entry.get())
        reorientation_z = float(self.reorientation_z_entry.get())
        points = int(self.points_entry.get())
        min_distance = float(self.min_distance_entry.get())

        if reorientation_xy > 20:
            self.status_label.config(text='Reorientation XY angle must be <= 20.')
            return
        if reorientation_z > 60:
            self.status_label.config(text='Reorientation Z angle must be <= 60.')
            return

        num_trajectories = 1  # Adjust as needed
        for i in range(num_trajectories):
            filename = f'random_trajectory_{i + 1}.mod'
            generate_trajectory_and_save(filename, self.trajectories_directory, reorientation_xy, reorientation_z,
                                         points, min_distance)
        self.status_label.config(text='Random trajectory generated.')

    def send_random_trajectories(self):
        sendRandomTrajectoriesFTP(self.trajectories_directory)

    def validate_ip(self, ip):
        pattern = re.compile(r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$")
        if pattern.match(ip):
            return all(0 <= int(num) <= 255 for num in ip.split('.'))
        return False

    def validate_port(self, port):
        return port.isdigit() and 1000 <= int(port) <= 9999

def main(args=None):
    rclpy.init(args=args)
    node = RosbagGUI()
    rclpy.spin(node)
    rclpy.shutdown()

if __name__ == '__main__':
    main()
