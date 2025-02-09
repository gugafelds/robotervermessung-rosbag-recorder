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
from std_msgs.msg import Bool, Empty, Float64, UInt16
from std_msgs.msg import Empty
from rosbag_processor import RosbagProcessor
from random_point_gen import generate_trajectory_and_save
from random_point_gen_pick_place import generate_pick_place_trajectory_and_save, calibration_movement,gripper_down,gripper_up
from connect_socket import ConnectSocket
from ftp_download import sendRandomTrajectoriesFTP, changeFTPValue, getFTPTestFile
import time

class RosbagGUI(Node):
    def __init__(self):
        super().__init__('rosbag_gui')
        self.bags_directory = str(Path.home()) + '/robotervermessung-rosbag-recorder/data/rosbag_data/'
        self.logs_directory = str(Path.home()) + '/robotervermessung-rosbag-recorder/data/ftp_data/'
        self.trajectories_directory = str(Path.home()) + '/robotervermessung-rosbag-recorder/data/random_trajectories/'
        self.pick_place_trajectories_directory = str(Path.home()) + '/robotervermessung-rosbag-recorder/data/random_pick_place_trajectories/'
        self.stop_recording_sub = self.create_subscription(Bool,'/stop_recording_signal',self.stop_recording_callback,10)
        self.publisher_process = self.create_publisher(Empty, '/activate_rosbag_processor', 10)
        self.pub_weight = self.create_publisher(Float64, '/socket_data/weight', 10)
        self.pub_velocity_picking = self.create_publisher(UInt16, '/socket_data/velocity_picking', 10)
        self.pub_velocity_handling = self.create_publisher(UInt16, '/socket_data/velocity_handling', 10)
        self.record_process = None
        self.record_pickplace_process=None
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.selected_height=305
        self.init_ui()
    def stop_recording_callback(self, msg):
        if msg.data and self.recording_pickplace_btn.cget('text') == 'Stop Recording Pickplace' :
            self.stop_recording_pickplace()  
        elif msg.data and self.calibration_btn.cget('text') == 'Stop Recording Calibration' :
            self.stop_recording_calib()
    def init_ui(self):
            self.window = tk.Tk()
            self.window.title('Robotervermessung')
            self.window.geometry('1000x500')  # Increased width to accommodate new frame
            self.window.attributes('-zoomed', True)  # Für Linux
            self.automatic_mode = tk.BooleanVar(value=False)
            self.style = ttk.Style()
            # Configure overall application style
            self.style.configure('.', font=('Ubuntu', 10))
            self.style.configure('TLabelFrame', font=('Ubuntu', 14, 'bold'), foreground='black')
            self.style.configure('TButton', font=('Ubuntu Light', 10), foreground='black', background='SkyBlue3', padding=5)
            self.style.map('TButton', foreground=[('active', '!disabled', 'black')],
                        background=[('active', 'SkyBlue2')])
            self.style.configure('TEntry', font=('Ubuntu Light', 10), foreground='black', padding=5)
            self.style.configure('Green.TCombobox', fieldbackground='lightgreen')
            self.style.configure('Red.TCombobox', fieldbackground='red')
            self.style.map('Green.TCombobox', fieldbackground=[('readonly', 'lightgreen')])
            self.style.map('Red.TCombobox', fieldbackground=[('readonly', 'red')])
            self.setup_left_frame()
            self.setup_right_frame()
            self.setup_middle_frame()  # New Pick_Place frame
           # self.window.mainloop()

    
    def setup_middle_frame(self):
        def update_weight(*args):
            selected = self.weight_dropdown.get()
            self.weight_var.set(weight_mapping[selected]['weight'])
            self.safety_distance_entry.delete(0, tk.END)
            self.safety_distance_entry.insert(0, str(weight_mapping[selected]['safety_distance']))
            self.selected_height = weight_mapping[selected]['height']
        Pick_Place_frame = ttk.Frame(self.window)
        Pick_Place_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=10, pady=10)
    
        Pick_Place_param_frame = ttk.LabelFrame(Pick_Place_frame, text='Random Pick & Place Trajectory Generation')
        Pick_Place_param_frame.pack(pady=10, padx=10, fill=tk.BOTH)
    
        # Configure grid columns to ensure alignment
        Pick_Place_param_frame.grid_columnconfigure(1, weight=1)
        
        # Configure consistent widths
        label_width = 20
        entry_width = 15
        row = 0
    
        # Velocity
        ttk.Label(Pick_Place_param_frame, text='Velocity (mm/s):', width=label_width, anchor='e').grid(row=row, column=0, padx=5, pady=5, sticky='e')
        self.velocity_entry = ttk.Entry(Pick_Place_param_frame, width=entry_width)
        self.velocity_entry.insert(0, '400')
        self.velocity_entry.grid(row=row, column=1, padx=5, pady=5, sticky='w')
        row += 1
    
        # Velocity Picking
        ttk.Label(Pick_Place_param_frame, text='Velocity Picking (mm/s):', width=label_width, anchor='e').grid(row=row, column=0, padx=5, pady=5, sticky='e')
        self.velocity_picking_entry = ttk.Entry(Pick_Place_param_frame, width=entry_width)
        self.velocity_picking_entry.insert(0, '400')
        self.velocity_picking_entry.grid(row=row, column=1, padx=5, pady=5, sticky='w')
        row += 1
    
        # Robot Movement
        ttk.Label(Pick_Place_param_frame, text='Robot Movement:', width=label_width, anchor='e').grid(row=row, column=0, padx=5, pady=5, sticky='e')
        self.movement_var = tk.StringVar()
        self.movement_dropdown = ttk.Combobox(Pick_Place_param_frame, 
                                            textvariable=self.movement_var,
                                            values=['MoveC','MoveL'],
                                            state='readonly',
                                            width=entry_width,
                                            style='Red.TCombobox')
        self.movement_dropdown.current(0)
        self.movement_dropdown.grid(row=row, column=1, padx=5, pady=5, sticky='w')
        row += 1
    
        # Cube Weight
        weight_mapping = {
            'Greifer + 0kg': {'weight': 13.5, 'safety_distance': 10, 'height': 305},
            'Greifer + 2.5kg': {'weight': 16, 'safety_distance': 80, 'height': 305},
            'Greifer + 5.1kg': {'weight': 18.6, 'safety_distance': 115, 'height': 305},
            'Greifer + 7.5kg': {'weight': 21, 'safety_distance': 145, 'height': 305},
            'Greifer + 10.kg': {'weight': 23.5, 'safety_distance': 180, 'height': 305},
            'Greifer + 15.2kg': {'weight': 28.7, 'safety_distance': 170, 'height': 275},
            'Greifer + 20.3kg': {'weight': 33.8, 'safety_distance': 210, 'height': 275}
            }
        ttk.Label(Pick_Place_param_frame, text='Cube Weight (kg):', width=label_width, anchor='e').grid(row=row, column=0, padx=5, pady=5, sticky='e')
        self.weight_var = tk.DoubleVar(value=13.5)
        self.weight_dropdown = ttk.Combobox(Pick_Place_param_frame,
            values=list(weight_mapping.keys()),
            state='readonly',
            width=entry_width,
            style='Green.TCombobox')
        self.weight_dropdown.current(0)
        self.weight_dropdown.grid(row=row, column=1, padx=5, pady=5, sticky='w')
        

        
        self.weight_dropdown.bind('<<ComboboxSelected>>', update_weight)
        row += 1
    
        # Handling Height
        ttk.Label(Pick_Place_param_frame, text='Handling Height (mm):', width=label_width, anchor='e').grid(row=row, column=0, padx=5, pady=5, sticky='e')
        self.handling_height_entry = ttk.Entry(Pick_Place_param_frame, width=entry_width)
        self.handling_height_entry.insert(0, '150')
        self.handling_height_entry.grid(row=row, column=1, padx=5, pady=5, sticky='w')
        row += 1
    
        # Gripping Height
        ttk.Label(Pick_Place_param_frame, text='Gripping Height (mm):', width=label_width, anchor='e').grid(row=row, column=0, padx=5, pady=5, sticky='e')
        self.gripping_height_entry = ttk.Entry(Pick_Place_param_frame, width=entry_width)
        self.gripping_height_entry.insert(0, '40')
        self.gripping_height_entry.grid(row=row, column=1, padx=5, pady=5, sticky='w')
        self.gripping_height_entry.bind('<FocusOut>', self.validate_gripping_height)
        row += 1
    
        # Safety Distance
        ttk.Label(Pick_Place_param_frame, text='Safety Distance (mm):', width=label_width, anchor='e').grid(row=row, column=0, padx=5, pady=5, sticky='e')
        self.safety_distance_entry = ttk.Entry(Pick_Place_param_frame, width=entry_width)
        selected = self.weight_dropdown.get()
        self.safety_distance_entry.insert(0, str(weight_mapping[selected]['safety_distance']))
        self.safety_distance_entry.grid(row=row, column=1, padx=5, pady=5, sticky='w')
        row += 1
    
        # Reorientation Z
        ttk.Label(Pick_Place_param_frame, text='Reorientation Z:', width=label_width, anchor='e').grid(row=row, column=0, padx=5, pady=5, sticky='e')
        self.reorientation_z_entry = ttk.Entry(Pick_Place_param_frame, width=entry_width)
        self.reorientation_z_entry.insert(0, '60')
        self.reorientation_z_entry.grid(row=row, column=1, padx=5, pady=5, sticky='w')
        row += 1
    
        # Min Distance
        ttk.Label(Pick_Place_param_frame, text='Min. Distance (mm):', width=label_width, anchor='e').grid(row=row, column=0, padx=5, pady=5, sticky='e')
        self.min_distance_entry = ttk.Entry(Pick_Place_param_frame, width=entry_width)
        self.min_distance_entry.insert(0, '10')
        self.min_distance_entry.grid(row=row, column=1, padx=5, pady=5, sticky='w')
        row += 1
    
        # Pick & Place Trajectories
        ttk.Label(Pick_Place_param_frame, text='Pick & Place Trajectories:', width=label_width, anchor='e').grid(row=row, column=0, padx=5, pady=5, sticky='e')
        self.pick_place_entry = ttk.Entry(Pick_Place_param_frame, width=entry_width)
        self.pick_place_entry.insert(0, '100')
        self.pick_place_entry.grid(row=row, column=1, padx=5, pady=5, sticky='w')
        row += 1
    
        # Iterations
        ttk.Label(Pick_Place_param_frame, text='Iterations:', width=label_width, anchor='e').grid(row=row, column=0, padx=5, pady=5, sticky='e')
        self.iterations_entry = ttk.Entry(Pick_Place_param_frame, width=entry_width)
        self.iterations_entry.insert(0, '3')
        self.iterations_entry.grid(row=row, column=1, padx=5, pady=5, sticky='w')
        row += 1
    
        # Buttons
        button_frame = ttk.Frame(Pick_Place_param_frame)
        button_frame.grid(row=row, column=0, columnspan=2, pady=15)
        
        ttk.Button(button_frame, text='Generate Pick & Place Trajectory', 
                  command=self.generate_pick_place_trajectory, 
                  width=30).pack(pady=5)
        
        ttk.Button(button_frame, text='Send Pick & Place Trajectory', 
                  command=self.send_Pick_Place_trajectory, 
                  width=30).pack(pady=5)
        ttk.Button(button_frame, text='Gripper Down',
                  command=self.Gripper_Down,
                  width=30).pack(pady=5)
                  
        ttk.Button(button_frame, text='Gripper Up',
                  command=self.Gripper_Up,
                  width=30).pack(pady=5)
        
        style = ttk.Style()
        style.configure('Green.TEntry', fieldbackground='lightgreen')
        style.configure('Red.TEntry', fieldbackground='red')
        style.configure('Green.TCombobox', fieldbackground='lightgreen')
        style.configure('Red.TCombobox', fieldbackground='red')

        self.velocity_entry.configure(style='Green.TEntry')
        self.handling_height_entry.configure(style='Green.TEntry')
        self.weight_dropdown.configure(style='Green.TCombobox')
        self.velocity_picking_entry.configure(style='Red.TEntry')
        self.movement_dropdown.configure(style='Red.TCombobox')
        self.gripping_height_entry.configure(style='Red.TEntry')
        self.safety_distance_entry.configure(style='Red.TEntry')
        self.reorientation_z_entry.configure(style='Red.TEntry')
        self.min_distance_entry.configure(style='Red.TEntry')
        self.pick_place_entry.configure(style='Red.TEntry')
        self.iterations_entry.configure(style='Red.TEntry')

    def setup_left_frame(self):
        left_frame = ttk.Frame(self.window)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10, pady=10)

        # PC Connection Section
        pc_frame = ttk.LabelFrame(left_frame, text='PC Connection')
        pc_frame.pack(pady=10, padx=10, fill=tk.BOTH)

        self.pc_ip_entry = ttk.Entry(pc_frame, width=25, style='TEntry')  
        self.pc_ip_entry.insert(0, '10.150.136.211') 
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
        self.mocap_ip_entry.insert(0, '134.147.229.179')
        self.mocap_ip_entry.grid(row=0, column=1, padx=5, pady=5)
        ttk.Label(mocap_frame, text='MoCap IP:').grid(row=0, column=0, padx=5, pady=5, sticky='e')

        self.mocap_port_entry = ttk.Entry(mocap_frame, width=15, style='TEntry')  
        self.mocap_port_entry.insert(0, '3883')
        self.mocap_port_entry.grid(row=1, column=1, padx=5, pady=5)
        ttk.Label(mocap_frame, text='Port:').grid(row=1, column=0, padx=5, pady=5, sticky='e')

        ttk.Button(mocap_frame, text='Connect MoCap', command=self.connect_mocap, style='TButton').grid(row=2, columnspan=2, pady=5)

        # Create a frame to hold both record sections
        records_container = ttk.Frame(left_frame)
        records_container.pack(fill=tk.BOTH, expand=True)

        # Original Recording Section
        record_frame = ttk.LabelFrame(records_container, text="Record Data")
        record_frame.pack(pady=10, padx=10, fill=tk.BOTH)

        self.recording_btn = ttk.Button(record_frame, text='Start Recording', command=self.start_stop_record, style='TButton')
        self.recording_btn.pack(pady=5)

        ttk.Button(record_frame, text='Generate CSV from ROSBAG', command=self.activate_rosbag_processor).pack(pady=10)

        # Pick Place Recording Section - Now packed below the original recording section
        record_pickplace_frame = ttk.LabelFrame(records_container, text="Record Data Pickplace")
        record_pickplace_frame.pack(pady=10, padx=10, fill=tk.BOTH)

        self.recording_pickplace_btn = ttk.Button(record_pickplace_frame, text='Start Recording Pickplace', 
                                                command=self.start_stop_record_pickplace, style='TButton')
        self.recording_pickplace_btn.pack(pady=5)
        self.calibration_btn = ttk.Button(record_pickplace_frame, text='Start Calibration Run', 
                                 command=self.calibration_run, style='TButton')
        self.calibration_btn.pack(pady=5)

        ttk.Button(record_pickplace_frame, text='Generate CSV from ROSBAG Pickplace', 
                command=self.activate_rosbag_processor_pickplace).pack(pady=10)
        ttk.Checkbutton(record_pickplace_frame, text='Automatic Mode', 
                    variable=self.automatic_mode, 
                    command=self.on_automatic_mode_change).pack(pady=5)
                    
        self.iterations_frame = ttk.Frame(record_pickplace_frame)
        self.iterations_frame.pack(pady=5)
        ttk.Label(self.iterations_frame, text='Iterations:').pack(side=LEFT, padx=5)
        self.automatic_iteration_entry = ttk.Entry(self.iterations_frame, width=10)
        self.automatic_iteration_entry.pack(side=LEFT)
        self.iterations_frame.pack_forget() 
        self.count_frame = ttk.Frame(record_pickplace_frame)
        ttk.Label(self.count_frame, text='Count:').pack(side=LEFT, padx=5)
        self.count_entry = ttk.Entry(self.count_frame, width=10)
        self.count_entry.pack(side=LEFT)

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
    def validate_gripping_height(self, event=None):
        """Validate the gripping height value"""
        try:
            gripping_height = float(self.gripping_height_entry.get())
            if gripping_height < 40:
                messagebox.showerror("Invalid Input", "Gripping height must be at least 40mm!")
                self.gripping_height_entry.delete(0, tk.END)
                self.gripping_height_entry.insert(0, "40")
                return False
            return True
        except ValueError:
            messagebox.showerror("Invalid Input", "Please enter a valid number for gripping height!")
            self.gripping_height_entry.delete(0, tk.END)
            self.gripping_height_entry.insert(0, "40")
            return False
    def start_stop_record_pickplace(self):
        if self.record_pickplace_process is None:
            self.trajectory_pickplace = simpledialog.askstring("Input", "Enter pickplace trajectory description:")
            if self.trajectory_pickplace is None:
                self.status_label.config(text='Pickplace recording canceled.')
                return

            self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            bag_file = Path(self.bags_directory) / f'record_{self.timestamp}_pickplace_{str(self.velocity_entry.get())}v_{str(int(round(self.weight_var.get() - 13.5, 1)))}kg_{str((self.handling_height_entry.get()))}mm_{self.trajectory_pickplace}'
            
            self.record_pickplace_process = subprocess.Popen(['ros2', 'bag', 'record', '-o', str(bag_file),
                                                    '/vrpn_mocap/abb4400_tcp/pose', '/vrpn_mocap/abb4400_tcp/twist',
                                                    '/vrpn_mocap/abb4400_tcp/accel',
                                                    'socket_data/position', 'socket_data/orientation',
                                                    'socket_data/tcp_speed', 'socket_data/joint_states',
                                                    'socket_data/achieved_position','socket_data/do_value','socket_data/weight','socket_data/movement_type','socket_data/velocity_picking','/socket_data/velocity_handling','/imu'])

            self.recording_pickplace_btn.config(text='Stop Recording Pickplace', style='Green.TButton')
            self.status_label.config(text=f'Pickplace recording started: {bag_file}')
            self.get_logger().info(f'Pickplace recording started: {bag_file}')
            time.sleep(1.5)
            msg = Float64()
            msg.data = float(self.weight_var.get())
            self.pub_weight.publish(msg)
            msg = UInt16()
            msg.data = int(self.velocity_picking_entry.get())
            self.pub_velocity_picking.publish(msg)
            msg = UInt16()
            msg.data = int(self.velocity_entry.get())
            self.pub_velocity_handling.publish(msg)
            #input_time = simpledialog.askfloat("Input", "Enter pickplace recording duration (in seconds):")
            #timer = threading.Timer(float(input_time), self.stop_recording_pickplace)
            #timer.start()
            #stop_recording=False
            changeFTPValue("1")#Startbedingung für roboterbewegung setzten
            
            #stop recording aufrufen wenn ende der pick place anwendung mittelslisten to node       
        else:
            self.stop_recording_pickplace()

    def stop_recording_pickplace(self):
        if self.record_pickplace_process:
            self.record_pickplace_process.terminate()
            self.record_pickplace_process.wait()
            self.record_pickplace_process = None
            self.recording_pickplace_btn.config(text='Start Recording Pickplace', style='Red.TButton')
            self.status_label.config(text='Pickplace recording stopped.')
            changeFTPValue("0")
            # stoppen der roboterbewgung
            getFTPTestFile(self.logs_directory)
            my_file = Path(self.logs_directory) / "ProgramExecution"
            if my_file.is_file():
                renamed_file = f'record_{self.timestamp}_pickplace_{str(self.velocity_entry.get())}v_{str(int(round(self.weight_var.get() - 13.5, 1)))}kg_{str((self.handling_height_entry.get()))}mm_{self.trajectory_pickplace}_rapid_log'
                os.rename(my_file, os.path.join(self.logs_directory, renamed_file))
                self.get_logger().info(f'Renamed and saved pickplace log: {renamed_file}')
            self.get_logger().info('Pickplace recording stopped.')
    def calibration_run(self):
        num_trajectories = 1  # Adjust as needed
        for i in range(num_trajectories):
            filename = f'random_trajectory_{i + 1}.mod'
            calibration_movement(filename, self.pick_place_trajectories_directory)
        self.status_label.config(text='Calibration Trajectory generated')
        sendRandomTrajectoriesFTP(self.pick_place_trajectories_directory)
        self.status_label.config(text='Calibration Trajectory sent.')
        if self.record_pickplace_process is None:
            self.trajectory_pickplace = simpledialog.askstring("Input", "Enter Calibration trajectory description:")
            if self.trajectory_pickplace is None:
                self.status_label.config(text='Calibration recording canceled.')
                return

            self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            bag_file = Path(self.bags_directory) / f'record_{self.timestamp}_pickplace_calibration_run_{self.trajectory_pickplace}'
            
            self.record_pickplace_process = subprocess.Popen(['ros2', 'bag', 'record', '-o', str(bag_file),
                                                    '/vrpn_mocap/abb4400_tcp/pose', '/vrpn_mocap/abb4400_tcp/twist',
                                                    '/vrpn_mocap/abb4400_tcp/accel',
                                                    'socket_data/position', 'socket_data/orientation',
                                                    'socket_data/tcp_speed', 'socket_data/joint_states',
                                                    'socket_data/achieved_position','socket_data/do_value','socket_data/weight','socket_data/movement_type','socket_data/velocity_picking','/socket_data/velocity_handling','/imu'])

            self.calibration_btn.config(text='Stop Recording Calibration', style='Green.TButton')
            self.status_label.config(text=f'Calibration recording started: {bag_file}')
            self.get_logger().info(f'Calibration recording started: {bag_file}')
            time.sleep(1.5)
            msg = Float64()
            msg.data = float(0)
            self.pub_weight.publish(msg)
            #input_time = simpledialog.askfloat("Input", "Enter pickplace recording duration (in seconds):")
            #timer = threading.Timer(float(input_time), self.stop_recording_pickplace)
            #timer.start()
            #stop_recording=False
            changeFTPValue("1")#Startbedingung für roboterbewegung setzten
            
            #stop recording aufrufen wenn ende der pick place anwendung mittelslisten to node       
        else:
            self.stop_recording_calib()

    def stop_recording_calib(self):
        if self.record_pickplace_process:
            self.record_pickplace_process.terminate()
            self.record_pickplace_process.wait()
            self.record_pickplace_process = None
            self.calibration_btn.config(text='Start Calibration Run', style='Red.TButton')
            self.status_label.config(text='Calibration recording stopped.')
            changeFTPValue("0")
            # stoppen der roboterbewgung
            getFTPTestFile(self.logs_directory)
            my_file = Path(self.logs_directory) / "ProgramExecution"
            if my_file.is_file():
                renamed_file = f'record_{self.timestamp}_pickplace_calibration_run_{self.trajectory_pickplace}_rapid_log'
                os.rename(my_file, os.path.join(self.logs_directory, renamed_file))
                self.get_logger().info(f'Renamed and saved pickplace calibration log: {renamed_file}')
            self.get_logger().info('Calibration recording stopped.')
    def Gripper_Down(self):
        num_trajectories = 1  # Adjust as needed
        for i in range(num_trajectories):
            filename = f'random_trajectory_{i + 1}.mod'
            gripper_down(filename, self.pick_place_trajectories_directory)
        
        sendRandomTrajectoriesFTP(self.pick_place_trajectories_directory)
        changeFTPValue("1")
        time.sleep(3)
        changeFTPValue("0")
        self.status_label.config(text='Gripper Down')
    def Gripper_Up(self):
        num_trajectories = 1  # Adjust as needed
        for i in range(num_trajectories):
            filename = f'random_trajectory_{i + 1}.mod'
            gripper_up(filename, self.pick_place_trajectories_directory)
        self.status_label.config(text='Gripper Up')
        sendRandomTrajectoriesFTP(self.pick_place_trajectories_directory)
        changeFTPValue("1")
        time.sleep(3)
        changeFTPValue("0")
        self.status_label.config(text='Gripper Up')
    def activate_rosbag_processor_pickplace(self):
        rosbag_processor = RosbagProcessor()  # You might want to add specific processing for pickplace data
        self.status_label.config(text='Pickplace processing completed.')
        self.get_logger().info('Pickplace processing completed.')

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

    def generate_pick_place_trajectory(self):
        try:
            velocity = int(self.velocity_entry.get())
            velocity_picking = int(self.velocity_picking_entry.get())
            movement_type = self.movement_var.get()
            reorientation_z = float(self.reorientation_z_entry.get())
            min_distance = float(self.min_distance_entry.get())
            pick_place_traj = int(self.pick_place_entry.get())
            iterations = int(self.iterations_entry.get())
            selected = self.weight_dropdown.get()
            if round(self.weight_var.get() - 13.5, 1)==0.0:
                weight = f"cube := load0;\n"
            else:
                weight = f"cube := [{round(self.weight_var.get() - 13.5, 1)}, [0, 0, {self.selected_height}], [1, 0, 0, 0], 1, 0, 0];\n"
            handling_height=int(self.handling_height_entry.get())
            gripping_height=int(self.gripping_height_entry.get())
            safety_distance_edge=int(self.safety_distance_entry.get())
            if reorientation_z > 60:
                self.status_label.config(text='Reorientation Z angle must be <= 60.')
                return
            
            num_trajectories = 1  # Adjust as needed
            for i in range(num_trajectories):
                filename = f'random_trajectory_{i + 1}.mod'
                generate_pick_place_trajectory_and_save(filename, self.pick_place_trajectories_directory, velocity, velocity_picking, movement_type, reorientation_z, min_distance, pick_place_traj, iterations,weight,handling_height,gripping_height,safety_distance_edge)
            self.status_label.config(text='Pick_Place trajectory generated.')

            
        except ValueError:
            messagebox.showerror("Invalid Input", "Please enter valid numbers for all fields.")
            self.status_label.config(text='Invalid input values.')

    def send_Pick_Place_trajectory(self):
        # Implement the logic to send the Pick_Place trajectory
        # This could be similar to sendRandomTrajectoriesFTP but for Pick_Place trajectories
        sendRandomTrajectoriesFTP(self.pick_place_trajectories_directory)
        self.status_label.config(text='Pick Place trajectory sent.')

#def main(args=None):
#    rclpy.init(args=args)
#    node = RosbagGUI()
#    rclpy.spin(node)
#    rclpy.shutdown()





    def on_automatic_mode_change(self):
        if self.automatic_mode.get():
            self.iterations_frame.pack(pady=5)
        else:
            self.iterations_frame.pack_forget()
def main(args=None):
    rclpy.init(args=args)
    node = RosbagGUI()

    # ROS-Spin läuft in separatem Thread
    ros_thread = threading.Thread(target=lambda: rclpy.spin(node))
    ros_thread.daemon = True
    ros_thread.start()

    # GUI mainloop läuft im Hauptthread
    node.window.mainloop()

    # Nach Schließen des Fensters
    rclpy.shutdown()

if __name__ == '__main__':
    main()

