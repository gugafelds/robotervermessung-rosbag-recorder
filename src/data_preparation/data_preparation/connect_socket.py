import socket
from datetime import datetime
import rclpy
import threading
from rclpy.node import Node
from geometry_msgs.msg import Point, Pose, Quaternion
from std_msgs.msg import Float64, String, Float64MultiArray,Bool,UInt8, UInt16
from sensor_msgs.msg import JointState
import re

class ConnectSocket(Node):
    def __init__(self, HOST, PORT):
        super().__init__('connect_socket')
        self.pub_position = self.create_publisher(Point, '/socket_data/position', 10)
        self.pub_orientation = self.create_publisher(Quaternion, '/socket_data/orientation', 10)
        self.pub_tcp_speed = self.create_publisher(Float64, '/socket_data/tcp_speed', 10)
        self.pub_joint_states = self.create_publisher(JointState, '/socket_data/joint_states', 10)
        self.pub_stop_recording = self.create_publisher(Bool, '/stop_recording_signal', 10)
        #self.pub_weight = self.create_publisher(Float64, '/socket_data/weight', 10)
        #self.pub_velocity_picking = self.create_publisher(UInt16, '/socket_data/velocity_picking', 10)
        #self.pub_velocity_handling = self.create_publisher(UInt16, '/socket_data/velocity_handling', 10)
        self.pub_do = self.create_publisher(UInt8, '/socket_data/do_value', 10)
        self.pub_movement_type = self.create_publisher(String, '/socket_data/movement_type', 10)
        self.pub_acceleration_pi=self.create_publisher(Float64, '/socket_data/rpiacc', 10)
        self.string_publisher = self.create_publisher(String, '/socket_data', 10)
        self.pub_achieved_position = self.create_publisher(Pose, '/socket_data/achieved_position', 10)
        self.start_time = datetime.now()
        self.current_time = datetime.now()
        self.received = 0
        self.data_received_per_second = []
 
        # Create a TCP/IP socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

                # Bind the socket to the address and port
        self.server_socket.bind((HOST, PORT))
 
        # Set the server in listening mode
        self.server_socket.listen()
 
        self.get_logger().info(f"Server is listening on {HOST}:{PORT}")

    def handle_client(self, client_socket, client_address):
        """Handle a single client connection"""
        try:
            self.get_logger().info(f"Connection accepted from {client_address}")
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break
                self.received += 1
                self.publish_data(data.decode())
                
                time = datetime.now()
                if time.second != self.current_time.second:
                    self.data_received_per_second.append(self.received)
                    self.get_logger().info(f"Between {self.current_time} and {time} received: {self.received}")
                    self.current_time = time
                    self.received = 0
        except Exception as e:
            self.get_logger().error(f"Error handling client {client_address}: {e}")
        finally:
            client_socket.close()
            self.get_logger().info(f"Connection to {client_address} closed")
    def run_server(self):
        """Run server and accept multiple clients"""
        self.active_clients = []

        while True:
            try:
                # Accept an incoming connection
                client_socket, client_address = self.server_socket.accept()

                # Create new thread for client
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, client_address)
                )

                # Start thread and add to active clients
                client_thread.start()
                self.active_clients.append({
                    'thread': client_thread,
                    'socket': client_socket,
                    'address': client_address
                })

                # Clean up disconnected clients
                self.active_clients = [
                    client for client in self.active_clients 
                    if client['thread'].is_alive()
                ]

                self.get_logger().info(f"Active clients: {len(self.active_clients)}")

            except Exception as e:
                self.get_logger().error(f"Error accepting connection: {e}")
    def publish_data(self, data):
        

        # Publish raw string data
        string_msg = String()
        string_msg.data = data
        self.string_publisher.publish(string_msg)
        if data.startswith(';rpi_acc'):
            msg= Float64()
            #msg.data=msg.data = float(data[7:])
            msg.data = float(data[8:data.find(';', 7)])
            self.pub_acceleration_pi.publish(msg)
        if data.startswith('DO'):
            msg = UInt8()
            msg.data = int(data[2])
            self.pub_do.publish(msg)
        if data.startswith('END'):
            # Send stop signal
            stop_msg = Bool()
            stop_msg.data = True
            self.pub_stop_recording.publish(stop_msg)
        #if data.startswith('Weight'):
        #    msg = Float64()
        #    parts= data.split(';')
        #    msg.data = float(parts[0][6:])
        #    self.pub_weight.publish(msg)
        #if data.startswith('VelocityPicking'):
        #    # Sucht nach Zahlen zwischen [ und dem ersten Komma
        #    match = re.search(r'\[(\d+),', data)
        #    if match:
        #        msg = UInt16()
        #        msg.data = int(match.group(1))
        #        self.pub_velocity_picking.publish(msg)
        #if data.startswith('VelocityHandling'):
        #    # Sucht nach Zahlen zwischen [ und dem ersten Komma
        #    match = re.search(r'\[(\d+),', data)
        #    if match:
        #        msg = UInt16()
        #        msg.data = int(match.group(1))
        #        self.pub_velocity_handling.publish(msg)
        if data.startswith('px'):
            position = Point()
            for part in data.split(';'):
                try:
                    if part.startswith('px'):
                        position.x = float(part[2:])
                    elif part.startswith('py'):
                        position.y = float(part[2:])
                    elif part.startswith('pz'):
                        position.z = float(part[2:])
                except ValueError as e:
                    self.get_logger().error(f'Error converting position value: {e}')
            self.pub_position.publish(position)

        elif data.startswith("linear") or data.startswith("circular"):
            msg = String()
            parts = data.split(';')
            msg.data = parts[0]
            self.pub_movement_type.publish(msg)

        elif data.startswith('q'):
            orientation = Quaternion()
            for part in data.split(';'):
                try:
                    if part.startswith('q1'):
                        orientation.w = float(part[2:])
                    elif part.startswith('q2'):
                        orientation.x = float(part[2:])
                    elif part.startswith('q3'):
                        orientation.y = float(part[2:])
                    elif part.startswith('q4'):
                        orientation.z = float(part[2:])
                except ValueError as e:
                    self.get_logger().error(f'Error converting orientation value: {e}')
            self.pub_orientation.publish(orientation)

        elif data.startswith('sp'):
            tcp_speed = Float64()
            for part in data.split(';'):
                try:
                    if part.startswith('sp'):
                        tcp_speed.data = float(part[2:])
                except ValueError as e:
                    self.get_logger().error(f'Error converting speed value: {e}')
            self.pub_tcp_speed.publish(tcp_speed)

        if data.startswith('j'):
            joint_state_msg = JointState()
            joint_state_msg.header.stamp = self.get_clock().now().to_msg()
            joint_state_msg.name = [f'j{i+1}' for i in range(6)]
            joint_state_msg.position = []
            
            for part in data.split(';'):
                try:
                    if part.startswith('j'):
                        joint_state_msg.position.append(float(part[2:]))
                except ValueError as e:
                    self.get_logger().error(f'Error converting joint value: {e}')
            self.pub_joint_states.publish(joint_state_msg)

        if data.startswith('apx'):
            achieved_position = Pose()
            for part in data.split(';'):
                try:
                    if part.startswith('apx'):
                        achieved_position.position.x = float(part[3:])
                    elif part.startswith('apy'):
                        achieved_position.position.y = float(part[3:])
                    elif part.startswith('apz'):
                        achieved_position.position.z = float(part[3:])
                    elif part.startswith('aqw'):
                        achieved_position.orientation.w = float(part[3:])
                    elif part.startswith('aqx'):
                        achieved_position.orientation.x = float(part[3:])
                    elif part.startswith('aqy'):
                        achieved_position.orientation.y = float(part[3:])
                    elif part.startswith('aqz'):
                        achieved_position.orientation.z = float(part[3:])
                except ValueError as e:
                    self.get_logger().error(f'Error converting achieved position value: {e}')
            self.pub_achieved_position.publish(achieved_position)

    def send_message(self, message):
        self.server_socket.sendall(message.encode('utf-8'))
 
def main(args=None):
    rclpy.init(args=args)
    node = ConnectSocket()
    try:
        node.run_server()
    except KeyboardInterrupt:
        node.get_logger().info("Shutting down server.")
    rclpy.shutdown()
 
if __name__ == '__main__':
    main()