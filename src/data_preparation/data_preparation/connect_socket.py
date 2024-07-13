import socket
from datetime import datetime
import rclpy
from rclpy.node import Node
from geometry_msgs.msg import Point, Pose, Quaternion
from std_msgs.msg import Float64, String, Float64MultiArray
from sensor_msgs.msg import JointState
 
class ConnectSocket(Node):
    def __init__(self, HOST, PORT):
        super().__init__('connect_socket')
        self.pub_position = self.create_publisher(Point, '/socket_data/position', 10)
        self.pub_orientation = self.create_publisher(Quaternion, '/socket_data/orientation', 10)
        self.pub_tcp_speed = self.create_publisher(Float64, '/socket_data/tcp_speed', 10)
        self.pub_joint_states = self.create_publisher(JointState, '/socket_data/joint_states', 10)
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


    def run_server(self):
        while True:
            # Accept an incoming connection
            client_socket, client_address = self.server_socket.accept()
            with client_socket:
                self.get_logger().info(f"Connection accepted from {client_address}")
 
                # Receive data from the client
                while True:
                    data = client_socket.recv(1024)  # Maximum amount of data to receive is 1024 bytes
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
 
                self.get_logger().info(f"Connection to {client_address} closed")
 
    def publish_data(self, data):
        # Publish raw string data
        string_msg = String()
        string_msg.data = data
        self.string_publisher.publish(string_msg)

        if data.startswith('px'):
            position = Point()
            for part in data.split(';'):
                if part.startswith('px'):
                    position.x = float(part[2:])
                if part.startswith('py'):
                    position.y = float(part[2:])
                if part.startswith('pz'):
                    position.z = float(part[2:])
            self.pub_position.publish(position)
        
        elif data.startswith('q'):
            orientation = Quaternion()
            for part in data.split(';'):
                if part.startswith('q1'):
                    orientation.w = float(part[2:])
                elif part.startswith('q2'):
                    orientation.x = float(part[2:])
                elif part.startswith('q3'):
                    orientation.y = float(part[2:])
                elif part.startswith('q4'):
                    orientation.z = float(part[2:])
            self.pub_orientation.publish(orientation)

        elif data.startswith('sp'):
            tcp_speed = Float64()
            for part in data.split(';'):
                if part.startswith('sp'):
                    tcp_speed.data = float(part[2:])
            self.pub_tcp_speed.publish(tcp_speed)

        if data.startswith('j'):
            # Initialize JointState message
            joint_state_msg = JointState()
            joint_state_msg.header.stamp = self.get_clock().now().to_msg()

            # Assuming joint names j1 to j6
            joint_state_msg.name = [f'j{i+1}' for i in range(6)]
            joint_state_msg.position = []

            for part in data.split(';'):
                if part.startswith('j'):
                    joint_state_msg.position.append(float(part[2:]))

            self.pub_joint_states.publish(joint_state_msg)
        
        if data.startswith('apx'):
            achieved_position = Pose()
            for part in data.split(';'):
                if part.startswith('apx'):
                    achieved_position.position.x = float(part[3:])
                if part.startswith('apy'):
                    achieved_position.position.y = float(part[3:])
                if part.startswith('apz'):
                    achieved_position.position.z = float(part[3:])
                if part.startswith('aqw'):
                    achieved_position.orientation.w = float(part[4:])
                if part.startswith('aqx'):
                    achieved_position.orientation.x = float(part[4:])
                if part.startswith('aqy'):
                    achieved_position.orientation.y = float(part[4:])
                if part.startswith('aqz'):
                    achieved_position.orientation.z = float(part[4:])
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