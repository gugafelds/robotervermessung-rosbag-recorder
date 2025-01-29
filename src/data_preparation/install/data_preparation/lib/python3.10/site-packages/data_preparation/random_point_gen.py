import numpy as np
from scipy.spatial.transform import Rotation as R
import random

# Function to create the rotation matrix for the angles around the X, Y and Z axes
def rotation_matrix(rx, ry, rz):
    Rx = np.array([[1, 0, 0],
                   [0, np.cos(rx), -np.sin(rx)],
                   [0, np.sin(rx), np.cos(rx)]])
    
    Ry = np.array([[np.cos(ry), 0, np.sin(ry)],
                   [0, 1, 0],
                   [-np.sin(ry), 0, np.cos(ry)]])
    
    Rz = np.array([[np.cos(rz), -np.sin(rz), 0],
                   [np.sin(rz), np.cos(rz), 0],
                   [0, 0, 1]])
    
    return np.dot(np.dot(Rz, Ry), Rx)

# Set the origin and orientation "Home"
home_position = np.array([750.0, -300.0, 1730.0])
home_orientation_quaternion = [0.0642142, 0.60757, 0.386589, 0.690857]
home_rotation = R.from_quat(home_orientation_quaternion).as_matrix()

# Step 1: Generate a grid of points inside the cube with a resolution defined as follows
resolution = 20  # mm
x = np.arange(0, 630 + resolution, resolution)
y = np.arange(0, 630 + resolution, resolution)
z = np.arange(0, 630 + resolution, resolution)
X, Y, Z = np.meshgrid(x, y, z)
points = np.vstack([X.ravel(), Y.ravel(), Z.ravel()]).T
print(y)
print(points)
# Translate the origin of the cube to align (0, 630, 630) with "Home"
translation = home_position - np.array([0, 630, 630])
points = points + translation

# Function to convert the rotation matrix into a quaternion
def rotation_to_quaternion(rotation_matrix):
    return R.from_matrix(rotation_matrix).as_quat()

# Function to generate a trajectory and save it in a MOD file
def generate_trajectory_and_save(filename, directory, reorientation_xy, reorientation_z, number_points, min_distance):
    filename = directory + filename
    trajectory_length = number_points
    selected_points = []
    np.random.shuffle(points)  # Shuffle the points for random distribution
    for point in points:
        if len(selected_points) < trajectory_length:
            if is_far_enough(point, np.array(selected_points), min_distance):
                selected_points.append(point)
        else:
            break

    selected_points = np.array(selected_points)

    # Step 2: Make sure we have exactly trajectory_length points
    if len(selected_points) > trajectory_length:
        selected_points = selected_points[:trajectory_length]
    elif len(selected_points) < trajectory_length:
        print("Warning: Not enough points could be found with the specified minimum distance.")

    # Open the file to write the trajectory points
    with open(filename, 'w') as txtfile:
        txtfile.write("MODULE RandomPointsDefinition\n")
        for i, point in enumerate(selected_points):
            rx = np.deg2rad(random.uniform(-reorientation_xy, reorientation_xy))
            ry = np.deg2rad(random.uniform(-reorientation_xy, reorientation_xy))
            rz = np.deg2rad(random.uniform(-reorientation_z, reorientation_z))
            R_local = rotation_matrix(rx, ry, rz)
            R_tool = home_rotation @ R_local  # Apply the "Home" orientation rotation to the local orientation
            quaternion = rotation_to_quaternion(R_tool)
            
            # Writing in RAPID ABB format
            txtfile.write(f'PERS robtarget p{i+1}:=[[{point[0]},{point[1]},{point[2]}],'
                          f'[{quaternion[0]},{quaternion[1]},{quaternion[2]},{quaternion[3]}],'
                          f'[-1,0,0,0],[9E+09,9E+09,9E+09,9E+09,9E+09,9E+09]];\n')
        
        txtfile.write("PROC run_random_points0()\n")
        
        for i, point in enumerate(selected_points):
            txtfile.write(f"reorient_target := p{i+1};\n")
            txtfile.write("move_to_reorient;\n")

        txtfile.write("ENDPROC\n")

        txtfile.write("ENDMODULE")

    print(f'Random trajectory generated and saved in {filename}')

# Function to check the minimum distance between points
def is_far_enough(new_point, selected_points, min_distance):
    if len(selected_points) == 0:
        return True
    distances = np.linalg.norm(selected_points - new_point, axis=1)
    return np.all(distances >= min_distance)

