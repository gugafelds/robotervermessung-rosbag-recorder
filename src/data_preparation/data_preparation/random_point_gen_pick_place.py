import numpy as np
from scipy.spatial.transform import Rotation as R
import random
from pathlib import Path

def euler_to_quaternion(x_deg, y_deg, z_deg):
    # Convert degrees to radians
    x = np.radians(x_deg)
    y = np.radians(y_deg)
    z = np.radians(z_deg)
    
    # Compute cos and sin of half angles
    cx = np.cos(x/2)
    sx = np.sin(x/2)
    cy = np.cos(y/2)
    sy = np.sin(y/2)
    cz = np.cos(z/2)
    sz = np.sin(z/2)
    
    # Compute quaternion components
    w = round(cx*cy*cz + sx*sy*sz, 8)
    qx = round(sx*cy*cz - cx*sy*sz, 8)
    qy = round(cx*sy*cz + sx*cy*sz, 8)
    qz = round(cx*cy*sz - sx*sy*cz, 8)
    
    return [w, qx, qy, qz]
def calculate_support_point(point1, point2):
    """
    Berechnet einen Stützpunkt zwischen zwei Punkten für einen Halbkreis.
    Wählt den Stützpunkt, der näher am Ursprung (0,0) liegt.
    
    Parameters:
    point1: np.array - Erster Punkt [x, y, z]
    point2: np.array - Zweiter Punkt [x, y, z]
    
    Returns:
    np.array - Stützpunkt [x, y, z], der näher am Ursprung liegt
    """
    # Mittelpunkt zwischen den beiden Punkten berechnen
    center = (point1 + point2) / 2
    
    # Vektor von point1 zu point2
    direction = point2 - point1
    
    # Länge des Vektors berechnen
    length = np.linalg.norm(direction[:2])  # Nur x und y berücksichtigen
    
    # Normalenvektor berechnen (um 90° gedreht in der xy-Ebene)
    normal = np.array([-direction[1], direction[0], 0])
    
    # Wenn Länge 0, direkt Mittelpunkt zurückgeben
    if length == 0:
        return center
    
    # Normalisieren und mit halber Länge multiplizieren
    normal = normal / np.linalg.norm(normal) * (length / 2)
    
    # Beide möglichen Stützpunkte berechnen
    support_point1 = center + normal
    support_point2 = center - normal
    
    # Z-Koordinate für beide Punkte beibehalten
    support_point1[2] = point1[2]
    support_point2[2] = point1[2]
    
    # Distanz zum Ursprung (0,0) für beide Punkte berechnen
    # Nur x und y Koordinaten berücksichtigen
    dist1 = np.linalg.norm(support_point1[:2])
    dist2 = np.linalg.norm(support_point2[:2])
    
    # Den Punkt zurückgeben, der näher am Ursprung liegt
    return support_point1 if dist1 < dist2 else support_point2

def transform_points_grid(points, origin_table, rotation_angle):
    """
    Transformiert alle Punkte aus einem numpy array von Punkten
    
    Parameters:
    points: np.array([[x1,y1,z1], [x2,y2,z2], ...]) - Array von Punkten
    origin_table: tuple (x, y) - Ursprung von KS2 in KS1
    rotation_angle: float - Rotationswinkel in Grad
    
    Returns:
    np.array - Transformierte Punkte

    """
    # Vektorisierte Version der Transformation
    theta = np.radians(rotation_angle)
    dx, dy = origin_table
    
    # Verschiebung
    points_shifted = points.copy()
    points_shifted[:, 0] += dx
    points_shifted[:, 1] += dy
    
    # Relative Position zum Drehpunkt
    x_rel = points_shifted[:, 0] - origin_table[0]
    y_rel = points_shifted[:, 1] - origin_table[1]
    
    # Rotation und finale Position
    transformed_points = points.copy()
    transformed_points[:, 0] = x_rel * np.cos(theta) - y_rel * np.sin(theta) + origin_table[0]
    transformed_points[:, 1] = x_rel * np.sin(theta) + y_rel * np.cos(theta) + origin_table[1]
    
    return transformed_points

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

def filter_points_by_height(points, z_value=None, z_min=None, z_max=None):
   
    if z_value is not None:
        mask = (points[:, 2] == z_value)
    else:
        mask = (points[:, 2] >= z_min) & (points[:, 2] <= z_max)
    return points[mask]




# Step 1: Generate a surface of points above the table with a resolution defined as follows
# Definiere die Auflösung der Punkte


# Definiere die Sicherheitsabstände
#WaitTime=1

def rotation_to_quaternion(rotation_matrix):
    return R.from_matrix(rotation_matrix).as_quat()

# Function to generate a trajectory and save it in a MOD file
def generate_pick_place_trajectory_and_save(filename, directory,velocity,velocity_picking,robot_movement, reorientation_z, min_distance, count_trajectories, iterations,weight,handling_height,gripping_height,safety_distance_edge):
    resolution = 20  # mm
    calculation=0
    table_height=700
      # mm Abstand zum Rand vom Tisch
     # mm Abstand vom Tisch beim Handhaben des Objekts
    total_gripping_height=gripping_height+table_height
    
    total_handling_height=table_height+gripping_height+handling_height
    #min_handling_height=50 # mm Höhe über Greifpunkt
    #total_min__handling_height=table_height+gripping_height+min_handling_height
    #max_handling_height=300 # mm Workingspace of Pick and Place 
    #total_max__handling_height=table_height+gripping_height+max_handling_height

    # Set the origin and orientation "Home"
    rotation_angle_laser = 0  # winkel zwischen x achsen durch messsystem  bestimmen positiv:von oben gegen uhrzeigersinn, negative von oben im uhrzeigersinn um ursprung gedreht
    #x_deg, y_deg, z_deg = 180, 0, 210+rotation_angle_laser #winkel um erstes werkstück zu greifen
    x_deg, y_deg, z_deg = 0, 0, 0+rotation_angle_laser #winkel um erstes werkstück zu greifen
    origin_table=([1170,-500]) #später durch messsystem ausfüllen

    start_orientation_quaternion = euler_to_quaternion(x_deg, y_deg, z_deg)
    home_rotation = R.from_quat(start_orientation_quaternion).as_matrix()
    starting_coordinates_cube_x_y = np.array([[145, 180, total_handling_height]]) #x und y koordinate des würfelmittelspunkts bezogen auf tisch koordinatensystem eintragen
    starting_point=transform_points_grid(starting_coordinates_cube_x_y,origin_table,rotation_angle_laser).flatten()
    # Erstelle x und y Koordinaten mit Sicherheitsabstand
    x = np.arange(safety_distance_edge, 1000-safety_distance_edge, resolution)
    y = np.arange(safety_distance_edge, 1000-safety_distance_edge, resolution)
    z = np.arange(total_gripping_height, total_handling_height+resolution, resolution) # + resolution to be safe that points are created at the preferred handling height

    X, Y, Z = np.meshgrid(x, y ,z)

    # Wandle in Punkteliste um
    points = np.vstack([X.ravel(), Y.ravel(), Z.ravel()]).T

    # Translate the origin of the picking surface to align (x,y,z) of "Table Edge"
    #Erweiterung für Laserscanner notwendig jetzt erstmal nur für Tisch auf Position (1170/-50)
    points=transform_points_grid(points,origin_table, rotation_angle_laser)
    # Function to convert the rotation matrix into a quaternion
    
    filename = directory + filename
    data_filename = directory + "trajectories_data_pick_place.mod"
    trajectory_length = count_trajectories # -1 last Pick_Place Trajectory is back to starting position 
    selected_points = []
    points_handling=filter_points_by_height(points,total_handling_height)
   
    np.random.shuffle(points_handling)  # Shuffle the points for random distribution
    for point in points_handling:
        if len(selected_points) == 0:
                selected_points.append(starting_point)
        elif len(selected_points) < trajectory_length:
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
    with open(filename, 'w') as txtfile:
        #punkt definitonen
        txtfile.write("MODULE RandomPointsDefinition\n")
        txtfile.write(f'PERS robtarget phome:=[[0,0,0],'
                          f'[{start_orientation_quaternion[0]},{start_orientation_quaternion[1]},{start_orientation_quaternion[2]},{start_orientation_quaternion[3]}],'
                          f'[-1,0,0,0],[9E+09,9E+09,9E+09,9E+09,9E+09,9E+09]];\n')
        if robot_movement=='MoveL':
            for i, point in enumerate(selected_points):
                if i==0: #erste greifposition oberhalb und am würfel implementieren
                    txtfile.write(f'PERS robtarget p{i+1}:=[[{starting_point[0]},{starting_point[1]},{starting_point[2]}],'
                            f'[{start_orientation_quaternion[0]},{start_orientation_quaternion[1]},{start_orientation_quaternion[2]},{start_orientation_quaternion[3]}],'
                            f'[-1,0,0,0],[9E+09,9E+09,9E+09,9E+09,9E+09,9E+09]];\n')
                    txtfile.write(f'PERS robtarget p{i+2}:=[[{starting_point[0]},{starting_point[1]},{total_gripping_height}],'
                            f'[{start_orientation_quaternion[0]},{start_orientation_quaternion[1]},{start_orientation_quaternion[2]},{start_orientation_quaternion[3]}],'
                            f'[-1,0,0,0],[9E+09,9E+09,9E+09,9E+09,9E+09,9E+09]];\n')    
                else:
                    rz = random.uniform(-reorientation_z, reorientation_z)
                    z_rotation = R.from_euler('x', rz, degrees=True).as_matrix()
                    R_tool=np.dot(z_rotation, home_rotation)
                    quaternion = rotation_to_quaternion(R_tool)
                    # Writing in RAPID ABB format
                    txtfile.write(f'PERS robtarget p{i*2+1}:=[[{point[0]},{point[1]},{point[2]}],'
                                    f'[{quaternion[0]},{quaternion[1]},{quaternion[2]},{quaternion[3]}],'
                                    f'[-1,0,0,0],[9E+09,9E+09,9E+09,9E+09,9E+09,9E+09]];\n')
                    txtfile.write(f'PERS robtarget p{i*2+2}:=[[{point[0]},{point[1]},{total_gripping_height}],'
                                    f'[{quaternion[0]},{quaternion[1]},{quaternion[2]},{quaternion[3]}],'
                                    f'[-1,0,0,0],[9E+09,9E+09,9E+09,9E+09,9E+09,9E+09]];\n')
                 
            txtfile.write("PROC run_random_points0()\n")
            txtfile.write(f"weight := {weight};\n")
            txtfile.write(f"velocity := [{velocity},500,5000,1000];\n")
            txtfile.write(f"velocity_picking := [{velocity_picking},500,5000,1000];\n")   
            txtfile.write(f"counter_reachable := -1;\n")
            txtfile.write(f"total_traj:= {iterations*len(selected_points)};\n")
            txtfile.write(f"current_traj := 0;\n")
            txtfile.write(f"counter_linear := 0;\n")
            txtfile.write(f"iteration := 0;\n")     
            txtfile.write(f'FOR i FROM 1 TO {iterations} DO\n')
            txtfile.write(f"final_traj:=false;\n") 
            txtfile.write(f"SetDO GripperClose,0;\n")
            txtfile.write(f'reorient_target_place:= phome;\n')
            txtfile.write(f'reorient_target_pick:= phome;\n')
            txtfile.write(f'reorient_target_place:= p1;\n')
            txtfile.write(f'reorient_target_pick:= p2;\n')
            txtfile.write(f"IF isReachable_pick_place=false THEN\n")
            txtfile.write(f"break;\n")
            txtfile.write(f'TPWrite"Start Position not reachable!";\n')
            txtfile.write(f"ENDIF\n")
            for i, point in enumerate(selected_points):
                if i == 0:
                    calculation=i+1
                else:
                    calculation=i*2+1
                #txtfile.write(f"MoveL p{calculation},v10,fine,SchunkGreifer;\nMoveL p{calculation+1},v10,fine,SchunkGreifer;\n")#um punkte in rapid anzeigen zu lassen
                txtfile.write(f"reorient_target_place := p{calculation};\n")
                txtfile.write(f"reorient_target_pick := p{calculation+1};\n")
                txtfile.write(f'TPWrite"Trajectory: "+ValToStr(current_traj)+"/"+ValToStr(total_traj)+" excecuting!";\n')
                txtfile.write(f"moveL_PickPlace;\n")
                txtfile.write(f"current_traj := current_traj+1;\n")
            txtfile.write(f"final_traj:=true;\n")
            txtfile.write(f"reorient_target_place := p1;\n")
            txtfile.write(f"reorient_target_pick := p2;\n")
            txtfile.write(f'TPWrite"Trajectory: "+ValToStr(current_traj)+"/"+ValToStr(total_traj)+" excecuting!";\n')
            txtfile.write(f"moveL_PickPlace;\n")
            txtfile.write(f'iteration:=iteration+1;\n')
            txtfile.write(f'TPWrite "Iteration "+ValToStr(iteration)+" finished!";\n')
            txtfile.write(f'TPWrite "Count: "+ValToStr(counter_reachable+1-i)+" MoveL !";\n')
            txtfile.write("ENDFOR\n")
            txtfile.write(f'TPWrite "Completely finished!";\n')
            txtfile.write(f'movement_string:="END";\n')
            txtfile.write(f'WaitTime 2;\n')
            txtfile.write(f'movement_string:="x";\n')
            
            txtfile.write("ENDPROC\n")
            
        elif robot_movement=='MoveC':
            for i, point in enumerate(selected_points):
                if i==0: #erste greifposition oberhalb und am würfel implementieren
                    txtfile.write(f'PERS robtarget p{i+1}:=[[{starting_point[0]},{starting_point[1]},{starting_point[2]}],'
                            f'[{start_orientation_quaternion[0]},{start_orientation_quaternion[1]},{start_orientation_quaternion[2]},{start_orientation_quaternion[3]}],'
                            f'[-1,0,0,0],[9E+09,9E+09,9E+09,9E+09,9E+09,9E+09]];\n')
                    txtfile.write(f'PERS robtarget p{i+2}:=[[{starting_point[0]},{starting_point[1]},{total_gripping_height}],'
                            f'[{start_orientation_quaternion[0]},{start_orientation_quaternion[1]},{start_orientation_quaternion[2]},{start_orientation_quaternion[3]}],'
                            f'[-1,0,0,0],[9E+09,9E+09,9E+09,9E+09,9E+09,9E+09]];\n')    
                else:
                    rz = random.uniform(-reorientation_z, reorientation_z)
                    z_rotation = R.from_euler('x', rz, degrees=True).as_matrix()
                    R_tool=np.dot(z_rotation, home_rotation)
                    quaternion = rotation_to_quaternion(R_tool)

                    z_rotation_support = R.from_euler('x', rz/2, degrees=True).as_matrix()
                    R_tool_support=np.dot(z_rotation_support, home_rotation)
                    quaternion_support = rotation_to_quaternion(R_tool_support)

                    # Writing in RAPID ABB format
                    support_point=calculate_support_point(selected_points[(i-1)],selected_points[(i)])
                    txtfile.write(f'PERS robtarget p{i*3}:=[[{support_point[0]},{support_point[1]},{support_point[2]}],'
                                    f'[{quaternion_support[0]},{quaternion_support[1]},{quaternion_support[2]},{quaternion_support[3]}],'
                                    f'[-1,0,0,0],[9E+09,9E+09,9E+09,9E+09,9E+09,9E+09]];\n')
                    txtfile.write(f'PERS robtarget p{i*3+1}:=[[{point[0]},{point[1]},{total_handling_height}],'
                                    f'[{quaternion[0]},{quaternion[1]},{quaternion[2]},{quaternion[3]}],'
                                    f'[-1,0,0,0],[9E+09,9E+09,9E+09,9E+09,9E+09,9E+09]];\n')
                    txtfile.write(f'PERS robtarget p{i*3+2}:=[[{point[0]},{point[1]},{total_gripping_height}],'
                                    f'[{quaternion[0]},{quaternion[1]},{quaternion[2]},{quaternion[3]}],'
                                    f'[-1,0,0,0],[9E+09,9E+09,9E+09,9E+09,9E+09,9E+09]];\n')

            support_point=calculate_support_point(selected_points[(-1)],selected_points[(0)])
            txtfile.write(f'PERS robtarget p{len(selected_points)*3}:=[[{support_point[0]},{support_point[1]},{total_handling_height}],'
                                    f'[{quaternion_support[0]},{quaternion_support[1]},{quaternion_support[2]},{quaternion_support[3]}],'
                                    f'[-1,0,0,0],[9E+09,9E+09,9E+09,9E+09,9E+09,9E+09]];\n')#Stützpunkt für Rückweg zu Startpos
            txtfile.write("PROC run_random_points0()\n")
            txtfile.write(f"weight := {weight};\n")
            txtfile.write(f"velocity := [{velocity},500,5000,1000];\n")
            txtfile.write(f"velocity_picking := [{velocity_picking},500,5000,1000];\n")   
            txtfile.write(f"counter_reachable := 0;\n")
            txtfile.write(f"total_traj:= {iterations*len(selected_points)};\n")
            txtfile.write(f"current_traj := 0;\n")
            txtfile.write(f"counter_linear := 0;\n")
            txtfile.write(f"counter_circular := 0;\n")
            txtfile.write(f"iteration := 0;\n")   
            txtfile.write(f'FOR i FROM 1 TO {iterations} DO\n')
            txtfile.write(f"final_traj:=false;\n") 
            txtfile.write(f"SetDO GripperClose,0;\n")
            txtfile.write(f'reorient_target_support:= phome;\n')
            txtfile.write(f'reorient_target_place:= phome;\n')
            txtfile.write(f'reorient_target_pick:= phome;\n')
            for i, point in enumerate(selected_points[:-1]):
                if i == 0:
                    calculation=i+1
                    txtfile.write(f"reorient_target_support := p{calculation};\n")
                    txtfile.write(f"reorient_target_place := p{calculation};\n")
                    txtfile.write(f"reorient_target_pick := p{calculation+1};\n")
                    txtfile.write(f"IF isReachable_pick_place=false THEN\n")
                    txtfile.write(f"break;\n")
                    txtfile.write(f'TPWrite"Start Position not reachable!";\n')
                    txtfile.write(f"ENDIF\n")
                    txtfile.write(f"moveL_PickPlace;\n")
                    txtfile.write(f"current_traj := current_traj+1;\n")
                else:
                    calculation=i*3
                    txtfile.write(f"Traj_{i}:\n")
                    txtfile.write(f"IF isReachable_pick_place THEN\n")
                    txtfile.write(f'TPWrite"Trajectory: "+ValToStr(current_traj)+"/"+ValToStr(total_traj)+" excecuting!";\n')
                    txtfile.write(f"current_traj := current_traj+1;\n")
                    txtfile.write(f'reorient_target_support:= phome;\n')
                    txtfile.write(f'reorient_target_place:= phome;\n')
                    txtfile.write(f'reorient_target_pick:= phome;\n')
                    txtfile.write(f"reorient_target_support := p{calculation};\n")
                    txtfile.write(f"reorient_target_place := p{calculation+1};\n")
                    txtfile.write(f"reorient_target_pick := p{calculation+2};\n")
                    txtfile.write(f"IF isReachable_MoveC THEN\n")
                    txtfile.write(f"direction;\n")
                    txtfile.write(f"moveC_PickPlace;\n")
                    
                    txtfile.write(f"counter_circular:=counter_circular+1;\n")
                    txtfile.write(f"ELSEIF isReachable_pick_place=true THEN\n")
                    txtfile.write(f"moveL_PickPlace;\n")
                    txtfile.write(f"counter_linear:=counter_linear+1;\n")
                    
                    txtfile.write(f"ELSE\n")
                    
                    txtfile.write(f"ENDIF\n")
                    txtfile.write(f"ELSE\n")
                    txtfile.write(f"reorient_target_place := p{calculation+4};\n")
                    txtfile.write(f"reorient_target_pick := p{calculation+5};\n")
                    txtfile.write(f"IF isReachable_pick_place THEN\n")
                    txtfile.write(f"moveL_PickPlace;\n")
                    txtfile.write(f"counter_linear:=counter_linear+1;\n")
                    txtfile.write(f'TPWrite"Trajectory: "+ValToStr(current_traj)+"/"+ValToStr(total_traj)+" excecuting!";\n')
                    txtfile.write(f"current_traj := current_traj+2;\n")
                    txtfile.write(f"GOTO Traj_{i+2};\n")
                    txtfile.write(f"ELSE\n")
                    txtfile.write(f'TPWrite"Trajectory: "+ValToStr(current_traj)+"/"+ValToStr(total_traj)+" excecuting!";\n')
                    txtfile.write(f"current_traj := current_traj+1;\n")
                    txtfile.write(f"ENDIF\n")
                    txtfile.write(f"ENDIF\n")
                    
            txtfile.write(f"Traj_{len(selected_points)-1}:\n")
            txtfile.write(f"IF isReachable_pick_place THEN\n")
            txtfile.write(f'TPWrite"Trajectory: "+ValToStr(current_traj)+"/"+ValToStr(total_traj)+" excecuting!";\n')
            txtfile.write(f"current_traj := current_traj+1;\n")
            txtfile.write(f'reorient_target_support:= phome;\n')
            txtfile.write(f'reorient_target_place:= phome;\n')
            txtfile.write(f'reorient_target_pick:= phome;\n')
            txtfile.write(f"reorient_target_support := p{calculation+3};\n")
            txtfile.write(f"reorient_target_place := p{calculation+4};\n")
            txtfile.write(f"reorient_target_pick := p{calculation+5};\n")
            txtfile.write(f"IF isReachable_MoveC THEN\n")
            txtfile.write(f"direction;\n")
            txtfile.write(f"moveC_PickPlace;\n")
            txtfile.write(f"counter_circular:=counter_circular+1;\n")
            txtfile.write(f"ELSEIF isReachable_pick_place=true THEN\n")
            txtfile.write(f"moveL_PickPlace;\n")
            txtfile.write(f"counter_linear:=counter_linear+1;\n")
            txtfile.write(f"ENDIF\n")
            txtfile.write(f"ELSE\n")
            txtfile.write(f'TPWrite"Trajectory: "+ValToStr(current_traj)+"/"+ValToStr(total_traj)+" excecuting!";\n')
            txtfile.write(f"current_traj := current_traj+1;\n")
            txtfile.write(f"ENDIF\n")
            txtfile.write(f"Traj_{len(selected_points)}:\n")
            txtfile.write(f"final_traj:=true;\n")
            txtfile.write(f"IF isReachable_pick_place THEN\n")
            txtfile.write(f'TPWrite"Trajectory: "+ValToStr(current_traj)+"/"+ValToStr(total_traj)+" excecuting!";\n')
            txtfile.write(f"reorient_target_support := p{len(selected_points)*3};\n")
            txtfile.write(f"reorient_target_place := p{1};\n")
            txtfile.write(f"reorient_target_pick := p{2};\n")
            txtfile.write(f"IF isReachable_MoveC THEN\n")
            txtfile.write(f"direction;\n")
            txtfile.write(f"moveC_PickPlace;\n")
            txtfile.write(f"counter_circular:=counter_circular+1;\n")
            txtfile.write(f"ELSEIF isReachable_pick_place=true THEN\n")
            txtfile.write(f"moveL_PickPlace;\n")
            txtfile.write(f"counter_linear:=counter_linear+1;\n")
            txtfile.write(f"ELSE\n")
            txtfile.write(f"ENDIF\n")
            txtfile.write(f"ELSE\n")
            txtfile.write(f'TPWrite"Trajectory: "+ValToStr(current_traj)+"/"+ValToStr(total_traj)+" excecuting!";\n')
            txtfile.write(f"reorient_target_place := p{1};\n")
            txtfile.write(f"reorient_target_pick := p{2};\n")
            txtfile.write(f"moveL_PickPlace;\n")
            txtfile.write(f"counter_linear:=counter_linear+1;\n")
            txtfile.write(f"ENDIF\n")
            
            txtfile.write(f'iteration:=iteration+1;\n')
            txtfile.write(f'TPWrite "Iteration "+ValToStr(iteration)+" finished!";\n')
            txtfile.write(f'TPWrite "Count: "+ValToStr(counter_circular)+" MoveC !";\n')
            txtfile.write(f'TPWrite "Count: "+ValToStr(counter_linear)+" MoveL !";\n')
            txtfile.write("ENDFOR\n")
            txtfile.write(f'TPWrite "Completely finished!";\n')
            txtfile.write(f'movement_string:="END";\n')
            txtfile.write(f'WaitTime 2;\n')
            txtfile.write(f'movement_string:="x";\n')
            txtfile.write("ENDPROC\n")    
        txtfile.write("ENDMODULE")
    print(f'Random trajectory generated and saved in {filename}')
    print(f'Velocities generated and saved in {data_filename}')
# Function to check the minimum distance between points
def is_far_enough(new_point, selected_points, min_distance):
    if len(selected_points) == 0:
        return True
    distances = np.linalg.norm(selected_points - new_point, axis=1)
    return np.all(distances >= min_distance)
generate_pick_place_trajectory_and_save('test.mod',str(Path.home()) + '/robotervermessung-rosbag-recorder/data/random_pick_place_trajectories/',200,200,'MoveC',30,100,10,1,5,40,200,70)
def calibration_movement(filename, directory):
    # + resolution to be safe that points are created at the preferred handling height
    filename = directory + filename
    data_filename = directory + "trajectories_data_pick_place.mod"
    with open(filename, 'w') as txtfile:
        #punkt definitonen
        txtfile.write("MODULE RandomPointsDefinition\n")
        txtfile.write(f'PERS robtarget p1:=[[1315,-320,740],'
                          f'[1,0,0,0],'
                          f'[-1,0,0,0],[9E+09,9E+09,9E+09,9E+09,9E+09,9E+09]];\n')
        txtfile.write("PROC run_random_points0()\n")
        txtfile.write(f"velocity := [300,500,5000,1000];\n")
        txtfile.write(f"velocity_picking := [600,500,5000,1000];\n")  

        txtfile.write(f"SetDO GripperClose,0;\n")
        txtfile.write(f'WaitTime 1;\n')
        txtfile.write(f'MoveL p1, velocity, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=p1;\n')
        txtfile.write(f'WaitTime 2;\n')

        txtfile.write(f'MoveL Offs(p1, 200, 0, 0), velocity, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 200, 0, 0);\n')
        txtfile.write(f'WaitTime 2;\n')

        txtfile.write(f'MoveL Offs(p1, 400, 0, 0), velocity, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 400, 0, 0);\n')
        txtfile.write(f'WaitTime 2;\n')
        
        txtfile.write(f'MoveL Offs(p1, 400, 200, 0), velocity_picking, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 400, 200, 0);\n')
        txtfile.write(f'WaitTime 2;\n')
        
        txtfile.write(f'MoveL Offs(p1, 400, 400, 0), velocity, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 400, 400, 0);\n')
        txtfile.write(f'WaitTime 2;\n')
        
        txtfile.write(f'MoveL Offs(p1, 200, 400, 0), velocity_picking, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 200, 400, 0);\n')
        txtfile.write(f'WaitTime 2;\n')
        
        txtfile.write(f'MoveL Offs(p1, 0, 400, 0), velocity, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 0, 400, 0);\n')
        txtfile.write(f'WaitTime 2;\n')

        txtfile.write(f'MoveL Offs(p1, 0, 200, 0), velocity_picking, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 0, 200, 0);\n')
        txtfile.write(f'WaitTime 2;\n')
        ###################################################
        
        txtfile.write(f'MoveL Offs(p1, 0, 200, 150), velocity,fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 0, 200, 150);\n')
        txtfile.write(f'WaitTime 2;\n')
        
        txtfile.write(f'MoveL Offs(p1, 200, 0, 150), velocity_picking, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 200, 0, 150);\n')
        txtfile.write(f'WaitTime 2;\n')
        
        txtfile.write(f'MoveL Offs(p1, 400, 200, 150), velocity, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 400, 200, 150);\n')
        txtfile.write(f'WaitTime 2;\n')
        
        txtfile.write(f'MoveL Offs(p1, 200, 400, 150), velocity_picking, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 200, 400, 150);\n')
        txtfile.write(f'WaitTime 2;\n')
        ######################################################
        txtfile.write(f'MoveL Offs(p1, 200, 400, 300), velocity, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 200, 400, 300);\n')
        txtfile.write(f'WaitTime 2;\n')
        
        txtfile.write(f'MoveL Offs(p1, 400, 400, 300), velocity_picking, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 400, 400, 300);\n')
        txtfile.write(f'WaitTime 2;\n')
        
        txtfile.write(f'MoveL Offs(p1, 400, 200, 300), velocity, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 400, 200, 300);\n')
        txtfile.write(f'WaitTime 2;\n')
        
        txtfile.write(f'MoveL Offs(p1, 400, 0, 300), velocity_picking, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 400, 0, 300);\n')
        txtfile.write(f'WaitTime 2;\n')
        
        txtfile.write(f'MoveL Offs(p1, 200, 0, 300), velocity, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 200, 0, 300);\n')
        txtfile.write(f'WaitTime 2;\n')

        txtfile.write(f'MoveL Offs(p1, 0, 0, 300), velocity_picking, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 0, 0, 300);\n')
        txtfile.write(f'WaitTime 2;\n')
        
        txtfile.write(f'MoveL Offs(p1, 0, 200, 300), velocity, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 0, 200, 300);\n')
        txtfile.write(f'WaitTime 2;\n')
        
        txtfile.write(f'MoveL Offs(p1, 0, 400, 300), velocity_picking, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 0, 400, 300);\n')
        txtfile.write(f'WaitTime 2;\n')

        txtfile.write(f'MoveL Offs(p1, 0, 0, 200), velocity_picking, fine, SchunkGreifer;\n')
        txtfile.write(f'currPos:=Offs(p1, 0, 0, 200);\n')
        txtfile.write(f'WaitTime 2;\n')
        
        txtfile.write(f'TPWrite "Completely finished!";\n')
        txtfile.write(f'movement_string:="END";\n')
        txtfile.write(f'WaitTime 2;\n')
        txtfile.write(f'movement_string:="x";\n')
        txtfile.write("ENDPROC\n")
        txtfile.write("ENDMODULE")
    print(f'Random trajectory generated and saved in {filename}')
    print(f'Velocities generated and saved in {data_filename}')
#generate_pick_place_trajectory_and_save('test.mod', 'D:/Noel/Uni_Master/Masterarbeit/PythonSkripte/PickAndPlace/RandomTrajectories/',4000,4000, 'MoveL', 44, 10,10, 1)