import psycopg2
import csv
from datetime import datetime
import os
import numpy as np
from psycopg2.extras import Json

# PostgreSQL connection details
#PG_HOST = 'localhost'
#PG_PORT = '5433'
#PG_DATABASE = 'robotervermessung'
#PG_USER = 'gugafelds'
#PG_PASSWORD = '200195Beto!'

PG_HOST = '134.147.100.22'
PG_PORT = '5432'
PG_DATABASE = 'robotervermessung'
PG_USER = 'postgres'
PG_PASSWORD = '200195Beto'

def connect_to_db():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )

def safe_float(value):
    try:
        return float(value) if value.strip() else None
    except ValueError:
        return None

def parse_filename(filename):
    parts = filename.split('_')
    date_str = parts[1]
    time_str = parts[2]

    year = int(date_str[:4])
    month = int(date_str[4:6])
    day = int(date_str[6:])
    hour = int(time_str[:2])
    minute = int(time_str[2:4])
    second = int(time_str[4:])

    return datetime(year, month, day, hour, minute, second)

def read_trajectory_data(file_path):
    data = {
        'timestamp_ist': [], 'x_ist': [], 'y_ist': [], 'z_ist': [],
        'tcp_velocity_ist': [], 'tcp_acceleration': [],
        'qx_ist': [], 'qy_ist': [], 'qz_ist': [], 'qw_ist': [],
        'timestamp_soll': [], 'x_soll': [], 'y_soll': [], 'z_soll': [],
        'qx_soll': [], 'qy_soll': [], 'qz_soll': [], 'qw_soll': [],
        'tcp_velocity_soll': [], 'joint_state_soll': []
    }

    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data['timestamp_ist'].append(row['timestamp'])
            data['x_ist'].append(safe_float(row['pv_x']))
            data['y_ist'].append(safe_float(row['pv_y']))
            data['z_ist'].append(safe_float(row['pv_z']))
            data['tcp_velocity_ist'].append(safe_float(row['tcp_speedv']))
            data['tcp_acceleration'].append(safe_float(row['tcp_accelv']))
            data['qx_ist'].append(safe_float(row['ov_x']))
            data['qy_ist'].append(safe_float(row['ov_y']))
            data['qz_ist'].append(safe_float(row['ov_z']))
            data['qw_ist'].append(safe_float(row['ov_w']))
            data['timestamp_soll'].append(row['timestamp'])  # Assuming same timestamp for soll
            data['x_soll'].append(safe_float(row['ps_x']))
            data['y_soll'].append(safe_float(row['ps_y']))
            data['z_soll'].append(safe_float(row['ps_z']))
            data['qx_soll'].append(safe_float(row['os_x']))
            data['qy_soll'].append(safe_float(row['os_y']))
            data['qz_soll'].append(safe_float(row['os_z']))
            data['qw_soll'].append(safe_float(row['os_w']))
            data['tcp_velocity_soll'].append(safe_float(row['tcp_speeds']))
            data['joint_state_soll'].append([safe_float(row[f'joint_{i}']) for i in range(1, 7)])

    return data

def insert_data(cursor, data, trajectory_header_id):
    insert_query = '''
    INSERT INTO trajectories.trajectories_data 
    (trajectory_header_id, timestamp_ist, x_ist, y_ist, z_ist, 
    tcp_velocity_ist, tcp_acceleration, 
    qx_ist, qy_ist, qz_ist, qw_ist, 
    timestamp_soll, x_soll, y_soll, z_soll, 
    qx_soll, qy_soll, qz_soll, qw_soll, 
    tcp_velocity_soll, joint_state_soll)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    cursor.execute(insert_query, (
        trajectory_header_id,
        data['timestamp_ist'],
        data['x_ist'],
        data['y_ist'],
        data['z_ist'],
        data['tcp_velocity_ist'],
        data['tcp_acceleration'],
        data['qx_ist'],
        data['qy_ist'],
        data['qz_ist'],
        data['qw_ist'],
        data['timestamp_soll'],
        data['x_soll'],
        data['y_soll'],
        data['z_soll'],
        data['qx_soll'],
        data['qy_soll'],
        data['qz_soll'],
        data['qw_soll'],
        data['tcp_velocity_soll'],
        data['joint_state_soll']
    ))

def get_header_input(file_path):
    filename = os.path.basename(file_path)
    recording_date = parse_filename(filename)
    data_id = f"robot0{int(recording_date.timestamp())}"

    print("Please enter the following header information:")
    return {
        'data_id': data_id,
        'robot_model': input("Robot Model: "),
        'trajectory_type': input("Trajectory Type: "),
        'path_solver': input("Path Solver: "),
        'recording_date': recording_date,
        'real_robot': input("Real Robot (True/False): ").lower() == 'true',
        'number_of_points_ist': int(input("Number of Points (IST): ")),
        'number_of_points_soll': int(input("Number of Points (SOLL): ")),
        'sample_frequency_ist': float(input("Sample Frequency (IST): ")),
        'sample_frequency_soll': float(input("Sample Frequency (SOLL): ")),
        'source_data_ist': input("Source Data (IST): "),
        'source_data_soll': input("Source Data (SOLL): "),
        'evaluation_source': input("Evaluation Source: ")
    }

def insert_header(cursor, header_data):
    insert_query = '''
    INSERT INTO trajectories.trajectories_header 
    (data_id, robot_model, trajectory_type, path_solver, recording_date, real_robot, 
    number_of_points_ist, number_of_points_soll, sample_frequency_ist, sample_frequency_soll, 
    source_data_ist, source_data_soll, evaluation_source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id
    '''
    cursor.execute(insert_query, tuple(header_data.values()))
    return cursor.fetchone()[0]

def main():
    conn = connect_to_db()
    cursor = conn.cursor()

    try:
        file_path = "/home/gugafelds/robotervermessung-rosbag-recorder/data/csv_data/record_20240712_132006_all_final.csv"

        print("Reading trajectory data...")
        trajectory_data = read_trajectory_data(file_path)
        num_points = len(trajectory_data['timestamp_ist'])
        print(f"Read {num_points} data points.")

        print("Getting header information...")
        header_data = get_header_input(file_path)
        header_data['number_of_points_ist'] = num_points
        header_data['number_of_points_soll'] = num_points
        
        print("Inserting header...")
        header_id = insert_header(cursor, header_data)
        
        print("Inserting data into database...")
        insert_data(cursor, trajectory_data, header_data['data_id'])

        conn.commit()
        print(f"Trajectory uploaded successfully! Inserted data for header ID {header_id}")

    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")
        import traceback
        print("Detailed error information:")
        print(traceback.format_exc())

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
