import os
import re
import psycopg2
import csv
import sys
from psycopg2 import sql
from datetime import datetime
from tqdm import tqdm

# Database connection parameters
DB_PARAMS = {
    'dbname': 'robotervermessung',
    'user': 'postgres',
    'password': '200195Beto',
    'host': '134.147.100.22',
    'port': '5432'
}

# Mapping of CSV columns to database columns for bahn_accel_ist
ACCEL_MAPPING = {
    'tcp_accelv__x': 'tcp_accel_x',
    'tcp_accelv__y': 'tcp_accel_y',
    'tcp_accelv__z': 'tcp_accel_z',
    'tcp_accelv': 'tcp_accel_ist',
    'tcp_accelv_angular_x': 'tcp_angular_accel_x',
    'tcp_accelv_angular_y': 'tcp_angular_accel_y',
    'tcp_accelv_angular_z': 'tcp_angular_accel_z',
    'tcp_accelv_angular': 'tcp_angular_accel_ist'
}

# Mapping of CSV columns to database columns for bahn_joint_states
JOINT_MAPPING = {
    'joint_1': 'joint_1',
    'joint_2': 'joint_2',
    'joint_3': 'joint_3',
    'joint_4': 'joint_4',
    'joint_5': 'joint_5',
    'joint_6': 'joint_6'
}

POSE_MAPPING = {
    'pv_x': 'x_ist',
    'pv_y': 'y_ist',
    'pv_z': 'z_ist',
    'ov_x': 'qx_ist',
    'ov_y': 'qy_ist',
    'ov_z': 'qz_ist',
    'ov_w': 'qw_ist'
}

RAPID_EVENTS_MAPPING = {
    'ap_x': 'x_reached',
    'ap_y': 'y_reached',
    'ap_z': 'z_reached',
    'aq_x': 'qx_reached',
    'aq_y': 'qy_reached',
    'aq_z': 'qz_reached',
    'aq_w': 'qw_reached'
}

ORIENTATION_SOLL_MAPPING = {
    'os_x': 'qx_soll',
    'os_y': 'qy_soll',
    'os_z': 'qz_soll',
    'os_w': 'qw_soll'
}

POSITION_SOLL_MAPPING = {
    'ps_x': 'x_soll',
    'ps_y': 'y_soll',
    'ps_z': 'z_soll',

}

TWIST_IST_MAPPING = {
    'tcp_speedv_x': 'tcp_speed_x',
    'tcp_speedv_y': 'tcp_speed_y',
    'tcp_speedv_z': 'tcp_speed_z',
    'tcp_speedv': 'tcp_speed_ist',
    'tcp_angularv_x': 'tcp_angular_x',
    'tcp_angularv_y': 'tcp_angular_y',
    'tcp_angularv_z': 'tcp_angular_z',
    'tcp_angularv': 'tcp_angular_ist'
}

TWIST_SOLL_MAPPING = {
    'tcp_speeds': 'tcp_speed_soll'
}

def get_user_choice():
    choice = input().strip().lower()
    return choice == 'y'

def connect_to_db():
    """Establish a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL: {error}")
        sys.exit(1)

def convert_timestamp(ts):
    """
    Convert a nanosecond timestamp to a datetime object.
    """
    try:
        # Convert from nanoseconds to seconds
        timestamp_seconds = int(ts) / 1_000_000_000.0
        return datetime.fromtimestamp(timestamp_seconds)
    except ValueError as e:
        print(f"Error converting timestamp {ts}: {e}")
        return None

def calculate_frequencies(rows, column_mapping):
    """Calculate the frequency of data publication for a specific column mapping."""
    timestamps = [convert_timestamp(row['timestamp']) for row in rows if row.get(list(column_mapping.keys())[0])]
    if not timestamps:
        return 0.0
    diffs = [(timestamps[i + 1] - timestamps[i]).total_seconds() for i in range(len(timestamps) - 1)]
    avg_diff = sum(diffs) / len(diffs) if diffs else 0
    return 1 / avg_diff if avg_diff > 0 else 0.0

def insert_bahn_info(conn, data):
    """Insert data into the bahn_info table."""
    with conn.cursor() as cur:
        insert_query = sql.SQL("""
            INSERT INTO bewegungsdaten.bahn_info 
            (bahn_id, robot_model, bahnplanung, recording_date, start_time, end_time, 
             source_data_ist, source_data_soll, record_filename, 
             number_of_points, frequency_pose_ist, frequency_position_soll, 
             frequency_orientation_soll, frequency_twist_ist, frequency_twist_soll, 
             frequency_accel_ist, frequency_joint_states, calibration_run)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        try:
            cur.execute(insert_query, data)
            conn.commit()
            print(f"Data inserted successfully into bahn_info")
        except (Exception, psycopg2.Error) as error:
            conn.rollback()
            print(f"Error inserting data into bahn_info: {error}")
            print(f"Query: {cur.query}")  # This will print the actual SQL query with parameters
            print(f"Data: {data}")  # This will print the data being inserted

def insert_pose_data(conn, data):
    """Insert data into the bewegungsdaten.bahn_pose_ist table."""
    with conn.cursor() as cur:
        insert_query = sql.SQL("""
            INSERT INTO bewegungsdaten.bahn_pose_ist 
            (bahn_id, segment_id, timestamp, x_ist, y_ist, z_ist, qx_ist, qy_ist, qz_ist, qw_ist, 
             source_data_ist)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        try:
            cur.executemany(insert_query, data)
            conn.commit()
            print(f"Data inserted successfully into bahn_pose_ist")
        except (Exception, psycopg2.Error) as error:
            conn.rollback()
            print(f"Error inserting data into bahn_pose_ist: {error}")

def insert_position_soll_data(conn, data):
    """Insert data into the bewegungsdaten.bahn_position_soll table."""
    with conn.cursor() as cur:
        insert_query = sql.SQL("""
            INSERT INTO bewegungsdaten.bahn_position_soll 
            (bahn_id, segment_id, timestamp, x_soll, y_soll, z_soll, source_data_soll)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """)
        try:
            cur.executemany(insert_query, data)
            conn.commit()
            print(f"Data inserted successfully into bahn_position_soll")
        except (Exception, psycopg2.Error) as error:
            conn.rollback()
            print(f"Error inserting data into bahn_position_soll: {error}")

def insert_twist_soll_data(conn, data):
    """Insert data into the bewegungsdaten.bahn_twist_soll table."""
    with conn.cursor() as cur:
        insert_query = sql.SQL("""
            INSERT INTO bewegungsdaten.bahn_twist_soll 
            (bahn_id, segment_id, timestamp, tcp_speed_soll, source_data_soll)
            VALUES (%s, %s, %s, %s, %s)
        """)
        try:
            cur.executemany(insert_query, data)
            conn.commit()
            print(f"Data inserted successfully into bahn_twist_soll")
        except (Exception, psycopg2.Error) as error:
            conn.rollback()
            print(f"Error inserting data into bahn_twist_soll: {error}")


def insert_orientation_soll_data(conn, data):
    """Insert data into the bewegungsdaten.bahn_orientation_soll table."""
    with conn.cursor() as cur:
        insert_query = sql.SQL("""
            INSERT INTO bewegungsdaten.bahn_orientation_soll 
            (bahn_id, segment_id, timestamp, qx_soll, qy_soll, qz_soll, qw_soll, source_data_soll)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """)
        try:
            cur.executemany(insert_query, data)
            conn.commit()
            print(f"Data inserted successfully into bahn_orientation_soll")
        except (Exception, psycopg2.Error) as error:
            conn.rollback()
            print(f"Error inserting data into bahn_orientation_soll: {error}")

def insert_accel_data(conn, data):
    """Insert data into the bewegungsdaten.bahn_accel_ist table."""
    with conn.cursor() as cur:
        insert_query = sql.SQL("""
            INSERT INTO bewegungsdaten.bahn_accel_ist 
            (bahn_id, segment_id, timestamp, tcp_accel_x, tcp_accel_y, tcp_accel_z, tcp_accel_ist, 
             tcp_angular_accel_x, tcp_angular_accel_y, tcp_angular_accel_z, tcp_angular_accel_ist, 
             source_data_ist)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        try:
            cur.executemany(insert_query, data)
            conn.commit()
            print(f"Data inserted successfully into bahn_accel_ist")
        except (Exception, psycopg2.Error) as error:
            conn.rollback()
            print(f"Error inserting data into bahn_accel_ist: {error}")

def insert_twist_ist_data(conn, data):
    """Insert data into the bewegungsdaten.bahn_twist_ist table."""
    with conn.cursor() as cur:
        insert_query = sql.SQL("""
            INSERT INTO bewegungsdaten.bahn_twist_ist 
            (bahn_id, segment_id, timestamp, tcp_speed_x, tcp_speed_y, tcp_speed_z, tcp_speed_ist, 
             tcp_angular_x, tcp_angular_y, tcp_angular_z, tcp_angular_ist, 
             source_data_ist)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        try:
            cur.executemany(insert_query, data)
            conn.commit()
            print(f"Data inserted successfully into bahn_twist_ist")
        except (Exception, psycopg2.Error) as error:
            conn.rollback()
            print(f"Error inserting data into bahn_twist_ist: {error}")

def insert_rapid_events_data(conn, data):
    """Insert data into the bewegungsdaten.bahn_rapid_events table."""
    with conn.cursor() as cur:
        insert_query = sql.SQL("""
            INSERT INTO bewegungsdaten.bahn_rapid_events 
            (bahn_id, segment_id, timestamp, x_reached, y_reached, z_reached, qx_reached, qy_reached, qz_reached, qw_reached, source_data_soll)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        try:
            cur.executemany(insert_query, data)
            conn.commit()
            print(f"Data inserted successfully into bahn_rapid_events")
        except (Exception, psycopg2.Error) as error:
            conn.rollback()
            print(f"Error inserting data into bahn_rapid_events: {error}")

def insert_joint_data(conn, data):
    """Insert data into the bahn_joint_states table."""
    with conn.cursor() as cur:
        insert_query = sql.SQL("""
            INSERT INTO bewegungsdaten.bahn_joint_states 
            (bahn_id, segment_id, timestamp, joint_1, joint_2, joint_3, joint_4, joint_5, joint_6, 
             source_data_soll)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        try:
            cur.executemany(insert_query, data)
            conn.commit()
            print(f"Data inserted successfully into bahn_joint_states")
        except (Exception, psycopg2.Error) as error:
            conn.rollback()
            print(f"Error inserting data into bahn_joint_states: {error}")

def extract_record_part(file_path):
    if 'record' in file_path:
        filename = os.path.basename(file_path)
        match = re.search(r'(record_\d{8}_\d{6})', filename)
        if match:
            return match.group(1)
    return None

def process_csv(file_path, upload_database, robot_model, bahnplanung, source_data_ist, source_data_soll):
    """Process the CSV file and insert data into bahn_accel_ist, bahn_joint_states, and bahn_pose_ist tables."""
    conn = None
    try:
        if upload_database:
            conn = connect_to_db()

        with open(file_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)  # Convert the reader to a list to process in reverse order
            
            # Identify the range of rows to keep based on the 'ap_x' column
            first_ap_x_index = next((i for i, row in enumerate(rows) if row.get('ap_x', '').strip()), None)
            last_ap_x_index = next((i for i, row in enumerate(reversed(rows)) if row.get('ap_x', '').strip()), None)

            if first_ap_x_index is None or last_ap_x_index is None:
                # Calculate the actual last index from the end of the list
                first_timestamp = rows[0].get('timestamp')
                last_timestamp = rows[-1].get('timestamp')

                recording_date = convert_timestamp(first_timestamp)
                start_time = recording_date
                end_time = convert_timestamp(last_timestamp)

                print(recording_date)
                print(f"Start Time: {start_time}")
                print(f"End Time: {end_time}")
                # Filter rows based on the identified range
                filtered_rows = rows
            else:
                # Calculate the actual last index from the end of the list
                last_ap_x_index = len(rows) - 1 - last_ap_x_index

                first_timestamp = rows[0].get('timestamp')
                first_ap_x_timestamp = rows[first_ap_x_index].get('timestamp')
                last_ap_x_timestamp = rows[last_ap_x_index].get('timestamp')

                recording_date = convert_timestamp(first_timestamp)
                start_time = convert_timestamp(first_ap_x_timestamp)
                end_time = convert_timestamp(last_ap_x_timestamp)

                print(recording_date)
                print(f"Start Time: {start_time}")
                print(f"End Time: {end_time}")
                # Filter rows based on the identified range
                filtered_rows = rows[first_ap_x_index:last_ap_x_index + 1]
            
            total_rows = len(filtered_rows)
            accel_data = []
            twist_ist_data = []
            twist_soll_data = []
            joint_data = []
            pose_data = []
            rapid_events_data = []
            orientation_soll_data = []
            position_soll_data = []
            bahn_info_data = []

            bahn_id = None
            segment_counter = 0
            current_segment_id = None

            record_filename = extract_record_part(file_path)
            
            accel_rows_processed = 0
            joint_rows_processed = 0
            twist_ist_rows_processed = 0
            twist_soll_rows_processed = 0
            rapid_events_rows_processed = 0
            pose_rows_processed = 0
            orientation_soll_rows_processed = 0
            position_soll_rows_processed = 0
            calibration_run = False

            # Variables for bahn_info table
            number_of_points = 0

            if "calibration_run" in file_path:
                calibration_run = True
            else:
                calibration_run = False
            
            for row in tqdm(filtered_rows, total=total_rows, desc="Processing CSV", unit="row"):
                timestamp = row['timestamp']
                if bahn_id is None:
                    bahn_id = timestamp[:9]
                    current_segment_id = f"{bahn_id}_{segment_counter}"

                # Check for new segment
                if row.get('ap_x', '').strip():
                    segment_counter += 1
                    current_segment_id = f"{bahn_id}_{segment_counter}"
     

                # Process bahn_accel_ist data if all required fields are present and non-empty
                if all(row.get(csv_col, '').strip() for csv_col in ACCEL_MAPPING):
                    accel_row = [
                        bahn_id,
                        current_segment_id,
                        timestamp
                    ]
                    accel_row.extend([row[csv_col] for csv_col in ACCEL_MAPPING])
                    accel_row.append('vicon')  # source_data_ist
                    accel_data.append(accel_row)
                    accel_rows_processed += 1

                # Process bahn_joint_states data if all joint values are present and non-empty
                if all(row.get(col, '').strip() for col in JOINT_MAPPING):
                    joint_row = [
                        bahn_id,
                        current_segment_id,
                        timestamp
                    ]
                    joint_row.extend([row[col] for col in JOINT_MAPPING])
                    joint_row.append(source_data_soll)  # source_data_soll
                    joint_data.append(joint_row)
                    joint_rows_processed += 1

                # Process bahn_pose_ist data if all required fields are present and non-empty
                if all(row.get(csv_col, '').strip() for csv_col in POSE_MAPPING):
                    pose_row = [
                        bahn_id,
                        current_segment_id,
                        timestamp
                    ]
                    pose_row.extend([row[csv_col] for csv_col in POSE_MAPPING])
                    pose_row.append(source_data_ist)  # source_data_ist
                    pose_data.append(pose_row)
                    pose_rows_processed += 1

                # Process bahn_rapid_events data if all required fields are present and non-empty
                if all(row.get(csv_col, '').strip() for csv_col in RAPID_EVENTS_MAPPING):
                    rapid_events_row = [
                        bahn_id,
                        current_segment_id,
                        timestamp
                    ]
                    rapid_events_row.extend([row[csv_col] for csv_col in RAPID_EVENTS_MAPPING])
                    rapid_events_row.append(source_data_soll)  # source_data_ist
                    rapid_events_data.append(rapid_events_row)
                    number_of_points += 1
                    rapid_events_rows_processed += 1
                
                # Process bahn_orientation_soll data if all required fields are present and non-empty
                if all(row.get(csv_col, '').strip() for csv_col in ORIENTATION_SOLL_MAPPING):
                    orientation_soll_row = [
                        bahn_id,
                        current_segment_id,
                        timestamp
                    ]
                    orientation_soll_row.extend([row[csv_col] for csv_col in ORIENTATION_SOLL_MAPPING])
                    orientation_soll_row.append(source_data_soll)  # source_data_soll
                    orientation_soll_data.append(orientation_soll_row)
                    orientation_soll_rows_processed += 1
                
                # Process bahn_position_soll data if all required fields are present and non-empty
                if all(row.get(csv_col, '').strip() for csv_col in POSITION_SOLL_MAPPING):
                    position_soll_row = [
                        bahn_id,
                        current_segment_id,
                        timestamp,
                    ]
                    position_soll_row.extend([row[csv_col] for csv_col in POSITION_SOLL_MAPPING])
                    position_soll_row.append(source_data_soll)  # source_data_soll
                    position_soll_data.append(position_soll_row)
                    position_soll_rows_processed += 1
                
                # Process bahn_twist_ist data if all required fields are present and non-empty
                if all(row.get(csv_col, '').strip() for csv_col in TWIST_IST_MAPPING):
                    twist_ist_row = [
                        bahn_id,
                        current_segment_id,
                        timestamp
                    ]
                    twist_ist_row.extend([row[csv_col] for csv_col in TWIST_IST_MAPPING])
                    twist_ist_row.append(source_data_ist)  # source_data_ist
                    twist_ist_data.append(twist_ist_row)
                    twist_ist_rows_processed += 1
                
                # Process bahn_twist_soll data if all required fields are present and non-empty
                if all(row.get(csv_col, '').strip() for csv_col in TWIST_SOLL_MAPPING):
                    twist_soll_row = [
                        bahn_id,
                        current_segment_id,
                        timestamp
                    ]
                    twist_soll_row.extend([row[csv_col] for csv_col in TWIST_SOLL_MAPPING])
                    twist_soll_row.append(source_data_soll)  # source_data_soll
                    twist_soll_data.append(twist_soll_row)
                    twist_soll_rows_processed += 1
                
            # Calculate frequencies
            frequency_pose_ist = calculate_frequencies(filtered_rows, POSE_MAPPING)
            frequency_position_soll = calculate_frequencies(filtered_rows, POSITION_SOLL_MAPPING)
            frequency_orientation_soll = calculate_frequencies(filtered_rows, ORIENTATION_SOLL_MAPPING)
            frequency_twist_ist = calculate_frequencies(filtered_rows, TWIST_IST_MAPPING)
            frequency_twist_soll = calculate_frequencies(filtered_rows, TWIST_SOLL_MAPPING)
            frequency_accel_ist = calculate_frequencies(filtered_rows, ACCEL_MAPPING)
            frequency_joint_states = calculate_frequencies(filtered_rows, JOINT_MAPPING)
            # Prepare data for bahn_info table
            
            bahn_info_data = (
                bahn_id,
                robot_model,
                bahnplanung,
                recording_date,
                start_time,
                end_time,
                source_data_ist,
                source_data_soll,
                record_filename,
                number_of_points,
                frequency_pose_ist,
                frequency_position_soll,
                frequency_orientation_soll,
                frequency_twist_ist,
                frequency_twist_soll,
                frequency_accel_ist,
                frequency_joint_states,
                calibration_run
            )

            if upload_database:
                # Insert data into bahn_info table
                #insert_accel_data(conn, accel_data)
                #insert_joint_data(conn, joint_data)
                #insert_pose_data(conn, pose_data)
                #insert_orientation_soll_data(conn, orientation_soll_data)
                #insert_position_soll_data(conn, position_soll_data)
                #insert_twist_ist_data(conn, twist_ist_data)
                #insert_twist_soll_data(conn, twist_soll_data)
                #insert_rapid_events_data(conn, rapid_events_data)
                insert_bahn_info(conn, bahn_info_data)

            else:
                print("Data processing completed. Database upload skipped.")
        
        print(f"\nTotal rows processed in range: {total_rows}")
        print(f"Rows processed for acceleration data: {accel_rows_processed}")
        print(f"Rows processed for velocity ist data: {twist_ist_rows_processed}")
        print(f"Rows processed for velocity soll data: {twist_soll_rows_processed}")
        print(f"Rows processed for joint data: {joint_rows_processed}")
        print(f"Rows processed for pose data: {pose_rows_processed}")
        print(f"Rows processed for orientation soll data: {orientation_soll_rows_processed}")
        print(f"Rows processed for position soll data: {position_soll_rows_processed}")
        print(f"Rows processed for rapid events data: {rapid_events_rows_processed}")

        print(f"Rows skipped for acceleration data: {total_rows - accel_rows_processed}")
        print(f"Rows skipped for velocity ist data: {total_rows - twist_ist_rows_processed}")
        print(f"Rows skipped for velocity soll data: {total_rows - twist_soll_rows_processed}")
        print(f"Rows skipped for joint data: {total_rows - joint_rows_processed}")
        print(f"Rows skipped for pose data: {total_rows - pose_rows_processed}")
        print(f"Rows skipped for orientation soll data: {total_rows - orientation_soll_rows_processed}")
        print(f"Rows skipped for position soll data: {total_rows - position_soll_rows_processed}")
        print(f"Rows skipped for rapid events data: {total_rows - rapid_events_rows_processed}")
    
    except Exception as e:
        print(f"An error occurred while processing the CSV: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python script.py <path_to_csv_file> <robot_model> <bahnplanung> <source_data_ist> <source_data_soll>")
        sys.exit(1)
    
    csv_file_path = sys.argv[1]
    robot_model = sys.argv[2]
    bahnplanung = sys.argv[3]
    source_data_ist = sys.argv[4]
    source_data_soll = sys.argv[5]

    upload_database = get_user_choice()
    process_csv(csv_file_path, upload_database, robot_model, bahnplanung, source_data_ist, source_data_soll)