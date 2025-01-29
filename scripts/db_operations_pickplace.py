import psycopg2
from psycopg2 import sql
import numpy as np
class DatabaseOperationsPickPlace:
    def __init__(self, db_params):
        self.db_params = db_params

    def connect_to_db(self):
        try:
            return psycopg2.connect(**self.db_params)
        except (Exception, psycopg2.Error) as error:
            print(f"Error while connecting to PostgreSQL: {error}")
            raise

    def check_bahn_id_exists(self, conn, table_name, bahn_id):
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM handhabungsdaten.{table_name} WHERE bahn_id = %s", (bahn_id,))
            return cur.fetchone()[0] > 0

    def insert_bahn_info(self, conn, data):
        """
        Insert multiple bahn (track) information records into the database.
        
        Args:
            conn: PostgreSQL database connection
            data: Tuple containing arrays of values where array indices correspond to the same record
                First element (data[0]) must be an array of bahn_ids
        """
        # Validate input data
        if not data or not data[0]:
            raise ValueError("No data provided for insertion")
        
        # Get the length of the first array (bahn_ids)
        n_entries = len(data[0])
        
        # Validate that all array inputs have the same length
        array_indices = [i for i, value in enumerate(data) 
                        if isinstance(value, (list, tuple, np.ndarray))]
        
        for idx in array_indices:
            if len(data[idx]) != n_entries:
                raise ValueError(f"Array at index {idx} has different length than bahn_ids array")
        
        # Process each entry
        for i in range(n_entries):
            # Create data tuple for current entry
            current_data = tuple(
                value[i] if isinstance(value, (list, tuple, np.ndarray)) else value
                for value in data
            )
            
            # Skip if bahn_id already exists
            if self.check_bahn_id_exists(conn, 'bahn_info', current_data[0]):
                print(f"bahn_info for bahn_id {current_data[0]} already exists. Skipping insertion.")
                continue
                
            # Prepare insert query
            insert_query = sql.SQL("""
                INSERT INTO handhabungsdaten.bahn_info (
                    bahn_id, robot_model, bahnplanung, recording_date, start_time, end_time,
                    source_data_ist, source_data_soll, record_filename,
                    np_ereignisse, frequency_pose_ist, frequency_position_soll,
                    frequency_orientation_soll, frequency_twist_ist, frequency_twist_soll,
                    frequency_accel_ist, frequency_joint_states, calibration_run, np_pose_ist,
                    np_twist_ist, np_accel_ist, np_pos_soll, np_orient_soll, np_twist_soll,
                    np_jointstates, weight, x_start_pos,
                y_start_pos,
                z_start_pos,
                x_end_pos,
                y_end_pos,
                z_end_pos,
                handling_height,velocity_picking,velocity_handling,qx_start,qy_start,qz_start,qw_start,qx_end,qy_end,qz_end,qw_end                 
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """)
        # Execute insert with transaction handling
            try:
                with conn.cursor() as cur:
                    cur.execute(insert_query, current_data)
                    conn.commit()
                    print(f"Data inserted successfully into bahn_info for bahn_id {current_data[0]}")
            except (Exception, psycopg2.Error) as error:
                conn.rollback()
                print(f"Error inserting data into bahn_info for bahn_id {current_data[0]}: {error}")
                # Optionally re-raise the error if you want to handle it at a higher level
                # raise

    def insert_pose_data(self, conn, data):
        if not data:
            print("No pose data to insert.")
            return

        bahn_id = data[0][0]
        if self.check_bahn_id_exists(conn, 'bahn_pose_ist', bahn_id):
            print(f"Pose data for bahn_id {bahn_id} already exists. Skipping insertion.")
            return

        with conn.cursor() as cur:
            insert_query = sql.SQL("""
                INSERT INTO handhabungsdaten.bahn_pose_ist 
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

    def insert_position_soll_data(self, conn, data):
        if not data:
            print("No position_soll data to insert.")
            return

        bahn_id = data[0][0]
        if self.check_bahn_id_exists(conn, 'bahn_position_soll', bahn_id):
            print(f"Position_soll data for bahn_id {bahn_id} already exists. Skipping insertion.")
            return

        with conn.cursor() as cur:
            insert_query = sql.SQL("""
                INSERT INTO handhabungsdaten.bahn_position_soll 
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

    def insert_twist_soll_data(self, conn, data):
        if not data:
            print("No twist_soll data to insert.")
            return

        bahn_id = data[0][0]
        if self.check_bahn_id_exists(conn, 'bahn_twist_soll', bahn_id):
            print(f"Twist_soll data for bahn_id {bahn_id} already exists. Skipping insertion.")
            return

        with conn.cursor() as cur:
            insert_query = sql.SQL("""
                INSERT INTO handhabungsdaten.bahn_twist_soll 
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

    def insert_orientation_soll_data(self, conn, data):
        if not data:
            print("No orientation_soll data to insert.")
            return

        bahn_id = data[0][0]
        if self.check_bahn_id_exists(conn, 'bahn_orientation_soll', bahn_id):
            print(f"Orientation_soll data for bahn_id {bahn_id} already exists. Skipping insertion.")
            return

        with conn.cursor() as cur:
            insert_query = sql.SQL("""
                INSERT INTO handhabungsdaten.bahn_orientation_soll 
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

    def insert_accel_data(self, conn, data):
        if not data:
            print("No accel data to insert.")
            return

        bahn_id = data[0][0]
        if self.check_bahn_id_exists(conn, 'bahn_accel_ist', bahn_id):
            print(f"Accel data for bahn_id {bahn_id} already exists. Skipping insertion.")
            return

        with conn.cursor() as cur:
            insert_query = sql.SQL("""
                INSERT INTO handhabungsdaten.bahn_accel_ist 
                (bahn_id, segment_id, timestamp, tcp_accel_ist, 
                source_data_ist)
                VALUES (%s, %s, %s, %s, %s)
            """)
            try:
                cur.executemany(insert_query, data)
                conn.commit()
                print(f"Data inserted successfully into bahn_accel_ist")
            except (Exception, psycopg2.Error) as error:
                conn.rollback()
                print(f"Error inserting data into bahn_accel_ist: {error}")

    def insert_twist_ist_data(self, conn, data):
        if not data:
            print("No twist_ist data to insert.")
            return

        bahn_id = data[0][0]
        if self.check_bahn_id_exists(conn, 'bahn_twist_ist', bahn_id):
            print(f"Twist_ist data for bahn_id {bahn_id} already exists. Skipping insertion.")
            return

        with conn.cursor() as cur:
            insert_query = sql.SQL("""
                INSERT INTO handhabungsdaten.bahn_twist_ist 
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

    def insert_rapid_events_data(self, conn, data):
        if not data:
            print("No rapid_events data to insert.")
            return

        bahn_id = data[0][0]
        if self.check_bahn_id_exists(conn, 'bahn_events', bahn_id):
            print(f"Rapid_events data for bahn_id {bahn_id} already exists. Skipping insertion.")
            return

        with conn.cursor() as cur:
            insert_query = sql.SQL("""
                INSERT INTO handhabungsdaten.bahn_events 
                (bahn_id, segment_id, timestamp, x_reached, y_reached, z_reached, qx_reached, qy_reached, qz_reached, qw_reached, source_data_soll, movement_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """)
            try:
                cur.executemany(insert_query, data)
                conn.commit()
                print(f"Data inserted successfully into bahn_events")
            except (Exception, psycopg2.Error) as error:
                conn.rollback()
                print(f"Error inserting data into bahn_events: {error}")

    def insert_joint_data(self, conn, data):
        if not data:
            print("No joint data to insert.")
            return

        bahn_id = data[0][0]
        if self.check_bahn_id_exists(conn, 'bahn_joint_states', bahn_id):
            print(f"Joint data for bahn_id {bahn_id} already exists. Skipping insertion.")
            return

        with conn.cursor() as cur:
            insert_query = sql.SQL("""
                INSERT INTO handhabungsdaten.bahn_joint_states 
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