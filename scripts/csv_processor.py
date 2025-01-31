import csv
import os
import re
import numpy as np
import math
from datetime import datetime, timedelta
from tqdm import tqdm
from db_config import MAPPINGS
from db_config_pick_place import MAPPINGS_PICKPLACE
import numpy as np
from scipy.optimize import curve_fit
class CSVProcessor:
    def __init__(self, file_path):
        self.file_path = file_path
        self.mappings = MAPPINGS
        self.mappings_pickplace=MAPPINGS_PICKPLACE
    
    def process_csv(self, upload_database, robot_model, bahnplanung, source_data_ist, source_data_soll,pickplace):
        """Process the CSV file and prepare data for database insertion."""
        try:
            with open(self.file_path, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)  # Convert the reader to a list to process in reverse order
                
            if pickplace==False:
                # Find the first and last appearances of 'ap_x'
                first_ap_x_index = next((i for i, row in enumerate(rows) if row.get('ap_x', '').strip()), None)
                last_ap_x_index = next((i for i, row in enumerate(reversed(rows)) if row.get('ap_x', '').strip()), None)

                if first_ap_x_index is None or last_ap_x_index is None:
                    # If 'ap_x' is not found, use the entire dataset
                    first_timestamp = self.convert_timestamp(rows[0]['timestamp'])
                    last_timestamp = self.convert_timestamp(rows[-1]['timestamp'])
                else:
                    # Calculate the actual last index from the end of the list
                    last_ap_x_index = len(rows) - 1 - last_ap_x_index

                    # Get timestamps for the first and last 'ap_x' appearances
                    first_ap_x_timestamp = self.convert_timestamp(rows[first_ap_x_index]['timestamp'])
                    last_ap_x_timestamp = self.convert_timestamp(rows[last_ap_x_index]['timestamp'])

                    # Extend the range by 1 second on both sides
                    first_timestamp = first_ap_x_timestamp - timedelta(seconds=1)
                    last_timestamp = last_ap_x_timestamp + timedelta(seconds=1)

                # Find the indices for the extended range
                start_index = next(
                    (i for i, row in enumerate(rows) if self.convert_timestamp(row['timestamp']) >= first_timestamp), 0)
                end_index = next((i for i in range(len(rows) - 1, -1, -1) if
                                  self.convert_timestamp(rows[i]['timestamp']) <= last_timestamp), len(rows) - 1)

                filtered_rows = rows[start_index:end_index + 1]

                recording_date = self.convert_timestamp(rows[0]['timestamp'])
                start_time = self.convert_timestamp(filtered_rows[0]['timestamp'])
                end_time = self.convert_timestamp(filtered_rows[-1]['timestamp'])

                print(f"Recording Date: {recording_date}")
                print(f"Start Time: {start_time}")
                print(f"End Time: {end_time}")

                total_rows = len(filtered_rows)
                processed_data = {key: [] for key in self.mappings.keys()}
                processed_data['bahn_info_data'] = []

                bahn_id = None
                segment_counter = 0
                current_segment_id = None

                record_filename = self.extract_record_part()

                rows_processed = {key: 0 for key in self.mappings.keys()}
                calibration_run = "calibration_run" in self.file_path

                point_counts = {
                    'np_ereignisse': 0,
                    'np_pose_ist': 0,
                    'np_twist_ist': 0,
                    'np_accel_ist': 0,
                    'np_pos_soll': 0,
                    'np_orient_soll': 0,
                    'np_twist_soll': 0,
                    'np_jointstates': 0
                }

                for row in tqdm(filtered_rows, total=total_rows, desc="Processing CSV", unit="row"):
                    timestamp = row['timestamp']
                    if bahn_id is None:
                        bahn_id = timestamp[:9]
                        current_segment_id = f"{bahn_id}_{segment_counter}"

                    if row.get('ap_x', '').strip():
                        point_counts['np_ereignisse'] += 1
                        segment_counter += 1
                        current_segment_id = f"{bahn_id}_{segment_counter}"

                    for mapping_name, mapping in self.mappings.items():
                        processed_data[mapping_name], rows_processed[mapping_name], point_counts = self.process_mapping(
                            row, mapping, bahn_id, current_segment_id, timestamp,
                            source_data_ist if mapping_name in ['ACCEL_MAPPING', 'POSE_MAPPING',
                                                                'TWIST_IST_MAPPING'] else source_data_soll,
                            processed_data[mapping_name], rows_processed[mapping_name],
                            mapping_name, point_counts
                        )

                frequencies = {
                    f"frequency_{key.lower().replace('_mapping', '')}": self.calculate_frequencies(filtered_rows,
                                                                                                   mapping)
                    for key, mapping in self.mappings.items()
                }

                if frequencies['frequency_pose'] == 0 or rows_processed['POSE_MAPPING'] == 0:
                    source_data_ist = "abb_websocket"

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
                    point_counts['np_ereignisse'],
                    frequencies['frequency_pose'],
                    frequencies['frequency_position_soll'],
                    frequencies['frequency_orientation_soll'],
                    frequencies['frequency_twist_ist'],
                    frequencies['frequency_twist_soll'],
                    frequencies['frequency_accel'],
                    frequencies['frequency_joint'],
                    calibration_run,
                    point_counts['np_pose_ist'],
                    point_counts['np_twist_ist'],
                    point_counts['np_accel_ist'],
                    point_counts['np_pos_soll'],
                    point_counts['np_orient_soll'],
                    point_counts['np_twist_soll'],
                    point_counts['np_jointstates']
                )

                processed_data['bahn_info_data'] = bahn_info_data

                self.print_processing_stats(total_rows, rows_processed, point_counts)

                return processed_data
            else:
                weight=None
                velocity_picking=None
                velocity_handling=None
                processed_data = {key: [] for key in self.mappings_pickplace.keys()}
                processed_data['bahn_info_data'] = []
                
                current_position = 0
                total_rows = len(rows)
                
                # Find the last zero from the end (actual end point)
                final_end_index = total_rows - 1  # Default to last row if no '0' found
                last_one_index = 0
                last_one_index = next((i for i, row in enumerate(reversed(rows)) if row.get('DO_Signal', '').strip() == '1.0'), None)
                if last_one_index is not None:
                    last_one_index = len(rows)-1 - last_one_index
                    final_end_index = next((i for i, row in enumerate(rows[last_one_index:], last_one_index) if row.get('DO_Signal', '').strip() == '0.0'), None)
                    final_end_index += 1

                last_value=0
                count_traj=0
                for i, row in enumerate(rows):
    # Prüft ob DO_Signal '1.0' ist (mit Leerzeichen entfernt)
                    if velocity_picking ==None and row.get('Velocity Picking', '').strip()!="":
                            velocity_picking = int(float(row['Velocity Picking']))
                        
                    if velocity_handling ==None and row.get('Velocity Handling', '').strip()!="":
                        velocity_handling = int(float(row['Velocity Handling']))
                    if weight ==None and row.get('Weight', '').strip()!="":
                            weight = int(float(row['Weight']))
                    if row.get('DO_Signal', '').strip() == '1.0' and last_value!="1.0":
                        count_traj+=1
                        last_value='1.0'
                    elif row.get('DO_Signal', '').strip() == '0.0' and last_value!="0.0":
                        last_value='0.0'
                frequencies = {
                    f"frequency_{key.lower().replace('_mapping', '')}": [0]*count_traj
                    for key, mapping in self.mappings_pickplace.items()
                }
                # Initialize bahn_info_data structure before the loop
                bahn_id=[0]*count_traj
                bahn_start_time=[None]*count_traj
                bahn_end_time=[None]*count_traj
                qx_start=[None]*count_traj
                qy_start=[None]*count_traj
                qz_start=[None]*count_traj
                qw_start=[None]*count_traj
                qx_end=[None]*count_traj
                qy_end=[None]*count_traj
                qz_end=[None]*count_traj
                qw_end=[None]*count_traj
                point_counts = {
                        'np_ereignisse': [0]*count_traj,
                        'np_pose_ist': [0]*count_traj,
                        'np_twist_ist': [0]*count_traj,
                        'np_accel_ist': [0]*count_traj,
                        'np_pos_soll': [0]*count_traj,
                        'np_orient_soll': [0]*count_traj,
                        'np_twist_soll': [0]*count_traj,
                        'np_jointstates': [0]*count_traj,
                    }
                recording_date = self.convert_timestamp(rows[0]['timestamp'])
                bahn_count=0
                handling_height=[None]*count_traj
                x_start_pos=[None]*count_traj
                y_start_pos=[None]*count_traj
                z_start_pos=[None]*count_traj
                x_end_pos=[None]*count_traj
                y_end_pos=[None]*count_traj
                z_end_pos=[None]*count_traj
                while current_position < final_end_index:
                    # Find next bahn start
                    bahn_start = None
                    for i in range(current_position, len(rows)):
                        if rows[i]['DO_Signal'] == '1.0':
                            # Gefunden erste 1, jetzt rückwärts zur ersten 0 gehen
                            temp = i
                            #while temp >= 0 and rows[temp]['DO_Signal'] in ['1.0', '']:
                            #    temp -= 1
                            bahn_start = temp+2  # +1 weil wir die Position nach der letzten 0 wollen
                            break
                    # Find bahn end
                    bahn_end = None
                    for i in range(bahn_start + 1, len(rows)):
                        if rows[i]['DO_Signal'] == '0.0':
                            bahn_end = i+2
                            break
                    
                    if bahn_end is None:
                        break
                    
                    # Get bahn rows
                    bahn_rows = rows[bahn_start:bahn_end]
                    
                    # Get bahn timing information
                    bahn_start_time[bahn_count] = self.convert_timestamp(bahn_rows[0]['timestamp'])
                    bahn_end_time[bahn_count] = self.convert_timestamp(bahn_rows[-1]['timestamp'])
                    
                    # Generate bahn_id from timestamp
                    bahn_id[bahn_count] = f"{bahn_rows[0]['timestamp'][:11]}"
                    
                    # Calculate frequencies for this specific bahn
                    
                    
                    segment_counter = 0
                    segment_last = None
                    direction = None
                    
                    # Process each row in the bahn
                    
                    x_points = []
                    y_points = []   
                    for row in bahn_rows:
                        if segment_counter==3:
                            break
                        timestamp = row['timestamp']
                        current_segment_id = f"{bahn_id[bahn_count]}_{segment_counter}"
                        
                        if segment_counter!=1:
                            direction='linear'
                        elif segment_counter==1:
                            direction = self.calculate_direction(bahn_rows)
                        
                        # Create new segment if ap_x has value
                        if row.get('ap_x', '').strip():
                            if segment_counter==2:
                                x_end_pos[bahn_count]=row['ap_x']
                                y_end_pos[bahn_count]=row['ap_y']
                                z_end_pos[bahn_count]=row['ap_z']
                                qx_end[bahn_count]=row['aq_x']
                                qy_end[bahn_count]=row['aq_y']
                                qz_end[bahn_count]=row['aq_z']
                                qw_end[bahn_count]=row['aq_w']
                                z_start_pos[bahn_count]=row['ap_z']
                            
                            if x_start_pos[bahn_count]==None:
                                x_start_pos[bahn_count]=row['ap_x']
                                y_start_pos[bahn_count]=row['ap_y']
                                qx_start[bahn_count]=row['aq_x']
                                qy_start[bahn_count]=row['aq_y']
                                qz_start[bahn_count]=row['aq_z']
                                qw_start[bahn_count]=row['aq_w']
                            if handling_height[bahn_count]==None and segment_counter==1:
                                handling_height[bahn_count]=row['ap_z']
                            
                            point_counts['np_ereignisse'][bahn_count] += 1
                            segment_counter += 1
                                #z_start_pos[bahn_count]='740'
                        # Process each mapping
                        
                        for mapping_name, mapping in self.mappings_pickplace.items():
                            if mapping_name != 'bahn_info_data':
                                if mapping_name in ['POSE_MAPPING', 'TWIST_IST_MAPPING']: 
                                    data_source = source_data_ist 
                                elif mapping_name in ['ACCEL_MAPPING'] :
                                     data_source = "PI_Sensehat" 
                                else:
                                    data_source = source_data_soll
                                if mapping_name == 'RAPID_EVENTS_MAPPING':
                                    if any(row.get(csv_col, '').strip() for csv_col in mapping):
                                        data_row = [bahn_id[bahn_count], current_segment_id, timestamp]
                                        for csv_col in mapping:
                                            value = row.get(csv_col, '').strip()
                                            data_row.append(value if value else None)
                                        data_row.append(data_source)
                                        data_row.append(direction)
                                        processed_data[mapping_name].append(data_row)
                                else:
                                    if all(row.get(csv_col, '').strip() for csv_col in mapping):
                                        data_row = [bahn_id[bahn_count], current_segment_id, timestamp]
                                        data_row.extend([row[csv_col] for csv_col in mapping])
                                        data_row.append(data_source)
                                        processed_data[mapping_name].append(data_row)
                                        
                                        # Update point counts
                                        if mapping_name == 'POSE_MAPPING':
                                            point_counts['np_pose_ist'][bahn_count] += 1
                                        elif mapping_name == 'TWIST_IST_MAPPING':
                                            point_counts['np_twist_ist'][bahn_count] += 1
                                        elif mapping_name == 'ACCEL_MAPPING':
                                            point_counts['np_accel_ist'][bahn_count] += 1
                                        elif mapping_name == 'POSITION_SOLL_MAPPING':
                                            point_counts['np_pos_soll'][bahn_count] += 1
                                        elif mapping_name == 'ORIENTATION_SOLL_MAPPING':
                                            point_counts['np_orient_soll'][bahn_count] += 1
                                        elif mapping_name == 'TWIST_SOLL_MAPPING':
                                            point_counts['np_twist_soll'][bahn_count] += 1
                                        elif mapping_name == 'JOINT_MAPPING':
                                            point_counts['np_jointstates'][bahn_count] += 1
                    
                    for key, mapping in self.mappings_pickplace.items():
                        k = f"frequency_{key.lower().replace('_mapping', '')}"
                        frequencies[k][bahn_count] = self.calculate_frequencies_pickplace(bahn_rows, mapping)
                    current_position = bahn_end + 1
                    bahn_count+=1
                #f"frequency_{key.lower().replace('_mapping', '')}"[bahn_count]: self.calculate_frequencies_pickplace(bahn_rows, mapping) for key, mapping in self.mappings.items()
                # After processing all rows for this bahn, append the bahn info
                record_filename = self.extract_record_part()
                rows_processed = {key: 0 for key in self.mappings_pickplace.keys()}
                calibration_run = "calibration_run" in self.file_path
                
                bahn_info_data = (
                bahn_id,
                robot_model,
                bahnplanung,
                recording_date,
                bahn_start_time,
                bahn_end_time,
                source_data_ist,
                source_data_soll,
                record_filename,
                point_counts['np_ereignisse'],
                frequencies['frequency_pose'],
                frequencies['frequency_position_soll'],
                frequencies['frequency_orientation_soll'],
                frequencies['frequency_twist_ist'],
                frequencies['frequency_twist_soll'],
                frequencies['frequency_accel'],
                frequencies['frequency_joint'],
                calibration_run,
                point_counts['np_pose_ist'],
                point_counts['np_twist_ist'],
                point_counts['np_accel_ist'],
                point_counts['np_pos_soll'],
                point_counts['np_orient_soll'],
                point_counts['np_twist_soll'],
                point_counts['np_jointstates'],
                weight,
                x_start_pos,
                y_start_pos,
                z_start_pos,
                x_end_pos,
                y_end_pos,
                z_end_pos,
                handling_height,
                velocity_handling,
                velocity_picking,
                qx_start,
                qy_start,
                qz_start,
                qw_start,
                qx_end,
                qy_end,
                qz_end,
                qw_end,
            )
                # Move to next bahn
                
                processed_data['bahn_info_data'] = bahn_info_data

                self.print_processing_stats(total_rows, rows_processed, point_counts)
                
                return processed_data
        except Exception as e:
                    print(f"Error processing CSV: {str(e)}")
                    raise  

    

# Funktion zur Berechnung der Richtung
    def convert_to_float(self,value):
        """Hilfsfunktion zur Umwandlung eines Strings in einen float."""
        try:
            return float(value)
        except ValueError:
            return None  # Falls der Wert nicht in einen float umgewandelt werden kann, None zurückgeben

    def calculate_direction(self, bahn_rows):
        """
        Bestimmt die Richtung basierend auf allen Punkten zwischen Start und Ende in der Tabelle.
        :param bahn_rows: Liste von Dictionaries mit den Schlüsseln 'ps_x' und 'ps_y'
        :return: 'linear', 'circularleft' oder 'circularright'
        """
        # Schritt 1: Erster Punkt (ps_x, ps_y) mit Zahlenwert finden
        first_point = None
        start = None
        for index, row in enumerate(bahn_rows):
            ap_x = self.convert_to_float(row['ap_x'])
            ap_y = self.convert_to_float(row['ap_y'])
            if ap_x is not None and ap_y is not None:
                first_point = (ap_x, ap_y)
                start = index + 2
                break
            
        if not first_point:
            raise ValueError("Kein gültiger erster Punkt gefunden.")
        
        # Schritt 2: Letzter Punkt (ps_x, ps_y) mit Zahlenwert finden
        last_point = None
        end = None
        for index, row in enumerate(bahn_rows[start:]):
            ap_x = self.convert_to_float(row['ap_x'])
            ap_y = self.convert_to_float(row['ap_y'])
            if ap_x is not None and ap_y is not None:
                last_point = (ap_x, ap_y)
                end = index + start
                break
            
        if not last_point:
            raise ValueError("Kein gültiger letzter Punkt gefunden.")
        
        # Schritt 3: Alle gültigen ps_x, ps_y Punkte zwischen Start und Ende sammeln
        points = []
        for row in bahn_rows[start:end]:
            ps_x = self.convert_to_float(row['ps_x'])
            ps_y = self.convert_to_float(row['ps_y'])
            if ps_x is not None and ps_y is not None:
                points.append((ps_x, ps_y))
        
        if not points:
            raise ValueError("Keine gültigen Punkte für die Kurvenbestimmung gefunden.")
        
        # Berechnung der Distanz zwischen dem ersten und letzten Punkt (Referenzgerade)
        x1, y1 = first_point  # Erster Punkt
        x2, y2 = last_point   # Letzter Punkt
    
        # Berechne die Distanz zwischen dem ersten und letzten Punkt
        p1p2_distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)
        
        # Funktion zur Bestimmung des kürzesten Abstands eines Punktes zur Linie
        def get_shortest_distance(xm, ym):
            # Geradengleichung: Ax + By + C = 0
            A = y2 - y1
            B = x1 - x2
            C = x2 * y1 - x1 * y2
            return abs(A * xm + B * ym + C) / math.sqrt(A**2 + B**2)
        
        # Bestimmung des besten Fits (linear oder circular)
        linear_points = []
        circular_left_points = []
        circular_right_points = []
    
        for (ps_x, ps_y) in points:
            shortest_distance = get_shortest_distance(ps_x, ps_y)
            if shortest_distance <= 0.1 * p1p2_distance:
                linear_points.append((ps_x, ps_y))
            else:
                # Bestimme, ob der Punkt links oder rechts der Linie liegt
                side_test = (x2 - x1) * (ps_y - y1) - (y2 - y1) * (ps_x - x1)
                if side_test > 0:
                    circular_left_points.append((ps_x, ps_y))
                else:
                    circular_right_points.append((ps_x, ps_y))
    
    # Entscheidung über die Kurve
        if len(linear_points) > len(circular_left_points) and len(linear_points) > len(circular_right_points):
            return "linear"
        elif len(circular_left_points) > len(circular_right_points):
            return "circularleft"
        else:
            return "circularright"


    
    def process_mapping(self, row, mapping, bahn_id, current_segment_id, timestamp, source_data, data_list,
                            rows_processed, mapping_name, point_counts):
            if mapping_name == 'RAPID_EVENTS_MAPPING':
                if any(row.get(csv_col, '').strip() for csv_col in mapping):
                    data_row = [bahn_id, current_segment_id, timestamp]
                    for csv_col in mapping:
                        value = row.get(csv_col, '').strip()
                        data_row.append(value if value else None)
                    data_row.append(source_data)
                    data_list.append(data_row)
                    rows_processed += 1
            else:
                if all(row.get(csv_col, '').strip() for csv_col in mapping):
                    data_row = [bahn_id, current_segment_id, timestamp]
                    data_row.extend([row[csv_col] for csv_col in mapping])
                    data_row.append(source_data)
                    data_list.append(data_row)
                    rows_processed += 1

                    # Update point counts
                    if mapping_name == 'POSE_MAPPING':
                        point_counts['np_pose_ist'] += 1
                    elif mapping_name == 'TWIST_IST_MAPPING':
                        point_counts['np_twist_ist'] += 1
                    elif mapping_name == 'ACCEL_MAPPING':
                        point_counts['np_accel_ist'] += 1
                    elif mapping_name == 'POSITION_SOLL_MAPPING':
                        point_counts['np_pos_soll'] += 1
                    elif mapping_name == 'ORIENTATION_SOLL_MAPPING':
                        point_counts['np_orient_soll'] += 1
                    elif mapping_name == 'TWIST_SOLL_MAPPING':
                        point_counts['np_twist_soll'] += 1
                    elif mapping_name == 'JOINT_MAPPING':
                        point_counts['np_jointstates'] += 1

            return data_list, rows_processed, point_counts
    @staticmethod
    def convert_timestamp(ts):
        try:
            timestamp_seconds = int(ts) / 1_000_000_000.0
            return datetime.fromtimestamp(timestamp_seconds)
        except ValueError as e:
            print(f"Error converting timestamp {ts}: {e}")
            return None

    def calculate_frequencies(self, rows, column_mapping):
        timestamps = [self.convert_timestamp(row['timestamp']) for row in rows if row.get(list(column_mapping.keys())[0])]
        if not timestamps:
            return 0.0
        diffs = [(timestamps[i + 1] - timestamps[i]).total_seconds() for i in range(len(timestamps) - 1)]
        avg_diff = sum(diffs) / len(diffs) if diffs else 0
        return 1 / avg_diff if avg_diff > 0 else 0.0
    def calculate_frequencies_pickplace(self, rows, column_mapping):
        timestamps = [self.convert_timestamp(row['timestamp']) for row in rows if row.get(list(column_mapping.keys())[0])]
        if not timestamps:
            return 0.0
        diffs = [(timestamps[i + 1] - timestamps[i]).total_seconds() for i in range(len(timestamps) - 1)]
        avg_diff = sum(diffs) / len(diffs) if diffs else 0
        return 1 / avg_diff if avg_diff > 0 else 0.0

    def extract_record_part(self):
        if 'record' in self.file_path:
            filename = os.path.basename(self.file_path)
            match = re.search(r'(record_.*?\d{8}_\d{6})', filename)
            if match:
                return match.group(1)
        return None
    

    @staticmethod
    def print_processing_stats(total_rows, rows_processed, point_counts):
        print(f"\nTotal rows processed in range: {total_rows}")
        for key, value in rows_processed.items():
            print(f"Rows processed for {key}: {value}")
            print(f"Rows skipped for {key}: {total_rows - value}")

        print("\nPoint counts:")
        for key, value in point_counts.items():
            print(f"{key}: {value}")