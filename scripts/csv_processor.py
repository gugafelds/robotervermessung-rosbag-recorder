# csv_processor.py
import csv
import os
import re
from datetime import datetime
from tqdm import tqdm
from db_config import MAPPINGS

class CSVProcessor:
    def __init__(self, file_path):
        self.file_path = file_path
        self.mappings = MAPPINGS

    def process_csv(self, upload_database, robot_model, bahnplanung, source_data_ist, source_data_soll):
        """Process the CSV file and prepare data for database insertion."""
        try:
            with open(self.file_path, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)  # Convert the reader to a list to process in reverse order
                
                # Identify the range of rows to keep based on the 'ap_x' column
                first_ap_x_index = next((i for i, row in enumerate(rows) if row.get('ap_x', '').strip()), None)
                last_ap_x_index = next((i for i, row in enumerate(reversed(rows)) if row.get('ap_x', '').strip()), None)

                if first_ap_x_index is None or last_ap_x_index is None:
                    # Calculate the actual last index from the end of the list
                    first_timestamp = rows[0].get('timestamp')
                    last_timestamp = rows[-1].get('timestamp')

                    recording_date = self.convert_timestamp(first_timestamp)
                    start_time = recording_date
                    end_time = self.convert_timestamp(last_timestamp)

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

                    recording_date = self.convert_timestamp(first_timestamp)
                    start_time = self.convert_timestamp(first_ap_x_timestamp)
                    end_time = self.convert_timestamp(last_ap_x_timestamp)

                    print(recording_date)
                    print(f"Start Time: {start_time}")
                    print(f"End Time: {end_time}")
                    # Filter rows based on the identified range
                    filtered_rows = rows[first_ap_x_index:last_ap_x_index + 1]
                
                total_rows = len(filtered_rows)
                processed_data = {key: [] for key in self.mappings.keys()}
                processed_data['bahn_info_data'] = []

                bahn_id = None
                segment_counter = 0
                current_segment_id = None

                record_filename = self.extract_record_part()
                
                rows_processed = {key: 0 for key in self.mappings.keys()}
                calibration_run = False

                # Initialize number_of_points to 0
                number_of_points = 0

                if "calibration_run" in self.file_path:
                    calibration_run = True
                else:
                    calibration_run = False
                
                for row in tqdm(filtered_rows, total=total_rows, desc="Processing CSV", unit="row"):
                    timestamp = row['timestamp']
                    if bahn_id is None:
                        bahn_id = timestamp[:9]
                        current_segment_id = f"{bahn_id}_{segment_counter}"

                    # Check for new segment based on any RAPID_EVENTS data
                    if any(row.get(col, '').strip() for col in self.mappings['RAPID_EVENTS_MAPPING']):
                        segment_counter += 1
                        current_segment_id = f"{bahn_id}_{segment_counter}"
                        number_of_points += 1
         
                    # Process data for each mapping
                    for mapping_name, mapping in self.mappings.items():
                        processed_data[mapping_name], rows_processed[mapping_name] = self.process_mapping(
                            row, mapping, bahn_id, current_segment_id, timestamp, 
                            source_data_ist if mapping_name in ['ACCEL_MAPPING', 'POSE_MAPPING', 'TWIST_IST_MAPPING'] else source_data_soll,
                            processed_data[mapping_name], rows_processed[mapping_name],
                            mapping_name
                        )
                        
                # Calculate frequencies
                frequencies = {
                    f"frequency_{key.lower().replace('_mapping', '')}": self.calculate_frequencies(filtered_rows, mapping)
                    for key, mapping in self.mappings.items()
                }
                
                # Check if frequency_pose_ist is zero or if no data was found in POSE_MAPPING
                if frequencies['frequency_pose'] == 0 or rows_processed['POSE_MAPPING'] == 0:
                    source_data_ist = "abb_websocket"
                
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
                    frequencies['frequency_pose'],
                    frequencies['frequency_position_soll'],
                    frequencies['frequency_orientation_soll'],
                    frequencies['frequency_twist_ist'],
                    frequencies['frequency_twist_soll'],
                    frequencies['frequency_accel'],
                    frequencies['frequency_joint'],
                    calibration_run
                )

                processed_data['bahn_info_data'] = bahn_info_data

                self.print_processing_stats(total_rows, rows_processed)

                return processed_data

        except Exception as e:
            print(f"An error occurred while processing the CSV: {e}")
            return None

    def process_mapping(self, row, mapping, bahn_id, current_segment_id, timestamp, source_data, data_list, rows_processed, mapping_name):
        if mapping_name == 'RAPID_EVENTS_MAPPING':
            # Check if any RAPID_EVENTS data is present
            if any(row.get(csv_col, '').strip() for csv_col in mapping):
                data_row = [
                    bahn_id,
                    current_segment_id,
                    timestamp
                ]
                for csv_col in mapping:
                    value = row.get(csv_col, '').strip()
                    data_row.append(value if value else None)
                data_row.append(source_data)
                data_list.append(data_row)
                rows_processed += 1
        else:
            # For other mappings, use the original logic
            if all(row.get(csv_col, '').strip() for csv_col in mapping):
                data_row = [
                    bahn_id,
                    current_segment_id,
                    timestamp
                ]
                data_row.extend([row[csv_col] for csv_col in mapping])
                data_row.append(source_data)
                data_list.append(data_row)
                rows_processed += 1
        
        return data_list, rows_processed

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

    def extract_record_part(self):
        if 'record' in self.file_path:
            filename = os.path.basename(self.file_path)
            match = re.search(r'(record_\d{8}_\d{6})', filename)
            if match:
                return match.group(1)
        return None

    def print_processing_stats(self, total_rows, rows_processed):
        print(f"\nTotal rows processed in range: {total_rows}")
        for key, value in rows_processed.items():
            print(f"Rows processed for {key}: {value}")
            print(f"Rows skipped for {key}: {total_rows - value}")