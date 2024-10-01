import os
import traceback
from db_gui import CSVToPostgreSQLGUI
from db_operations import DatabaseOperations
from csv_processor import CSVProcessor
from db_config import DB_PARAMS, MAPPINGS


class CSVToPostgreSQLApp:
    def __init__(self):
        self.db_ops = DatabaseOperations(DB_PARAMS)
        self.gui = CSVToPostgreSQLGUI(self.process_csv)

    def process_csv(self, params):
        try:
            file_or_folder_path = params['file_or_folder_path']
            if os.path.isdir(file_or_folder_path):
                self.process_folder(file_or_folder_path, params)
            else:
                self.process_single_file(file_or_folder_path, params)
        except Exception as e:
            self.gui.update_status(f'An error occurred: {str(e)}')
            self.gui.update_status(f'Error details: {traceback.format_exc()}')

    def process_folder(self, folder_path, params):
        csv_files = [f for f in os.listdir(folder_path) if f.startswith('record_') and f.endswith('.csv')]
        total_files = len(csv_files)

        for i, csv_file in enumerate(csv_files, 1):
            file_path = os.path.join(folder_path, csv_file)
            self.gui.update_status(f"Processing file {i} of {total_files}: {csv_file}")
            try:
                self.process_single_file(file_path, params)
            except Exception as e:
                self.gui.update_status(f'Error processing {csv_file}: {str(e)}')
                self.gui.update_status(f'Error details: {traceback.format_exc()}')
            self.gui.update_progress((i / total_files) * 100)

    def process_single_file(self, file_path, params):
        self.gui.update_status(f"Processing file: {os.path.basename(file_path)}")
        csv_processor = CSVProcessor(file_path)
        processed_data = csv_processor.process_csv(
            params['upload_database'],
            params['robot_model'],
            params['bahnplanung'],
            params['source_data_ist'],
            params['source_data_soll']
        )

        if processed_data is None:
            self.gui.update_status("An error occurred while processing the CSV file.")
            return

        if params['upload_database']:
            self.upload_to_database(processed_data)

        self.print_processing_stats(processed_data, params['upload_database'])

    def upload_to_database(self, processed_data):
        self.gui.update_status("Starting database operations...")
        conn = self.db_ops.connect_to_db()
        try:
            operations = [
                ('bahn_info', self.db_ops.insert_bahn_info, processed_data['bahn_info_data']),
                ('rapid_events', self.db_ops.insert_rapid_events_data, processed_data['RAPID_EVENTS_MAPPING']),
                ('pose', self.db_ops.insert_pose_data, processed_data['POSE_MAPPING']),
                ('position_soll', self.db_ops.insert_position_soll_data, processed_data['POSITION_SOLL_MAPPING']),
                ('orientation_soll', self.db_ops.insert_orientation_soll_data,
                 processed_data['ORIENTATION_SOLL_MAPPING']),
                ('twist_ist', self.db_ops.insert_twist_ist_data, processed_data['TWIST_IST_MAPPING']),
                ('twist_soll', self.db_ops.insert_twist_soll_data, processed_data['TWIST_SOLL_MAPPING']),
                ('accel', self.db_ops.insert_accel_data, processed_data['ACCEL_MAPPING']),
                ('joint', self.db_ops.insert_joint_data, processed_data['JOINT_MAPPING'])
            ]

            for i, (operation_name, insert_func, data) in enumerate(operations, 1):
                self.gui.update_status(f"Processing {operation_name} data...")
                insert_func(conn, data)
                self.gui.update_progress(i / len(operations) * 100)
                self.gui.update_status(f"Completed {operation_name} data processing.")

        except Exception as e:
            self.gui.update_status(f'Error during database upload: {str(e)}')
            self.gui.update_status(f'Error details: {traceback.format_exc()}')
        finally:
            conn.close()

    def print_processing_stats(self, processed_data, upload_database):
        try:
            stats = [f"{data_type}: {len(data) if isinstance(data, list) else 1}"
                     for data_type, data in processed_data.items()
                     if data_type != 'bahn_info_data' and data]
            stats_message = "Data processing completed.\n" + "\n".join(stats)
            if upload_database:
                stats_message += "\nData uploaded to PostgreSQL."
            self.gui.update_status(stats_message)
        except Exception as e:
            self.gui.update_status(f'Error printing processing stats: {str(e)}')
            self.gui.update_status(f'Error details: {traceback.format_exc()}')

    def run(self):
        self.gui.run()


if __name__ == "__main__":
    app = CSVToPostgreSQLApp()
    app.run()