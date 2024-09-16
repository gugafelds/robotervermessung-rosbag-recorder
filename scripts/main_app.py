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
            csv_processor = CSVProcessor(params['file_path'])
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
                conn = self.db_ops.connect_to_db()
                try:
                    total_operations = 9  # bahn_info and rapid_events
                    operations_completed = 0

                    # Insert bahn_info data
                    self.db_ops.insert_bahn_info(conn, processed_data['bahn_info_data'])
                    operations_completed += 1
                    self.gui.update_progress(operations_completed / total_operations * 100)
                    
                    # Insert rapid_events data
                    self.db_ops.insert_rapid_events_data(conn, processed_data['RAPID_EVENTS_MAPPING'])
                    operations_completed += 1
                    self.gui.update_progress(operations_completed / total_operations * 100)

                    self.db_ops.insert_pose_data(conn, processed_data['POSE_MAPPING'])
                    operations_completed += 1
                    self.gui.update_progress(operations_completed / total_operations * 100)

                    self.db_ops.insert_position_soll_data(conn, processed_data['POSITION_SOLL_MAPPING'])
                    operations_completed += 1
                    self.gui.update_progress(operations_completed / total_operations * 100)

                    self.db_ops.insert_orientation_soll_data(conn, processed_data['ORIENTATION_SOLL_MAPPING'])
                    operations_completed += 1
                    self.gui.update_progress(operations_completed / total_operations * 100)

                    self.db_ops.insert_twist_ist_data(conn, processed_data['TWIST_IST_MAPPING'])
                    operations_completed += 1
                    self.gui.update_progress(operations_completed / total_operations * 100)

                    self.db_ops.insert_twist_soll_data(conn, processed_data['TWIST_SOLL_MAPPING'])
                    operations_completed += 1
                    self.gui.update_progress(operations_completed / total_operations * 100)

                    self.db_ops.insert_accel_data(conn, processed_data['ACCEL_MAPPING'])
                    operations_completed += 1
                    self.gui.update_progress(operations_completed / total_operations * 100)

                    self.db_ops.insert_joint_data(conn, processed_data['JOINT_MAPPING'])
                    operations_completed += 1
                    self.gui.update_progress(operations_completed / total_operations * 100)

                finally:
                    conn.close()

            # Print processing statistics
            stats = [f"{data_type}: {len(data)}" for data_type, data in processed_data.items() if data_type != 'bahn_info_data']
            stats_message = "Data processing completed.\n" + "\n".join(stats)
            if params['upload_database']:
                stats_message += "\nData uploaded to PostgreSQL."
            else:
                stats_message += "\nDatabase upload skipped."

            self.gui.update_status(stats_message)

        except Exception as e:
            self.gui.update_status(f'An error occurred: {str(e)}')

    def run(self):
        self.gui.run()

if __name__ == "__main__":
    app = CSVToPostgreSQLApp()
    app.run()