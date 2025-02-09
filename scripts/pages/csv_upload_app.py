import streamlit as st
import pandas as pd
import io
from scripts.components.db_config import DB_PARAMS
from scripts.components.db_operations import DatabaseOperations
from scripts.components.csv_processor import CSVProcessor

class CSVUploadApp:
    def __init__(self):
        self.db_ops = DatabaseOperations(DB_PARAMS)

    def run(self):
        st.title('CSV zu PostgreSQL hochladen')

        # File Upload
        st.markdown('<p class="custom-label">Wählen Sie CSV-Datei(en) aus</p>', unsafe_allow_html=True)
        uploaded_files = st.file_uploader("", type="csv", accept_multiple_files=True)

        # Input Fields
        st.markdown('<p class="custom-label">Robotermodell</p>', unsafe_allow_html=True)
        robot_model = st.text_input('', value='abb_irb4400')

        st.markdown('<p class="custom-label">Bahnplanung</p>', unsafe_allow_html=True)
        bahnplanung = st.text_input('', value='abb_steuerung')

        st.markdown('<p class="custom-label">Quelle der IST-Daten</p>', unsafe_allow_html=True)
        source_data_ist = st.text_input('', value='vicon')

        st.markdown('<p class="custom-label">Quelle der SOLL-Daten</p>', unsafe_allow_html=True)
        source_data_soll = st.text_input('', value='abb_websocket')

        # Upload to PostgreSQL Checkbox
        st.markdown('<p class="custom-label">In PostgreSQL hochladen</p>', unsafe_allow_html=True)
        upload_database = st.checkbox('', value=True)

        if st.button('Verarbeitung starten'):
            if uploaded_files:
                self.process_multiple_files(uploaded_files, robot_model, bahnplanung, source_data_ist, source_data_soll, upload_database)
            else:
                st.error('Bitte laden Sie mindestens eine CSV-Datei hoch.')

    def process_multiple_files(self, uploaded_files, robot_model, bahnplanung, source_data_ist, source_data_soll, upload_database):
        total_files = len(uploaded_files)
        for i, uploaded_file in enumerate(uploaded_files, 1):
            st.write(f"Verarbeite Datei {i} von {total_files}: {uploaded_file.name}")
            progress_bar = st.progress(0)
            status_area = st.empty()

            try:
                # Read the uploaded file into a pandas DataFrame
                df = pd.read_csv(uploaded_file)
                filename = uploaded_file.name

                # Convert DataFrame to CSV string
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                csv_string = csv_buffer.getvalue()

                # Create a temporary file-like object
                file_like_object = io.StringIO(csv_string)

                # Process the file-like object
                csv_processor = CSVProcessor(file_like_object)
                processed_data = csv_processor.process_csv(
                    upload_database,
                    robot_model,
                    bahnplanung,
                    source_data_ist,
                    source_data_soll,
                    filename
                )

                if processed_data is None:
                    st.error(f"Bei der Verarbeitung der CSV-Datei ist ein Fehler aufgetreten: {filename}")
                    continue

                if upload_database:
                    self.upload_to_database(processed_data, progress_bar, status_area)

                self.print_processing_stats(processed_data, upload_database, status_area)

            except Exception as e:
                st.error(f'Bei der Verarbeitung von {filename} ist ein Fehler aufgetreten: {str(e)}')

            st.write(f"Verarbeitung der Datei {i} von {total_files} abgeschlossen: {uploaded_file.name}")
            st.write("---")  # Add a separator between file processing results

    def upload_to_database(self, processed_data, progress_bar, status_area):
        status_area.text("Starte Datenbankoperationen...")
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
                status_area.text(f"Verarbeite {operation_name} Daten...")
                insert_func(conn, data)
                progress_bar.progress(i / len(operations))
                status_area.text(f"Verarbeitung von {operation_name} Daten abgeschlossen.")

        except Exception as e:
            st.error(f'Fehler beim Hochladen in die Datenbank: {str(e)}')
        finally:
            conn.close()

    @staticmethod
    def print_processing_stats(self, processed_data, upload_database, status_area):
        try:
            stats = [f"{data_type}: {len(data) if isinstance(data, list) else 1}"
                     for data_type, data in processed_data.items()
                     if data_type != 'bahn_info_data' and data]
            stats_message = "Datenverarbeitung abgeschlossen.\n" + "\n".join(stats)
            if upload_database:
                stats_message += "\nDaten in PostgreSQL hochgeladen."
            status_area.text(stats_message)
        except Exception as e:
            st.error(f'Fehler beim Drucken der Verarbeitungsstatistiken: {str(e)}')


if __name__ == "__main__":
    app = CSVUploadApp()
    app.run()