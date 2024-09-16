import psycopg2
from pymongo import MongoClient
from datetime import datetime
import time
import logging
import json

# MongoDB connection string
MONGO_CONNECTION_STRING = 'mongodb+srv://gugafelds:200195Beto@cluster0.su3gj7l.mongodb.net/'

# MongoDB collection details
MONGO_DB = 'robotervermessung'
MONGO_COLLECTION_HEADER = 'header'
MONGO_COLLECTION_DATA = 'data'
MONGO_COLLECTION_METRICS = 'metrics'

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

try:
    # Connect to MongoDB using connection string
    mongo_client = MongoClient(MONGO_CONNECTION_STRING)
    mongo_db = mongo_client[MONGO_DB]
    mongo_collection_header = mongo_db[MONGO_COLLECTION_HEADER]
    mongo_collection_data = mongo_db[MONGO_COLLECTION_DATA]
    mongo_collection_metrics = mongo_db[MONGO_COLLECTION_METRICS]

    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )
    pg_cur = pg_conn.cursor()

    # SQL query to create schema if it does not exist
    create_schema_query = '''
    CREATE SCHEMA IF NOT EXISTS trajectories
    '''
    pg_cur.execute(create_schema_query)
    pg_conn.commit()

    # SQL queries to create tables
    create_header_table_query = '''
    CREATE TABLE IF NOT EXISTS trajectories.trajectories_header (
        id SERIAL PRIMARY KEY,
        data_id VARCHAR(50),
        robot_model VARCHAR(50),
        trajectory_type VARCHAR(50),
        path_solver VARCHAR(50),
        recording_date TIMESTAMP,
        real_robot BOOLEAN,
        number_of_points_ist INT,
        number_of_points_soll INT,
        sample_frequency_ist DOUBLE PRECISION,
        sample_frequency_soll DOUBLE PRECISION,
        source_data_ist VARCHAR(50),
        source_data_soll VARCHAR(50),
        evaluation_source VARCHAR(50)
    )
    '''
    pg_cur.execute(create_header_table_query)

    create_data_table_query = '''
    CREATE TABLE IF NOT EXISTS trajectories.trajectories_data (
        id SERIAL PRIMARY KEY,
        trajectory_header_id VARCHAR(50),
        timestamp_ist TEXT[],
        x_ist DOUBLE PRECISION[],
        y_ist DOUBLE PRECISION[],
        z_ist DOUBLE PRECISION[],
        tcp_velocity_ist DOUBLE PRECISION[],
        tcp_acceleration DOUBLE PRECISION[],
        qx_ist DOUBLE PRECISION[],
        qy_ist DOUBLE PRECISION[],
        qz_ist DOUBLE PRECISION[],
        qw_ist DOUBLE PRECISION[],
        timestamp_soll TEXT[],
        x_soll DOUBLE PRECISION[],
        y_soll DOUBLE PRECISION[],
        z_soll DOUBLE PRECISION[],
        qx_soll DOUBLE PRECISION[],
        qy_soll DOUBLE PRECISION[],
        qz_soll DOUBLE PRECISION[],
        qw_soll DOUBLE PRECISION[],
        tcp_velocity_soll DOUBLE PRECISION[],
        joint_state_soll DOUBLE PRECISION[][],
        events_soll DOUBLE PRECISION[][]
    )
    '''
    pg_cur.execute(create_data_table_query)

    create_metrics_lcss_table_query = '''
    CREATE TABLE IF NOT EXISTS trajectories.trajectories_metrics_lcss (
        id SERIAL PRIMARY KEY,
        trajectory_header_id VARCHAR(50),
        lcss_max_distance DOUBLE PRECISION,
        lcss_average_distance DOUBLE PRECISION,
        lcss_distances DOUBLE PRECISION[],
        lcss_X DOUBLE PRECISION[],
        lcss_Y DOUBLE PRECISION[],
        lcss_accdist DOUBLE PRECISION[],
        lcss_path DOUBLE PRECISION[],
        lcss_score DOUBLE PRECISION,
        lcss_threshold DOUBLE PRECISION,
        metric_type VARCHAR(50)
    )
    '''
    pg_cur.execute(create_metrics_lcss_table_query)

    create_metrics_dtw_johnen_table_query = '''
    CREATE TABLE IF NOT EXISTS trajectories.trajectories_metrics_dtw_johnen (
        id SERIAL PRIMARY KEY,
        trajectory_header_id VARCHAR(50),
        dtw_max_distance DOUBLE PRECISION,
        dtw_average_distance DOUBLE PRECISION,
        dtw_distances DOUBLE PRECISION[],
        dtw_X DOUBLE PRECISION[],
        dtw_Y DOUBLE PRECISION[],
        dtw_accdist DOUBLE PRECISION[],
        dtw_path DOUBLE PRECISION[],
        metric_type VARCHAR(50)
    )
    '''
    pg_cur.execute(create_metrics_dtw_johnen_table_query)

    create_metrics_discrete_frechet_table_query = '''
    CREATE TABLE IF NOT EXISTS trajectories.trajectories_metrics_discrete_frechet (
        id SERIAL PRIMARY KEY,
        trajectory_header_id VARCHAR(50),
        frechet_max_distance DOUBLE PRECISION,
        frechet_average_distance DOUBLE PRECISION,
        frechet_distances DOUBLE PRECISION[],
        frechet_matrix DOUBLE PRECISION[],
        frechet_path DOUBLE PRECISION[],
        metric_type VARCHAR(50)
    )
    '''
    pg_cur.execute(create_metrics_discrete_frechet_table_query)

    create_metrics_dtw_standard_table_query = '''
    CREATE TABLE IF NOT EXISTS trajectories.trajectories_metrics_dtw_standard (
        id SERIAL PRIMARY KEY,
        trajectory_header_id VARCHAR(50),
        dtw_max_distance DOUBLE PRECISION,
        dtw_average_distance DOUBLE PRECISION,
        dtw_distances DOUBLE PRECISION[],
        dtw_X DOUBLE PRECISION[],
        dtw_Y DOUBLE PRECISION[],
        dtw_accdist DOUBLE PRECISION[],
        dtw_path DOUBLE PRECISION[],
        metric_type VARCHAR(50)
    )
    '''
    pg_cur.execute(create_metrics_dtw_standard_table_query)

    create_metrics_euclidean_table_query = '''
    CREATE TABLE IF NOT EXISTS trajectories.trajectories_metrics_euclidean (
        id SERIAL PRIMARY KEY,
        trajectory_header_id VARCHAR(50),
        euclidean_distances DOUBLE PRECISION[],
        euclidean_max_distance DOUBLE PRECISION,
        euclidean_average_distance DOUBLE PRECISION,
        euclidean_standard_deviation DOUBLE PRECISION,
        euclidean_intersections JSONB,
        metric_type VARCHAR(50)
    )
    '''
    pg_cur.execute(create_metrics_euclidean_table_query)

    pg_conn.commit()

    # Function to normalize MongoDB document for header collection
    def normalize_header_document(doc):
        return {
            'data_id': doc.get('data_id', str(round(time.time()))),
            'robot_model': doc.get('robot_model', ''),
            'trajectory_type': doc.get('trajectory_type', ''),
            'path_solver': doc.get('path_solver', ''),
            'recording_date': doc.get('recording_date', datetime.utcnow()),
            'real_robot': doc.get('real_robot', False),
            'number_of_points_ist': doc.get('number_of_points_ist', 0),
            'number_of_points_soll': doc.get('number_of_points_soll', 0),
            'sample_frequency_ist': float(doc.get('sample_frequency_ist', 0.0)) if doc.get('sample_frequency_ist') else None,
            'sample_frequency_soll': float(doc.get('sample_frequency_soll', 0.0)) if doc.get('sample_frequency_soll') else None,
            'source_data_ist': doc.get('source_data_ist', ''),
            'source_data_soll': doc.get('source_data_soll', ''),
            'evaluation_source': doc.get('evaluation_source', '')
        }

    # Function to normalize MongoDB document for data collection
    def normalize_data_document(doc):
        normalized_doc = {
            'trajectory_header_id': doc.get('trajectory_header_id', ''),
            'timestamp_ist': doc.get('timestamp_ist', []),
            'x_ist': doc.get('x_ist', []),
            'y_ist': doc.get('y_ist', []),
            'z_ist': doc.get('z_ist', []),
            'tcp_velocity_ist': doc.get('tcp_velocity_ist', []),
            'tcp_acceleration': doc.get('tcp_acceleration', []),
            'qx_ist': doc.get('qx_ist', []),
            'qy_ist': doc.get('qy_ist', []),
            'qz_ist': doc.get('qz_ist', []),
            'qw_ist': doc.get('qw_ist', []),
            'timestamp_soll': doc.get('timestamp_soll', []),
            'x_soll': doc.get('x_soll', []),
            'y_soll': doc.get('y_soll', []),
            'z_soll': doc.get('z_soll', []),
            'qx_soll': doc.get('qx_soll', []),
            'qy_soll': doc.get('qy_soll', []),
            'qz_soll': doc.get('qz_soll', []),
            'qw_soll': doc.get('qw_soll', []),
            'tcp_velocity_soll': None,
            'joint_state_soll': doc.get('joint_state_soll', []),
            'events_soll': doc.get('events_soll', [])
        }

        try:
            # Handle tcp_velocity_soll based on its type
            if doc.get('tcp_velocity_soll') is not None:
                if isinstance(doc['tcp_velocity_soll'], list):
                    normalized_doc['tcp_velocity_soll'] = [float(value) for value in doc['tcp_velocity_soll']]
                else:
                    normalized_doc['tcp_velocity_soll'] = [float(doc['tcp_velocity_soll'])]  # Convert single value to list of float
        except Exception as e:
            logging.error(f"Error normalizing document: {e}")
            logging.error(f"Document: {doc}")

        # Convert empty lists to None (optional step)
        normalized_doc = {k: v if v else None for k, v in normalized_doc.items()}

        return normalized_doc

    # Function to normalize MongoDB document for metrics collection (lcss)
    def normalize_metrics_lcss_document(doc):
        return {
            'trajectory_header_id': doc.get('trajectory_header_id', ''),
            'lcss_max_distance': doc.get('lcss_max_distance', None),
            'lcss_average_distance': doc.get('lcss_average_distance', None),
            'lcss_distances': doc.get('lcss_distances', []),
            'lcss_X': doc.get('lcss_X', []),
            'lcss_Y': doc.get('lcss_Y', []),
            'lcss_accdist': doc.get('lcss_accdist', []),
            'lcss_path': doc.get('lcss_path', []),
            'lcss_score': doc.get('lcss_score', None),
            'lcss_threshold': doc.get('lcss_threshold', None)
        }

    # Function to normalize MongoDB document for metrics collection (dtw_johnen)
    def normalize_metrics_dtw_johnen_document(doc):
        return {
            'trajectory_header_id': doc.get('trajectory_header_id', ''),
            'dtw_max_distance': doc.get('dtw_max_distance', None),
            'dtw_average_distance': doc.get('dtw_average_distance', None),
            'dtw_distances': doc.get('dtw_distances', []),
            'dtw_X': doc.get('dtw_X', []),
            'dtw_Y': doc.get('dtw_Y', []),
            'dtw_accdist': doc.get('dtw_accdist', []),
            'dtw_path': doc.get('dtw_path', []),
            'metric_type': doc.get('metric_type', 'dtw_johnen')
        }

    # Updated function to normalize MongoDB document for metrics collection (euclidean)
    def normalize_metrics_euclidean_document(doc):
        normalized_doc = {
            'trajectory_header_id': doc.get('trajectory_header_id', ''),
            'euclidean_distances': doc.get('euclidean_distances', []),
            'euclidean_max_distance': doc.get('euclidean_max_distance', None),
            'euclidean_average_distance': doc.get('euclidean_average_distance', None),
            'euclidean_standard_deviation': doc.get('euclidean_standard_deviation', None),
            'metric_type': doc.get('metric_type', 'euclidean')
        }
        
        # Convert euclidean_intersections to JSON string
        euclidean_intersections = doc.get('euclidean_intersections', [])
        try:
            normalized_doc['euclidean_intersections'] = json.dumps(euclidean_intersections)
        except TypeError as e:
            print(f"Error converting euclidean_intersections to JSON: {e}")
            print(f"euclidean_intersections: {euclidean_intersections}")
            normalized_doc['euclidean_intersections'] = '[]'  # Fallback to empty array if conversion fails
        
        return normalized_doc

    # Function to normalize MongoDB document for metrics collection (discrete_frechet)
    def normalize_metrics_discrete_frechet_document(doc):
        return {
            'trajectory_header_id': doc.get('trajectory_header_id', ''),
            'frechet_max_distance': doc.get('frechet_max_distance', None),
            'frechet_average_distance': doc.get('frechet_average_distance', None),
            'frechet_distances': doc.get('frechet_distances', []),
            'frechet_matrix': doc.get('frechet_matrix', []),
            'frechet_path': doc.get('frechet_path', []),
            'metric_type': doc.get('metric_type', 'discrete_frechet')
        }

    # Function to normalize MongoDB document for metrics collection (dtw_standard)
    def normalize_metrics_dtw_standard_document(doc):
        return {
            'trajectory_header_id': doc.get('trajectory_header_id', ''),
            'dtw_max_distance': doc.get('dtw_max_distance', None),
            'dtw_average_distance': doc.get('dtw_average_distance', None),
            'dtw_distances': doc.get('dtw_distances', []),
            'dtw_X': doc.get('dtw_X', []),
            'dtw_Y': doc.get('dtw_Y', []),
            'dtw_accdist': doc.get('dtw_accdist', []),
            'dtw_path': doc.get('dtw_path', []),
            'metric_type': doc.get('metric_type', 'dtw_standard')
        }

    # Fetch data from MongoDB header collection and insert into PostgreSQL
    for doc in mongo_collection_header.find():
        normalized_doc = normalize_header_document(doc)

        # Generate dynamic insert query based on the fields present
        columns = ', '.join(normalized_doc.keys())
        values = ', '.join([f"%({k})s" for k in normalized_doc.keys()])
        insert_header_query = f'''
        INSERT INTO trajectories.trajectories_header ({columns}) 
        VALUES ({values})
        '''

        pg_cur.execute(insert_header_query, normalized_doc)
        pg_conn.commit()

    print("Header data migration completed successfully.")

    # # Fetch data from MongoDB data collection and insert into PostgreSQL
    # for doc in mongo_collection_data.find():
    #     normalized_doc = normalize_data_document(doc)

    #     # Generate dynamic insert query based on the fields present
    #     columns = ', '.join(normalized_doc.keys())
    #     values = ', '.join([f"%({k})s" for k in normalized_doc.keys()])
    #     insert_data_query = f'''
    #     INSERT INTO trajectories.trajectories_data ({columns}) 
    #     VALUES ({values})
    #     '''

    #     pg_cur.execute(insert_data_query, normalized_doc)
    #     pg_conn.commit()

    # print("Data data migration completed successfully.")

    # # Fetch and insert LCSS metrics data
    # for doc in mongo_collection_metrics.find({'metric_type': 'lcss'}):
    #     normalized_doc = normalize_metrics_lcss_document(doc)

    #     columns = ', '.join(normalized_doc.keys())
    #     values = ', '.join([f"%({k})s" for k in normalized_doc.keys()])
    #     insert_metrics_lcss_query = f'''
    #     INSERT INTO trajectories.trajectories_metrics_lcss ({columns}) 
    #     VALUES ({values})
    #     '''

    #     pg_cur.execute(insert_metrics_lcss_query, normalized_doc)
    #     pg_conn.commit()

    # print("Metrics data migration (LCSS) completed successfully.")

    # # Fetch and insert DTW Johnen metrics data
    # for doc in mongo_collection_metrics.find({'metric_type': 'dtw_johnen'}):
    #     normalized_doc = normalize_metrics_dtw_johnen_document(doc)

    #     columns = ', '.join(normalized_doc.keys())
    #     values = ', '.join([f"%({k})s" for k in normalized_doc.keys()])
    #     insert_metrics_dtw_johnen_query = f'''
    #     INSERT INTO trajectories.trajectories_metrics_dtw_johnen ({columns}) 
    #     VALUES ({values})
    #     '''

    #     pg_cur.execute(insert_metrics_dtw_johnen_query, normalized_doc)
    #     pg_conn.commit()

    # print("Metrics data migration (DTW Johnen) completed successfully.")

    # # Fetch and insert Discrete Frechet metrics data
    # for doc in mongo_collection_metrics.find({'metric_type': 'discrete_frechet'}):
    #     normalized_doc = normalize_metrics_discrete_frechet_document(doc)

    #     columns = ', '.join(normalized_doc.keys())
    #     values = ', '.join([f"%({k})s" for k in normalized_doc.keys()])
    #     insert_metrics_discrete_frechet_query = f'''
    #     INSERT INTO trajectories.trajectories_metrics_discrete_frechet ({columns}) 
    #     VALUES ({values})
    #     '''

    #     pg_cur.execute(insert_metrics_discrete_frechet_query, normalized_doc)
    #     pg_conn.commit()

    # print("Metrics data migration (Discrete Frechet) completed successfully.")

    # # Fetch and insert DTW Standard metrics data
    # for doc in mongo_collection_metrics.find({'metric_type': 'dtw_standard'}):
    #     normalized_doc = normalize_metrics_dtw_standard_document(doc)

    #     columns = ', '.join(normalized_doc.keys())
    #     values = ', '.join([f"%({k})s" for k in normalized_doc.keys()])
    #     insert_metrics_dtw_standard_query = f'''
    #     INSERT INTO trajectories.trajectories_metrics_dtw_standard ({columns}) 
    #     VALUES ({values})
    #     '''

    #     pg_cur.execute(insert_metrics_dtw_standard_query, normalized_doc)
    #     pg_conn.commit()

    # print("Metrics data migration (DTW Standard) completed successfully.")

    # # Fetch data from MongoDB metrics collection (with metric_type "euclidean") and insert into PostgreSQL
    # for doc in mongo_collection_metrics.find({'metric_type': 'euclidean'}):
    #     try:
    #         normalized_doc = normalize_metrics_euclidean_document(doc)

    #         # Generate dynamic insert query based on the fields present
    #         columns = ', '.join(normalized_doc.keys())
    #         values = ', '.join([f"%({k})s" for k in normalized_doc.keys()])
    #         insert_metrics_euclidean_query = f'''
    #         INSERT INTO trajectories.trajectories_metrics_euclidean ({columns}) 
    #         VALUES ({values})
    #         '''

    #         pg_cur.execute(insert_metrics_euclidean_query, normalized_doc)
    #         pg_conn.commit()
    #     except Exception as e:
    #         print(f"Error inserting euclidean metric document: {e}")
    #         print(f"Problematic document: {doc}")
    #         pg_conn.rollback()  # Rollback the transaction in case of error

    # print("Metrics data migration (Euclidean) completed successfully.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the cursor and connection
    if 'pg_cur' in locals() and pg_cur:
        pg_cur.close()
    if 'pg_conn' in locals() and pg_conn:
        pg_conn.close()

    if 'mongo_client' in locals() and mongo_client:
        mongo_client.close()

print("Migration process completed.")
