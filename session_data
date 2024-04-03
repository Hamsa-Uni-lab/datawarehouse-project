import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import fastf1
import time
import pandas as pd
from airflow.hooks.S3_hook import S3Hook
import logging
import s3fs
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

# Specify your AWS credentials if not already configured via environment variables or IAM roles
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_session_token = os.getenv("AWS_SESSION_TOKEN")

def read_csv_files_from_s3(bucket_name, prefix, local_directory):
    # Setting up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Check if there are any existing CSV files in the local directory
    existing_csv_files = [file for file in os.listdir(local_directory) if file.endswith('.csv')]

    # If existing CSV files are found, append timestamp and move them to the archive folder
    if existing_csv_files:
        archive_folder = os.path.join(local_directory, 'archives')
        os.makedirs(archive_folder, exist_ok=True)
        
        for csv_file in existing_csv_files:
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            archived_csv_file = f"{csv_file.split('.')[0]}_{timestamp}.csv"
            os.rename(os.path.join(local_directory, csv_file), os.path.join(archive_folder, archived_csv_file))
            logger.info(f"Archived existing CSV file: {csv_file}")

    s3_hook = S3Hook(aws_conn_id='my_aws_connection')
    s3_files = s3_hook.list_keys(bucket_name, prefix=prefix)
    if not s3_files:
        logger.info(f"No CSV files found in S3 bucket: {bucket_name} with prefix: {prefix}")
        return
    else:
        logger.info(str(s3_files))

    for s3_file in s3_files:
        if s3_file.endswith('.csv'):
            logger.info(f"Reading CSV file: {s3_file}")
            s3_key = f"{bucket_name}/{s3_file}"

            # Create a file system object using s3fs and read the file into a DataFrame
            s3 = s3fs.S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key, token=aws_session_token)
            with s3.open(s3_key, 'rb') as f:
                df = pd.read_csv(f)

            logger.info(df.head())

            # Save the DataFrame as a CSV file locally
            local_file_path = os.path.join(local_directory, s3_file.split('/')[-1])
            df.to_csv(local_file_path, index=False)
            logger.info(f"CSV file saved locally: {local_file_path}")

def fetch_session_data(year, location, identifier, csv_file_path):
    # Setting up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    all_sessions = pd.DataFrame(columns=['Year', 'Location', 'Identifier', 'DriverNumber', 'BroadcastName', 'Abbreviation', 'DriverId', 'TeamName',
                                            'TeamColor', 'TeamId', 'FirstName', 'LastName', 'FullName',
                                            'HeadshotUrl', 'CountryCode', 'Position', 'ClassifiedPosition',
                                            'GridPosition', 'Q1', 'Q2', 'Q3', 'Time', 'Status', 'Points'])

    try:
        session_data = fastf1.get_session(year, location, identifier)
        session_data.load()

        session_data_df = session_data.results
        session_data_df['Year'] = year
        session_data_df['Location'] = location
        session_data_df['Identifier'] = identifier

        if not session_data_df.empty:
            if not os.path.exists(csv_file_path) or os.path.getsize(csv_file_path) == 0:
                all_sessions.to_csv(csv_file_path, mode='a', index=False, header=True)
                logger.info(f"Column headers: {all_sessions} written to: {csv_file_path}")

            all_sessions = pd.concat([all_sessions, session_data_df], ignore_index=True)
            all_sessions.to_csv(csv_file_path, mode='a', index=False, header=False)
            logger.info(f"Session data appended to: {csv_file_path}")

    except ValueError as e:
        logger.info(f"Error: {e}")
    except KeyError as e:
        logger.info(f"KeyError: 'DriverNumber' key not found in driver_info: {e}")
    except AttributeError as e:
        logger.info(f"AttributeError: 'driver_info' attribute not found in session_data: {e}")

def fetch_all_sessions():
    # Setting up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    identifiers = ['R', 'Q', 'S', 'SQ', 'SS', 'FP1', 'FP2', 'FP3']
    csv_file_path = "/opt/airflow/dags/session_data_1950_to_1955.csv"
    api_calls_in_hour = 0
    start_time = time.time()
    
    # Check if CSV file exists and get the last date recorded
    if os.path.isfile(csv_file_path):
        last_date_df = pd.read_csv(csv_file_path, usecols=['Year', 'Location', 'Identifier'])
        last_year, last_location, last_identifier = last_date_df.iloc[-1]
        last_year = int(last_year)
    else:
        last_year, last_location, last_identifier = None, None, None
    
    for year in range(last_year or 1950, 1955):
        logger.info(f"YEAR>>> {year}")
        event_schedule = fastf1.get_event_schedule(year)
        unique_locations = event_schedule['Location'].unique()
        logger.info(f"LOCATION>>> {unique_locations}")

        for location in unique_locations:
            logger.info(f"location>>> {location}")
            
            # Start from the last recorded date in the CSV file
            if year == last_year and location == last_location:
                idx = identifiers.index(last_identifier)
                identifiers = identifiers[idx:]
                
            for identifier in identifiers:
                fetch_session_data(year, location, identifier, csv_file_path)
                api_calls_in_hour += 1
                
                if api_calls_in_hour == 190:
                    logger.info("API LIMIT REACHED 190")
                    elapsed_time = time.time() - start_time
                    if elapsed_time < 60:
                        time.sleep(3600 - elapsed_time)
                    start_time = time.time()
                    api_calls_in_hour = 0

def copy_csv_to_s3(local_file, s3_bucket, s3_key):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    s3_hook = S3Hook(aws_conn_id='my_aws_connection') 
    s3_hook.load_file(filename=local_file, key=s3_key, bucket_name=s3_bucket, replace=True)

def load_to_redshift(local_file_path):
    # Read the header names from the CSV file
    if os.path.exists(local_file_path):
        with open(local_file_path, 'r') as file:
            header_line = file.readline().strip()
            column_names = header_line.split(',')

       # Generate the SQL query to create the table
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS session_data (
        {', '.join([f'{column} VARCHAR(MAX)' for column in column_names])}
        );
        """

        # Execute the SQL query to create the table
        create_table_task = PostgresOperator(
            task_id='create_table_task',
            postgres_conn_id='redshift_connection',  # Airflow connection ID for Redshift
            sql=create_table_query,
            autocommit=True  # Set autocommit to True to execute the query
        )

        # Execute the SQL query to copy data from CSV file to Redshift table
        copy_table_task = PostgresOperator(
            task_id='copy_table_task',

            postgres_conn_id='redshift_connection',  # Airflow connection ID for Redshift
            sql=f"""
            COPY session_data 
            FROM 's3://dwdag/transformed/session_data_1950_to_1955.csv'
            ACCESS_KEY_ID '{aws_access_key_id}'
            SECRET_ACCESS_KEY '{aws_secret_access_key}'
            SESSION_TOKEN '{aws_session_token}'
            CSV
            IGNOREHEADER 1
            """
        )

        return [create_table_task, copy_table_task]
    return []

with DAG('Updated_session_data',    
        default_args=default_args,
         schedule_interval="0 0 * * *",
         start_date=datetime(2024, 3, 2),
         catchup=False) as dag:

    read_csv_files_task = PythonOperator(
        task_id='read_csv_files_task',
        python_callable=read_csv_files_from_s3,
        op_kwargs={'bucket_name': 'dwdag', 'prefix': 'transformed/', 'local_directory': '/opt/airflow/dags/'}
    )

    fetch_all_sessions_task = PythonOperator(
        task_id='fetch_all_sessions',
        python_callable=fetch_all_sessions
    )

    copy_task = PythonOperator(
        task_id='copy_task',
        python_callable=copy_csv_to_s3,
        op_kwargs={'local_file': '/opt/airflow/dags/session_data_1950_to_1955.csv', 's3_bucket': 'dwdag', 's3_key': 'transformed/session_data_1950_to_1955.csv'}
    )

    load_tasks = load_to_redshift('/opt/airflow/dags/session_data_1950_to_1955.csv')

if load_tasks:
    read_csv_files_task >> fetch_all_sessions_task >> copy_task >> load_tasks[0] >> load_tasks[1]
else:
    read_csv_files_task >> fetch_all_sessions_task >> copy_task
