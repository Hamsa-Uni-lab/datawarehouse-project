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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

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
    s3_files = s3_hook.list_keys("dwdag", prefix="transformed/")
    if not s3_files:
        logger.info(f"No CSV files found in S3 bucket: {bucket_name} with prefix: {prefix}")
        return
    else:
        logger.info(str(s3_files))

    for s3_file in s3_files:
        if s3_file.endswith('.csv'):
            logger.info(f"Reading CSV file: {s3_file}")
            s3_key = "s3://dwdag/transformed/event_schedule_1950_to_2023.csv"
    
            # Specify your AWS credentials if not already configured via environment variables or IAM roles
            aws_access_key_id = 'ASIAWTPZGYEKJSNZ245X'
            aws_secret_access_key = 'LOLvtvJLMxqxByJd2hCzix5tN5S2nqiv1IpQJDMA'
            aws_session_token = 'FwoGZXIvYXdzEFMaDAe/YvrLp43JAdJPJCLFAZ8JU1qx2uwTPkwaH1wrB2F+w26H/TMl3GLkP9h6q80XVkt94mEC8ax61LzByM5F+Y8U4cyJ/G6g1qhhcEbYfp4KKX1o4Uzizh5mCWhYBYHfKItZPZ298kbs2lxjgt2gPjlhsf7U7SbEItLg1VoV7mIn86shH5KDYRncaRcG54LZU624Br8tzqjFDCGaq/gv68M/OlKPs00JzhXEYzXdfhF6Fv4qARQGCCoX2aSNQKuxwIzKk1sSAhsQS+dd5+yho98ZZY3aKLOtwq8GMi1oVVvkZknUvCkYnY7vjLVf+5KRfAV778E/OdfdTofSu82SEEFC4mr65JBiVHw='
            # Specify the S3 bucket and file path
            bucket_name = 'dwdag'
            file_path = 'transformed/event_schedule_1950_to_2023.csv'

            # Create a file system object using s3fs and read the file into a DataFrame
            s3 = s3fs.S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key, token=aws_session_token)
            with s3.open(f"{bucket_name}/{file_path}", 'rb') as f:
                df = pd.read_csv(f)

            #df = pd.read_csv(s3_key)
            logger.info(df.head())

            # Save the DataFrame as a CSV file locally
            local_file_path = f"{local_directory}/{s3_file.split('/')[-1]}"
            df.to_csv(local_file_path, index=False)
            logger.info(f"CSV file saved locally: {local_file_path}")

local_directory = '/opt/airflow/dags'
bucket_name = 's3://dwdag/'
prefix = 'transformed/'

def fetch_and_save_event_schedule():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    # Define the file path for the CSV file
    csv_file_path = "/opt/airflow/dags/event_schedule_1950_to_2023.csv"

    # Check if the CSV file already exists
    if os.path.exists(csv_file_path):
        # If the CSV file exists, load it to get the last date
        last_date_df = pd.read_csv(csv_file_path, nrows=1)
        last_date = last_date_df.iloc[-1]['EventDate']

        # Extract year and location from the last date in the CSV file
        last_year = int(last_date[:4])
        last_location = last_date[5:]

        # Fetch event schedule data starting from the year and location in the CSV file
        all_events = []
        for year in range(last_year, 2024):
            events_year = fastf1.get_event_schedule(year)
            if isinstance(events_year, pd.DataFrame):
                if year == last_year:
                    # Filter events starting from the last location in the last year
                    events_year = events_year[events_year['EventDate'] >= last_date]
                all_events.append(events_year)

        # Concatenate all event data into a single DataFrame
        combined_events = pd.concat(all_events, ignore_index=True)
    else:
        # If the CSV file does not exist, fetch event schedule data for each year from 1950 to 2023
        all_events = []
        for year in range(1950, 2024):
            events_year = fastf1.get_event_schedule(year)
            if isinstance(events_year, pd.DataFrame):
                all_events.append(events_year)

        # Concatenate all event data into a single DataFrame
        combined_events = pd.concat(all_events, ignore_index=True)

    # Remove specified columns from the DataFrame
    columns_to_remove = ['OfficialEventName', 'EventFormat', 'Session1Date', 'Session2Date', 'Session3Date', 'Session4Date', 'Session5Date', 'F1ApiSupport']
    combined_events.drop(columns=columns_to_remove, inplace=True)

    # Write the combined DataFrame to a CSV file
    combined_events.to_csv(csv_file_path, index=False)

    logger.info(f"Event schedule saved to: {csv_file_path}")

def copy_csv_to_s3(local_file, s3_bucket, s3_key):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    s3_hook = S3Hook(aws_conn_id='my_aws_connection') 
    s3_hook.load_file(filename=local_file, key=s3_key, bucket_name=s3_bucket, replace=True)

local_directory = '/opt/airflow/dags/'
s3_bucket_name = 'dwdag'
s3_key_prefix = 's3://dwdag/dags/'

# Get list of CSV files in the local directory
csv_files = [f for f in os.listdir(local_directory) if f.endswith('.csv')]

for csv_file in csv_files:
    task_id = f'copy_{csv_file}_to_s3'
    local_file_path = os.path.join(local_directory, csv_file)
    s3_key = f"{s3_key_prefix}{csv_file}"


with DAG('Updated_event_schedule',    
        default_args=default_args,
         schedule_interval="0 0 * * *",
         start_date=datetime(2024, 3, 2),
         catchup=False) as dag:

    read_csv_files_task = PythonOperator(
        task_id='read_csv_files_task',
        python_callable=read_csv_files_from_s3,
        op_kwargs={'bucket_name': 'arn:aws:s3:::dwdag', 'prefix': 'transformed/', 'local_directory': '/opt/airflow/dags/'}
    )

    fetch_transform_event_schedule_task = PythonOperator(
        task_id='fetch_transform_event_schedule_task',
        python_callable=fetch_and_save_event_schedule
    )

    copy_task = PythonOperator(
        task_id='copy_task',
        python_callable=copy_csv_to_s3,
        op_kwargs={'local_file': '/opt/airflow/dags/event_schedule_1950_to_2023.csv', 's3_bucket': 'dwdag', 's3_key': 'transformed/event_schedule_1950_to_2023.csv'}
    )

read_csv_files_task >> fetch_transform_event_schedule_task >> copy_task
