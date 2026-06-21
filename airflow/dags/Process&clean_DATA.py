from airflow.sdk import Asset

# Define the tripwire (Now called an Asset!)
clean_data_asset = Asset('file://opt/airflow/dags/input_data/final_clean_data.csv')

from airflow.decorators import dag, task, task_group
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
import os
import re

# The tables our guards and workers use
# Change this line at the top of DAG 1
INPUT_FILE = '/opt/airflow/dags/input_data/tiktok_google_play_reviews.csv'
STEP1_FILE = '/opt/airflow/dags/input_data/step1.csv'
STEP2_FILE = '/opt/airflow/dags/input_data/step2.csv'
FINAL_FILE = '/opt/airflow/dags/input_data/final_clean_data.csv'

@dag(
    dag_id='process_and_clean_data',
    start_date=datetime(2023, 1, 1),
    schedule=None, 
    catchup=False
)
def data_pipeline():

    # 1. The Watcher
    watcher = FileSensor(
        task_id='wait_for_file',
        filepath=INPUT_FILE,
        poke_interval=10,
    )

    # 2. The Sorter
    @task.branch(task_id='check_file_empty')
    def check_file_contents():
        if os.path.getsize(INPUT_FILE) == 0:
            return 'log_empty_file'
        else:
            # We must point exactly to the TaskGroup's name!
            return 'clean_the_data.replace_nulls' 

    # 3.1 The Empty Path
    log_empty = BashOperator(
        task_id='log_empty_file',
        bash_command='echo "The file is totally empty! The robots can go back to sleep."'
    )

    # 3.2 The Cleaning Team (TaskGroup)
    @task_group(group_id='clean_the_data')
    def cleaning_department():
        
        @task(task_id='replace_nulls')
        def task_replace_nulls():
            df = pd.read_csv(INPUT_FILE)
            # Replace actual empty values AND the text word "null" with "-"
            df.fillna("-", inplace=True)
            df.replace("null", "-", inplace=True)
            df.to_csv(STEP1_FILE, index=False)
            return STEP1_FILE # Hand the new box to the next robot

        @task(task_id='sort_by_date')
        def task_sort_data(file_to_open):
            df = pd.read_csv(file_to_open)
            # Sort the data by the created_date column
            df.sort_values(by='at', inplace=True)
            df.to_csv(STEP2_FILE, index=False)
            return STEP2_FILE

       # cleaning department/ We attach the tripwire to this specific robot! 
        @task(task_id='clean_content_column', outlets=[clean_data_asset])
        def task_clean_content(file_to_open):
            df = pd.read_csv(file_to_open)
            # Regex magic: Keep only word characters (\w), spaces (\s), and standard punctuation
            df['content'] = df['content'].apply(
                lambda x: re.sub(r'[^\w\s.,!?\'"-]', '', str(x))
            )
            df.to_csv(FINAL_FILE, index=False)
            return FINAL_FILE

        # Define the assembly line inside the Cleaning Room
        # Task 1 feeds into Task 2, which feeds into Task 3
        file_1 = task_replace_nulls()
        file_2 = task_sort_data(file_1)
        final_file = task_clean_content(file_2)

    # 4. Setting the Master Conveyor Belt Order
    sorter = check_file_contents() 
    cleaning_team = cleaning_department()

    # The watcher passes to the sorter. 
    # The sorter passes to EITHER the empty log OR the cleaning team!
    watcher >> sorter >> [log_empty, cleaning_team]

# Turn the lights on!
data_pipeline()