

from airflow.decorators import dag, task
from airflow.sdk import Asset  # <-- The new Airflow 3 vocabulary!
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime
import pandas as pd

# The exact same tripwire we defined in DAG 1, now called an Asset
clean_data_asset = Asset('file://opt/airflow/dags/input_data/final_clean_data.csv')
FINAL_FILE = '/opt/airflow/dags/input_data/final_clean_data.csv'

@dag(
    dag_id='load_data_to_mongodb',
    start_date=datetime(2023, 1, 1),
    schedule=[clean_data_asset], # DAG 2 is listening to the Asset tripwire!
    catchup=False
)
def mongo_loader_pipeline():

    @task(task_id='push_to_mongo')
    def load_to_mongo():
        # 1. Open the clean box of data
        df = pd.read_csv(FINAL_FILE)
        
        # 2. Convert the Pandas spreadsheet into a list of dictionaries for MongoDB
        records = df.to_dict(orient='records')
        
        # 3. Open the Address Book and find the MongoDB connection we made in the UI
        hook = MongoHook(mongo_conn_id='mongo_default')
        
        # 4. Dump the data into the Big Red Toy Box!
        hook.insert_many(
            mongo_collection='clean_reviews', 
            docs=records, 
            mongo_db='airflow_project'
        )
        
        print(f"Successfully loaded {len(records)} records into MongoDB!")

    # Start the delivery truck robot
    load_to_mongo()

# Turn the lights on
mongo_loader_pipeline()