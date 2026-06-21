


from airflow.sdk import dag, task
from datetime import datetime



@dag(
        dag_id="first_dag",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False,
)

def first_dag():

    @task.python
    def first_task():
        print("This is the first task and it will run first")

    @task.python
    def second_task():
        print("This is my second task")
    
    @task.python
    def third_task():
        print("This is my third task")  

    # Set the task dependencies

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


first_dag()