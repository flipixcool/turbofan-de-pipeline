from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime

@dag(start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False)
def taskgroup_demo():

    with TaskGroup("source_a") as group_a:
        @task(task_id="source_a.extract")
        def extract_a():
            return {"source": "a", "rows": 100}

        @task(task_id="source_a.load")
        def load_a(data):
            print(f"loading {data}")

        load_a(extract_a())

    with TaskGroup("source_b") as group_b:
        @task(task_id="source_b.extract")
        def extract_b():
            return {"source": "b", "rows": 200}

        @task(task_id="source_b.load")
        def load_b(data):
            print(f"loading {data}")

        load_b(extract_b())

taskgroup_demo()