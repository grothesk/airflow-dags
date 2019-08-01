import time
from pprint import pprint

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='one-hot-minute',
    default_args=args,
    schedule_interval=None,
)


# [START howto_operator_python]
def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'


run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)
# [END howto_operator_python]


# [START howto_operator_python_kwargs]
def my_sleeping_function():
    """This is a function that will run within the DAG execution"""
    print('Gute Nacht!')
    time.sleep(30)
    print('Guten Morgen!')


# Generate 10 sleeping tasks, sleeping 30 seconds
for i in range(10):
    task = PythonOperator(
        task_id='sleep_' + str(i),
        python_callable=my_sleeping_function,
        dag=dag,
    )

    run_this >> task
# [END howto_operator_python_kwargs]
