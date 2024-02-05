from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowException


from include.main import GreatExpectationsManager
suite_name = "my_suite_v1"
input_data_path = "include/data_with_no_error.csv"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'great_expectations_with_airflow_dag',
    default_args=default_args,
    description='A simple DAG with Great Expectations',

)

def check_data_with_great_expectations():
    ge_manager = GreatExpectationsManager(suite_name)
    validation_result = ge_manager.validate(input_data_path)
    print(validation_result)
    validation_status = validation_result.success
    if validation_status:
        return True
    else:
        raise AirflowException("The validation has failed, consider checking DAG logs.")
    

def do_something_else():
    print("Validation of data was complete, you can proceed with everything else")
    return True

task_read_csv = PythonOperator(
    task_id='validate_data_against_expectations',
    python_callable=check_data_with_great_expectations,
    dag=dag,
)

task_validated = PythonOperator(
    task_id='do_everything_else',
    python_callable=do_something_else,
    dag=dag,
)


task_read_csv >> task_validated

if __name__ == "__main__":
    dag.cli()
