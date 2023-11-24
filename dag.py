from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago
from airflow.models import Variable

import uuid
import json
import requests
import psycopg2
from datetime import datetime, timedelta

def request_data_from_api(endpoint, file_path):

    response = requests.get(endpoint)
    result = response.json()

    with open(file_path, "w") as file:
        json.dump(result, file, indent=4, ensure_ascii=False)

def persist_data_from_api(postgres_parameters, file_path):

    json_uuid = uuid.uuid4()

    connection = psycopg2.connect(
        user=postgres_parameters["user"],
        password=postgres_parameters["password"],
        host=postgres_parameters["host"],
        port=postgres_parameters["port"],
        database=postgres_parameters["database"]
    )

    cursor = connection.cursor()

    with open(file_path, "r") as file:
        result = json.load(file)

    if list(result.keys()) != ['data', 'metadata'] or any(list(row.keys()) != ['id', 'name', 'age', 'city'] for row in result['data']):
        raise AirflowException("File structure is not correct.")

    values = [[value for value in d.values()] + [str(json_uuid)] for d in result["data"]]
    args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s)", x).decode("utf-8") for x in values)
    cursor.execute("INSERT INTO data_table VALUES " + args_str) 

    connection.commit()


    metadata_insert_query = "INSERT INTO metadata_table (metadata_id, metadata) VALUES (%s, %s);"
    metadata_json = json.dumps(result['metadata'])
    cursor.execute(metadata_insert_query, (str(json_uuid), metadata_json))

    connection.commit()

    cursor.close()
    connection.close()

def generate_execution_logs(endpoint_url, file_path, **kwargs):
    html_content = """
    <html>
    <body>
    <p><b>Seven media pipeline execution result.</b></p>
    <br>
    <p>Date: _DATE_</p>
    <p>API URL: _URL_</p>
    <p>Number of rows: _ROWS_</p>
    <p>Start time: _START_</p>
    <p>End time: _END_</p>
    <p>Duration: _DURATION_</p>
    </body>
    </html>
    """

    start_date = kwargs['dag_run'].get_task_instance('check_api_availability').start_date
    end_date = kwargs['dag_run'].get_task_instance('persist_data').end_date
    duration = end_date - start_date

    start_date = str(start_date).split(".")[0]
    end_date = str(end_date).split(".")[0]
    duration = str(duration).split(".")[0]

    with open(file_path, "r") as file:
        result = json.load(file)
    rows = str(len(result["data"]))
    

    html_content = html_content.replace('_DATE_', formatted_date)
    html_content = html_content.replace('_URL_', endpoint_url)
    html_content = html_content.replace('_ROWS_', rows)
    html_content = html_content.replace('_START_', start_date)
    html_content = html_content.replace('_END_', end_date)
    html_content = html_content.replace('_DURATION_', duration)
    print(html_content)

    kwargs['ti'].xcom_push(key="html_content", value=html_content)


email_on_failure = Variable.get("email_on_failure", default_var=None)
email_on_failure = [email_str.strip() for email_str in email_on_failure.split(',')]

email_on_success = Variable.get("email_on_success", default_var=None)
email_on_success = [email_str.strip() for email_str in email_on_success.split(',')]

endpoint_url = Variable.get("endpoint_url", default_var=None)

postgres_parameters = {
    "user": Variable.get("postgres_user", default_var=None),
    "password": Variable.get("postgres_password", default_var=None),
    "host": Variable.get("postgres_host", default_var=None),
    "port": Variable.get("postgres_port", default_var=None),
    "database": Variable.get("postgres_database", default_var=None),
}

formatted_date = datetime.now().strftime("%Y-%m-%d")
file_path = f"result-{formatted_date}.json"

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': email_on_failure, 
    'email_on_failure': True, 
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'seven_media_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *',
)


check_api_availability = HttpSensor(
    task_id='check_api_availability',
    http_conn_id=None,
    endpoint=endpoint_url,
    method='GET',
    mode='poke',
    timeout=120, 
    poke_interval=30,
    dag=dag,
)

request_data = PythonOperator(
    task_id='request_data',
    python_callable=request_data_from_api,
    op_args=[endpoint_url, file_path],
    provide_context=True,
    dag=dag,
)

persist_data = PythonOperator(
    task_id='persist_data',
    python_callable=persist_data_from_api,
    op_args=[postgres_parameters, file_path],
    provide_context=True,
    dag=dag,
)

generate_logs = PythonOperator(
    task_id='generate_logs',
    python_callable=generate_execution_logs,
    op_args=[endpoint_url, file_path],
    provide_context=True,
    dag=dag,
)

send_email_with_status = EmailOperator(
    task_id='send_email_with_status',
    to=email_on_success,
    subject='Airflow DAG Execution Completed.',
    html_content='{{ ti.xcom_pull(key="html_content") }}',
    dag=dag,
)

check_api_availability >> request_data >> persist_data >> generate_logs >> send_email_with_status
