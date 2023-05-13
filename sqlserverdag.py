from __future__ import print_function
import datetime
from airflow import models
from airflow.models import Variable
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.operators.email_operator import EmailOperator
common_config=Variable.get("common_config",deserialize_json=True)
table_list=common_config["table_list"]

default_dag_args = {
    'email': 'gcpdev3@gmail.com',
    'email_on_success': True,
    'start_date': datetime.datetime(2023,5,11),
}
with models.DAG(
        'batch_load_job_sql_server_with_email',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    dataflow_bash = bash_operator.BashOperator(
        task_id='sql_dataflow_job',
        bash_command='python3 /home/gcpdev3/airflow_project/dags/sqlairflow.py --runner DataflowRunner --project myproject  --region us-central1 --job_name sqljobrunv1 --temp_location  gs://personal_poc/tem  --dfBucket "personal_poc" --service_account_email myproject.iam.gserviceaccount.com --tables {0} --setup_file /home/gcpdev3/airflow_project/dags/setup.py --connDetail  "dbadmin~|*1234~|*sql_server_ip_address~|*1433~|*sandy"'.format(table_list)

    )

    send_email = EmailOperator(
        task_id='send_email_notification_on_success',
        to='sandeep.mohanty1998@gmail.com',
        subject='Task Completed: Sql Server data load',
        html_content='The data load activity from sql server to bigquery has been completed successfully.'
    )

    # Define the order in which the tasks complete by using the >> and <<
    # operators. In this example, hello_python executes before goodbye_bash.
    dataflow_bash >> send_email
