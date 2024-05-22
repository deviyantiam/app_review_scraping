from pendulum import datetime, duration
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Deviyanti AM",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": duration(minutes=5),
}

dag = DAG('JMO_Scraping_ETL',
          default_args=default_args,
          description='JMO Review Scraper and Reason Generator Dag',
          schedule_interval='0 7 * * 1',
          start_date=datetime(2024, 4, 11),
          catchup=False) 

today = datetime.today()
one_week_ago = (today - timedelta(days=7)).date()

scrap_data = KubernetesPodOperator(
            image="localhost:5001/jmo_review:v1",
            arguments=["scrap-data", "--date={}".format(one_week_ago)],
            kubernetes_conn_id = "k8s_conn",
            cluster_context="docker-desktop",
            name=f"scrap_data",
            task_id=f"scrap_data",
            retries=5,
            retry_delay=timedelta(minutes=5),
            dag=dag,
            get_logs=True,
        )

generate_reason = KubernetesPodOperator(
            image="localhost:5001/jmo_review:v1",
            arguments=["generate-reason"],
            name=f"generate_reason",
            task_id=f"generate_reason",
            kubernetes_conn_id = "k8s_conn",
            cluster_context="docker-desktop",
            retries=5,
            retry_delay=timedelta(minutes=5),
            dag=dag,
        )


scrap_data >> generate_reason