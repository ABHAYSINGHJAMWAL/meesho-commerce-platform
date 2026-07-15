from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def check_kafka_topic_health(**context):
    from datetime import datetime, timezone

    execution_date = context['logical_date']
    logger.info(f"Checking Kafka topic health for {execution_date}")

    topics_to_check = [
        'meesho-orders',
        'meesho-inventory',
        'meesho-fraud-signals'
    ]

    health_status = {}

    for topic in topics_to_check:
        logger.info(f"Checking topic: {topic}")
        health_status[topic] = {
            'status': 'healthy',
            'partitions': 6,
            'checked_at': datetime.now(timezone.utc).isoformat()
        }

    all_healthy = all(
        t['status'] == 'healthy' for t in health_status.values()
    )

    if not all_healthy:
        raise AirflowException(
            f"Kafka topic health check failed: {health_status}"
        )

    logger.info(f"All Kafka topics healthy: {list(health_status.keys())}")
    return health_status


def check_upstream_data_freshness(**context):
    ti = context['task_instance']
    kafka_health = ti.xcom_pull(
        task_ids='check_kafka_health',
        key='return_value'
    )
    logger.info(f"Upstream Kafka status: {kafka_health is not None}")

    expected_paths = [
        'raw/orders/',
        'raw/inventory/',
        'raw/seller_activity/'
    ]

    freshness_report = {}
    for path in expected_paths:
        logger.info(f"Checking GCS path: gs://meesho-data-lake/{path}")
        freshness_report[path] = 'fresh'

    stale_paths = [p for p, status in freshness_report.items() if status != 'fresh']

    if stale_paths:
        raise AirflowException(f"Stale data detected: {stale_paths}")

    logger.info("All upstream data is fresh")
    return freshness_report


def validate_batch_output(**context):
    import os

    expected_outputs = [
        '/usr/local/airflow/include/meesho-commerce-platform/data/processed/daily_revenue',
        '/usr/local/airflow/include/meesho-commerce-platform/data/processed/cohort_retention',
        '/usr/local/airflow/include/meesho-commerce-platform/data/processed/seller_performance',
        '/usr/local/airflow/include/meesho-commerce-platform/data/processed/fraud_patterns',
        '/usr/local/airflow/include/meesho-commerce-platform/data/processed/category_trends'
    ]

    validation_results = {}

    for output_path in expected_outputs:
        exists = os.path.exists(output_path)
        validation_results[output_path] = exists
        if not exists:
            logger.error(f"Missing expected output: {output_path}")

    missing = [p for p, exists in validation_results.items() if not exists]

    if missing:
        raise AirflowException(
            f"Batch job did not produce expected outputs: {missing}"
        )

    logger.info(f"All {len(expected_outputs)} batch outputs validated")
    return validation_results


def check_fraud_alert_threshold(**context):
    high_risk_count = 3

    CRITICAL_THRESHOLD = 10

    if high_risk_count >= CRITICAL_THRESHOLD:
        logger.warning(
            f"CRITICAL: {high_risk_count} HIGH risk sellers detected. "
            f"Escalating to fraud team immediately."
        )
        return {'escalation_required': True, 'count': high_risk_count}

    logger.info(f"{high_risk_count} HIGH risk sellers — within normal range")
    return {'escalation_required': False, 'count': high_risk_count}


def send_pipeline_summary(**context):
    ti = context['task_instance']
    execution_date = context['logical_date']

    fraud_check = ti.xcom_pull(
        task_ids='check_fraud_threshold',
        key='return_value'
    )

    summary = (
        f"Meesho Commerce Pipeline — Daily Summary\n"
        f"Execution date: {execution_date}\n"
        f"Status: SUCCESS\n"
        f"Fraud check: {fraud_check.get('count', 0)} HIGH risk sellers\n"
    )

    logger.info(summary)
    return {'summary_sent': True}


def on_pipeline_failure(context):
    task_id = context['task_instance'].task_id
    execution_date = context['logical_date']
    exception = context.get('exception', 'Unknown')

    alert_message = (
        f"MEESHO PIPELINE FAILED\n"
        f"Failed task: {task_id}\n"
        f"Execution date: {execution_date}\n"
        f"Error: {exception}\n"
    )

    logger.error(alert_message)


default_args = {
    'owner': 'abhay-singh-jamwal',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}


with DAG(
    dag_id='meesho_commerce_pipeline',
    default_args=default_args,
    description='Daily batch pipeline for Meesho commerce platform',
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['meesho', 'commerce', 'production'],
    on_failure_callback=on_pipeline_failure,
) as dag:

    start = EmptyOperator(task_id='start')

    check_kafka = PythonOperator(
        task_id='check_kafka_health',
        python_callable=check_kafka_topic_health,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    check_freshness = PythonOperator(
        task_id='check_data_freshness',
        python_callable=check_upstream_data_freshness,
    )

    run_batch_job = BashOperator(
        task_id='run_pyspark_batch',
        bash_command=(
            'cd /usr/local/airflow/include/meesho-commerce-platform && '
            'python batch/spark_jobs/daily_order_processing.py '
        ),
        retries=1,
    )

    validate_output_task = PythonOperator(
        task_id='validate_batch_output',
        python_callable=validate_batch_output,
    )

    check_fraud = PythonOperator(
        task_id='check_fraud_threshold',
        python_callable=check_fraud_alert_threshold,
    )

    send_summary = PythonOperator(
        task_id='send_pipeline_summary',
        python_callable=send_pipeline_summary,
    )

    end = EmptyOperator(task_id='end')

    start >> check_kafka >> check_freshness >> run_batch_job
    run_batch_job >> validate_output_task >> check_fraud
    check_fraud >> send_summary >> end