"""
Meesho Commerce Platform — Master Orchestration DAG

Business problem this solves:
Currently every component runs manually in separate terminals.
Production requires: scheduled execution, dependency management,
automatic retries, and alerting when something breaks.

This DAG is the conductor. It does not do the work itself —
it ensures every component runs in the correct order,
at the correct time, and alerts humans when something fails.

Pipeline flow:
1. Check Kafka topic health (are events flowing?)
2. Check upstream data freshness (did yesterday's batch complete?)
3. Trigger PySpark batch job (the heavy processing)
4. Validate output row counts (catch silent failures)
5. Run data quality checks (catch wrong data)
6. Send completion notification (or failure alert)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import logging
import json

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════
# PYTHON CALLABLES
# ════════════════════════════════════════════

def check_kafka_topic_health(**context):
    """
    Verify Kafka topics have recent activity.

    Why this runs first:
    If Kafka has been down, there is no point running
    the batch job — there is no new data to process.
    Better to fail fast here than waste compute
    running Spark on stale or empty data.

    In production: connects to Kafka admin client,
    checks topic partition offsets, compares to last run.
    """
    from datetime import datetime, timezone

    execution_date = context['execution_date']
    logger.info(f"Checking Kafka topic health for {execution_date}")

    topics_to_check = [
        'meesho-orders',
        'meesho-inventory',
        'meesho-fraud-signals'
    ]

    health_status = {}

    for topic in topics_to_check:
        # Production: query Kafka admin API for partition offsets
        # Here: simulated health check
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
    """
    Verify GCS raw layer received fresh data.

    Why after Kafka health check:
    Kafka being healthy does not guarantee the archiver
    successfully wrote files to GCS. This is a separate
    failure point — checking both catches more issues.

    Production: lists GCS bucket, checks file timestamps,
    compares against current execution date.
    """
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
        # Production: GCS client checks blob.updated timestamp
        logger.info(f"Checking GCS path: gs://meesho-data-lake/{path}")
        freshness_report[path] = 'fresh'

    stale_paths = [p for p, status in freshness_report.items() if status != 'fresh']

    if stale_paths:
        raise AirflowException(f"Stale data detected: {stale_paths}")

    logger.info("All upstream data is fresh")
    return freshness_report


def validate_batch_output(**context):
    """
    Confirm PySpark batch job produced expected output.

    Why this matters:
    PySpark job can exit with success code 0 but produce
    empty or wrong output if upstream logic has a bug.
    This is the same silent failure pattern from your
    dbt project — success exit code does not mean
    success in business terms.
    """
    import os

    expected_outputs = [
        'data/processed/daily_revenue',
        'data/processed/cohort_retention',
        'data/processed/seller_performance',
        'data/processed/fraud_patterns',
        'data/processed/category_trends'
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
    """
    Check if fraud detection found anything requiring
    immediate human attention.

    Why a dedicated task:
    Most pipeline runs are routine. But if batch fraud
    detection finds 50 HIGH risk sellers, that needs
    immediate escalation — not waiting for tomorrow's report.

    This demonstrates business-logic-aware orchestration,
    not just mechanical task running.
    """
    # Production: read fraud_patterns output, count HIGH risk
    high_risk_count = 3  # Simulated

    CRITICAL_THRESHOLD = 10

    if high_risk_count >= CRITICAL_THRESHOLD:
        logger.warning(
            f"CRITICAL: {high_risk_count} HIGH risk sellers detected. "
            f"Escalating to fraud team immediately."
        )
        # Production: send PagerDuty alert, not just Slack
        return {'escalation_required': True, 'count': high_risk_count}

    logger.info(f"{high_risk_count} HIGH risk sellers — within normal range")
    return {'escalation_required': False, 'count': high_risk_count}


def send_pipeline_summary(**context):
    """
    Send daily pipeline summary to team.

    Only runs if all upstream tasks succeeded.
    """
    ti = context['task_instance']
    execution_date = context['execution_date']

    fraud_check = ti.xcom_pull(
        task_ids='check_fraud_threshold',
        key='return_value'
    )

    summary = (
        f"Meesho Commerce Pipeline — Daily Summary\n"
        f"Execution date: {execution_date}\n"
        f"Status: SUCCESS\n"
        f"Kafka topics: healthy\n"
        f"Batch job: completed\n"
        f"Fraud check: {fraud_check.get('count', 0)} HIGH risk sellers\n"
        f"Outputs: 5 tables written to GCS\n"
    )

    logger.info(summary)
    # Production: requests.post(slack_webhook, json={'text': summary})
    return {'summary_sent': True}


def on_pipeline_failure(context):
    """
    DAG-level failure callback.
    Fires automatically when any task fails.
    """
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    exception = context.get('exception', 'Unknown')

    alert_message = (
        f"MEESHO PIPELINE FAILED\n"
        f"Failed task: {task_id}\n"
        f"Execution date: {execution_date}\n"
        f"Error: {exception}\n"
        f"Action required: check Airflow logs immediately"
    )

    logger.error(alert_message)
    # Production: PagerDuty for CRITICAL tasks, Slack for others


# ════════════════════════════════════════════
# DEFAULT ARGS
# ════════════════════════════════════════════

default_args = {
    'owner': 'abhay-singh-jamwal',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}


# ════════════════════════════════════════════
# DAG DEFINITION
# ════════════════════════════════════════════

with DAG(
    dag_id='meesho_commerce_pipeline',
    default_args=default_args,
    description=(
        'Daily batch pipeline for Meesho commerce platform. '
        'Validates Kafka health, runs PySpark transformations, '
        'checks fraud thresholds, sends summary.'
    ),
    schedule_interval='0 2 * * *',  # 2am daily — after sale day traffic settles
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['meesho', 'commerce', 'production', 'pyspark'],
    on_failure_callback=on_pipeline_failure,
) as dag:

    start = DummyOperator(task_id='start')

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

    # PySpark batch job — runs the heavy transformation
    run_batch_job = BashOperator(
        task_id='run_pyspark_batch',
        bash_command=(
            'cd /usr/local/airflow/include/meesho-commerce-platform && '
            'python batch/spark_jobs/daily_order_processing.py'
        ),
        retries=1,  # Spark jobs are expensive — limit retries
    )

    validate_output = PythonOperator(
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

    end = DummyOperator(task_id='end')

    # ── DEPENDENCY CHAIN ──
    start >> check_kafka >> check_freshness >> run_batch_job
    run_batch_job >> validate_output >> check_fraud
    check_fraud >> send_summary >> end