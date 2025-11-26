"""
Airflow DAG: fraud_detection_training

Daily retraining pipeline for fraud detection model.

Schedule: daily at 03:00 (cron: 0 3 * * *)

Tasks:
  - validate_environment: uses FraudDetectionTraining.validate_environment()
  - training_task: calls FraudDetectionTraining (if training method exists it will be invoked, otherwise reads Kafka stub)
  - clean_up_task: cleanup, always runs (trigger_rule=all_done)

Doc: This pipeline uses XGBoost, reads data from Kafka and logs/registers artifacts to MLflow. The objective is to optimize Precision for the fraud detection model.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Import the trainer from app_models
try:
    from app_models.fraud_detection_training import FraudDetectionTraining
except Exception:  # pragma: no cover - fallback for local linting
    # Keep import error visible in logs when DAG loads in environments where package paths differ
    FraudDetectionTraining = None

LOG = logging.getLogger(__name__)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='fraud_detection_training',
    default_args=default_args,
    description='Daily retraining for fraud detection model (XGBoost + Kafka + MLflow) optimizing Precision',
    schedule_interval='0 3 * * *',  # daily at 03:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'training', 'fraud_detection'],
) as dag:

    dag.doc_md = (
        """
        ## Fraud Detection Retraining DAG

        This DAG performs daily retraining of the fraud detection model. The pipeline:

        - Uses XGBoost for training
        - Reads data from Kafka topic(s)
        - Logs experiments, metrics and artifacts to MLflow
        - Optimizes model selection for Precision as a primary metric

        Tasks:
        1. validate_environment (PythonOperator) — validates MinIO, MLflow and Kafka availability using `FraudDetectionTraining.validate_environment()`
        2. training_task (PythonOperator) — invokes `FraudDetectionTraining` to perform training (or a safe no-op if training is not implemented)
        3. clean_up_task (PythonOperator) — cleanup operations, runs regardless of previous task states

        Scheduling: runs every day at 03:00 (server local time)
        """
    )


    def _validate_environment(**context) -> bool:
        """Calls FraudDetectionTraining.validate_environment() and returns boolean result."""
        if FraudDetectionTraining is None:
            LOG.error("FraudDetectionTraining class is not importable")
            raise RuntimeError("FraudDetectionTraining not available in Python path")

        trainer = FraudDetectionTraining()
        result = trainer.validate_environment()
        if not result:
            LOG.error("Environment validation failed — check MinIO, MLflow, Kafka")
            # raise to mark Airflow task as failed
            raise RuntimeError("Environment validation failed")

        LOG.info("Environment validation succeeded")
        return True


    def _training_task(**context) -> dict:
        """Instantiate FraudDetectionTraining and run training if-available, otherwise run a safe placeholder flow."""
        if FraudDetectionTraining is None:
            LOG.error("FraudDetectionTraining class is not importable")
            raise RuntimeError("FraudDetectionTraining not available in Python path")

        trainer = FraudDetectionTraining()

        # Best-effort: prefer a "train", "run_training" or "train_model" method if present.
        if hasattr(trainer, 'train'):
            LOG.info("Calling trainer.train()")
            res = getattr(trainer, 'train')()
        elif hasattr(trainer, 'run_training'):
            LOG.info("Calling trainer.run_training()")
            getattr(trainer, 'run_training')()
        elif hasattr(trainer, 'train_model'):
            LOG.info("Calling trainer.train_model()")
            res = getattr(trainer, 'train_model')()
        else:
            LOG.warning("No concrete training method found on FraudDetectionTraining; running safe placeholder (read_data_from_kafka)")
            # Use existing read_data_from_kafka stub (it will return an empty list until implemented)
            trainer.read_data_from_kafka()
            LOG.info("Training placeholder completed — implement a training method in FraudDetectionTraining for real training")

        # Return training result (if any) so it can be used by downstream tasks via XCom
        try:
            return res  # may be dict
        except NameError:
            return {}


    def _clean_up(**context) -> bool:
        """Minimal cleanup step executed regardless of previous state."""
        try:
            LOG.info("Running cleanup operations for retraining DAG")
            # If a persistent trainer instance is used, cleanup should be implemented there.
            # Here we keep it minimal and safe.
            return True
        except Exception as e:  # pragma: no cover - robust cleanup should never crash the DAG
            LOG.warning(f"Cleanup encountered an error: {e}")
            return True


    validate_env = PythonOperator(
        task_id='validate_environment',
        python_callable=_validate_environment,
        provide_context=True,
    )

    training = PythonOperator(
        task_id='training_task',
        python_callable=_training_task,
        provide_context=True,
    )

    clean_up = PythonOperator(
        task_id='clean_up_task',
        python_callable=_clean_up,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )


    def _evaluate_and_promote(**context) -> dict:
        """Compare trained candidate against production model via MLflow and promote if appropriate."""
        if FraudDetectionTraining is None:
            LOG.error("FraudDetectionTraining class is not importable")
            raise RuntimeError("FraudDetectionTraining not available in Python path")

        ti = context['ti']
        training_result = ti.xcom_pull(task_ids='training_task')
        if not training_result or not isinstance(training_result, dict):
            LOG.error("No training result found in XCom; cannot evaluate/promote model")
            raise RuntimeError("Missing training result for promotion")

        run_id = training_result.get('run_id')
        if run_id is None:
            LOG.error("Training result missing run_id; cannot promote")
            raise RuntimeError("Missing run_id in training_result")

        trainer = FraudDetectionTraining()

        # Use config's registered model name if available
        registered_name = trainer.config.get('mlflow', {}).get('registered_model_name', None)

        promote_result = trainer.compare_and_promote(run_id, registered_model_name=registered_name)
        LOG.info(f"Promotion decision: {promote_result}")
        return promote_result

    evaluate_and_promote = PythonOperator(
        task_id='evaluate_and_promote',
        python_callable=_evaluate_and_promote,
        provide_context=True,
    )


    # DAG sequence
    validate_env >> training >> evaluate_and_promote >> clean_up
