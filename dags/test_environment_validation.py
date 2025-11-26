"""
Тестовый DAG для проверки валидации окружения

Проверяет подключение к MLflow, MinIO и Kafka
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Добавляем путь к модулю app_models
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'SRC', 'airflow'))

from app_models import FraudDetectionTraining


def validate_environment_task():
    """
    Задача для проверки валидации окружения
    """
    trainer = FraudDetectionTraining()
    is_valid = trainer.validate_environment()
    
    if not is_valid:
        raise Exception("Environment validation failed!")
    
    return "Environment validation passed successfully!"


# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_environment_validation',
    default_args=default_args,
    description='Test environment validation for MLflow, MinIO and Kafka',
    schedule_interval=None,  # Запуск вручную
    catchup=False,
    tags=['test', 'validation', 'mlops'],
)

# Задача валидации окружения
validate_task = PythonOperator(
    task_id='validate_environment',
    python_callable=validate_environment_task,
    dag=dag,
)

validate_task
