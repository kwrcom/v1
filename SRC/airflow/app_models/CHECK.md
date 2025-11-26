# Проверка класса FraudDetectionTraining

## Созданные файлы:

1. **SRC/airflow/app_models/fraud_detection_training.py** - основной класс
2. **SRC/airflow/app_models/__init__.py** - инициализация модуля
3. **dags/test_environment_validation.py** - тестовый DAG

## Класс FraudDetectionTraining

### Методы:

- `__init__(config_path)` - инициализация, загрузка переменных из .env
- `load_config(config_path)` - чтение config.yml
- `check_minio_connection()` - проверка MinIO через boto3, создание бакета mlflow
- `validate_environment()` - валидация MLflow, MinIO, Kafka
- `read_data_from_kafka()` - заглушка для чтения данных из Kafka

## Проверка через Airflow

### 1. Сборка и запуск

```bash
# Сборка образов
docker compose build airflow-webserver

# Запуск всех сервисов
docker compose up -d
```

### 2. Проверка логов

```bash
# Логи Airflow webserver
docker compose logs -f airflow-webserver | grep -E "(MLflow|MinIO|Kafka)"
```

### 3. Запуск тестового DAG

1. Откройте Airflow UI: http://localhost:8080
2. Найдите DAG `test_environment_validation`
3. Нажмите кнопку "Trigger DAG"
4. Проверьте логи задачи `validate_environment`

### Ожидаемые логи:

```
[1/3] Validating MinIO/S3 connection...
✅ Bucket 'mlflow' already exists
✅ Successfully wrote test object to 'mlflow/test_connection.txt'
✅ MinIO connection verified successfully

[2/3] Validating MLflow Tracking Server connection...
✅ Experiment 'fraud_detection' already exists
✅ MLflow connection verified successfully

[3/3] Validating Kafka connection...
✅ Kafka topic 'transactions' is available
✅ Kafka connection verified successfully

Environment Validation Results:
MinIO/S3:  ✅ PASSED
MLflow:    ✅ PASSED
Kafka:     ✅ PASSED
✅ All environment validations passed!
```

## Проверка MinIO UI

1. Откройте MinIO Console: http://localhost:9001
2. Войдите (minioadmin / minioadmin)
3. Проверьте наличие бакета `mlflow`
4. Внутри должны появиться папки от MLflow экспериментов

## Ручная проверка из контейнера

```bash
# Войти в контейнер Airflow
docker compose exec airflow-webserver bash

# Запустить тестовый скрипт
cd /opt/airflow
python -c "from app_models import FraudDetectionTraining; t = FraudDetectionTraining(); t.validate_environment()"
```

## Проверка переменных окружения

```bash
# Проверка переменных MinIO/S3
docker compose exec airflow-webserver env | grep -E "AWS_ACCESS_KEY_ID|MINIO"

# Проверка переменных MLflow
docker compose exec airflow-webserver env | grep MLFLOW

# Проверка переменных Kafka
docker compose exec airflow-webserver env | grep KAFKA
```
