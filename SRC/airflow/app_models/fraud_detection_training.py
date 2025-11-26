"""
Основной класс для обучения модели обнаружения мошенничества

Выполняет валидацию окружения, подключение к MLflow и MinIO,
чтение данных из Kafka и обучение XGBoost модели.
"""

import os
import logging
from typing import Dict, Any, Optional

import yaml
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import mlflow
from confluent_kafka import Consumer, KafkaException

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FraudDetectionTraining:
    """
    Класс для обучения модели обнаружения мошенничества
    
    Выполняет:
    - Валидацию окружения (MLflow, MinIO, Kafka)
    - Чтение данных из Kafka
    - Обучение XGBoost модели
    - Логирование в MLflow
    """
    
    def __init__(self, config_path: str = "/opt/airflow/config.yml"):
        """
        Инициализация класса обучения
        
        Args:
            config_path: путь к файлу конфигурации config.yml
        """
        logger.info("Initializing FraudDetectionTraining...")
        
        # Загружаем конфигурацию из config.yml
        self.config = self.load_config(config_path)
        
        # Получаем переменные окружения из .env
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        self.mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.kafka_topic = os.getenv("KAFKA_TOPIC", "transactions")
        
        # Инициализация boto3 клиента для MinIO
        self.s3_client = None
        
        # Инициализация Kafka consumer
        self.kafka_consumer = None
        
        logger.info("FraudDetectionTraining initialized successfully")
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Загрузка конфигурации из config.yml
        
        Args:
            config_path: путь к файлу конфигурации
            
        Returns:
            словарь с конфигурацией
        """
        logger.info(f"Loading configuration from {config_path}...")
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            logger.info("Configuration loaded successfully")
            logger.info(f"MLflow experiment: {config.get('mlflow', {}).get('experiment_name')}")
            logger.info(f"Model name: {config.get('mlflow', {}).get('registered_model_name')}")
            
            return config
            
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML configuration: {e}")
            raise
    
    def check_minio_connection(self) -> bool:
        """
        Проверка подключения к MinIO/S3 и создание бакета если отсутствует
        
        Использует boto3 для подключения к S3-совместимому MinIO.
        Проверяет существование бакета 'mlflow' и создает его при необходимости.
        
        Returns:
            True если подключение успешно, False в противном случае
        """
        logger.info("Checking MinIO/S3 connection...")
        
        try:
            # Получаем S3 endpoint из config.yml
            s3_endpoint_url = self.config.get('mlflow', {}).get('s3_endpoint_url', self.minio_endpoint)
            
            logger.info(f"S3 Endpoint: {s3_endpoint_url}")
            logger.info(f"AWS Access Key: {self.aws_access_key_id[:4]}***")
            
            # Создаем boto3 клиент для MinIO
            self.s3_client = boto3.client(
                's3',
                endpoint_url=s3_endpoint_url,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                config=Config(signature_version='s3v4'),
                region_name='us-east-1'
            )
            
            # Проверяем существование бакета 'mlflow'
            bucket_name = 'mlflow'
            
            try:
                self.s3_client.head_bucket(Bucket=bucket_name)
                logger.info(f"✅ Bucket '{bucket_name}' already exists")
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                
                if error_code == '404':
                    # Бакет не существует - создаем его
                    logger.warning(f"Bucket '{bucket_name}' does not exist. Creating...")
                    
                    try:
                        self.s3_client.create_bucket(Bucket=bucket_name)
                        logger.info(f"✅ Bucket '{bucket_name}' created successfully")
                        
                    except ClientError as create_error:
                        logger.error(f"Failed to create bucket '{bucket_name}': {create_error}")
                        return False
                else:
                    logger.error(f"Error checking bucket: {e}")
                    return False
            
            # Проверяем возможность записи в бакет
            test_key = 'test_connection.txt'
            test_content = 'MinIO connection test'
            
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=test_key,
                Body=test_content.encode('utf-8')
            )
            
            logger.info(f"✅ Successfully wrote test object to '{bucket_name}/{test_key}'")
            
            # Удаляем тестовый объект
            self.s3_client.delete_object(Bucket=bucket_name, Key=test_key)
            logger.info(f"✅ Test object deleted from '{bucket_name}/{test_key}'")
            
            logger.info("✅ MinIO connection verified successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ MinIO connection failed: {e}")
            return False
    
    def validate_environment(self) -> bool:
        """
        Валидация всего окружения перед началом обучения
        
        Проверяет:
        - Подключение к MinIO/S3
        - Подключение к MLflow Tracking Server
        - Доступность Kafka
        
        Returns:
            True если все проверки пройдены, False в противном случае
        """
        logger.info("="*60)
        logger.info("Starting environment validation...")
        logger.info("="*60)
        
        validation_results = {
            'minio': False,
            'mlflow': False,
            'kafka': False
        }
        
        # 1. Проверка MinIO/S3
        logger.info("\n[1/3] Validating MinIO/S3 connection...")
        validation_results['minio'] = self.check_minio_connection()
        
        # 2. Проверка MLflow
        logger.info("\n[2/3] Validating MLflow Tracking Server connection...")
        try:
            # Устанавливаем tracking URI из config.yml
            tracking_uri = self.config.get('mlflow', {}).get('tracking_uri', self.mlflow_tracking_uri)
            mlflow.set_tracking_uri(tracking_uri)
            
            logger.info(f"MLflow Tracking URI: {tracking_uri}")
            
            # Получаем или создаем эксперимент
            experiment_name = self.config.get('mlflow', {}).get('experiment_name', 'fraud_detection')
            
            try:
                experiment = mlflow.get_experiment_by_name(experiment_name)
                if experiment is None:
                    logger.info(f"Creating new experiment: {experiment_name}")
                    experiment_id = mlflow.create_experiment(
                        experiment_name,
                        artifact_location=self.config.get('mlflow', {}).get('artifact_location', 's3://mlflow/fraud_detection')
                    )
                    logger.info(f"✅ Experiment '{experiment_name}' created with ID: {experiment_id}")
                else:
                    logger.info(f"✅ Experiment '{experiment_name}' already exists (ID: {experiment.experiment_id})")
                
                mlflow.set_experiment(experiment_name)
                
                validation_results['mlflow'] = True
                logger.info("✅ MLflow connection verified successfully")
                
            except Exception as e:
                logger.error(f"❌ Failed to set MLflow experiment: {e}")
                validation_results['mlflow'] = False
                
        except Exception as e:
            logger.error(f"❌ MLflow connection failed: {e}")
            validation_results['mlflow'] = False
        
        # 3. Проверка Kafka
        logger.info("\n[3/3] Validating Kafka connection...")
        try:
            # Получаем конфигурацию Kafka из config.yml
            kafka_config = self.config.get('kafka', {})
            bootstrap_servers = kafka_config.get('bootstrap_servers', self.kafka_bootstrap_servers)
            
            logger.info(f"Kafka Bootstrap Servers: {bootstrap_servers}")
            logger.info(f"Kafka Topic: {self.kafka_topic}")
            
            # Создаем тестовый consumer для проверки подключения
            test_consumer = Consumer({
                'bootstrap.servers': bootstrap_servers,
                'group.id': 'fraud_detection_validation_test',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            })
            
            # Получаем метаданные топика для проверки
            metadata = test_consumer.list_topics(topic=self.kafka_topic, timeout=5)
            
            if self.kafka_topic in metadata.topics:
                logger.info(f"✅ Kafka topic '{self.kafka_topic}' is available")
                logger.info(f"   Partitions: {len(metadata.topics[self.kafka_topic].partitions)}")
                validation_results['kafka'] = True
            else:
                logger.warning(f"⚠️  Kafka topic '{self.kafka_topic}' not found (will be auto-created)")
                validation_results['kafka'] = True  # Auto-create enabled
            
            test_consumer.close()
            logger.info("✅ Kafka connection verified successfully")
            
        except KafkaException as e:
            logger.error(f"❌ Kafka connection failed: {e}")
            validation_results['kafka'] = False
        except Exception as e:
            logger.error(f"❌ Kafka validation error: {e}")
            validation_results['kafka'] = False
        
        # Итоговая проверка
        logger.info("\n" + "="*60)
        logger.info("Environment Validation Results:")
        logger.info("="*60)
        logger.info(f"MinIO/S3:  {'✅ PASSED' if validation_results['minio'] else '❌ FAILED'}")
        logger.info(f"MLflow:    {'✅ PASSED' if validation_results['mlflow'] else '❌ FAILED'}")
        logger.info(f"Kafka:     {'✅ PASSED' if validation_results['kafka'] else '❌ FAILED'}")
        logger.info("="*60)
        
        all_passed = all(validation_results.values())
        
        if all_passed:
            logger.info("✅ All environment validations passed!")
        else:
            logger.error("❌ Some environment validations failed. Cannot proceed with training.")
        
        return all_passed
    
    def read_data_from_kafka(self, max_messages: int = 10000, timeout_seconds: int = 30) -> list:
        """
        Чтение данных из Kafka топика 'transactions'
        
        Заглушка для чтения потока транзакций из Kafka.
        В будущем будет реализовано полное чтение и парсинг JSON сообщений.
        
        Args:
            max_messages: максимальное количество сообщений для чтения
            timeout_seconds: таймаут чтения в секундах
            
        Returns:
            список транзакций (словарей)
        """
        logger.info(f"Reading data from Kafka topic '{self.kafka_topic}'...")
        logger.info(f"Max messages: {max_messages}, Timeout: {timeout_seconds}s")
        
        transactions = []
        
        try:
            # Получаем конфигурацию Kafka из config.yml
            kafka_config = self.config.get('kafka', {})
            
            # Создаем Kafka consumer
            consumer_config = {
                'bootstrap.servers': kafka_config.get('bootstrap_servers', self.kafka_bootstrap_servers),
                'group.id': kafka_config.get('consumer_group_id', 'fraud_detection_training'),
                'auto.offset.reset': kafka_config.get('auto_offset_reset', 'earliest'),
                'enable.auto.commit': kafka_config.get('enable_auto_commit', True)
            }
            
            self.kafka_consumer = Consumer(consumer_config)
            self.kafka_consumer.subscribe([self.kafka_topic])
            
            logger.info(f"✅ Subscribed to topic '{self.kafka_topic}'")
            logger.info("📊 Starting to consume messages...")
            
            # Заглушка: в будущем здесь будет реальное чтение
            # import json
            # message_count = 0
            # while message_count < max_messages:
            #     msg = self.kafka_consumer.poll(timeout=1.0)
            #     if msg is None:
            #         continue
            #     if msg.error():
            #         logger.error(f"Consumer error: {msg.error()}")
            #         continue
            #     
            #     transaction = json.loads(msg.value().decode('utf-8'))
            #     transactions.append(transaction)
            #     message_count += 1
            
            logger.warning("⚠️  read_data_from_kafka is currently a stub")
            logger.info("   This method will be implemented in the next step")
            
            return transactions
            
        except Exception as e:
            logger.error(f"❌ Error reading from Kafka: {e}")
            return transactions
        
        finally:
            if self.kafka_consumer:
                self.kafka_consumer.close()
                logger.info("Kafka consumer closed")


if __name__ == "__main__":
    """
    Тестовый запуск для проверки валидации окружения
    """
    # Создаем экземпляр класса
    trainer = FraudDetectionTraining()
    
    # Выполняем валидацию окружения
    is_valid = trainer.validate_environment()
    
    if is_valid:
        logger.info("\n🎉 Environment is ready for ML training!")
    else:
        logger.error("\n❌ Environment validation failed. Please fix the issues above.")
