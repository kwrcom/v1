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
import pandas as pd
from typing import List

from sklearn.model_selection import train_test_split, RandomizedSearchCV, StratifiedKFold
from sklearn.pipeline import Pipeline as SkPipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.metrics import precision_score, recall_score, f1_score, roc_auc_score, precision_recall_curve, roc_curve
import matplotlib.pyplot as plt
import io

from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline as ImbPipeline

from xgboost import XGBClassifier
import joblib
import json
import tempfile
import os
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

        # 4. Проверка доступности библиотек для обучения
        logger.info("\n[4/4] Validating training dependencies (XGBoost, scikit-learn, imbalanced-learn)...")
        try:
            import xgboost  # noqa: F401
            import sklearn  # noqa: F401
            import imblearn  # noqa: F401
            validation_results['training_deps'] = True
            logger.info("✅ Required training packages are importable: xgboost, sklearn, imblearn")
        except Exception as e:
            logger.error(f"❌ Training dependency import failed: {e}")
            validation_results['training_deps'] = False
        
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

    def _build_feature_matrix(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Build additional temporal and behavioral features for the training dataset.

        Args:
            df: input DataFrame containing raw transaction events

        Returns:
            DataFrame with new engineered features appended
        """
        # Ensure timestamp is datetime
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

        # Temporal features
        if 'timestamp' in df.columns:
            df['transaction_hour'] = df['timestamp'].dt.hour
            df['transaction_dayofweek'] = df['timestamp'].dt.dayofweek
            df['is_weekend'] = df['transaction_dayofweek'].isin([5, 6]).astype(int)

        # Behavioral features: per-user aggregates
        if 'user_id' in df.columns:
            # average and std amount per user
            df['user_avg_amount'] = df.groupby('user_id')['amount'].transform('mean')
            df['user_std_amount'] = df.groupby('user_id')['amount'].transform('std').fillna(0.0)
            df['user_txn_count'] = df.groupby('user_id')['transaction_id'].transform('count')

            # time since previous transaction for the same user
            try:
                df = df.sort_values(['user_id', 'timestamp'])
                df['prev_txn_ts'] = df.groupby('user_id')['timestamp'].shift(1)
                df['time_since_last_txn'] = (df['timestamp'] - df['prev_txn_ts']).dt.total_seconds().fillna(-1)
                df.drop(columns=['prev_txn_ts'], inplace=True)
            except Exception:
                df['time_since_last_txn'] = -1

        # Amount-based features
        df['amount_deviation'] = df['amount'] / (df.get('typical_spend', pd.Series([1])) + 1e-9)

        # Fill NaNs that can break downstream transformers
        df.fillna({'merchant_category': 'missing', 'device_type': 'missing', 'country': 'missing', 'currency': 'missing'}, inplace=True)

        return df

    def _optimize_threshold(self, y_true: List[int], y_probs: List[float], min_recall: float = 0.5) -> float:
        """
        Find a decision threshold based on maximizing precision subject to a minimum recall.

        Args:
            y_true: true labels
            y_probs: predicted probabilities for the positive class
            min_recall: minimum recall constraint

        Returns:
            Selected threshold value (float)
        """
        precisions, recalls, thresholds = precision_recall_curve(y_true, y_probs)

        # precision_recall_curve returns arrays where thresholds has len n-1
        best_thresh = 0.5
        best_precision = 0.0

        # Iterate over thresholds and pick the one with highest precision subject to recall >= min_recall
        for p, r, t in zip(precisions[:-1], recalls[:-1], thresholds):
            if r >= min_recall and p >= best_precision:
                best_precision = p
                best_thresh = t

        # If no threshold satisfies min_recall, fallback to threshold that maximizes F1
        if best_precision == 0.0:
            best_f1 = 0.0
            for p, r, t in zip(precisions[:-1], recalls[:-1], thresholds):
                if p + r > 0:
                    f1 = 2 * p * r / (p + r)
                    if f1 > best_f1:
                        best_f1 = f1
                        best_thresh = t

        return float(best_thresh)

    def train_model(self,
                    data: Optional[pd.DataFrame] = None,
                    target_col: str = 'is_fraud',
                    test_size: float = 0.2,
                    random_state: int = 42,
                    sampling_strategy: str = 'smote',
                    n_iter: int = 20,
                    cv: int = 3,
                    scoring: str = 'precision',
                    n_jobs: int = 1,
                    min_recall: float = 0.5) -> Dict[str, Any]:
        """
        End-to-end training routine for fraud detection using XGBoost.

        Steps implemented:
        - Feature engineering (temporal + behavioral)
        - Train/test split (stratified)
        - Class imbalance handling using SMOTE (recommended) or fallback
        - Hyperparameter tuning via RandomizedSearchCV
        - Post-training threshold optimization (aimed to optimize Precision)
        - MLflow logging and (best-effort) model registration

        Args:
            data: optional DataFrame; if None, reads from Kafka via read_data_from_kafka()
            target_col: name of the target column (default 'is_fraud')
            test_size: fraction for hold-out validation
            random_state: random seed for reproducibility
            sampling_strategy: 'smote' or 'scale_pos_weight' fallback
            n_iter: RandomizedSearchCV n_iter
            cv: number of cross-validation folds
            scoring: scoring metric for RandomizedSearchCV
            n_jobs: parallel jobs
            min_recall: minimum recall constraint during threshold optimization

        Returns:
            dict with training metadata (run_id, best_params, metrics, threshold, model_uri)
        """
        logger.info("Starting train_model workflow...")

        # 1) Load data
        if data is None:
            logger.info("No DataFrame provided to train_model(), attempting to read data from Kafka stub...")
            rows = self.read_data_from_kafka(max_messages=10000)
            if not rows:
                logger.error("No data available for training (read_data_from_kafka returned empty list). Aborting training.")
                raise RuntimeError("No training data available")

            df = pd.DataFrame(rows)
        else:
            df = data.copy()

        logger.info(f"Loaded dataset with {len(df)} rows")

        # 2) Basic sanity checks
        if target_col not in df.columns:
            logger.error(f"Target column '{target_col}' missing in dataset")
            raise ValueError(f"Target column '{target_col}' not found in data")

        # Drop rows with missing target
        df = df.dropna(subset=[target_col])

        # 3) Feature engineering
        df = self._build_feature_matrix(df)

        # Define feature lists
        all_columns = list(df.columns)
        excluded = [target_col, 'transaction_id', 'user_id', 'timestamp']
        feature_columns = [c for c in all_columns if c not in excluded]

        # Prefer numeric vs categorical split
        numeric_cols = [c for c in feature_columns if pd.api.types.is_numeric_dtype(df[c])]
        categorical_cols = [c for c in feature_columns if c not in numeric_cols]

        logger.info(f"Numeric features: {numeric_cols}")
        logger.info(f"Categorical features: {categorical_cols}")

        # 4) Train / validation split
        X = df[feature_columns]
        y = df[target_col].astype(int)

        if len(y.unique()) < 2:
            logger.error("Training aborted: target does not contain at least two classes")
            raise RuntimeError("Target has fewer than 2 classes")

        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=test_size, stratify=y, random_state=random_state
        )

        logger.info(f"Training set: {len(X_train)} rows | Validation set: {len(X_val)} rows")

        # 5) Build preprocessing pipeline
        numeric_transformer = SkPipeline([
            ('imputer', SimpleImputer(strategy='median')),
            ('scaler', StandardScaler())
        ])

        categorical_transformer = SkPipeline([
            ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
            ('onehot', OneHotEncoder(handle_unknown='ignore', sparse=False))
        ])

        preprocessor = ColumnTransformer(
            transformers=[
                ('num', numeric_transformer, numeric_cols),
                ('cat', categorical_transformer, categorical_cols)
            ], remainder='drop'
        )

        # 6) Build model pipeline (with SMOTE)
        classifier = XGBClassifier(
            objective='binary:logistic',
            use_label_encoder=False,
            random_state=random_state,
            n_jobs=n_jobs,
            verbosity=0
        )

        if sampling_strategy == 'smote':
            sampler = SMOTE(random_state=random_state)
            pipeline = ImbPipeline(steps=[('preproc', preprocessor), ('smote', sampler), ('clf', classifier)])
            logger.info("SMOTE sampling will be applied during training")
        else:
            # fallback - use only preprocessing and classifier
            pipeline = ImbPipeline(steps=[('preproc', preprocessor), ('clf', classifier)])
            logger.info("SMOTE not applied; using 'scale_pos_weight' or no sampling depending on config")

        # 7) Hyperparameter search
        param_dist = {
            'clf__n_estimators': [100, 200, 500],
            'clf__max_depth': [3, 5, 7, 10],
            'clf__learning_rate': [0.01, 0.03, 0.05, 0.1],
            'clf__subsample': [0.6, 0.8, 1.0],
            'clf__colsample_bytree': [0.6, 0.8, 1.0],
            'clf__gamma': [0, 1, 5]
        }

        cv_splitter = StratifiedKFold(n_splits=cv, shuffle=True, random_state=random_state)

        search = RandomizedSearchCV(
            estimator=pipeline,
            param_distributions=param_dist,
            n_iter=n_iter,
            scoring=scoring,
            cv=cv_splitter,
            verbose=2,
            random_state=random_state,
            n_jobs=n_jobs,
            refit=True
        )

        logger.info("Starting RandomizedSearchCV for hyperparameter tuning...")
        search.fit(X_train, y_train)

        logger.info(f"RandomizedSearchCV complete. Best score: {search.best_score_:.4f}")
        logger.info(f"Best parameters: {search.best_params_}")

        # 8) Evaluate on validation set and compute optimal threshold
        best_estimator = search.best_estimator_

        logger.info("Scoring best estimator on validation set...")
        # predict probabilities
        if hasattr(best_estimator, 'predict_proba'):
            y_val_probs = best_estimator.predict_proba(X_val)[:, 1]
        else:
            # if pipeline doesn't implement predict_proba, use decision_function
            try:
                y_val_probs = best_estimator.decision_function(X_val)
            except Exception:
                y_val_probs = best_estimator.predict(X_val)

        # Default threshold optimization: maximize precision subject to min_recall
        selected_threshold = self._optimize_threshold(y_val.tolist(), y_val_probs.tolist(), min_recall=min_recall)

        # Apply threshold to obtain final predictions
        y_pred = (y_val_probs >= selected_threshold).astype(int)

        precision = precision_score(y_val, y_pred, zero_division=0)
        recall = recall_score(y_val, y_pred, zero_division=0)
        f1 = f1_score(y_val, y_pred, zero_division=0)
        try:
            auc = roc_auc_score(y_val, y_val_probs)
        except Exception:
            auc = float('nan')

        logger.info(f"Validation results — Precision: {precision:.4f}, Recall: {recall:.4f}, F1: {f1:.4f}, AUC: {auc:.4f}")

        # 9) MLflow logging
        mlflow.set_tracking_uri(self.config.get('mlflow', {}).get('tracking_uri', self.mlflow_tracking_uri))
        experiment_name = self.config.get('mlflow', {}).get('experiment_name', 'fraud_detection')
        mlflow.set_experiment(experiment_name)

        result: Dict[str, Any] = {}
        with mlflow.start_run() as run:
            run_id = run.info.run_id
            logger.info(f"MLflow run started: {run_id}")

            # Log params
            mlflow.log_param('sampling_strategy', sampling_strategy)
            mlflow.log_param('random_state', random_state)
            mlflow.log_param('test_size', test_size)
            mlflow.log_param('n_iter', n_iter)
            mlflow.log_param('cv', cv)

            # Best params mapping
            for k, v in search.best_params_.items():
                mlflow.log_param(k, v)

            # Log metrics
            mlflow.log_metric('val/precision', float(precision))
            mlflow.log_metric('val/recall', float(recall))
            mlflow.log_metric('val/f1', float(f1))
            if not (auc is None or (isinstance(auc, float) and (auc != auc))):
                mlflow.log_metric('val/auc', float(auc))

            # Prepare tmp folder for artifacts
            tmp = tempfile.mkdtemp()

            # Create and log ROC and Precision-Recall curve images
            try:
                # ROC curve
                fpr, tpr, roc_thresholds = roc_curve(y_val, y_val_probs)
                pr_precision, pr_recall, pr_thresholds = precision_recall_curve(y_val, y_val_probs)

                roc_fig = plt.figure(figsize=(6, 4))
                plt.plot(fpr, tpr, label=f'AUC = {auc:.4f}')
                plt.plot([0, 1], [0, 1], linestyle='--', color='gray')
                plt.xlabel('False Positive Rate')
                plt.ylabel('True Positive Rate')
                plt.title('ROC Curve')
                plt.legend(loc='lower right')
                roc_path = os.path.join(tmp, 'roc_curve.png')
                roc_fig.savefig(roc_path)
                plt.close(roc_fig)
                mlflow.log_artifact(roc_path, artifact_path='model_artifacts')

                # Precision-Recall curve
                pr_fig = plt.figure(figsize=(6, 4))
                plt.plot(pr_recall, pr_precision, label='PR Curve')
                plt.xlabel('Recall')
                plt.ylabel('Precision')
                plt.title('Precision-Recall Curve')
                plt.legend(loc='lower left')
                pr_path = os.path.join(tmp, 'pr_curve.png')
                pr_fig.savefig(pr_path)
                plt.close(pr_fig)
                mlflow.log_artifact(pr_path, artifact_path='model_artifacts')
            except Exception as e:
                logger.warning(f"Failed to create/log ROC/PR curves: {e}")

            # Save and log pipeline artifact (Pickle)
            tmp = tempfile.mkdtemp()
            model_path = os.path.join(tmp, 'pipeline.joblib')
            joblib.dump(best_estimator, model_path)
            mlflow.log_artifact(model_path, artifact_path='model_artifacts')

            # Log threshold
            threshold_file = os.path.join(tmp, 'decision_threshold.json')
            with open(threshold_file, 'w', encoding='utf-8') as fh:
                json.dump({'threshold': float(selected_threshold)}, fh)
            mlflow.log_artifact(threshold_file, artifact_path='model_artifacts')

            # Register model (best-effort)
            registered_name = self.config.get('mlflow', {}).get('registered_model_name', 'fraud_detection_xgboost')
            try:
                # Log artifact as a model to MLflow (sklearn serialization)
                mlflow.sklearn.log_model(best_estimator, artifact_path='model')
                model_uri = f"runs:/{run_id}/model"
                logger.info(f"Logging model to MLflow at {model_uri}")

                # Register model
                registered_model = mlflow.register_model(model_uri, registered_name)
                model_version = registered_model.version
                logger.info(f"Registered model {registered_name} version {model_version}")
                result['registered_model_version'] = model_version
            except Exception as e:
                logger.warning(f"Model registration failed or registry not available: {e}")
                model_uri = f"runs:/{run_id}/model"

            result.update({
                'run_id': run_id,
                'model_uri': model_uri,
                'best_params': search.best_params_,
                'threshold': float(selected_threshold),
                'metrics': {'precision': float(precision), 'recall': float(recall), 'f1': float(f1), 'auc': float(auc)}
            })

        logger.info("Training completed and logged to MLflow")
        return result

    def compare_and_promote(self,
                            run_id: str,
                            registered_model_name: Optional[str] = None,
                            primary_metric: str = 'val/precision',
                            min_improvement_pct: float = 0.0) -> Dict[str, Any]:
        """
        Compare a candidate run/model to the current Production model (by primary_metric) 
        and promote the candidate to Production if it improves upon production.

        Args:
            run_id: MLflow run id for the candidate model
            registered_model_name: model name in the registry; defaults to config setting
            primary_metric: the metric key to compare (e.g., 'val/precision')
            min_improvement_pct: required relative improvement over production (0.0 means any improvement)

        Returns:
            dict with promotion decision and related metadata
        """
        mlflow.set_tracking_uri(self.config.get('mlflow', {}).get('tracking_uri', self.mlflow_tracking_uri))
        client = mlflow.tracking.MlflowClient()

        if registered_model_name is None:
            registered_model_name = self.config.get('mlflow', {}).get('registered_model_name', 'fraud_detection_xgboost')

        logger.info(f"Comparing candidate run {run_id} against registry model '{registered_model_name}' using metric '{primary_metric}'")

        # Get candidate metrics
        try:
            candidate_run = client.get_run(run_id)
            candidate_metrics = candidate_run.data.metrics
            candidate_value = candidate_metrics.get(primary_metric)
        except Exception as e:
            logger.error(f"Failed to fetch candidate run metrics for {run_id}: {e}")
            raise

        if candidate_value is None:
            logger.error(f"Candidate run {run_id} does not contain metric '{primary_metric}'")
            raise ValueError(f"Candidate missing metric {primary_metric}")

        # Find existing production model versions
        prod_versions = client.get_latest_versions(name=registered_model_name, stages=['Production'])

        production_value = None
        production_version = None

        if prod_versions:
            # pick the highest version among production versions (should be one, but handle multiple)
            production_version = sorted(prod_versions, key=lambda v: int(v.version))[-1]
            try:
                prod_run = client.get_run(production_version.run_id)
                production_value = prod_run.data.metrics.get(primary_metric)
            except Exception as e:
                logger.warning(f"Unable to load production run metrics for version {production_version.version}: {e}")

        logger.info(f"Candidate {primary_metric}: {candidate_value}, Production {primary_metric}: {production_value}")

        decision = {
            'candidate_run_id': run_id,
            'candidate_value': float(candidate_value),
            'production_value': float(production_value) if production_value is not None else None,
            'promoted': False,
            'notes': ''
        }

        # Determine if candidate is better than production
        should_promote = False
        if production_value is None:
            logger.info("No existing Production model found — promoting candidate by default")
            should_promote = True
        else:
            # Relative improvement check
            try:
                prod_val = float(production_value)
                cand_val = float(candidate_value)
                required = prod_val * (1.0 + float(min_improvement_pct) / 100.0)
                if cand_val > required:
                    should_promote = True
            except Exception as e:
                logger.error(f"Comparison error: {e}")

        # Find candidate model version corresponding to run_id
        candidate_version = None
        try:
            for v in client.search_model_versions(f"name='{registered_model_name}'"):
                if v.run_id == run_id:
                    candidate_version = v
                    break
        except Exception as e:
            logger.warning(f"Error searching model versions for run_id {run_id}: {e}")

        if not candidate_version:
            logger.error(f"Candidate model version not found in registry for run {run_id}. Ensure the model was registered previously.")
            decision['notes'] = 'candidate_version_not_found'
            return decision

        if should_promote:
            logger.info(f"Promoting model version {candidate_version.version} to Production")
            try:
                client.transition_model_version_stage(
                    name=registered_model_name,
                    version=candidate_version.version,
                    stage='Production',
                    archive_existing_versions=True
                )

                # Tag model-version with decision metadata
                client.set_model_version_tag(registered_model_name, candidate_version.version, 'promoted_by', 'fraud_detection_training')
                client.set_model_version_tag(registered_model_name, candidate_version.version, f'metric/{primary_metric}', str(candidate_value))
                if production_version:
                    client.set_model_version_tag(registered_model_name, candidate_version.version, 'baseline_prod_version', production_version.version)

                decision['promoted'] = True
                decision['promoted_version'] = candidate_version.version
                decision['notes'] = 'promoted'

                # Ensure model artifact (.joblib or .pkl) exists in MLflow artifact store (MinIO).
                # Attempt to download the trained pipeline artifact and upload to MinIO bucket
                try:
                    artifact_path = 'model_artifacts/pipeline.joblib'
                    tmpdir = tempfile.mkdtemp()
                    dst = client.download_artifacts(run_id, artifact_path, dst_path=tmpdir)
                    # dst is path to the artifact file or directory; find file
                    local_model_file = dst

                    # Ensure s3 client available
                    if self.s3_client is None:
                        self.check_minio_connection()

                    if self.s3_client:
                        bucket = 'mlflow'
                        key = f"models/{registered_model_name}/{candidate_version.version}/pipeline.joblib"
                        with open(local_model_file, 'rb') as fh:
                            self.s3_client.put_object(Bucket=bucket, Key=key, Body=fh)
                        logger.info(f"Uploaded model artifact to MinIO: s3://{bucket}/{key}")
                        decision['minio_artifact'] = f"s3://{bucket}/{key}"
                except Exception as exc:
                    logger.warning(f"Failed to download/log artifact to MinIO: {exc}")

            except Exception as e:
                logger.error(f"Failed to transition model version stage: {e}")
                decision['notes'] = f'transition_failed: {e}'
        else:
            logger.info("Candidate did not improve production model — not promoting")
            decision['notes'] = 'rejected_due_to_no_improvement'

        return decision


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
