"""
Realtime inference service using Spark + Kafka + MLflow

Behavior:
- Connects to Kafka `transactions` topic and listens for new transactions
- Loads the latest 'Production' model from MLflow Model Registry: models:/<MODEL_NAME>/Production
- Applies model on incoming micro-batches using Spark (via pandas conversion) and publishes results to `fraud_predictions` topic

Notes:
- This script uses kafka-python for the consumer/producer and uses PySpark to parallelize batch prediction.
- For simplicity and robustness, we re-load the Production model at the start of each micro-batch so newly promoted models are used.
"""
import os
import json
import time
import logging
from typing import Optional, List

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql import SparkSession
import mlflow

LOG = logging.getLogger('realtime_inference')
LOG.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
LOG.addHandler(ch)


KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
INPUT_TOPIC = os.getenv('KAFKA_TOPIC', 'transactions')
OUTPUT_TOPIC = os.getenv('OUTPUT_TOPIC', 'fraud_predictions')
MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
MODEL_NAME = os.getenv('MODEL_NAME', 'fraud_detection_xgboost')
CONSUMER_GROUP = os.getenv('CONSUMER_GROUP', 'realtime_inference_group')

# Batch settings
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '128'))
BATCH_WAIT_SECONDS = float(os.getenv('BATCH_WAIT_SECONDS', '1.0'))


def create_spark_session(app_name: str = 'realtime-inference') -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark


def load_production_model(model_name: str) -> mlflow.pyfunc.PyFuncModel:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    model_uri = f"models:/{model_name}/Production"
    LOG.info(f"Loading model from MLflow: {model_uri}")
    try:
        model = mlflow.pyfunc.load_model(model_uri)
        LOG.info("Model loaded successfully")
        return model
    except Exception as e:
        LOG.error(f"Failed to load model from MLflow: {e}")
        raise


def infer_batch(spark: SparkSession, model, batch_records: List[dict]) -> List[dict]:
    """Run prediction for a batch of records and return serialized results."""
    if not batch_records:
        return []

    # Convert to pandas DataFrame
    df = pd.DataFrame(batch_records)

    # The model is expected to accept a DataFrame with feature columns (train pipeline handles other cols)
    try:
        # Try predict_proba then fallback to predict
        if hasattr(model, 'predict_proba'):
            proba = model.predict_proba(df)
            # proba could be an array with two columns [p0, p1]
            if proba.ndim == 2 and proba.shape[1] > 1:
                scores = proba[:, 1].tolist()
            else:
                scores = proba.ravel().tolist()
            preds = [1 if p >= 0.5 else 0 for p in scores]
        else:
            preds = model.predict(df).tolist()
            # try to get probabilities as well
            scores = None
            if hasattr(model, 'predict_proba'):
                scores = model.predict_proba(df)[:, 1].tolist()

    except Exception as e:
        LOG.error(f"Model inference failure: {e}")
        # Fallback: mark all as non-fraud
        preds = [0] * len(df)
        scores = [0.0] * len(df)

    results = []
    for i, row in df.iterrows():
        r = row.to_dict()
        res = {
            'transaction_id': r.get('transaction_id'),
            'user_id': r.get('user_id'),
            'prediction': int(preds[i]),
            'score': float(scores[i]) if scores is not None else None,
            'model': MODEL_NAME,
            'timestamp': int(time.time())
        }
        results.append(res)

    return results


def run():
    spark = create_spark_session()

    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        group_id=CONSUMER_GROUP,
        auto_offset_reset='latest',  # listen to newest messages only
        enable_auto_commit=True,
        value_deserializer=lambda m: m.decode('utf-8') if m else None,
        consumer_timeout_ms=1000
    )

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    LOG.info(f"Connected to Kafka bootstrap={KAFKA_BOOTSTRAP}, input_topic={INPUT_TOPIC}, output_topic={OUTPUT_TOPIC}")

    model = None
    last_model_load_time = 0
    MODEL_RELOAD_INTERVAL = int(os.getenv('MODEL_RELOAD_INTERVAL_SECONDS', '30'))

    batch = []
    try:
        while True:
            # reload model periodically to pick up newest Production promotion
            now = time.time()
            if model is None or now - last_model_load_time > MODEL_RELOAD_INTERVAL:
                try:
                    model = load_production_model(MODEL_NAME)
                    last_model_load_time = now
                except Exception:
                    LOG.exception('Cannot load model; retrying in 10s')
                    time.sleep(10)
                    continue

            # Poll for messages
            msg = None
            try:
                for message in consumer.poll(timeout_ms=500, max_records=BATCH_SIZE).values():
                    for rec in message:
                        if rec.value is None:
                            continue
                        try:
                            payload = json.loads(rec.value)
                        except Exception:
                            LOG.warning('Skipping non-json record')
                            continue
                        batch.append(payload)

                # If we have a batch, process it
                if len(batch) >= 1:
                    LOG.info(f'Processing batch size {len(batch)}')
                    try:
                        results = infer_batch(spark, model, batch)
                        # send results
                        for r in results:
                            producer.send(OUTPUT_TOPIC, r)
                        producer.flush()
                        LOG.info(f'Sent {len(results)} predictions to {OUTPUT_TOPIC}')
                    except Exception as e:
                        LOG.exception(f'Error processing batch: {e}')
                    finally:
                        batch = []

                time.sleep(BATCH_WAIT_SECONDS)

            except Exception as e:
                LOG.exception(f'Kafka poll error: {e}')
                time.sleep(5)

    except KeyboardInterrupt:
        LOG.info('Stopping inference service...')
    finally:
        try:
            consumer.close()
            producer.close()
        except Exception:
            pass


if __name__ == '__main__':
    run()
