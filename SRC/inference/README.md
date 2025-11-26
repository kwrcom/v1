# Realtime Inference Service

This service listens to Kafka transactions and performs realtime scoring using the latest model in MLflow Model Registry.

Key points:

- Built to use PySpark for batch/pandemic parallelism and kafka-python for reliable producers/consumers.
- Loads the latest `Production` model from MLflow Registry: `models:/<MODEL_NAME>/Production`.
- Reads from input topic (configurable via `KAFKA_TOPIC`) and writes predictions to `fraud_predictions` topic.

Environment variables (defaults configured in `docker-compose.yml`):

- KAFKA_BOOTSTRAP_SERVERS (e.g., kafka:9092)
- KAFKA_TOPIC (input, default `transactions`)
- OUTPUT_TOPIC (output, default `fraud_predictions`)
- MLFLOW_TRACKING_URI (e.g., http://mlflow:5000)
- MODEL_NAME (registered model name, default `fraud_detection_xgboost`)
- BATCH_SIZE / BATCH_WAIT_SECONDS to control micro-batch processing

Run locally (simple):

```bash
# from the repo root
docker build -t realtime_inference:latest -f SRC/inference/Dockerfile SRC/inference
docker run --env KAFKA_BOOTSTRAP_SERVERS=kafka:9092 --env MLFLOW_TRACKING_URI=http://mlflow:5000 realtime_inference:latest
```

In Compose: the service `inference` is already present and wired in `docker-compose.yml`.
