import os
import tempfile
import pandas as pd
import numpy as np

from app_models.fraud_detection_training import FraudDetectionTraining


def make_e2e_dataset(n=300, fraud_rate=0.05):
    rng = np.random.RandomState(123)
    rows = []
    for i in range(n):
        is_fraud = 1 if rng.rand() < fraud_rate else 0
        rows.append({
            'transaction_id': f'txn_{i}',
            'user_id': f'user_{rng.randint(0, 50)}',
            'timestamp': pd.Timestamp('2025-01-01T00:00:00Z') + pd.Timedelta(seconds=i * 60),
            'amount': float(rng.lognormal(mean=3.0, sigma=1.0)),
            'typical_spend': float(rng.uniform(5, 200)),
            'prev_transaction_count_1h': int(rng.randint(0, 10)),
            'prev_transaction_count_24h': int(rng.randint(0, 50)),
            'velocity_1h': float(rng.rand() * 5),
            'merchant_category': rng.choice(['electronics', 'travel', 'groceries']),
            'device_type': rng.choice(['mobile', 'desktop']),
            'country': rng.choice(['US', 'GB', 'DE']),
            'currency': 'USD',
            'merchant_risk_score': float(rng.rand()),
            'is_fraud': int(is_fraud)
        })

    return pd.DataFrame(rows)


def test_train_model_e2e_runs_and_logs(tmp_path):
    # Use a local file-backed MLflow tracking uri for the test
    mlflow_dir = tmp_path / 'mlflow'
    os.environ['MLFLOW_TRACKING_URI'] = f'file://{mlflow_dir}'

    trainer = FraudDetectionTraining()

    # Create a small dataset for a quick E2E run. Keep n_iter and cv very small to keep runtime low.
    df = make_e2e_dataset(n=200, fraud_rate=0.06)

    result = trainer.train_model(data=df, n_iter=2, cv=2, n_jobs=1, test_size=0.2, min_recall=0.5)

    assert isinstance(result, dict)
    assert 'run_id' in result
    assert 'threshold' in result
    assert 'metrics' in result
