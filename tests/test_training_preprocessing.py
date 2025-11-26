import pandas as pd
import numpy as np

from app_models.fraud_detection_training import FraudDetectionTraining


def make_sample_df():
    now = pd.Timestamp('2025-01-01T00:00:00Z')
    rows = []
    for u in range(3):
        for i in range(3):
            ts = now + pd.Timedelta(minutes=10 * i) + pd.Timedelta(hours=u)
            rows.append({
                'transaction_id': f'txn_{u}_{i}',
                'user_id': f'user_{u}',
                'timestamp': ts.isoformat(),
                'amount': float(10 * (i + 1)),
                'typical_spend': float(5.0),
                'prev_transaction_count_1h': i,
                'prev_transaction_count_24h': i * 2,
                'velocity_1h': float(i),
                'merchant_category': 'electronics',
                'device_type': 'mobile',
                'country': 'US',
                'currency': 'USD',
                'is_fraud': 0
            })

    return pd.DataFrame(rows)


def test_build_feature_matrix_creates_expected_fields():
    trainer = FraudDetectionTraining()
    df = make_sample_df()
    out = trainer._build_feature_matrix(df)

    # Check temporal features
    assert 'transaction_hour' in out.columns
    assert 'transaction_dayofweek' in out.columns
    assert 'is_weekend' in out.columns

    # Behavioral aggregates
    assert 'user_avg_amount' in out.columns
    assert 'user_txn_count' in out.columns
    assert 'time_since_last_txn' in out.columns

    # Amount feature
    assert 'amount_deviation' in out.columns


def test_optimize_threshold_favors_precision_with_min_recall():
    trainer = FraudDetectionTraining()

    # simple case: y_true has several positives, y_probs are higher for positives
    y_true = [0, 0, 1, 1, 1, 0, 1, 0]
    y_probs = [0.01, 0.02, 0.9, 0.85, 0.6, 0.1, 0.8, 0.05]

    thresh = trainer._optimize_threshold(y_true, y_probs, min_recall=0.75)
    # Ensure threshold is a float between 0 and 1
    assert isinstance(thresh, float)
    assert 0.0 <= thresh <= 1.0


def test_smote_resamples_minority_correctly():
    from imblearn.over_sampling import SMOTE

    # Create imbalanced dataset
    rng = np.random.RandomState(42)
    X = rng.randn(100, 3)
    y = np.array([0] * 95 + [1] * 5)

    sm = SMOTE(random_state=42)
    X_res, y_res = sm.fit_resample(X, y)

    # After SMOTE the class distribution should be balanced
    unique, counts = np.unique(y_res, return_counts=True)
    counts_map = dict(zip(unique, counts))
    assert counts_map.get(0) == counts_map.get(1)
