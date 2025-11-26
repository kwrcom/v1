"""CLI helper to run FraudDetectionTraining.validate_environment() locally.

Usage:
    python validate_environment_runner.py

This script will try to use the repository's config.yml (if present) so it's usable outside of Airflow container.
"""
import os
import sys
import logging

try:
    from app_models.fraud_detection_training import FraudDetectionTraining
except Exception as exc:  # pragma: no cover - helpful debug output
    print("Failed to import FraudDetectionTraining:", exc)
    raise

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def find_repo_config() -> str:
    # Try to locate config.yml in repository root (up from SRC/airflow/dags)
    here = os.path.abspath(os.path.dirname(__file__))
    candidates = [
        os.path.join(here, '..', '..', '..', 'config.yml'),
        os.path.join(here, '..', '..', 'config.yml'),
    ]

    for c in candidates:
        path = os.path.abspath(c)
        if os.path.exists(path):
            return path

    return None


def main() -> int:
    config_path = find_repo_config()
    if config_path:
        LOG.info(f"Using config at: {config_path}")
    else:
        LOG.warning("Repository config.yml not found — using default FraudDetectionTraining config path")

    try:
        trainer = FraudDetectionTraining(config_path) if config_path else FraudDetectionTraining()
        success = trainer.validate_environment()
        if success:
            LOG.info("Environment validation succeeded")
            return 0
        else:
            LOG.error("Environment validation failed")
            return 2

    except Exception as e:
        LOG.exception("Error running environment validation: %s", e)
        return 1


if __name__ == '__main__':
    sys.exit(main())
