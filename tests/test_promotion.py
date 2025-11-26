import os

import pytest

import mlflow

from app_models.fraud_detection_training import FraudDetectionTraining


class FakeRun:
    def __init__(self, run_id, metrics):
        self.info = type('o', (), {'run_id': run_id})
        self.data = type('o', (), {'metrics': metrics})


class FakeModelVersion:
    def __init__(self, version, run_id, stage='None'):
        self.version = str(version)
        self.run_id = run_id
        self.current_stage = stage


class FakeMlflowClient:
    def __init__(self, runs: dict, versions: list):
        # runs: dict run_id -> metrics dict
        self.runs = runs
        # versions: list of FakeModelVersion
        self.versions = versions
        self._transition_calls = []
        self._tags = []

    def get_run(self, run_id):
        if run_id not in self.runs:
            raise Exception('run not found')
        return FakeRun(run_id, self.runs[run_id])

    def get_latest_versions(self, name, stages=None):
        # return versions that are currently in requested stage(s)
        if stages is None:
            return self.versions
        return [v for v in self.versions if v.current_stage in stages]

    def search_model_versions(self, query):
        # ignore query parsing; return all
        return self.versions

    def transition_model_version_stage(self, name, version, stage, archive_existing_versions=False):
        self._transition_calls.append((name, version, stage, archive_existing_versions))
        # mark version as production
        for v in self.versions:
            if v.version == str(version):
                v.current_stage = stage

    def set_model_version_tag(self, name, version, key, value):
        self._tags.append((name, version, key, value))

    def download_artifacts(self, run_id, artifact_path, dst_path=None):
        # create a dummy file
        out_file = os.path.join(dst_path, 'pipeline.joblib')
        with open(out_file, 'wb') as f:
            f.write(b'fake')
        return out_file


def test_compare_and_promote_promotes_when_better(monkeypatch):
    fake_runs = {'prod_run': {'val/precision': 0.80}, 'cand_run': {'val/precision': 0.82}}
    prod_v = FakeModelVersion(version=1, run_id='prod_run', stage='Production')
    cand_v = FakeModelVersion(version=2, run_id='cand_run', stage='None')

    fake_client = FakeMlflowClient(fake_runs, [prod_v, cand_v])

    monkeypatch.setattr(mlflow.tracking, 'MlflowClient', lambda *args, **kwargs: fake_client)

    trainer = FraudDetectionTraining()
    # run compare
    res = trainer.compare_and_promote('cand_run', registered_model_name='fraud_detection_xgboost', primary_metric='val/precision')

    assert res['promoted'] is True
    assert res['promoted_version'] == '2'
    assert 'minio_artifact' in res or res['notes'] in ('promoted',)


def test_compare_and_promote_rejects_when_worse(monkeypatch):
    fake_runs = {'prod_run': {'val/precision': 0.80}, 'cand_run': {'val/precision': 0.79}}
    prod_v = FakeModelVersion(version=1, run_id='prod_run', stage='Production')
    cand_v = FakeModelVersion(version=2, run_id='cand_run', stage='None')

    fake_client = FakeMlflowClient(fake_runs, [prod_v, cand_v])

    monkeypatch.setattr(mlflow.tracking, 'MlflowClient', lambda *args, **kwargs: fake_client)

    trainer = FraudDetectionTraining()
    res = trainer.compare_and_promote('cand_run', registered_model_name='fraud_detection_xgboost', primary_metric='val/precision')

    assert res['promoted'] is False
    assert res['notes'] == 'rejected_due_to_no_improvement'
