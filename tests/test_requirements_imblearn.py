def test_imblearn_import():
    """Sanity test to ensure imbalanced-learn (SMOTE) is importable in the environment."""
    try:
        from imblearn.over_sampling import SMOTE
    except Exception as e:
        raise ImportError("imbalanced-learn is not installed or cannot be imported: " + str(e))

    assert SMOTE is not None
