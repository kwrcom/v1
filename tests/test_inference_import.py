def test_inference_module_importable():
    """Simple import test for the realtime_inference module."""
    try:
        import app_models  # ensure package context for inference imports
    except Exception:
        # package might not be a proper package; ignore
        pass

    from importlib import import_module
    m = import_module('SRC.inference.realtime_inference'.replace('/', '.').replace('\\', '.'))
    assert m is not None
