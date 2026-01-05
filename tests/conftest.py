import os
import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: tests requiring a live database")
    config.addinivalue_line("markers", "pipeline: end-to-end tests requiring data fixtures")


def pytest_collection_modifyitems(config, items):
    run_db = os.environ.get("RUN_DB_TESTS") == "1"
    run_pipeline = os.environ.get("RUN_PIPELINE_TESTS") == "1"

    skip_db = pytest.mark.skip(reason="set RUN_DB_TESTS=1 to run integration tests")
    skip_pipeline = pytest.mark.skip(reason="set RUN_PIPELINE_TESTS=1 to run pipeline tests")

    for item in items:
        if "integration" in item.keywords and not run_db:
            item.add_marker(skip_db)
        if "pipeline" in item.keywords and not run_pipeline:
            item.add_marker(skip_pipeline)
