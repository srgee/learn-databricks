# tests/conftest.py
import pytest
from databricks.connect import DatabricksSession


@pytest.fixture(scope='session')
def spark():
    return DatabricksSession.builder.getOrCreate()
