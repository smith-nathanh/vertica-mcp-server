"""
Pytest configuration and fixtures for Vertica MCP Server tests
"""

import pytest
import os
from unittest.mock import MagicMock, patch


@pytest.fixture
def mock_vertica_connection():
    """Mock Vertica connection for testing"""
    with patch('vertica_python.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        yield mock_conn, mock_cursor


@pytest.fixture
def sample_connection_string():
    """Sample connection string for testing"""
    return "vertica://testuser:testpass@localhost:5433/testdb"


@pytest.fixture
def sample_connection_info():
    """Sample connection info dictionary"""
    return {
        "host": "localhost",
        "port": 5433,
        "user": "testuser",
        "password": "testpass",
        "database": "testdb"
    }


@pytest.fixture
def mock_env_vars():
    """Mock environment variables"""
    with patch.dict(os.environ, {
        'DB_CONNECTION_STRING': 'vertica://testuser:testpass@localhost:5433/testdb',
        'DEBUG': 'false',
        'QUERY_LIMIT_SIZE': '100',
        'MAX_ROWS_EXPORT': '10000'
    }):
        yield


def pytest_configure(config):
    """Configure pytest markers"""
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow running"
    )