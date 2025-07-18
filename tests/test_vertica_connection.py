import pytest
from unittest.mock import MagicMock, patch, AsyncMock
import vertica_python
import os
import sys
from pathlib import Path

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from vertica_mcp_server.server import VerticaConnection


class TestVerticaConnection:
    """Test cases for VerticaConnection class"""

    @pytest.mark.unit
    def test_init_with_url_connection_string(self):
        """Test initialization with URL format connection string"""
        connection_string = "vertica://testuser:testpass@localhost:5433/testdb"
        vertica_conn = VerticaConnection(connection_string)
        
        assert vertica_conn.connection_string == connection_string
        assert vertica_conn.connection_info == {
            "host": "localhost",
            "port": 5433,
            "user": "testuser",
            "password": "testpass",
            "database": "testdb"
        }

    @pytest.mark.unit
    def test_init_with_simple_connection_string(self):
        """Test initialization with simple format connection string"""
        connection_string = "localhost:5433/testdb"
        
        with patch.dict(os.environ, {
            'VERTICA_USER': 'testuser',
            'VERTICA_PASSWORD': 'testpass'
        }):
            vertica_conn = VerticaConnection(connection_string)
            
            assert vertica_conn.connection_string == connection_string
            assert vertica_conn.connection_info == {
                "host": "localhost",
                "port": 5433,
                "database": "testdb",
                "user": "testuser",
                "password": "testpass"
            }

    @pytest.mark.unit
    def test_init_with_simple_connection_string_default_port(self):
        """Test initialization with simple format without port"""
        connection_string = "localhost/testdb"
        
        with patch.dict(os.environ, {
            'VERTICA_USER': 'testuser',
            'VERTICA_PASSWORD': 'testpass'
        }):
            vertica_conn = VerticaConnection(connection_string)
            
            assert vertica_conn.connection_info == {
                "host": "localhost",
                "port": 5433,
                "database": "testdb",
                "user": "testuser",
                "password": "testpass"
            }

    @pytest.mark.unit
    def test_init_with_empty_connection_string(self):
        """Test initialization with empty connection string"""
        with pytest.raises(ValueError, match="Database connection string is required"):
            VerticaConnection("")

    @pytest.mark.unit
    def test_init_with_none_connection_string(self):
        """Test initialization with None connection string"""
        with pytest.raises(ValueError, match="Database connection string is required"):
            VerticaConnection(None)

    @pytest.mark.unit
    def test_init_with_invalid_connection_string(self):
        """Test initialization with invalid connection string format"""
        with pytest.raises(ValueError, match="Invalid connection string format"):
            VerticaConnection("invalid_format")

    @pytest.mark.unit
    def test_parse_connection_string_url_format(self):
        """Test parsing URL format connection string"""
        connection_string = "vertica://testuser:testpass@localhost:5433/testdb"
        vertica_conn = VerticaConnection(connection_string)
        
        expected = {
            "host": "localhost",
            "port": 5433,
            "user": "testuser",
            "password": "testpass",
            "database": "testdb"
        }
        
        assert vertica_conn.connection_info == expected

    @pytest.mark.unit
    def test_parse_connection_string_url_format_default_port(self):
        """Test parsing URL format without port"""
        connection_string = "vertica://testuser:testpass@localhost/testdb"
        vertica_conn = VerticaConnection(connection_string)
        
        expected = {
            "host": "localhost",
            "port": 5433,
            "user": "testuser",
            "password": "testpass",
            "database": "testdb"
        }
        
        assert vertica_conn.connection_info == expected

    @pytest.mark.unit
    def test_parse_connection_string_url_format_no_database(self):
        """Test parsing URL format without database"""
        connection_string = "vertica://testuser:testpass@localhost:5433"
        vertica_conn = VerticaConnection(connection_string)
        
        expected = {
            "host": "localhost",
            "port": 5433,
            "user": "testuser",
            "password": "testpass",
            "database": None
        }
        
        assert vertica_conn.connection_info == expected

    @pytest.mark.unit
    @patch('vertica_python.connect')
    @pytest.mark.asyncio
    async def test_get_connection_success(self, mock_connect):
        """Test successful connection"""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        connection_string = "vertica://testuser:testpass@localhost:5433/testdb"
        vertica_conn = VerticaConnection(connection_string)
        
        connection = await vertica_conn.get_connection()
        
        assert connection == mock_connection
        mock_connect.assert_called_once_with(
            host="localhost",
            port=5433,
            user="testuser",
            password="testpass",
            database="testdb"
        )

    @pytest.mark.unit
    @patch('vertica_python.connect')
    @pytest.mark.asyncio
    async def test_get_connection_vertica_exception(self, mock_connect):
        """Test connection with Vertica exception"""
        mock_connect.side_effect = vertica_python.Error("Database connection failed")
        
        connection_string = "vertica://testuser:testpass@localhost:5433/testdb"
        vertica_conn = VerticaConnection(connection_string)
        
        with pytest.raises(vertica_python.Error):
            await vertica_conn.get_connection()

    @pytest.mark.unit
    @patch('vertica_python.connect')
    @pytest.mark.asyncio
    async def test_get_connection_generic_exception(self, mock_connect):
        """Test connection with generic exception"""
        mock_connect.side_effect = Exception("Generic connection error")
        
        connection_string = "vertica://testuser:testpass@localhost:5433/testdb"
        vertica_conn = VerticaConnection(connection_string)
        
        with pytest.raises(Exception):
            await vertica_conn.get_connection()

    @pytest.mark.unit
    def test_connection_string_edge_cases(self):
        """Test various edge cases in connection string parsing"""
        
        # Test URL with special characters in password
        connection_string = "vertica://testuser:test@pass@localhost:5433/testdb"
        vertica_conn = VerticaConnection(connection_string)
        
        assert vertica_conn.connection_info["password"] == "test@pass"
        
        # Test URL with no password
        connection_string = "vertica://testuser@localhost:5433/testdb"
        vertica_conn = VerticaConnection(connection_string)
        
        assert vertica_conn.connection_info["password"] is None

    @pytest.mark.unit
    def test_simple_format_default_env_vars(self):
        """Test simple format with default environment variables"""
        connection_string = "localhost:5433/testdb"
        
        # Clear environment variables to test defaults
        with patch.dict(os.environ, {}, clear=True):
            vertica_conn = VerticaConnection(connection_string)
            
            assert vertica_conn.connection_info["user"] == "dbadmin"
            assert vertica_conn.connection_info["password"] == ""