"""
Tests for gepado database client functionality.
"""
import os
from unittest.mock import Mock, patch, MagicMock
import pytest
from hypothesis import given, strategies as st

from mvh_copy_mb.gepado import GepadoClient, GepadoRecord, create_gepado_client_from_env


class TestGepadoRecord:
    """Test cases for GepadoRecord data class."""
    
    def test_gepado_record_creation(self):
        """Test basic GepadoRecord creation with all fields."""
        record = GepadoRecord(
            hl7_case_id="12345",
            vng="VNG_001",
            vnk="VNK_001", 
            ibe_g="IBE_G_001",
            ibe_k="IBE_K_001"
        )
        
        assert record.hl7_case_id == "12345"
        assert record.vng == "VNG_001"
        assert record.vnk == "VNK_001"
        assert record.ibe_g == "IBE_G_001"
        assert record.ibe_k == "IBE_K_001"
    
    def test_gepado_record_optional_fields(self):
        """Test GepadoRecord creation with optional fields as None."""
        record = GepadoRecord(hl7_case_id="12345")
        
        assert record.hl7_case_id == "12345"
        assert record.vng is None
        assert record.vnk is None
        assert record.ibe_g is None
        assert record.ibe_k is None


class TestGepadoClient:
    """Test cases for GepadoClient class."""
    
    def test_client_initialization(self):
        """Test GepadoClient initialization with connection parameters."""
        client = GepadoClient(
            host="test_host",
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        assert client.host == "test_host"
        assert client.database == "test_db"
        assert client.username == "test_user"
        assert client.password == "test_pass"
        assert client._connection is None
    
    @patch('mvh_copy_mb.gepado.pymssql.connect')
    def test_connect_success(self, mock_connect):
        """Test successful database connection."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        client = GepadoClient("host", "db", "user", "pass")
        connection = client.connect()
        
        assert connection == mock_connection
        assert client._connection == mock_connection
        mock_connect.assert_called_once_with(
            server="host",
            database="db",
            user="user",
            password="pass",
            timeout=30,
            login_timeout=30
        )
    
    @patch('mvh_copy_mb.gepado.pymssql.connect')
    def test_connect_failure(self, mock_connect):
        """Test database connection failure."""
        import pymssql
        mock_connect.side_effect = pymssql.Error("Connection failed")
        
        client = GepadoClient("host", "db", "user", "pass")
        
        with pytest.raises(pymssql.Error):
            client.connect()
    
    @patch('mvh_copy_mb.gepado.pymssql.connect')
    def test_query_record_found(self, mock_connect):
        """Test querying an existing record."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ("12345", "VNG_001", "VNK_001", "IBE_G_001", "IBE_K_001")
        mock_connect.return_value = mock_connection
        
        client = GepadoClient("host", "db", "user", "pass")
        record = client.query_record("12345")
        
        assert record is not None
        assert record.hl7_case_id == "12345"
        assert record.vng == "VNG_001"
        assert record.vnk == "VNK_001"
        assert record.ibe_g == "IBE_G_001"
        assert record.ibe_k == "IBE_K_001"
        
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('mvh_copy_mb.gepado.pymssql.connect')
    def test_query_record_not_found(self, mock_connect):
        """Test querying a non-existent record."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None
        mock_connect.return_value = mock_connection
        
        client = GepadoClient("host", "db", "user", "pass")
        record = client.query_record("nonexistent")
        
        assert record is None
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('mvh_copy_mb.gepado.pymssql.connect')
    def test_update_record_success(self, mock_connect):
        """Test successful record update."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        # Mock the guid query result
        mock_cursor.fetchone.return_value = ("test-guid-123",)
        mock_cursor.rowcount = 1
        mock_connect.return_value = mock_connection
        
        client = GepadoClient("host", "db", "user", "pass")
        updates = {"vng": "NEW_VNG", "ibe_g": "NEW_IBE_G"}
        result = client.update_record("12345", updates)
        
        assert result is True
        # Should be called multiple times: once for guid query, then for each update
        assert mock_cursor.execute.call_count >= 2
        mock_connection.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('mvh_copy_mb.gepado.pymssql.connect')
    def test_update_record_no_rows_affected(self, mock_connect):
        """Test update when no record is found for the HL7 case ID."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        # Mock no guid found for the HL7 case ID
        mock_cursor.fetchone.return_value = None
        mock_connect.return_value = mock_connection
        
        client = GepadoClient("host", "db", "user", "pass")
        updates = {"vng": "NEW_VNG"}
        result = client.update_record("nonexistent", updates)
        
        assert result is False
        # Should only call execute once for the guid query
        mock_cursor.execute.assert_called_once()
        mock_connection.commit.assert_not_called()
        mock_cursor.close.assert_called_once()
    
    @patch('mvh_copy_mb.gepado.pymssql.connect')
    def test_update_record_invalid_field(self, mock_connect):
        """Test update with invalid field name."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        # Mock guid found
        mock_cursor.fetchone.return_value = ("test-guid-123",)
        mock_connect.return_value = mock_connection
        
        client = GepadoClient("host", "db", "user", "pass")
        updates = {"invalid_field": "value"}
        result = client.update_record("12345", updates)
        
        assert result is False
    
    @patch('mvh_copy_mb.gepado.pymssql.connect')
    def test_update_record_empty_updates(self, mock_connect):
        """Test update with empty updates dictionary."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        client = GepadoClient("host", "db", "user", "pass")
        result = client.update_record("12345", {})
        
        assert result is True
    
    @patch('mvh_copy_mb.gepado.pymssql.connect')
    def test_close_connection(self, mock_connect):
        """Test closing database connection."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        client = GepadoClient("host", "db", "user", "pass")
        client.connect()
        client.close()
        
        mock_connection.close.assert_called_once()
        assert client._connection is None


class TestEnvironmentConfiguration:
    """Test cases for environment configuration usage."""
    
    @given(
        host=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_characters=['\x00'])),
        database=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_characters=['\x00'])),
        username=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_characters=['\x00'])),
        password=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_characters=['\x00']))
    )
    def test_property_environment_configuration_usage(self, host, database, username, password):
        """
        **Feature: gepado-integration, Property 2: Environment Configuration Usage**
        
        For any set of MSSQL environment variables (host, database, username, password),
        the system should consistently use these values when initializing the gepado client.
        
        **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5**
        """
        # Set up environment variables
        env_vars = {
            'MSSQL_HOST': host,
            'MSSQL_DATABASE': database,
            'MSSQL_USERNAME': username,
            'MSSQL_PASSWORD': password
        }
        
        with patch.dict(os.environ, env_vars, clear=False):
            client = create_gepado_client_from_env()
            
            # Verify that the client was created successfully
            assert client is not None
            
            # Verify that all environment variables are used consistently
            assert client.host == host
            assert client.database == database
            assert client.username == username
            assert client.password == password
    
    def test_create_client_missing_env_vars(self):
        """Test client creation with missing environment variables."""
        # Clear all MSSQL environment variables
        env_vars_to_clear = ['MSSQL_HOST', 'MSSQL_DATABASE', 'MSSQL_USERNAME', 'MSSQL_PASSWORD']
        
        with patch.dict(os.environ, {}, clear=False):
            # Remove any existing MSSQL env vars
            for var in env_vars_to_clear:
                os.environ.pop(var, None)
            
            client = create_gepado_client_from_env()
            assert client is None
    
    def test_create_client_partial_env_vars(self):
        """Test client creation with only some environment variables set."""
        env_vars = {
            'MSSQL_HOST': 'test_host',
            'MSSQL_DATABASE': 'test_db'
            # Missing MSSQL_USERNAME and MSSQL_PASSWORD
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            client = create_gepado_client_from_env()
            assert client is None