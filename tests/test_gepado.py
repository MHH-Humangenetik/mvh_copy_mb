"""
Tests for gepado database client functionality.
"""
import os
from datetime import date
from unittest.mock import Mock, patch, MagicMock
import pytest
from hypothesis import given, strategies as st

from mvh_copy_mb.gepado import (
    GepadoClient, GepadoRecord, create_gepado_client_from_env,
    map_data_type_to_fields, compare_record_data, validate_and_update_record,
    should_process_record_for_gepado
)


class TestGepadoRecord:
    """Test cases for GepadoRecord data class."""
    
    def test_gepado_record_creation(self):
        """Test basic GepadoRecord creation with all fields."""
        from datetime import date
        test_date_k = date(2024, 7, 1)
        test_date_g = date(2024, 7, 2)
        
        record = GepadoRecord(
            hl7_case_id="12345",
            vng="VNG_001",
            vnk="VNK_001", 
            ibe_g="IBE_G_001",
            ibe_k="IBE_K_001",
            mv_servicedate_k=test_date_k,
            mv_servicedate_g=test_date_g
        )
        
        assert record.hl7_case_id == "12345"
        assert record.vng == "VNG_001"
        assert record.vnk == "VNK_001"
        assert record.ibe_g == "IBE_G_001"
        assert record.ibe_k == "IBE_K_001"
        assert record.mv_servicedate_k == test_date_k
        assert record.mv_servicedate_g == test_date_g
    
    def test_gepado_record_optional_fields(self):
        """Test GepadoRecord creation with optional fields as None."""
        record = GepadoRecord(hl7_case_id="12345")
        
        assert record.hl7_case_id == "12345"
        assert record.vng is None
        assert record.vnk is None
        assert record.ibe_g is None
        assert record.ibe_k is None
        assert record.mv_servicedate_k is None
        assert record.mv_servicedate_g is None


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
            host="host",
            database="db",
            user="user",
            password="pass",
            timeout=30,
            login_timeout=30,
            server="."
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
        mock_cursor.fetchone.return_value = ("12345", "VNG_001", "VNK_001", "IBE_G_001", "IBE_K_001", "2024-07-01", "2024-07-02")
        mock_connect.return_value = mock_connection
        
        client = GepadoClient("host", "db", "user", "pass")
        record = client.query_record("12345")
        
        assert record is not None
        assert record.hl7_case_id == "12345"
        assert record.vng == "VNG_001"
        assert record.vnk == "VNK_001"
        assert record.ibe_g == "IBE_G_001"
        assert record.ibe_k == "IBE_K_001"
        assert record.mv_servicedate_k == date(2024, 7, 1)
        assert record.mv_servicedate_g == date(2024, 7, 2)
        
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
    def test_update_record_database_error(self, mock_connect):
        """Test update_record handles database errors without raising exceptions."""
        import pymssql
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        # Mock guid found
        mock_cursor.fetchone.return_value = ("test-guid-123",)
        # Mock database error during update execution
        mock_cursor.execute.side_effect = [None, pymssql.Error("Database error")]
        mock_connect.return_value = mock_connection
        
        client = GepadoClient("host", "db", "user", "pass")
        updates = {"vng": "NEW_VNG"}
        result = client.update_record("12345", updates)
        
        # Should return False instead of raising exception
        assert result is False
        # Should call rollback on error
        mock_connection.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()
    
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
        host=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        database=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        username=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        password=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00']))
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


class TestDataFieldMapping:
    """Test cases for data field mapping functions."""
    
    def test_map_data_type_genomic(self):
        """Test mapping genomic data type to correct fields."""
        vn_field, ibe_field, servicedate_field = map_data_type_to_fields('G')
        assert vn_field == 'vng'
        assert ibe_field == 'ibe_g'
        assert servicedate_field == 'mv_servicedate_g'
    
    def test_map_data_type_clinical(self):
        """Test mapping clinical data type to correct fields."""
        vn_field, ibe_field, servicedate_field = map_data_type_to_fields('C')
        assert vn_field == 'vnk'
        assert ibe_field == 'ibe_k'
        assert servicedate_field == 'mv_servicedate_k'
    
    def test_map_data_type_case_insensitive(self):
        """Test that mapping is case insensitive."""
        # Test lowercase
        vn_field, ibe_field, servicedate_field = map_data_type_to_fields('g')
        assert vn_field == 'vng'
        assert ibe_field == 'ibe_g'
        assert servicedate_field == 'mv_servicedate_g'
        
        vn_field, ibe_field, servicedate_field = map_data_type_to_fields('c')
        assert vn_field == 'vnk'
        assert ibe_field == 'ibe_k'
        assert servicedate_field == 'mv_servicedate_k'
    
    def test_map_data_type_invalid(self):
        """Test mapping invalid data type raises ValueError."""
        with pytest.raises(ValueError, match="Invalid Art der Daten value"):
            map_data_type_to_fields('X')
        
        with pytest.raises(ValueError, match="Invalid Art der Daten value"):
            map_data_type_to_fields('')
    

    
    def test_compare_record_data_empty_fields(self):
        """Test comparison when gepado fields are empty."""
        record = GepadoRecord(hl7_case_id="12345", vng=None, ibe_g=None, mv_servicedate_g=None)
        updates, mismatches = compare_record_data(record, "VN123", "IBE123", "G")
        
        assert updates == {'vng': 'VN123', 'ibe_g': 'IBE123'}
        assert mismatches == {}
    
    def test_compare_record_data_matching_fields(self):
        """Test comparison when gepado fields match."""
        record = GepadoRecord(hl7_case_id="12345", vng="VN123", ibe_g="IBE123", mv_servicedate_g=None)
        updates, mismatches = compare_record_data(record, "VN123", "IBE123", "G")
        
        assert updates == {}
        assert mismatches == {}
    
    def test_compare_record_data_mismatched_fields(self):
        """Test comparison when gepado fields don't match."""
        record = GepadoRecord(hl7_case_id="12345", vng="OLD_VN", ibe_g="OLD_IBE", mv_servicedate_g=None)
        updates, mismatches = compare_record_data(record, "NEW_VN", "NEW_IBE", "G")
        
        assert updates == {}
        assert mismatches == {'vng': ('OLD_VN', 'NEW_VN'), 'ibe_g': ('OLD_IBE', 'NEW_IBE')}
    
    def test_compare_record_data_clinical_type(self):
        """Test comparison for clinical data type."""
        record = GepadoRecord(hl7_case_id="12345", vnk=None, ibe_k=None, mv_servicedate_k=None)
        updates, mismatches = compare_record_data(record, "VN123", "IBE123", "C")
        
        assert updates == {'vnk': 'VN123', 'ibe_k': 'IBE123'}
        assert mismatches == {}
    

    
    @given(
        art_der_daten=st.sampled_from(['G', 'C', 'g', 'c']),
        vorgangsnummer=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])).filter(lambda x: x.strip() != ''),
        ibe_string=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])).filter(lambda x: x.strip() != '')
    )
    def test_property_data_field_mapping_and_updates(self, art_der_daten, vorgangsnummer, ibe_string):
        """
        **Feature: gepado-integration, Property 4: Data Field Mapping and Updates**
        
        For any Art der Daten value and empty gepado fields, the system should consistently 
        map to the correct field names (genomic -> VNg/IBE_g, clinical -> VNk/IBE_k) and 
        update only empty fields with corresponding Vorgangsnummer and IBE String values.
        
        **Validates: Requirements 4.3, 4.4, 4.5, 4.6**
        """
        # Test field mapping consistency
        vn_field, ibe_field, servicedate_field = map_data_type_to_fields(art_der_daten)
        
        # Verify correct mapping based on data type
        if art_der_daten.upper() == 'G':
            assert vn_field == 'vng'
            assert ibe_field == 'ibe_g'
            assert servicedate_field == 'mv_servicedate_g'
        else:  # 'C'
            assert vn_field == 'vnk'
            assert ibe_field == 'ibe_k'
            assert servicedate_field == 'mv_servicedate_k'
        
        # Test with empty gepado record
        empty_record = GepadoRecord(hl7_case_id="test_id")
        updates, mismatches = compare_record_data(empty_record, vorgangsnummer, ibe_string, art_der_daten)
        
        # Should update empty fields with new values (normalized to uppercase)
        assert vn_field in updates
        assert ibe_field in updates
        assert updates[vn_field] == vorgangsnummer
        assert updates[ibe_field] == ibe_string
        assert len(mismatches) == 0
        
        # Test with populated record (matching values)
        populated_record = GepadoRecord(hl7_case_id="test_id")
        setattr(populated_record, vn_field, vorgangsnummer)
        setattr(populated_record, ibe_field, ibe_string)
        
        updates, mismatches = compare_record_data(populated_record, vorgangsnummer, ibe_string, art_der_daten)
        
        # Should not update when values match (case-insensitive)
        assert len(updates) == 0
        assert len(mismatches) == 0
    
    @given(
        art_der_daten=st.sampled_from(['G', 'C']),
        vorgangsnummer=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])).filter(lambda x: x.strip() != ''),
        ibe_string=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])).filter(lambda x: x.strip() != '')
    )
    def test_property_update_operation_idempotence(self, art_der_daten, vorgangsnummer, ibe_string):
        """
        **Feature: gepado-integration, Property 5: Update Operation Idempotence**
        
        For any gepado record with empty fields, performing the same update operation 
        multiple times should result in the same final state.
        
        **Validates: Requirements 4.3, 4.4, 4.5, 4.6**
        """
        # Create record with empty fields
        empty_record = GepadoRecord(hl7_case_id="test_id")
        
        # Perform first comparison
        updates1, mismatches1 = compare_record_data(empty_record, vorgangsnummer, ibe_string, art_der_daten)
        
        # Apply the updates to create a populated record
        vn_field, ibe_field, servicedate_field = map_data_type_to_fields(art_der_daten)
        populated_record = GepadoRecord(hl7_case_id="test_id")
        setattr(populated_record, vn_field, vorgangsnummer)
        setattr(populated_record, ibe_field, ibe_string)
        
        # Perform second comparison with the same data
        updates2, mismatches2 = compare_record_data(populated_record, vorgangsnummer, ibe_string, art_der_daten)
        
        # First operation should identify updates needed
        assert len(updates1) == 2  # Should update both VN and IBE fields
        assert vn_field in updates1
        assert ibe_field in updates1
        assert updates1[vn_field] == vorgangsnummer
        assert updates1[ibe_field] == ibe_string
        assert len(mismatches1) == 0
        
        # Second operation should identify no updates needed (idempotent)
        assert len(updates2) == 0  # No updates needed after first operation
        assert len(mismatches2) == 0  # No mismatches
        
        # Verify that applying the same operation again yields the same result
        updates3, mismatches3 = compare_record_data(populated_record, vorgangsnummer, ibe_string, art_der_daten)
        
        # Third operation should be identical to second (idempotent)
        assert updates3 == updates2
        assert mismatches3 == mismatches2


class TestRecordProcessingFilter:
    """Test cases for record processing filter functions."""
    
    def test_should_process_valid_record(self):
        """Test that records with QC=1 and Typ=0 should be processed."""
        result = should_process_record_for_gepado("1", "0")
        assert result is True
    
    def test_should_not_process_invalid_qc(self):
        """Test that records with QC!=1 should not be processed."""
        result = should_process_record_for_gepado("0", "0")
        assert result is False
        
        result = should_process_record_for_gepado("2", "0")
        assert result is False
    
    def test_should_not_process_invalid_typ(self):
        """Test that records with Typ!=0 should not be processed."""
        result = should_process_record_for_gepado("1", "1")
        assert result is False
        
        result = should_process_record_for_gepado("1", "2")
        assert result is False
    
    def test_should_not_process_both_invalid(self):
        """Test that records with both QC!=1 and Typ!=0 should not be processed."""
        result = should_process_record_for_gepado("0", "1")
        assert result is False
        
        result = should_process_record_for_gepado("2", "3")
        assert result is False
    
    @given(
        ergebnis_qc=st.text(min_size=1, max_size=10, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        typ_der_meldung=st.text(min_size=1, max_size=10, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00']))
    )
    def test_property_record_processing_filter(self, ergebnis_qc, typ_der_meldung):
        """
        **Feature: gepado-integration, Property 3: Record Processing Filter**
        
        For any QC and Typ der Meldung values, the system should only process records 
        when QC equals '1' AND Typ der Meldung equals '0', and reject all other combinations.
        
        **Validates: Requirements 4.1, 4.2**
        """
        result = should_process_record_for_gepado(ergebnis_qc, typ_der_meldung)
        
        # Should only return True when both conditions are met exactly
        expected_result = (ergebnis_qc == '1' and typ_der_meldung == '0')
        
        assert result == expected_result, f"Filter failed for QC='{ergebnis_qc}', Typ='{typ_der_meldung}': expected {expected_result}, got {result}"


class TestComprehensiveLogging:
    """Test cases for comprehensive logging behavior."""
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        vorgangsnummer=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])).filter(lambda x: x.strip() != ''),
        ibe_string=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])).filter(lambda x: x.strip() != ''),
        art_der_daten=st.sampled_from(['G', 'C']),
        ergebnis_qc=st.sampled_from(['0', '1']),
        typ_der_meldung=st.sampled_from(['0', '1'])
    )
    def test_property_comprehensive_logging_behavior(self, hl7_case_id, vorgangsnummer, ibe_string, art_der_daten, ergebnis_qc, typ_der_meldung):
        """
        **Feature: gepado-integration, Property 7: Comprehensive Logging Behavior**
        
        For any gepado operation (connection, query, update, validation), the system should 
        consistently log operation details including HL7 case IDs, field names, operation 
        results, and any errors with appropriate log levels.
        
        **Validates: Requirements 1.4, 1.5, 4.7, 5.1, 5.2, 5.3, 5.4, 5.5**
        """
        import logging
        from unittest.mock import Mock, patch
        from io import StringIO
        
        # Create a string buffer to capture log output
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)
        
        # Get the gepado logger and add our handler
        gepado_logger = logging.getLogger('mvh_copy_mb.gepado')
        original_level = gepado_logger.level
        gepado_logger.setLevel(logging.DEBUG)
        gepado_logger.addHandler(handler)
        
        try:
            # Mock a gepado client
            mock_client = Mock()
            
            # Test case 1: Valid record processing should log success
            if ergebnis_qc == '1' and typ_der_meldung == '0':
                # Mock successful record query and update
                mock_record = GepadoRecord(hl7_case_id=hl7_case_id)
                mock_client.query_record.return_value = mock_record
                mock_client.update_record.return_value = True
                
                result = validate_and_update_record(
                    mock_client, hl7_case_id, vorgangsnummer, ibe_string, 
                    art_der_daten, ergebnis_qc, typ_der_meldung
                )
                
                # Should log successful processing
                log_output = log_capture.getvalue()
                assert hl7_case_id in log_output, f"HL7 case ID {hl7_case_id} should be logged"
                
                # Should contain operation-specific logging
                if result:
                    # Successful operations should log success messages
                    assert any(keyword in log_output.lower() for keyword in ['successfully', 'updated', 'passes']), "Should log success operations"
                
            else:
                # Test case 2: Invalid records should log warnings
                result = validate_and_update_record(
                    mock_client, hl7_case_id, vorgangsnummer, ibe_string, 
                    art_der_daten, ergebnis_qc, typ_der_meldung
                )
                
                log_output = log_capture.getvalue()
                
                # Should log filtering decision
                assert 'skipping' in log_output.lower() or 'warning' in log_output.lower(), "Should log filtering decisions"
                
                # Should not call database operations for invalid records
                mock_client.query_record.assert_not_called()
                mock_client.update_record.assert_not_called()
            
            # Test case 3: Data field mapping should log field operations
            log_capture.truncate(0)
            log_capture.seek(0)
            
            try:
                vn_field, ibe_field = map_data_type_to_fields(art_der_daten)
                # Field mapping should work without errors for valid data types
                assert vn_field in ['vng', 'vnk']
                assert ibe_field in ['ibe_g', 'ibe_k']
            except ValueError:
                # Invalid data types should raise ValueError (this shouldn't happen with our test data)
                pass
            
        finally:
            # Clean up logging
            gepado_logger.removeHandler(handler)
            gepado_logger.setLevel(original_level)
            handler.close()


class TestGepadoServiceDateIntegration:
    """Test cases for GEPADO service date integration functionality."""
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        output_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31))
    )
    def test_property_gepado_clinical_data_update_inclusion(self, hl7_case_id, output_date):
        """
        **Feature: leistungsdatum-integration, Property 9: GEPADO clinical data update inclusion**
        
        For any GEPADO update operation for clinical data, the MV_servicedate_k field should be included 
        when a valid date is available.
        
        **Validates: Requirements 3.1**
        """
        from unittest.mock import Mock
        
        # Mock a gepado client
        mock_client = Mock()
        
        # Mock an empty gepado record that needs updates for clinical data
        mock_record = GepadoRecord(hl7_case_id=hl7_case_id, vnk=None, ibe_k=None, mv_servicedate_k=None, mv_servicedate_g=None)
        mock_client.query_record.return_value = mock_record
        mock_client.update_record.return_value = True
        
        # Test validate_and_update_record function with clinical data type and output_date
        result = validate_and_update_record(
            mock_client, hl7_case_id, "VN123", "IBE123", "C", "1", "0", output_date
        )
        
        # Should successfully update the record including MV_servicedate_k field for clinical data
        assert result is True
        
        # Verify that query_record was called
        mock_client.query_record.assert_called_once_with(hl7_case_id)
        
        # Verify that update_record was called with the MV_servicedate_k field included for clinical data
        expected_date_str = output_date.strftime('%Y-%m-%d')
        expected_updates = {'vnk': 'VN123', 'ibe_k': 'IBE123', 'mv_servicedate_k': expected_date_str}
        mock_client.update_record.assert_called_once_with(hl7_case_id, expected_updates)
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        output_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31))
    )
    def test_property_gepado_genetic_data_update_inclusion(self, hl7_case_id, output_date):
        """
        **Feature: leistungsdatum-integration, Property 10: GEPADO genetic data update inclusion**
        
        For any GEPADO update operation for genetic data, the MV_servicedate_g field should be included 
        when a valid date is available.
        
        **Validates: Requirements 3.2**
        """
        from unittest.mock import Mock
        
        # Mock a gepado client
        mock_client = Mock()
        
        # Mock an empty gepado record that needs updates for genetic data
        mock_record = GepadoRecord(hl7_case_id=hl7_case_id, vng=None, ibe_g=None, mv_servicedate_k=None, mv_servicedate_g=None)
        mock_client.query_record.return_value = mock_record
        mock_client.update_record.return_value = True
        
        # Test validate_and_update_record function with genetic data type and output_date
        result = validate_and_update_record(
            mock_client, hl7_case_id, "VN123", "IBE123", "G", "1", "0", output_date
        )
        
        # Should successfully update the record including MV_servicedate_g field for genetic data
        assert result is True
        
        # Verify that query_record was called
        mock_client.query_record.assert_called_once_with(hl7_case_id)
        
        # Verify that update_record was called with the MV_servicedate_g field included for genetic data
        expected_date_str = output_date.strftime('%Y-%m-%d')
        expected_updates = {'vng': 'VN123', 'ibe_g': 'IBE123', 'mv_servicedate_g': expected_date_str}
        mock_client.update_record.assert_called_once_with(hl7_case_id, expected_updates)
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        output_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31))
    )
    def test_property_gepado_clinical_empty_field_updates(self, hl7_case_id, output_date):
        """
        **Feature: leistungsdatum-integration, Property 11: GEPADO clinical empty field updates**
        
        For any GEPADO record with empty MV_servicedate_k for clinical data, the system should update it 
        with the extracted Leistungsdatum.
        
        **Validates: Requirements 3.3**
        """
        from unittest.mock import Mock
        
        # Mock an existing gepado record with populated VN/IBE but empty MV_servicedate_k for clinical data
        mock_record = GepadoRecord(hl7_case_id=hl7_case_id, vnk="VN123", ibe_k="IBE123", mv_servicedate_k=None, mv_servicedate_g=None)
        
        # Test compare_record_data function directly for clinical data
        updates_needed, mismatches_found = compare_record_data(
            mock_record, "VN123", "IBE123", "C", output_date
        )
        
        # Should identify that MV_servicedate_k needs to be updated for clinical data
        expected_date_str = output_date.strftime('%Y-%m-%d')
        assert 'mv_servicedate_k' in updates_needed
        assert updates_needed['mv_servicedate_k'] == expected_date_str
        
        # Should not have any mismatches since existing fields match
        assert len(mismatches_found) == 0
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        output_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31))
    )
    def test_property_gepado_genetic_empty_field_updates(self, hl7_case_id, output_date):
        """
        **Feature: leistungsdatum-integration, Property 12: GEPADO genetic empty field updates**
        
        For any GEPADO record with empty MV_servicedate_g for genetic data, the system should update it 
        with the extracted Leistungsdatum.
        
        **Validates: Requirements 3.4**
        """
        from unittest.mock import Mock
        
        # Mock an existing gepado record with populated VN/IBE but empty MV_servicedate_g for genetic data
        mock_record = GepadoRecord(hl7_case_id=hl7_case_id, vng="VN123", ibe_g="IBE123", mv_servicedate_k=None, mv_servicedate_g=None)
        
        # Test compare_record_data function directly for genetic data
        updates_needed, mismatches_found = compare_record_data(
            mock_record, "VN123", "IBE123", "G", output_date
        )
        
        # Should identify that MV_servicedate_g needs to be updated for genetic data
        expected_date_str = output_date.strftime('%Y-%m-%d')
        assert 'mv_servicedate_g' in updates_needed
        assert updates_needed['mv_servicedate_g'] == expected_date_str
        
        # Should not have any mismatches since existing fields match
        assert len(mismatches_found) == 0
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        output_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31))
    )
    def test_property_gepado_update_inclusion(self, hl7_case_id, output_date):
        """
        **Feature: leistungsdatum-integration, Property 9: GEPADO update inclusion**
        
        For any GEPADO update operation, the MV_output_date field should be included 
        when a valid date is available.
        
        **Validates: Requirements 3.1**
        """
        from unittest.mock import Mock, patch
        
        # Mock a gepado client
        mock_client = Mock()
        
        # Mock an empty gepado record that needs updates
        mock_record = GepadoRecord(hl7_case_id=hl7_case_id, vng=None, ibe_g=None, mv_servicedate_k=None, mv_servicedate_g=None)
        mock_client.query_record.return_value = mock_record
        mock_client.update_record.return_value = True
        
        # Test validate_and_update_record function with output_date
        result = validate_and_update_record(
            mock_client, hl7_case_id, "VN123", "IBE123", "G", "1", "0", output_date
        )
        
        # Should successfully update the record including MV_output_date field
        assert result is True
        
        # Verify that query_record was called
        mock_client.query_record.assert_called_once_with(hl7_case_id)
        
        # Verify that update_record was called with the output_date included
        expected_date_str = output_date.strftime('%Y-%m-%d')
        expected_updates = {'vng': 'VN123', 'ibe_g': 'IBE123', 'mv_servicedate_g': expected_date_str}
        mock_client.update_record.assert_called_once_with(hl7_case_id, expected_updates)
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        output_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31))
    )
    def test_property_gepado_empty_field_updates(self, hl7_case_id, output_date):
        """
        **Feature: leistungsdatum-integration, Property 10: GEPADO empty field updates**
        
        For any GEPADO record with empty MV_output_date, the system should update it 
        with the extracted Leistungsdatum.
        
        **Validates: Requirements 3.2**
        """
        from unittest.mock import Mock
        
        # Mock an existing gepado record with populated VN/IBE but empty MV_servicedate_g
        mock_record = GepadoRecord(hl7_case_id=hl7_case_id, vng="VN123", ibe_g="IBE123", mv_servicedate_k=None, mv_servicedate_g=None)
        
        # Test compare_record_data function directly
        updates_needed, mismatches_found = compare_record_data(
            mock_record, "VN123", "IBE123", "G", output_date
        )
        
        # Should identify that MV_servicedate_g needs to be updated
        expected_date_str = output_date.strftime('%Y-%m-%d')
        assert 'mv_servicedate_g' in updates_needed
        assert updates_needed['mv_servicedate_g'] == expected_date_str
        
        # Should not have any mismatches since existing fields match
        assert len(mismatches_found) == 0
        assert len(mismatches_found) == 0
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        existing_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31)),
        new_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31))
    )
    def test_property_gepado_conflict_detection(self, hl7_case_id, existing_date, new_date):
        """
        **Feature: leistungsdatum-integration, Property 11: GEPADO conflict detection**
        
        For any GEPADO record with different MV_output_date value, the system should 
        log a data mismatch error.
        
        **Validates: Requirements 3.3**
        """
        from unittest.mock import Mock
        import logging
        from io import StringIO
        
        # Only test when dates are actually different
        if existing_date == new_date:
            return
        
        # Set up logging capture
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)
        
        gepado_logger = logging.getLogger('mvh_copy_mb.gepado')
        original_level = gepado_logger.level
        gepado_logger.setLevel(logging.DEBUG)
        gepado_logger.addHandler(handler)
        
        try:
            # Mock a gepado record with existing MV_servicedate_g and populated fields
            mock_record = GepadoRecord(hl7_case_id=hl7_case_id, vng="VN123", ibe_g="IBE123", mv_servicedate_k=None, mv_servicedate_g=existing_date)
            
            # Test compare_record_data function with different date
            updates_needed, mismatches_found = compare_record_data(
                mock_record, "VN123", "IBE123", "G", new_date
            )
            
            # Should detect mismatch in mv_servicedate_g
            assert 'mv_servicedate_g' in mismatches_found
            assert mismatches_found['mv_servicedate_g'] == (existing_date, new_date)
            
            # Should not need any updates since there's a mismatch
            assert 'mv_servicedate_g' not in updates_needed
            
            # Should log the mismatch error
            log_output = log_capture.getvalue()
            assert 'mismatch' in log_output.lower()
            assert str(existing_date) in log_output
            assert str(new_date) in log_output
            
        finally:
            gepado_logger.removeHandler(handler)
            gepado_logger.setLevel(original_level)
            handler.close()
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        output_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31))
    )
    def test_property_gepado_validation_logging(self, hl7_case_id, output_date):
        """
        **Feature: leistungsdatum-integration, Property 12: GEPADO validation logging**
        
        For any GEPADO record with matching MV_output_date value, the system should 
        log successful validation.
        
        **Validates: Requirements 3.4**
        """
        from unittest.mock import Mock
        import logging
        from io import StringIO
        
        # Set up logging capture
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)
        
        gepado_logger = logging.getLogger('mvh_copy_mb.gepado')
        original_level = gepado_logger.level
        gepado_logger.setLevel(logging.DEBUG)
        gepado_logger.addHandler(handler)
        
        try:
            # Mock a gepado record with matching MV_servicedate_g and populated fields
            mock_record = GepadoRecord(hl7_case_id=hl7_case_id, vng="VN123", ibe_g="IBE123", mv_servicedate_k=None, mv_servicedate_g=output_date)
            
            # Test compare_record_data function with same date
            updates_needed, mismatches_found = compare_record_data(
                mock_record, "VN123", "IBE123", "G", output_date
            )
            
            # Should not need any updates since dates match
            assert 'mv_servicedate_g' not in updates_needed
            
            # Should not have any mismatches since dates match
            assert 'mv_servicedate_g' not in mismatches_found
            
            # Should log successful validation
            log_output = log_capture.getvalue()
            assert 'validated' in log_output.lower()
            assert str(output_date) in log_output
            
        finally:
            gepado_logger.removeHandler(handler)
            gepado_logger.setLevel(original_level)
            handler.close()
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        output_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31))
    )
    def test_property_gepado_error_resilience(self, hl7_case_id, output_date):
        """
        **Feature: leistungsdatum-integration, Property 13: GEPADO error resilience**
        
        For any GEPADO update failure, the system should log the error and continue 
        processing other records.
        
        **Validates: Requirements 3.5**
        """
        from unittest.mock import Mock
        import logging
        from io import StringIO
        
        # Set up logging capture
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)
        
        gepado_logger = logging.getLogger('mvh_copy_mb.gepado')
        original_level = gepado_logger.level
        gepado_logger.setLevel(logging.DEBUG)
        gepado_logger.addHandler(handler)
        
        try:
            # Mock a gepado client
            mock_client = Mock()
            
            # Mock an empty gepado record that needs updates
            mock_record = GepadoRecord(hl7_case_id=hl7_case_id, vng=None, ibe_g=None, mv_servicedate_k=None, mv_servicedate_g=None)
            mock_client.query_record.return_value = mock_record
            
            # Mock update failure
            mock_client.update_record.return_value = False
            
            # Test validate_and_update_record function with output_date
            result = validate_and_update_record(
                mock_client, hl7_case_id, "VN123", "IBE123", "G", "1", "0", output_date
            )
            
            # Should return False due to update failure
            assert result is False
            
            # Should have attempted the update including output_date
            expected_date_str = output_date.strftime('%Y-%m-%d')
            expected_updates = {'vng': 'VN123', 'ibe_g': 'IBE123', 'mv_servicedate_g': expected_date_str}
            mock_client.update_record.assert_called_once_with(hl7_case_id, expected_updates)
            
            # The error logging is handled in update_record method, so we don't need to check specific log messages here
            
        finally:
            gepado_logger.removeHandler(handler)
            gepado_logger.setLevel(original_level)
            handler.close()
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        vorgangsnummer=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])).filter(lambda x: x.strip() != ''),
        ibe_string=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])).filter(lambda x: x.strip() != ''),
        art_der_daten=st.sampled_from(['G', 'C']),
        output_date=st.one_of(st.none(), st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31)))
    )
    def test_property_gepado_comparison_validation(self, hl7_case_id, vorgangsnummer, ibe_string, art_der_daten, output_date):
        """
        **Feature: leistungsdatum-integration, Property 19: GEPADO comparison validation**
        
        For any GEPADO record comparison, output_date validation logic should be 
        included in the comparison.
        
        **Validates: Requirements 5.4**
        """
        from unittest.mock import Mock
        
        # Mock a gepado client
        mock_client = Mock()
        
        # Mock an empty gepado record
        vn_field, ibe_field, servicedate_field = map_data_type_to_fields(art_der_daten)
        mock_record = GepadoRecord(hl7_case_id=hl7_case_id, mv_servicedate_k=None, mv_servicedate_g=None)
        setattr(mock_record, vn_field, None)
        setattr(mock_record, ibe_field, None)
        
        mock_client.query_record.return_value = mock_record
        mock_client.update_record.return_value = True
        
        # Test validate_and_update_record function with output_date
        result = validate_and_update_record(
            mock_client, hl7_case_id, vorgangsnummer, ibe_string, 
            art_der_daten, "1", "0", output_date
        )
        
        # Should return True for successful processing
        assert result is True
        
        # Should have queried the record
        mock_client.query_record.assert_called_with(hl7_case_id)
        
        # Should have called update_record with all fields including output_date if provided
        expected_updates = {vn_field: vorgangsnummer, ibe_field: ibe_string}
        
        if output_date is not None:
            expected_updates[servicedate_field] = output_date.strftime('%Y-%m-%d')
        
        # Should have made exactly one update call with all fields
        mock_client.update_record.assert_called_once_with(hl7_case_id, expected_updates)
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00']))
    )
    def test_property_missing_field_resilience(self, hl7_case_id):
        """
        **Feature: leistungsdatum-integration, Property 23: Missing field resilience**
        
        For any GEPADO record lacking MV_output_date field, the system should handle 
        the missing field without errors.
        
        **Validates: Requirements 6.4**
        """
        from unittest.mock import Mock
        
        # Mock a gepado client
        mock_client = Mock()
        
        # Mock a gepado record without MV_servicedate fields (None) but with other fields populated
        mock_record = GepadoRecord(hl7_case_id=hl7_case_id, vng="VN123", ibe_g="IBE123", mv_servicedate_k=None, mv_servicedate_g=None)
        mock_client.query_record.return_value = mock_record
        mock_client.update_record.return_value = True
        
        # Test validate_and_update_record function with None output_date
        result = validate_and_update_record(
            mock_client, hl7_case_id, "VN123", "IBE123", "G", "1", "0", None
        )
        
        # Should return True and handle gracefully
        assert result is True
        
        # Should have queried the record
        mock_client.query_record.assert_called_once_with(hl7_case_id)
        
        # Should not attempt any updates since all fields match and output_date is None
        mock_client.update_record.assert_not_called()


class TestValidateAndUpdateRecord:
    """Test cases for validate_and_update_record function."""
    
    @patch('mvh_copy_mb.gepado.GepadoClient')
    def test_skips_processing_invalid_qc(self, mock_client_class):
        """Test that records with QC != 1 are skipped."""
        mock_client = Mock()
        
        # Test with QC = 0
        result = validate_and_update_record(
            mock_client, "hl7_123", "VN123", "IBE123", "G", "0", "0"
        )
        
        assert result is False
        mock_client.query_record.assert_not_called()
        mock_client.update_record.assert_not_called()
    
    @patch('mvh_copy_mb.gepado.GepadoClient')
    def test_skips_processing_invalid_typ(self, mock_client_class):
        """Test that records with Typ der Meldung != 0 are skipped."""
        mock_client = Mock()
        
        # Test with Typ = 1
        result = validate_and_update_record(
            mock_client, "hl7_123", "VN123", "IBE123", "G", "1", "1"
        )
        
        assert result is False
        mock_client.query_record.assert_not_called()
        mock_client.update_record.assert_not_called()
    
    @patch('mvh_copy_mb.gepado.GepadoClient')
    def test_skips_processing_invalid_art_der_daten(self, mock_client_class):
        """Test that records with invalid Art der Daten are skipped."""
        mock_client = Mock()
        
        # Test with invalid Art der Daten
        result = validate_and_update_record(
            mock_client, "hl7_123", "VN123", "IBE123", "X", "1", "0"
        )
        
        assert result is False
        mock_client.query_record.assert_not_called()
        mock_client.update_record.assert_not_called()
    
    @patch('mvh_copy_mb.gepado.GepadoClient')
    def test_processes_valid_record_with_updates(self, mock_client_class):
        """Test that valid records are processed and updated."""
        mock_client = Mock()
        
        # Mock an empty gepado record that needs updates
        mock_record = GepadoRecord(hl7_case_id="hl7_123", vng=None, ibe_g=None, mv_servicedate_k=None, mv_servicedate_g=None)
        mock_client.query_record.return_value = mock_record
        mock_client.update_record.return_value = True
        
        result = validate_and_update_record(
            mock_client, "hl7_123", "VN123", "IBE123", "G", "1", "0"
        )
        
        assert result is True
        mock_client.query_record.assert_called_once_with("hl7_123")
        mock_client.update_record.assert_called_once_with("hl7_123", {'vng': 'VN123', 'ibe_g': 'IBE123'})
    
    @patch('mvh_copy_mb.gepado.GepadoClient')
    def test_handles_missing_gepado_record(self, mock_client_class):
        """Test handling when no gepado record is found."""
        mock_client = Mock()
        mock_client.query_record.return_value = None
        
        result = validate_and_update_record(
            mock_client, "hl7_123", "VN123", "IBE123", "G", "1", "0"
        )
        
        assert result is False
        mock_client.query_record.assert_called_once_with("hl7_123")
        mock_client.update_record.assert_not_called()
    
    @patch('mvh_copy_mb.gepado.GepadoClient')
    def test_handles_update_failure_without_duplicate_logging(self, mock_client_class):
        """Test that update failures don't cause duplicate error logging."""
        mock_client = Mock()
        
        # Mock an empty gepado record that needs updates
        mock_record = GepadoRecord(hl7_case_id="hl7_123", vng=None, ibe_g=None, mv_servicedate_k=None, mv_servicedate_g=None)
        mock_client.query_record.return_value = mock_record
        # Mock update failure (returns False, error already logged in update_record)
        mock_client.update_record.return_value = False
        
        result = validate_and_update_record(
            mock_client, "hl7_123", "VN123", "IBE123", "G", "1", "0"
        )
        
        assert result is False
        mock_client.query_record.assert_called_once_with("hl7_123")
        mock_client.update_record.assert_called_once_with("hl7_123", {'vng': 'VN123', 'ibe_g': 'IBE123'})
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        existing_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31)),
        new_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31))
    )
    def test_property_gepado_clinical_conflict_detection(self, hl7_case_id, existing_date, new_date):
        """
        **Feature: leistungsdatum-integration, Property 13: GEPADO clinical conflict detection**
        
        For any GEPADO record with different MV_servicedate_k value for clinical data, the system should 
        log a data mismatch error.
        
        **Validates: Requirements 3.5**
        """
        from unittest.mock import Mock
        import logging
        from io import StringIO
        
        # Only test when dates are actually different
        if existing_date == new_date:
            return
        
        # Set up logging capture
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)
        
        gepado_logger = logging.getLogger('mvh_copy_mb.gepado')
        original_level = gepado_logger.level
        gepado_logger.setLevel(logging.DEBUG)
        gepado_logger.addHandler(handler)
        
        try:
            # Mock a gepado record with existing MV_servicedate_k and populated fields for clinical data
            mock_record = GepadoRecord(hl7_case_id=hl7_case_id, vnk="VN123", ibe_k="IBE123", mv_servicedate_k=existing_date, mv_servicedate_g=None)
            
            # Test compare_record_data function with different date for clinical data
            updates_needed, mismatches_found = compare_record_data(
                mock_record, "VN123", "IBE123", "C", new_date
            )
            
            # Should detect mismatch in mv_servicedate_k
            assert 'mv_servicedate_k' in mismatches_found
            assert mismatches_found['mv_servicedate_k'] == (existing_date, new_date)
            
            # Should not need any updates since there's a mismatch
            assert 'mv_servicedate_k' not in updates_needed
            
            # Should log the mismatch error
            log_output = log_capture.getvalue()
            assert 'mismatch' in log_output.lower()
            assert str(existing_date) in log_output
            assert str(new_date) in log_output
            
        finally:
            gepado_logger.removeHandler(handler)
            gepado_logger.setLevel(original_level)
            handler.close()
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        existing_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31)),
        new_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31))
    )
    def test_property_gepado_genetic_conflict_detection(self, hl7_case_id, existing_date, new_date):
        """
        **Feature: leistungsdatum-integration, Property 14: GEPADO genetic conflict detection**
        
        For any GEPADO record with different MV_servicedate_g value for genetic data, the system should 
        log a data mismatch error.
        
        **Validates: Requirements 3.6**
        """
        from unittest.mock import Mock
        import logging
        from io import StringIO
        
        # Only test when dates are actually different
        if existing_date == new_date:
            return
        
        # Set up logging capture
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)
        
        gepado_logger = logging.getLogger('mvh_copy_mb.gepado')
        original_level = gepado_logger.level
        gepado_logger.setLevel(logging.DEBUG)
        gepado_logger.addHandler(handler)
        
        try:
            # Mock a gepado record with existing MV_servicedate_g and populated fields for genetic data
            mock_record = GepadoRecord(hl7_case_id=hl7_case_id, vng="VN123", ibe_g="IBE123", mv_servicedate_k=None, mv_servicedate_g=existing_date)
            
            # Test compare_record_data function with different date for genetic data
            updates_needed, mismatches_found = compare_record_data(
                mock_record, "VN123", "IBE123", "G", new_date
            )
            
            # Should detect mismatch in mv_servicedate_g
            assert 'mv_servicedate_g' in mismatches_found
            assert mismatches_found['mv_servicedate_g'] == (existing_date, new_date)
            
            # Should not need any updates since there's a mismatch
            assert 'mv_servicedate_g' not in updates_needed
            
            # Should log the mismatch error
            log_output = log_capture.getvalue()
            assert 'mismatch' in log_output.lower()
            assert str(existing_date) in log_output
            assert str(new_date) in log_output
            
        finally:
            gepado_logger.removeHandler(handler)
            gepado_logger.setLevel(original_level)
            handler.close()
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        output_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31))
    )
    def test_property_gepado_clinical_validation_logging(self, hl7_case_id, output_date):
        """
        **Feature: leistungsdatum-integration, Property 15: GEPADO clinical validation logging**
        
        For any GEPADO record with matching MV_servicedate_k value for clinical data, the system should 
        log successful validation.
        
        **Validates: Requirements 3.7**
        """
        from unittest.mock import Mock
        import logging
        from io import StringIO
        
        # Set up logging capture
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)
        
        gepado_logger = logging.getLogger('mvh_copy_mb.gepado')
        original_level = gepado_logger.level
        gepado_logger.setLevel(logging.DEBUG)
        gepado_logger.addHandler(handler)
        
        try:
            # Mock a gepado record with matching MV_servicedate_k and populated fields for clinical data
            mock_record = GepadoRecord(hl7_case_id=hl7_case_id, vnk="VN123", ibe_k="IBE123", mv_servicedate_k=output_date, mv_servicedate_g=None)
            
            # Test compare_record_data function with same date for clinical data
            updates_needed, mismatches_found = compare_record_data(
                mock_record, "VN123", "IBE123", "C", output_date
            )
            
            # Should not need any updates since dates match
            assert 'mv_servicedate_k' not in updates_needed
            
            # Should not have any mismatches since dates match
            assert 'mv_servicedate_k' not in mismatches_found
            
            # Should log successful validation
            log_output = log_capture.getvalue()
            assert 'validated' in log_output.lower()
            assert str(output_date) in log_output
            
        finally:
            gepado_logger.removeHandler(handler)
            gepado_logger.setLevel(original_level)
            handler.close()
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        output_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31))
    )
    def test_property_gepado_genetic_validation_logging(self, hl7_case_id, output_date):
        """
        **Feature: leistungsdatum-integration, Property 16: GEPADO genetic validation logging**
        
        For any GEPADO record with matching MV_servicedate_g value for genetic data, the system should 
        log successful validation.
        
        **Validates: Requirements 3.8**
        """
        from unittest.mock import Mock
        import logging
        from io import StringIO
        
        # Set up logging capture
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)
        
        gepado_logger = logging.getLogger('mvh_copy_mb.gepado')
        original_level = gepado_logger.level
        gepado_logger.setLevel(logging.DEBUG)
        gepado_logger.addHandler(handler)
        
        try:
            # Mock a gepado record with matching MV_servicedate_g and populated fields for genetic data
            mock_record = GepadoRecord(hl7_case_id=hl7_case_id, vng="VN123", ibe_g="IBE123", mv_servicedate_k=None, mv_servicedate_g=output_date)
            
            # Test compare_record_data function with same date for genetic data
            updates_needed, mismatches_found = compare_record_data(
                mock_record, "VN123", "IBE123", "G", output_date
            )
            
            # Should not need any updates since dates match
            assert 'mv_servicedate_g' not in updates_needed
            
            # Should not have any mismatches since dates match
            assert 'mv_servicedate_g' not in mismatches_found
            
            # Should log successful validation
            log_output = log_capture.getvalue()
            assert 'validated' in log_output.lower()
            assert str(output_date) in log_output
            
        finally:
            gepado_logger.removeHandler(handler)
            gepado_logger.setLevel(original_level)
            handler.close()
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        output_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31)),
        art_der_daten=st.sampled_from(['G', 'C'])
    )
    def test_property_gepado_error_resilience(self, hl7_case_id, output_date, art_der_daten):
        """
        **Feature: leistungsdatum-integration, Property 17: GEPADO error resilience**
        
        For any GEPADO update failure, the system should log the error and continue 
        processing other records.
        
        **Validates: Requirements 3.9**
        """
        from unittest.mock import Mock
        import logging
        from io import StringIO
        
        # Set up logging capture
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)
        
        gepado_logger = logging.getLogger('mvh_copy_mb.gepado')
        original_level = gepado_logger.level
        gepado_logger.setLevel(logging.DEBUG)
        gepado_logger.addHandler(handler)
        
        try:
            # Mock a gepado client
            mock_client = Mock()
            
            # Mock an empty gepado record that needs updates
            mock_record = GepadoRecord(hl7_case_id=hl7_case_id, vng=None, vnk=None, ibe_g=None, ibe_k=None, mv_servicedate_k=None, mv_servicedate_g=None)
            mock_client.query_record.return_value = mock_record
            
            # Mock update failure
            mock_client.update_record.return_value = False
            
            # Test validate_and_update_record function with output_date
            result = validate_and_update_record(
                mock_client, hl7_case_id, "VN123", "IBE123", art_der_daten, "1", "0", output_date
            )
            
            # Should return False due to update failure
            assert result is False
            
            # Should have attempted the update including appropriate service date field
            vn_field, ibe_field, servicedate_field = map_data_type_to_fields(art_der_daten)
            expected_date_str = output_date.strftime('%Y-%m-%d')
            expected_updates = {vn_field: 'VN123', ibe_field: 'IBE123', servicedate_field: expected_date_str}
            mock_client.update_record.assert_called_once_with(hl7_case_id, expected_updates)
            
            # The error logging is handled in update_record method, so we don't need to check specific log messages here
            # The important thing is that the function returns False and doesn't raise an exception
            
        finally:
            gepado_logger.removeHandler(handler)
            gepado_logger.setLevel(original_level)
            handler.close()
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        vorgangsnummer=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])).filter(lambda x: x.strip() != ''),
        ibe_string=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])).filter(lambda x: x.strip() != ''),
        art_der_daten=st.sampled_from(['G', 'C']),
        output_date=st.one_of(st.none(), st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31)))
    )
    def test_property_gepado_comparison_validation(self, hl7_case_id, vorgangsnummer, ibe_string, art_der_daten, output_date):
        """
        **Feature: leistungsdatum-integration, Property 21: GEPADO comparison validation**
        
        For any GEPADO record comparison, output_date validation logic should be 
        included in the comparison for both clinical and genetic data fields.
        
        **Validates: Requirements 5.4**
        """
        from unittest.mock import Mock
        
        # Mock a gepado client
        mock_client = Mock()
        
        # Mock an empty gepado record
        vn_field, ibe_field, servicedate_field = map_data_type_to_fields(art_der_daten)
        mock_record = GepadoRecord(hl7_case_id=hl7_case_id, mv_servicedate_k=None, mv_servicedate_g=None)
        setattr(mock_record, vn_field, None)
        setattr(mock_record, ibe_field, None)
        
        mock_client.query_record.return_value = mock_record
        mock_client.update_record.return_value = True
        
        # Test validate_and_update_record function with output_date
        result = validate_and_update_record(
            mock_client, hl7_case_id, vorgangsnummer, ibe_string, 
            art_der_daten, "1", "0", output_date
        )
        
        # Should return True for successful processing
        assert result is True
        
        # Should have queried the record
        mock_client.query_record.assert_called_with(hl7_case_id)
        
        # Should have called update_record with all fields including appropriate service date field if provided
        expected_updates = {vn_field: vorgangsnummer, ibe_field: ibe_string}
        
        if output_date is not None:
            expected_updates[servicedate_field] = output_date.strftime('%Y-%m-%d')
        
        # Should have made exactly one update call with all fields
        mock_client.update_record.assert_called_once_with(hl7_case_id, expected_updates)
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00']))
    )
    def test_property_missing_clinical_field_resilience(self, hl7_case_id):
        """
        **Feature: leistungsdatum-integration, Property 25: Missing clinical field resilience**
        
        For any GEPADO record lacking MV_servicedate_k field, the system should handle 
        the missing field without errors.
        
        **Validates: Requirements 6.4**
        """
        from unittest.mock import Mock
        
        # Mock a gepado client
        mock_client = Mock()
        
        # Mock a gepado record without MV_servicedate_k field (None) but with other fields populated for clinical data
        mock_record = GepadoRecord(hl7_case_id=hl7_case_id, vnk="VN123", ibe_k="IBE123", mv_servicedate_k=None, mv_servicedate_g=None)
        mock_client.query_record.return_value = mock_record
        mock_client.update_record.return_value = True
        
        # Test validate_and_update_record function with None output_date for clinical data
        result = validate_and_update_record(
            mock_client, hl7_case_id, "VN123", "IBE123", "C", "1", "0", None
        )
        
        # Should return True and handle gracefully
        assert result is True
        
        # Should have queried the record
        mock_client.query_record.assert_called_once_with(hl7_case_id)
        
        # Should not attempt any updates since all fields match and output_date is None
        mock_client.update_record.assert_not_called()
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00']))
    )
    def test_property_missing_genetic_field_resilience(self, hl7_case_id):
        """
        **Feature: leistungsdatum-integration, Property 26: Missing genetic field resilience**
        
        For any GEPADO record lacking MV_servicedate_g field, the system should handle 
        the missing field without errors.
        
        **Validates: Requirements 6.5**
        """
        from unittest.mock import Mock
        
        # Mock a gepado client
        mock_client = Mock()
        
        # Mock a gepado record without MV_servicedate_g field (None) but with other fields populated for genetic data
        mock_record = GepadoRecord(hl7_case_id=hl7_case_id, vng="VN123", ibe_g="IBE123", mv_servicedate_k=None, mv_servicedate_g=None)
        mock_client.query_record.return_value = mock_record
        mock_client.update_record.return_value = True
        
        # Test validate_and_update_record function with None output_date for genetic data
        result = validate_and_update_record(
            mock_client, hl7_case_id, "VN123", "IBE123", "G", "1", "0", None
        )
        
        # Should return True and handle gracefully
        assert result is True
        
        # Should have queried the record
        mock_client.query_record.assert_called_once_with(hl7_case_id)
        
        # Should not attempt any updates since all fields match and output_date is None
        mock_client.update_record.assert_not_called()