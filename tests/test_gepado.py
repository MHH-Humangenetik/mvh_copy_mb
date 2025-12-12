"""
Tests for gepado database client functionality.
"""
import os
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
        vn_field, ibe_field = map_data_type_to_fields('G')
        assert vn_field == 'vng'
        assert ibe_field == 'ibe_g'
    
    def test_map_data_type_clinical(self):
        """Test mapping clinical data type to correct fields."""
        vn_field, ibe_field = map_data_type_to_fields('C')
        assert vn_field == 'vnk'
        assert ibe_field == 'ibe_k'
    
    def test_map_data_type_case_insensitive(self):
        """Test that mapping is case insensitive."""
        # Test lowercase
        vn_field, ibe_field = map_data_type_to_fields('g')
        assert vn_field == 'vng'
        assert ibe_field == 'ibe_g'
        
        vn_field, ibe_field = map_data_type_to_fields('c')
        assert vn_field == 'vnk'
        assert ibe_field == 'ibe_k'
    
    def test_map_data_type_invalid(self):
        """Test mapping invalid data type raises ValueError."""
        with pytest.raises(ValueError, match="Invalid Art der Daten value"):
            map_data_type_to_fields('X')
        
        with pytest.raises(ValueError, match="Invalid Art der Daten value"):
            map_data_type_to_fields('')
    

    
    def test_compare_record_data_empty_fields(self):
        """Test comparison when gepado fields are empty."""
        record = GepadoRecord(hl7_case_id="12345", vng=None, ibe_g=None)
        updates, mismatches = compare_record_data(record, "VN123", "IBE123", "G")
        
        assert updates == {'vng': 'VN123', 'ibe_g': 'IBE123'}
        assert mismatches == {}
    
    def test_compare_record_data_matching_fields(self):
        """Test comparison when gepado fields match."""
        record = GepadoRecord(hl7_case_id="12345", vng="VN123", ibe_g="IBE123")
        updates, mismatches = compare_record_data(record, "VN123", "IBE123", "G")
        
        assert updates == {}
        assert mismatches == {}
    
    def test_compare_record_data_mismatched_fields(self):
        """Test comparison when gepado fields don't match."""
        record = GepadoRecord(hl7_case_id="12345", vng="OLD_VN", ibe_g="OLD_IBE")
        updates, mismatches = compare_record_data(record, "NEW_VN", "NEW_IBE", "G")
        
        assert updates == {}
        assert mismatches == {'vng': ('OLD_VN', 'NEW_VN'), 'ibe_g': ('OLD_IBE', 'NEW_IBE')}
    
    def test_compare_record_data_clinical_type(self):
        """Test comparison for clinical data type."""
        record = GepadoRecord(hl7_case_id="12345", vnk=None, ibe_k=None)
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
        vn_field, ibe_field = map_data_type_to_fields(art_der_daten)
        
        # Verify correct mapping based on data type
        if art_der_daten.upper() == 'G':
            assert vn_field == 'vng'
            assert ibe_field == 'ibe_g'
        else:  # 'C'
            assert vn_field == 'vnk'
            assert ibe_field == 'ibe_k'
        
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
        vn_field, ibe_field = map_data_type_to_fields(art_der_daten)
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
        mock_record = GepadoRecord(hl7_case_id="hl7_123", vng=None, ibe_g=None)
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