"""
Tests for HL7 case ID correction functionality.
"""
import pytest
from unittest.mock import Mock, patch
from hypothesis import given, strategies as st

from mvh_copy_mb.hl7_case_id_correction import (
    CaseValidationResult,
    validate_hl7_case_sapvisitingtype,
    get_patient_guid_for_case,
    find_correct_genomic_case,
    correct_hl7_case_id_for_gepado
)
from mvh_copy_mb.gepado import GepadoClient


class TestCaseValidationResult:
    """Test cases for CaseValidationResult data class."""
    
    def test_case_validation_result_creation(self):
        """Test basic CaseValidationResult creation."""
        result = CaseValidationResult(
            original_case_id="12345",
            is_valid=True,
            corrected_case_id="67890",
            patient_guid="patient-guid-123",
            error_message=None,
            correction_applied=True
        )
        
        assert result.original_case_id == "12345"
        assert result.is_valid is True
        assert result.corrected_case_id == "67890"
        assert result.patient_guid == "patient-guid-123"
        assert result.error_message is None
        assert result.correction_applied is True
    
    def test_case_validation_result_defaults(self):
        """Test CaseValidationResult with default values."""
        result = CaseValidationResult(
            original_case_id="12345",
            is_valid=False
        )
        
        assert result.original_case_id == "12345"
        assert result.is_valid is False
        assert result.corrected_case_id is None
        assert result.patient_guid is None
        assert result.error_message is None
        assert result.correction_applied is False


class TestValidateHL7CaseSapVisitingType:
    """Test cases for validate_hl7_case_sapvisitingtype function."""
    
    def test_validate_correct_sapvisitingtype(self):
        """Test validation with correct sapVisitingType 'GS'."""
        # Mock database connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ('GS',)
        
        client = GepadoClient("host", "db", "user", "pass")
        client._connection = mock_connection
        
        result = validate_hl7_case_sapvisitingtype(client, "test_case_123")
        
        assert result is True
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    def test_validate_incorrect_sapvisitingtype(self):
        """Test validation with incorrect sapVisitingType."""
        # Mock database connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ('OTHER',)
        
        client = GepadoClient("host", "db", "user", "pass")
        client._connection = mock_connection
        
        result = validate_hl7_case_sapvisitingtype(client, "test_case_123")
        
        assert result is False
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    def test_validate_case_not_found(self):
        """Test validation when case is not found."""
        # Mock database connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None
        
        client = GepadoClient("host", "db", "user", "pass")
        client._connection = mock_connection
        
        result = validate_hl7_case_sapvisitingtype(client, "nonexistent_case")
        
        assert result is False
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    def test_validate_database_error(self):
        """Test validation handles database errors."""
        import pymssql
        
        # Mock database connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = pymssql.Error("Database error")
        
        client = GepadoClient("host", "db", "user", "pass")
        client._connection = mock_connection
        
        with pytest.raises(Exception):
            validate_hl7_case_sapvisitingtype(client, "test_case_123")
        
        mock_cursor.close.assert_called_once()


class TestGetPatientGuidForCase:
    """Test cases for get_patient_guid_for_case function."""
    
    def test_get_patient_guid_success(self):
        """Test successful patient GUID extraction."""
        # Mock database connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ('patient-guid-123',)
        
        client = GepadoClient("host", "db", "user", "pass")
        client._connection = mock_connection
        
        result = get_patient_guid_for_case(client, "test_case_123")
        
        assert result == "patient-guid-123"
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    def test_get_patient_guid_not_found(self):
        """Test patient GUID extraction when case not found."""
        # Mock database connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None
        
        client = GepadoClient("host", "db", "user", "pass")
        client._connection = mock_connection
        
        result = get_patient_guid_for_case(client, "nonexistent_case")
        
        assert result is None
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    def test_get_patient_guid_database_error(self):
        """Test patient GUID extraction handles database errors."""
        import pymssql
        
        # Mock database connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = pymssql.Error("Database error")
        
        client = GepadoClient("host", "db", "user", "pass")
        client._connection = mock_connection
        
        with pytest.raises(Exception):
            get_patient_guid_for_case(client, "test_case_123")
        
        mock_cursor.close.assert_called_once()


class TestFindCorrectGenomicCase:
    """Test cases for find_correct_genomic_case function."""
    
    def test_find_single_correct_case(self):
        """Test finding single correct genomic case."""
        # Mock database connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [('correct_case_123',)]
        
        client = GepadoClient("host", "db", "user", "pass")
        client._connection = mock_connection
        
        result = find_correct_genomic_case(client, "patient-guid-123")
        
        assert result == "correct_case_123"
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    def test_find_no_correct_cases(self):
        """Test finding no correct genomic cases."""
        # Mock database connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        
        client = GepadoClient("host", "db", "user", "pass")
        client._connection = mock_connection
        
        result = find_correct_genomic_case(client, "patient-guid-123")
        
        assert result is None
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    def test_find_multiple_correct_cases(self):
        """Test finding multiple correct genomic cases."""
        # Mock database connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [('case_1',), ('case_2',), ('case_3',)]
        
        client = GepadoClient("host", "db", "user", "pass")
        client._connection = mock_connection
        
        result = find_correct_genomic_case(client, "patient-guid-123")
        
        assert result is None
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    def test_find_correct_case_database_error(self):
        """Test finding correct case handles database errors."""
        import pymssql
        
        # Mock database connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = pymssql.Error("Database error")
        
        client = GepadoClient("host", "db", "user", "pass")
        client._connection = mock_connection
        
        with pytest.raises(Exception):
            find_correct_genomic_case(client, "patient-guid-123")
        
        mock_cursor.close.assert_called_once()


class TestCorrectHL7CaseIdForGepado:
    """Test cases for correct_hl7_case_id_for_gepado function."""
    
    @patch('mvh_copy_mb.hl7_case_id_correction.validate_hl7_case_sapvisitingtype')
    def test_correct_case_no_correction_needed(self, mock_validate):
        """Test correction when case is already correct."""
        mock_validate.return_value = True
        
        client = Mock()
        result = correct_hl7_case_id_for_gepado(client, "correct_case_123")
        
        assert result == "correct_case_123"
        mock_validate.assert_called_once_with(client, "correct_case_123")
    
    @patch('mvh_copy_mb.hl7_case_id_correction.find_correct_genomic_case')
    @patch('mvh_copy_mb.hl7_case_id_correction.get_patient_guid_for_case')
    @patch('mvh_copy_mb.hl7_case_id_correction.validate_hl7_case_sapvisitingtype')
    def test_successful_correction(self, mock_validate, mock_get_guid, mock_find_case):
        """Test successful case correction."""
        mock_validate.return_value = False
        mock_get_guid.return_value = "patient-guid-123"
        mock_find_case.return_value = "corrected_case_456"
        
        client = Mock()
        result = correct_hl7_case_id_for_gepado(client, "incorrect_case_123")
        
        assert result == "corrected_case_456"
        mock_validate.assert_called_once_with(client, "incorrect_case_123")
        mock_get_guid.assert_called_once_with(client, "incorrect_case_123")
        mock_find_case.assert_called_once_with(client, "patient-guid-123")
    
    @patch('mvh_copy_mb.hl7_case_id_correction.get_patient_guid_for_case')
    @patch('mvh_copy_mb.hl7_case_id_correction.validate_hl7_case_sapvisitingtype')
    def test_correction_fails_no_patient_guid(self, mock_validate, mock_get_guid):
        """Test correction fails when patient GUID not found."""
        mock_validate.return_value = False
        mock_get_guid.return_value = None
        
        client = Mock()
        result = correct_hl7_case_id_for_gepado(client, "incorrect_case_123")
        
        assert result == "incorrect_case_123"
        mock_validate.assert_called_once_with(client, "incorrect_case_123")
        mock_get_guid.assert_called_once_with(client, "incorrect_case_123")
    
    @patch('mvh_copy_mb.hl7_case_id_correction.find_correct_genomic_case')
    @patch('mvh_copy_mb.hl7_case_id_correction.get_patient_guid_for_case')
    @patch('mvh_copy_mb.hl7_case_id_correction.validate_hl7_case_sapvisitingtype')
    def test_correction_fails_no_correct_case(self, mock_validate, mock_get_guid, mock_find_case):
        """Test correction fails when no correct case found."""
        mock_validate.return_value = False
        mock_get_guid.return_value = "patient-guid-123"
        mock_find_case.return_value = None
        
        client = Mock()
        result = correct_hl7_case_id_for_gepado(client, "incorrect_case_123")
        
        assert result == "incorrect_case_123"
        mock_validate.assert_called_once_with(client, "incorrect_case_123")
        mock_get_guid.assert_called_once_with(client, "incorrect_case_123")
        mock_find_case.assert_called_once_with(client, "patient-guid-123")
    
    @patch('mvh_copy_mb.hl7_case_id_correction.validate_hl7_case_sapvisitingtype')
    def test_correction_handles_database_error(self, mock_validate):
        """Test correction handles database errors gracefully."""
        import pymssql
        
        mock_validate.side_effect = Exception("Database error")
        
        client = Mock()
        result = correct_hl7_case_id_for_gepado(client, "test_case_123")
        
        assert result == "test_case_123"
        mock_validate.assert_called_once_with(client, "test_case_123")
    
    @patch('mvh_copy_mb.hl7_case_id_correction.validate_hl7_case_sapvisitingtype')
    def test_correction_handles_unexpected_error(self, mock_validate):
        """Test correction handles unexpected errors gracefully."""
        mock_validate.side_effect = Exception("Unexpected error")
        
        client = Mock()
        result = correct_hl7_case_id_for_gepado(client, "test_case_123")
        
        assert result == "test_case_123"
        mock_validate.assert_called_once_with(client, "test_case_123")


class TestHL7CaseIdCorrectionProperties:
    """Property-based tests for HL7 case ID correction functionality."""
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        sap_visiting_type=st.sampled_from(['GS', 'OTHER', 'CLINICAL', 'LAB'])
    )
    def test_property_sapvisitingtype_validation_accuracy(self, hl7_case_id, sap_visiting_type):
        """
        **Feature: hl7-case-id-correction, Property 1: sapVisitingType validation accuracy**
        
        For any hl7_case_id in GEPADO, querying its sapVisitingType should return the correct value 
        from av_ordermanagement and validation should pass only for 'GS' values.
        
        **Validates: Requirements 1.1**
        """
        # Mock database connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (sap_visiting_type,)
        
        client = GepadoClient("host", "db", "user", "pass")
        client._connection = mock_connection
        
        result = validate_hl7_case_sapvisitingtype(client, hl7_case_id)
        
        # Should return True only when sapVisitingType is 'GS'
        expected_result = (sap_visiting_type == 'GS')
        assert result == expected_result
        
        # Should have executed the query
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        patient_guid=st.text(min_size=10, max_size=50, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00']))
    )
    def test_property_patient_guid_extraction_consistency(self, hl7_case_id, patient_guid):
        """
        **Feature: hl7-case-id-correction, Property 4: Patient GUID extraction consistency**
        
        For any valid hl7_case_id, extracting the patient GUID should return the same value consistently.
        
        **Validates: Requirements 2.1**
        """
        # Mock database connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (patient_guid,)
        
        client = GepadoClient("host", "db", "user", "pass")
        client._connection = mock_connection
        
        # Call the function multiple times
        result1 = get_patient_guid_for_case(client, hl7_case_id)
        result2 = get_patient_guid_for_case(client, hl7_case_id)
        
        # Should return the same result consistently
        assert result1 == patient_guid
        assert result2 == patient_guid
        assert result1 == result2
        
        # Should have executed the query twice
        assert mock_cursor.execute.call_count == 2
        assert mock_cursor.close.call_count == 2
    
    @given(
        patient_guid=st.text(min_size=10, max_size=50, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        genomic_cases=st.lists(
            st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
            min_size=0, max_size=5
        )
    )
    def test_property_correct_case_search_completeness(self, patient_guid, genomic_cases):
        """
        **Feature: hl7-case-id-correction, Property 5: Correct case search completeness**
        
        For any patient GUID, searching for cases with sapVisitingType 'GS' should return all matching cases.
        
        **Validates: Requirements 2.2**
        """
        # Mock database connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        # Convert case list to tuple format expected by fetchall
        mock_cursor.fetchall.return_value = [(case,) for case in genomic_cases]
        
        client = GepadoClient("host", "db", "user", "pass")
        client._connection = mock_connection
        
        result = find_correct_genomic_case(client, patient_guid)
        
        # Should return single case if exactly one found, None otherwise
        if len(genomic_cases) == 1:
            assert result == genomic_cases[0]
        else:
            assert result is None
        
        # Should have executed the query
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        is_correct_case=st.booleans()
    )
    def test_property_correct_case_passthrough(self, hl7_case_id, is_correct_case):
        """
        **Feature: hl7-case-id-correction, Property 2: Correct case passthrough**
        
        For any hl7_case_id with sapVisitingType 'GS', the validation should pass and no correction should be attempted.
        
        **Validates: Requirements 1.2**
        """
        with patch('mvh_copy_mb.hl7_case_id_correction.validate_hl7_case_sapvisitingtype') as mock_validate:
            mock_validate.return_value = is_correct_case
            
            client = Mock()
            result = correct_hl7_case_id_for_gepado(client, hl7_case_id)
            
            if is_correct_case:
                # Should return original case ID without attempting correction
                assert result == hl7_case_id
                mock_validate.assert_called_once_with(client, hl7_case_id)
            else:
                # Should attempt correction (but will fail due to mocked functions)
                assert result == hl7_case_id  # Falls back to original on correction failure
                mock_validate.assert_called_once_with(client, hl7_case_id)
    
    @given(
        hl7_case_id=st.text(min_size=5, max_size=20, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])),
        sap_visiting_type=st.text(min_size=1, max_size=10, alphabet=st.characters(blacklist_categories=['Cs'], blacklist_characters=['\x00'])).filter(lambda x: x != 'GS')
    )
    def test_property_incorrect_case_detection(self, hl7_case_id, sap_visiting_type):
        """
        **Feature: hl7-case-id-correction, Property 3: Incorrect case detection**
        
        For any hl7_case_id with sapVisitingType not equal to 'GS', the system should identify it as incorrect.
        
        **Validates: Requirements 1.3**
        """
        # Mock database connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (sap_visiting_type,)
        
        client = GepadoClient("host", "db", "user", "pass")
        client._connection = mock_connection
        
        result = validate_hl7_case_sapvisitingtype(client, hl7_case_id)
        
        # Should return False for any sapVisitingType that is not 'GS'
        assert result is False
        
        # Should have executed the query
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()