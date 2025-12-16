"""
Tests for Leistungsdatum extraction functionality.
"""

import pytest
from datetime import date
from hypothesis import given, strategies as st

from mvh_copy_mb.leistungsdatum_extraction import parse_leistungsdatum


class TestLeistungsdatumExtraction:
    """Test cases for Leistungsdatum extraction functions."""
    
    def test_parse_leistungsdatum_basic(self):
        """Test basic Leistungsdatum parsing with valid examples."""
        # Valid hash string from documentation
        hash_string = "A123456789&20240701001&260530103&KDKK00001&0&O&9&1&C&2&1"
        result = parse_leistungsdatum(hash_string)
        assert result == date(2024, 7, 1)
        
        # Another valid example
        hash_string2 = "B987654321&20231225999&123456789&GRZA00001&0&R&9&1&G&1&1"
        result2 = parse_leistungsdatum(hash_string2)
        assert result2 == date(2023, 12, 25)
        
        # Edge case: leap year
        hash_string3 = "C111111111&20240229001&987654321&KDKB00002&0&H&9&2&C&3&1"
        result3 = parse_leistungsdatum(hash_string3)
        assert result3 == date(2024, 2, 29)
        
        # Invalid cases
        assert parse_leistungsdatum("") is None
        assert parse_leistungsdatum("A123456789") is None  # No second field
        assert parse_leistungsdatum("A123456789&2024070100&rest") is None  # Wrong length
        assert parse_leistungsdatum("A123456789&2024130100A&rest") is None  # Non-digit
        assert parse_leistungsdatum("A123456789&20241301001&rest") is None  # Invalid month
        assert parse_leistungsdatum("A123456789&20240732001&rest") is None  # Invalid day


class TestLeistungsdatumExtractionProperties:
    """Property-based tests for Leistungsdatum extraction."""
    
    @given(
        meldebestaetigung_code=st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", min_size=10, max_size=10),
        year=st.integers(min_value=2020, max_value=2030),
        month=st.integers(min_value=1, max_value=12),
        day=st.integers(min_value=1, max_value=28),  # Use 28 to avoid month-specific day issues
        counter=st.integers(min_value=1, max_value=999),
        remaining_fields=st.lists(
            st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", min_size=1, max_size=10),
            min_size=9, max_size=9
        )
    )
    def test_hash_string_parsing_extracts_correct_field(self, meldebestaetigung_code, year, month, day, counter, remaining_fields):
        """
        **Feature: leistungsdatum-integration, Property 1: Hash string parsing extracts correct field**
        
        For any valid Meldebestätigung hash string with at least 2 fields, 
        parsing should extract the second field (position 1) as the Leistungsdatum.
        **Validates: Requirements 1.1**
        """
        # Format the Leistungsdatum field as JJJJMMTTZZZ
        leistungsdatum_field = f"{year:04d}{month:02d}{day:02d}{counter:03d}"
        
        # Construct complete hash string
        all_fields = [meldebestaetigung_code, leistungsdatum_field] + remaining_fields
        hash_string = "&".join(all_fields)
        
        # Parse the Leistungsdatum
        result = parse_leistungsdatum(hash_string)
        
        # Should successfully extract the date
        assert result is not None
        assert result == date(year, month, day)
        
        # Verify consistency - multiple calls should return same result
        result2 = parse_leistungsdatum(hash_string)
        assert result == result2
    
    @given(
        meldebestaetigung_code=st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", min_size=10, max_size=10),
        year=st.integers(min_value=2020, max_value=2030),
        month=st.integers(min_value=1, max_value=12),
        day=st.integers(min_value=1, max_value=28),  # Use 28 to avoid month-specific day issues
        counter=st.integers(min_value=1, max_value=999),
        remaining_fields=st.lists(
            st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", min_size=1, max_size=10),
            min_size=9, max_size=9
        )
    )
    def test_date_format_validation(self, meldebestaetigung_code, year, month, day, counter, remaining_fields):
        """
        **Feature: leistungsdatum-integration, Property 2: Date format validation**
        
        For any 11-character string in JJJJMMTTZZZ format with valid date components, 
        the parser should successfully extract the date portion.
        **Validates: Requirements 1.2**
        """
        # Format the Leistungsdatum field as JJJJMMTTZZZ (exactly 11 characters)
        leistungsdatum_field = f"{year:04d}{month:02d}{day:02d}{counter:03d}"
        
        # Verify the field is exactly 11 characters
        assert len(leistungsdatum_field) == 11
        
        # Verify all characters are digits
        assert leistungsdatum_field.isdigit()
        
        # Construct complete hash string
        all_fields = [meldebestaetigung_code, leistungsdatum_field] + remaining_fields
        hash_string = "&".join(all_fields)
        
        # Parse the Leistungsdatum
        result = parse_leistungsdatum(hash_string)
        
        # Should successfully extract the date portion (first 8 characters)
        assert result is not None
        assert result == date(year, month, day)
        
        # Verify that the date portion extraction is correct
        date_portion = leistungsdatum_field[:8]
        assert date_portion == f"{year:04d}{month:02d}{day:02d}"
    
    @given(
        leistungsdatum_string=st.text(alphabet="0123456789", min_size=11, max_size=11)
    )
    def test_date_portion_extraction(self, leistungsdatum_string):
        """
        **Feature: leistungsdatum-integration, Property 3: Date portion extraction**
        
        For any valid Leistungsdatum string, extracting the date portion 
        should return exactly the first 8 characters.
        **Validates: Requirements 1.3**
        """
        # Verify input is exactly 11 digits
        assert len(leistungsdatum_string) == 11
        assert leistungsdatum_string.isdigit()
        
        # Create a hash string with this Leistungsdatum field
        hash_string = f"A123456789&{leistungsdatum_string}&260530103&KDKK00001&0&O&9&1&C&2&1"
        
        # Extract the first 8 characters (date portion)
        expected_date_portion = leistungsdatum_string[:8]
        
        # Verify the date portion is exactly 8 characters
        assert len(expected_date_portion) == 8
        
        # Try to parse - may succeed or fail depending on date validity
        result = parse_leistungsdatum(hash_string)
        
        if result is not None:
            # If parsing succeeded, verify the date corresponds to the first 8 characters
            year = int(expected_date_portion[:4])
            month = int(expected_date_portion[4:6])
            day = int(expected_date_portion[6:8])
            
            # The result should match the parsed date from first 8 characters
            try:
                expected_date = date(year, month, day)
                assert result == expected_date
            except ValueError:
                # If the date is invalid, parsing should have returned None
                # This is a contradiction, so the test should fail
                assert False, f"Parser returned date for invalid date components: {year}-{month}-{day}"
        
        # Verify that the counter portion (last 3 characters) is discarded
        counter_portion = leistungsdatum_string[8:]
        assert len(counter_portion) == 3
        
        # The counter should not affect the parsed date (if valid)
        # Test with different counter values but same date portion
        if result is not None:
            # Create alternative hash strings with different counters
            alt_counter1 = "001"
            alt_counter2 = "999"
            
            alt_leistungsdatum1 = expected_date_portion + alt_counter1
            alt_leistungsdatum2 = expected_date_portion + alt_counter2
            
            alt_hash1 = f"A123456789&{alt_leistungsdatum1}&260530103&KDKK00001&0&O&9&1&C&2&1"
            alt_hash2 = f"A123456789&{alt_leistungsdatum2}&260530103&KDKK00001&0&O&9&1&C&2&1"
            
            alt_result1 = parse_leistungsdatum(alt_hash1)
            alt_result2 = parse_leistungsdatum(alt_hash2)
            
            # All should return the same date (counter is discarded)
            assert alt_result1 == result
            assert alt_result2 == result
    
    @given(
        meldebestaetigung_code=st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", min_size=10, max_size=10),
        invalid_year=st.integers(min_value=1000, max_value=9999),
        invalid_month=st.one_of(
            st.integers(min_value=13, max_value=99),  # Invalid months > 12
            st.integers(min_value=0, max_value=0)     # Invalid month 0
        ),
        invalid_day=st.one_of(
            st.integers(min_value=32, max_value=99),  # Invalid days > 31
            st.integers(min_value=0, max_value=0)     # Invalid day 0
        ),
        counter=st.integers(min_value=1, max_value=999),
        remaining_fields=st.lists(
            st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", min_size=1, max_size=10),
            min_size=9, max_size=9
        )
    )
    def test_invalid_date_handling(self, meldebestaetigung_code, invalid_year, invalid_month, invalid_day, counter, remaining_fields):
        """
        **Feature: leistungsdatum-integration, Property 4: Invalid date handling**
        
        For any invalid date string, the system should return NULL and log an appropriate error.
        **Validates: Requirements 1.4**
        """
        # Create invalid date combinations
        test_cases = [
            # Invalid month
            f"{invalid_year:04d}{invalid_month:02d}15{counter:03d}",
            # Invalid day  
            f"{invalid_year:04d}06{invalid_day:02d}{counter:03d}",
            # February 30th (always invalid)
            f"{invalid_year:04d}0230{counter:03d}",
            # February 29th in non-leap year (if year is not divisible by 4)
            f"{invalid_year if invalid_year % 4 != 0 else invalid_year + 1:04d}0229{counter:03d}",
        ]
        
        for invalid_leistungsdatum in test_cases:
            # Ensure it's exactly 11 characters
            if len(invalid_leistungsdatum) != 11:
                continue
                
            # Construct hash string
            all_fields = [meldebestaetigung_code, invalid_leistungsdatum] + remaining_fields
            hash_string = "&".join(all_fields)
            
            # Parse should return None for invalid dates
            result = parse_leistungsdatum(hash_string)
            assert result is None, f"Expected None for invalid date {invalid_leistungsdatum}, got {result}"
    
    @given(
        malformed_input=st.one_of(
            st.text(max_size=50).filter(lambda x: "&" not in x or len(x.split("&")) < 2),  # No second field
            st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^*()_+-=[]{}|;:,.<>?", max_size=100),  # Non-digit chars
            st.just(""),  # Empty string
            st.just("&"),  # Just separator
            st.just("field1&"),  # Missing second field
            st.just("field1&short"),  # Second field too short
            st.just("field1&toolongfield123456789"),  # Second field too long
            st.just("field1&1234567890A"),  # Second field with non-digit
        )
    )
    def test_malformed_input_resilience(self, malformed_input):
        """
        **Feature: leistungsdatum-integration, Property 5: Malformed input resilience**
        
        For any malformed hash string, the system should handle the error gracefully 
        without crashing.
        **Validates: Requirements 1.5**
        """
        # The function should not crash and should return None for malformed input
        try:
            result = parse_leistungsdatum(malformed_input)
            # Should return None for any malformed input
            assert result is None, f"Expected None for malformed input '{malformed_input}', got {result}"
        except Exception as e:
            # Should not raise any exceptions - graceful error handling
            assert False, f"Function crashed with malformed input '{malformed_input}': {e}"
        
        # Test specific malformed cases that should return None
        malformed_cases = [
            None,  # None input (will be converted to empty string by function)
            "",    # Empty string
            "no_separators_here",  # No & separators
            "field1",  # Only one field
            "field1&",  # Missing second field content
            "field1&abc",  # Second field too short
            "field1&12345678901234567890",  # Second field too long
            "field1&1234567890A",  # Second field with letters
            "field1&12345 67890",  # Second field with spaces
            "field1&12345-67890",  # Second field with special chars
        ]
        
        # Test cases that might have valid Leistungsdatum but other issues
        edge_cases = [
            "&20240701001",  # Missing first field but valid second field - should still extract date
            "field1&20240701001&",  # Trailing separator - should still extract date
        ]
        
        for case in malformed_cases:
            try:
                if case is None:
                    # Test None input handling
                    result = parse_leistungsdatum(None)
                else:
                    result = parse_leistungsdatum(case)
                
                # All malformed cases should return None
                assert result is None, f"Expected None for malformed case '{case}', got {result}"
            except Exception as e:
                # Should not raise exceptions
                assert False, f"Function crashed with malformed case '{case}': {e}"
        
        # Test edge cases that might still extract valid dates
        for case in edge_cases:
            try:
                result = parse_leistungsdatum(case)
                # These cases might return a valid date or None, but should not crash
                # The important thing is graceful handling
                if result is not None:
                    # If a date is returned, it should be a valid date object
                    assert isinstance(result, date), f"Expected date object or None, got {type(result)}"
            except Exception as e:
                # Should not raise exceptions
                assert False, f"Function crashed with edge case '{case}': {e}"