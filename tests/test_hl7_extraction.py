"""
Tests for HL7 case ID extraction functionality.
"""

import pytest
from hypothesis import given, strategies as st

from mvh_copy_mb.hl7_extraction import extract_hl7_case_id


class TestHL7Extraction:
    """Test cases for HL7 case ID extraction functions."""
    
    def test_extract_hl7_case_id_basic(self):
        """Test basic HL7 case ID extraction."""
        # Valid patterns - single letter codes
        assert extract_hl7_case_id("HUMGEN_A_12345") == "12345"
        assert extract_hl7_case_id("Some text HUMGEN_B_67890 more text") == "67890"
        assert extract_hl7_case_id("HUMGEN_X_999") == "999"
        
        # Valid patterns - multi-letter codes (real-world examples)
        assert extract_hl7_case_id("HUMGEN_SE_25026987") == "25026987"
        assert extract_hl7_case_id("HUMGEN_FBREK_25478959") == "25478959"
        assert extract_hl7_case_id("Some text HUMGEN_SE_12345 more text") == "12345"
        
        # No pattern
        assert extract_hl7_case_id("No pattern here") is None
        assert extract_hl7_case_id("") is None
        assert extract_hl7_case_id("HUMGEN_A_") is None
        assert extract_hl7_case_id("HUMGEN_SE_") is None
        
        # Multiple patterns - should return first
        assert extract_hl7_case_id("HUMGEN_A_111 and HUMGEN_B_222") == "111"
        assert extract_hl7_case_id("HUMGEN_SE_111 and HUMGEN_FBREK_222") == "111"
    



class TestHL7ExtractionProperties:
    """Property-based tests for HL7 case ID extraction."""
    
    @given(
        case_id=st.text(alphabet=st.characters(whitelist_categories=("Nd",)), min_size=1, max_size=20),
        prefix_text=st.text(max_size=100),
        suffix_text=st.text(max_size=100).filter(lambda x: not x or not x[0].isdigit()),
        letter_code=st.text(alphabet=st.characters(whitelist_categories=("Lu", "Ll")), min_size=1, max_size=10)
    )
    def test_hl7_id_extraction_consistency(self, case_id, prefix_text, suffix_text, letter_code):
        """
        **Feature: gepado-integration, Property 1: HL7 ID Extraction Consistency**
        
        For any Meldebestätigung string containing a valid HUMGEN pattern, 
        extracting the HL7 case ID should return the same numeric value when called multiple times.
        **Validates: Requirements 3.1, 3.2**
        """
        # Create a Meldebestätigung string with embedded HUMGEN pattern
        meldebestaetigung = f"{prefix_text}HUMGEN_{letter_code}_{case_id}{suffix_text}"
        
        # Extract the case ID multiple times
        result1 = extract_hl7_case_id(meldebestaetigung)
        result2 = extract_hl7_case_id(meldebestaetigung)
        result3 = extract_hl7_case_id(meldebestaetigung)
        
        # Should return the same result every time
        assert result1 == result2 == result3
        
        # Should return the expected case ID
        assert result1 == case_id
        
        # The extracted ID should be numeric (guaranteed by regex)
        assert result1.isdigit()
    
    @given(
        case_id1=st.text(alphabet=st.characters(whitelist_categories=("Nd",)), min_size=1, max_size=20),
        case_id2=st.text(alphabet=st.characters(whitelist_categories=("Nd",)), min_size=1, max_size=20),
        middle_text=st.text(max_size=50).filter(lambda x: not x or not x[0].isdigit()),
        letter_code1=st.text(alphabet=st.characters(whitelist_categories=("Lu", "Ll")), min_size=1, max_size=10),
        letter_code2=st.text(alphabet=st.characters(whitelist_categories=("Lu", "Ll")), min_size=1, max_size=10)
    )
    def test_multiple_patterns_first_extracted(self, case_id1, case_id2, middle_text, letter_code1, letter_code2):
        """
        Test that when multiple HUMGEN patterns exist, the first valid numeric ID is extracted.
        Part of Property 1: HL7 ID Extraction Consistency.
        """
        # Ensure middle_text provides clear separation between patterns
        # Filter out cases where middle_text could cause pattern ambiguity
        if not middle_text or middle_text.strip() == "" or any(c.isalnum() for c in middle_text[:1]):
            middle_text = " "
        
        # Create string with multiple HUMGEN patterns
        meldebestaetigung = f"HUMGEN_{letter_code1}_{case_id1}{middle_text}HUMGEN_{letter_code2}_{case_id2}"
        
        # Should extract the first case ID
        result = extract_hl7_case_id(meldebestaetigung)
        assert result == case_id1
    
    @given(
        text_without_pattern=st.text().filter(lambda x: "HUMGEN_" not in x)
    )
    def test_no_pattern_returns_none(self, text_without_pattern):
        """
        Test that strings without HUMGEN patterns return None.
        Part of Property 1: HL7 ID Extraction Consistency.
        """
        result = extract_hl7_case_id(text_without_pattern)
        assert result is None
    

    
    @given(
        text_content=st.text(max_size=200),
        num_patterns=st.integers(min_value=2, max_value=5),
        case_ids=st.lists(
            st.text(alphabet=st.characters(whitelist_categories=("Nd",)), min_size=1, max_size=10),
            min_size=2, max_size=5
        ),
        letter_codes=st.lists(
            st.text(alphabet=st.characters(whitelist_categories=("Lu", "Ll")), min_size=1, max_size=10),
            min_size=2, max_size=5
        )
    )
    def test_hl7_pattern_handling_edge_cases(self, text_content, num_patterns, case_ids, letter_codes):
        """
        **Feature: gepado-integration, Property 6: HL7 Pattern Handling Edge Cases**
        
        For any Meldebestätigung string, when multiple HUMGEN patterns exist the first valid 
        numeric ID should be extracted, when no patterns exist gepado processing should be 
        skipped, and when extraction fails a warning should be logged.
        **Validates: Requirements 3.3, 3.4, 3.5**
        """
        # Ensure we have enough case_ids and letter_codes for the number of patterns
        if len(case_ids) < num_patterns:
            case_ids = case_ids * ((num_patterns // len(case_ids)) + 1)
        if len(letter_codes) < num_patterns:
            letter_codes = letter_codes * ((num_patterns // len(letter_codes)) + 1)
        
        case_ids = case_ids[:num_patterns]
        letter_codes = letter_codes[:num_patterns]
        
        # Test case 1: Multiple patterns - should extract first
        patterns = [f"HUMGEN_{letter_codes[i]}_{case_ids[i]}" for i in range(num_patterns)]
        meldebestaetigung_multiple = f"{text_content} {' '.join(patterns)}"
        
        result = extract_hl7_case_id(meldebestaetigung_multiple)
        assert result == case_ids[0], f"Expected first case ID {case_ids[0]}, got {result}"
        
        # Test case 2: No patterns - should return None
        text_without_patterns = text_content.replace("HUMGEN_", "NOTPATTERN_")
        result_none = extract_hl7_case_id(text_without_patterns)
        assert result_none is None, "Should return None when no HUMGEN patterns exist"
        
        # Test case 3: Invalid patterns (missing numeric part) - should return None
        invalid_pattern = f"{text_content} HUMGEN_{letter_codes[0]}_"
        result_invalid = extract_hl7_case_id(invalid_pattern)
        assert result_invalid is None, "Should return None for invalid HUMGEN patterns"
        
        # Test case 4: Empty string - should return None
        result_empty = extract_hl7_case_id("")
        assert result_empty is None, "Should return None for empty string"
        
        # Test case 5: Consistency - multiple calls should return same result
        result1 = extract_hl7_case_id(meldebestaetigung_multiple)
        result2 = extract_hl7_case_id(meldebestaetigung_multiple)
        assert result1 == result2, "Multiple calls should return consistent results"