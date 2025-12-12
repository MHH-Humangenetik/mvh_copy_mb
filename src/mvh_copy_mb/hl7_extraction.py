"""
HL7 case ID extraction module for gepado integration.

This module provides functions to extract HL7 case IDs from Meldebestätigung strings
containing HUMGEN patterns. The regex-based extraction ensures that only valid 
numeric IDs are returned.
"""

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

HUMGEN_PATTERN = re.compile(r'HUMGEN_\w+_(\d+).*')


def extract_hl7_case_id(meldebestaetigung: str) -> Optional[str]:
    """
    Extract HL7 case ID from string containing HUMGEN pattern.
    
    Searches for HUMGEN_<letters>_<numeric_id> pattern and extracts the numeric portion.
    If multiple patterns exist, returns the first valid numeric ID found.
    
    Args:
        meldebestaetigung: The string to search for HUMGEN pattern
        
    Returns:
        The extracted numeric HL7 case ID as string, or None if no valid pattern found
        
    Examples:
        >>> extract_hl7_case_id("Some text HUMGEN_SE_12345 more text")
        '12345'
        >>> extract_hl7_case_id("HUMGEN_FBREK_67890 and HUMGEN_SE_11111")
        '67890'
        >>> extract_hl7_case_id("No pattern here")
        None
    """
    if not meldebestaetigung:
        return None
        
    try:
        match = HUMGEN_PATTERN.search(meldebestaetigung)
        if match:
            case_id = match.group(1)
            logger.debug(f"Extracted HL7 case ID: {case_id}")
            return case_id
        else:
            logger.debug("No HUMGEN pattern found in Meldebestätigung")
            return None
            
    except Exception as e:
        logger.warning(f"Error extracting HL7 case ID: {e}")
        return None


