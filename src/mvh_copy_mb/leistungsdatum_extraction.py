"""
Leistungsdatum extraction module for Meldebestätigung hash strings.

This module provides functions to extract and parse Leistungsdatum (service date) 
from Meldebestätigung hash strings according to the MVGenomSeq documentation.
The Leistungsdatum appears as the second field in JJJJMMTTZZZ format.
"""

import logging
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)


def parse_leistungsdatum(hash_string: Optional[str]) -> Optional[date]:
    """
    Extract and parse Leistungsdatum from Meldebestätigung hash string.
    
    The Leistungsdatum is located in the second field (position 1) of the hash string
    and has the format JJJJMMTTZZZ where:
    - JJJJ = year (4 digits)
    - MM = month (2 digits) 
    - TT = day (2 digits)
    - ZZZ = counter (3 digits, discarded)
    
    Args:
        hash_string: The complete Meldebestätigung hash string with '&' separators
        
    Returns:
        Parsed date object from the first 8 characters (JJJJMMTT), or None if invalid
        
    Examples:
        >>> parse_leistungsdatum("A123456789&20240701001&260530103&KDKK00001&0&O&9&1&C&2&1")
        date(2024, 7, 1)
        >>> parse_leistungsdatum("A123456789&20241301001&260530103&KDKK00001&0&O&9&1&C&2&1")
        None
        >>> parse_leistungsdatum("invalid")
        None
    """
    if not hash_string:
        logger.debug("Empty hash string provided")
        return None
        
    try:
        # Split hash string by '&' separator
        fields = hash_string.split('&')
        
        # Check if we have at least 2 fields (need position 1)
        if len(fields) < 2:
            logger.warning(f"Hash string has insufficient fields: {len(fields)}, expected at least 2")
            return None
            
        # Extract Leistungsdatum field (position 1)
        leistungsdatum_field = fields[1]
        
        # Validate format: should be exactly 11 characters (JJJJMMTTZZZ)
        if len(leistungsdatum_field) != 11:
            logger.warning(f"Leistungsdatum field has invalid length: {len(leistungsdatum_field)}, expected 11")
            return None
            
        # Check if all characters are digits
        if not leistungsdatum_field.isdigit():
            logger.warning(f"Leistungsdatum field contains non-digit characters: {leistungsdatum_field}")
            return None
            
        # Extract date portion (first 8 characters: JJJJMMTT)
        date_portion = leistungsdatum_field[:8]
        
        # Parse year, month, day
        year = int(date_portion[:4])
        month = int(date_portion[4:6])
        day = int(date_portion[6:8])
        
        # Create and validate date
        parsed_date = date(year, month, day)
        
        logger.debug(f"Successfully parsed Leistungsdatum: {parsed_date}")
        return parsed_date
        
    except ValueError as e:
        logger.error(f"Invalid date values in Leistungsdatum field: {e}")
        return None
    except Exception as e:
        logger.warning(f"Error parsing Leistungsdatum from hash string: {e}")
        return None