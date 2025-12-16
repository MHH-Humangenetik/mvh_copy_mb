"""
HL7 Case ID Correction module for validating and correcting case references.

This module provides functionality to validate that hl7_case_id references have
the correct sapVisitingType ('GS' for genomic sequencing) and automatically
find the correct case when needed. This correction applies only to GEPADO
operations, preserving the original hl7_case_id for local storage and file naming.
"""

from dataclasses import dataclass
from typing import Optional
import logging

from .gepado import GepadoClient

logger = logging.getLogger(__name__)


@dataclass
class CaseValidationResult:
    """
    Result of HL7 case ID validation and correction process.
    
    Attributes:
        original_case_id: The original hl7_case_id from the Meldebestätigung
        is_valid: Whether the original case has correct sapVisitingType
        corrected_case_id: Corrected hl7_case_id if correction was successful
        patient_guid: Patient GUID extracted from the original case
        error_message: Error message if validation/correction failed
        correction_applied: Whether a correction was successfully applied
    """
    original_case_id: str
    is_valid: bool
    corrected_case_id: Optional[str] = None
    patient_guid: Optional[str] = None
    error_message: Optional[str] = None
    correction_applied: bool = False


def validate_hl7_case_sapvisitingtype(client: GepadoClient, hl7_case_id: str) -> bool:
    """
    Validate that hl7_case_id has correct sapVisitingType for genomic sequencing.
    
    Args:
        client: GepadoClient instance for database access
        hl7_case_id: The HL7 case ID to validate
        
    Returns:
        True if case has sapVisitingType 'GS', False otherwise
        
    Raises:
        Exception: If database query fails
    """
    if not client._connection:
        client.connect()
    
    try:
        cursor = client._connection.cursor()
        
        # Query sapVisitingType for the given hl7_case_id
        query = """
            SELECT sapVisitingType 
            FROM av_ordermanagement 
            WHERE hl7fallid = %s
        """
        
        cursor.execute(query, (hl7_case_id,))
        row = cursor.fetchone()
        
        if row:
            sap_visiting_type = row[0]
            is_valid = sap_visiting_type == 'GS'
            
            # Only log at debug level here to avoid duplicate logging
            # Main logging will be done in the orchestrator function
            logger.debug(f"HL7 case ID {hl7_case_id} sapVisitingType: {sap_visiting_type} (valid: {is_valid})")
            
            return is_valid
        else:
            logger.warning(f"HL7 case ID {hl7_case_id} not found in av_ordermanagement")
            return False
            
    except Exception as e:
        logger.error(f"Failed to validate sapVisitingType for HL7 case ID {hl7_case_id}: {e}")
        raise
    finally:
        if cursor:
            cursor.close()


def get_patient_guid_for_case(client: GepadoClient, hl7_case_id: str) -> Optional[str]:
    """
    Extract patient GUID from av_ordermanagement for a given hl7_case_id.
    
    Args:
        client: GepadoClient instance for database access
        hl7_case_id: The HL7 case ID to look up
        
    Returns:
        Patient GUID if found, None otherwise
        
    Raises:
        Exception: If database query fails
    """
    if not client._connection:
        client.connect()
    
    try:
        cursor = client._connection.cursor()
        
        # Query patient GUID for the given hl7_case_id
        query = """
            SELECT guid_patient 
            FROM av_ordermanagement 
            WHERE hl7fallid = %s
        """
        
        cursor.execute(query, (hl7_case_id,))
        row = cursor.fetchone()
        
        if row:
            patient_guid = row[0]
            # Don't log here - let the orchestrator handle logging to avoid duplicates
            return patient_guid
        else:
            # Don't log here - let the orchestrator function handle the logging
            # to avoid duplicate warnings
            return None
            
    except Exception as e:
        logger.error(f"Failed to get patient GUID for HL7 case ID {hl7_case_id}: {e}")
        raise
    finally:
        if cursor:
            cursor.close()


def find_correct_genomic_case(client: GepadoClient, patient_guid: str, original_case_id: str = None) -> Optional[str]:
    """
    Find correct genomic sequencing case for a patient.
    
    Args:
        client: GepadoClient instance for database access
        patient_guid: The patient GUID to search for
        original_case_id: Original case ID for enhanced logging (optional)
        
    Returns:
        Corrected hl7_case_id if exactly one correct case found, None otherwise
        
    Raises:
        Exception: If database query fails
    """
    if not client._connection:
        client.connect()
    
    try:
        cursor = client._connection.cursor()
        
        # Search for all cases with same patient GUID and sapVisitingType 'GS'
        query = """
            SELECT hl7fallid 
            FROM av_ordermanagement 
            WHERE guid_patient = %s AND sapVisitingType = 'GS'
        """
        
        cursor.execute(query, (patient_guid,))
        rows = cursor.fetchall()
        
        if len(rows) == 0:
            # Enhanced logging: Include patient GUID and original case ID when no correct cases exist
            if original_case_id:
                logger.warning(f"No genomic sequencing cases found for patient GUID {patient_guid} (original case ID: {original_case_id})")
            else:
                logger.warning(f"No genomic sequencing cases found for patient GUID {patient_guid}")
            return None
        elif len(rows) == 1:
            corrected_case_id = rows[0][0]
            logger.info(f"Found single correct genomic case {corrected_case_id} for patient GUID {patient_guid}")
            return corrected_case_id
        else:
            # Enhanced logging: Log all candidate case IDs for manual review
            case_ids = [row[0] for row in rows]
            if original_case_id:
                logger.error(f"Multiple genomic sequencing cases found for patient GUID {patient_guid} (original case ID: {original_case_id}). "
                           f"Manual review required for candidates: {', '.join(case_ids)}")
            else:
                logger.error(f"Multiple genomic sequencing cases found for patient GUID {patient_guid}. "
                           f"Manual review required for candidates: {', '.join(case_ids)}")
            return None
            
    except Exception as e:
        logger.error(f"Failed to find correct genomic case for patient GUID {patient_guid}: {e}")
        raise
    finally:
        if cursor:
            cursor.close()


def correct_hl7_case_id_for_gepado(client: GepadoClient, hl7_case_id: str) -> str:
    """
    Orchestrate the complete validation and correction process.
    
    Args:
        client: GepadoClient instance for database access
        hl7_case_id: Original hl7_case_id from Meldebestätigung
        
    Returns:
        Corrected hl7_case_id or original if no correction needed/possible
    """
    try:
        # Step 1: Validate sapVisitingType
        is_valid = validate_hl7_case_sapvisitingtype(client, hl7_case_id)
        
        if is_valid:
            # Consolidated logging: Single message for successful validation
            logger.info(f"HL7 case ID validation PASSED for {hl7_case_id} - sapVisitingType is 'GS', no correction needed")
            return hl7_case_id
        else:
            # Log the validation failure with specific reason
            logger.warning(f"HL7 case ID validation FAILED for {hl7_case_id} - sapVisitingType is not 'GS', attempting correction")
        
        # Step 2: Get patient GUID for incorrect case
        patient_guid = get_patient_guid_for_case(client, hl7_case_id)
        
        if not patient_guid:
            logger.error(f"Cannot correct HL7 case ID {hl7_case_id}: patient GUID not found in av_ordermanagement")
            return hl7_case_id
        
        logger.info(f"Found patient GUID {patient_guid} for HL7 case ID {hl7_case_id}, searching for correct genomic case")
        
        # Step 3: Find correct genomic case for this patient (pass original case ID for enhanced logging)
        corrected_case_id = find_correct_genomic_case(client, patient_guid, hl7_case_id)
        
        if corrected_case_id:
            # Enhanced logging: Detailed successful correction with both original and corrected IDs
            logger.info(f"HL7 case ID correction SUCCESSFUL: {hl7_case_id} -> {corrected_case_id} (patient GUID: {patient_guid})")
            return corrected_case_id
        else:
            # The specific reason for failure is already logged in find_correct_genomic_case
            # Just log the final outcome here
            logger.warning(f"HL7 case ID correction FAILED for {hl7_case_id}: returning original case ID")
            return hl7_case_id
            
    except Exception as e:
        # Enhanced error logging with more detailed information
        if "pymssql" in str(type(e)) or "database" in str(e).lower():
            logger.error(f"Database connection/query error during HL7 case ID correction for {hl7_case_id}: {type(e).__name__}: {e}")
        else:
            logger.error(f"Unexpected error during HL7 case ID correction for {hl7_case_id}: {type(e).__name__}: {e}")
        return hl7_case_id