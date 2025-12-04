"""
Web database service layer for grouping and managing Meldebestaetigung records.

This module provides web-specific database operations including record pairing,
priority group calculation, and batch done status updates.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from .database import MeldebestaetigungDatabase, MeldebestaetigungRecord

logger = logging.getLogger(__name__)


@dataclass
class RecordPair:
    """
    Represents a logical grouping of genomic and clinical records by Case ID.
    
    Attributes:
        case_id: The shared Case ID
        genomic: Genomic record (if exists)
        clinical: Clinical record (if exists)
        is_complete: Both genomic and clinical present
        is_valid: Complete + both have passing QC
        is_done: Both records marked done
        priority_group: 1, 2, or 3 for sorting
    """
    case_id: str
    genomic: Optional[MeldebestaetigungRecord]
    clinical: Optional[MeldebestaetigungRecord]
    is_complete: bool
    is_valid: bool
    is_done: bool
    priority_group: int


class WebDatabaseService:
    """
    Service layer for web-specific database operations.
    
    Provides methods for grouping records by Case ID, calculating priority groups,
    and managing done status for record pairs.
    """
    
    def __init__(self, db_path: Path):
        """
        Initialize the web database service.
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = db_path
    
    def get_all_records_grouped(self) -> List[RecordPair]:
        """
        Retrieve all records from the database grouped by Case ID.
        
        Returns:
            List of RecordPair objects sorted by priority group, Case ID, and data type
            
        Raises:
            Exception: If database operations fail
        """
        with MeldebestaetigungDatabase(self.db_path) as db:
            # Query all records from the database
            query = """
            SELECT 
                vorgangsnummer,
                meldebestaetigung,
                source_file,
                typ_der_meldung,
                indikationsbereich,
                art_der_daten,
                ergebnis_qc,
                case_id,
                gpas_domain,
                processed_at,
                is_done
            FROM meldebestaetigungen
            ORDER BY case_id, art_der_daten
            """
            
            results = db.conn.execute(query).fetchall()
            
            # Group records by Case ID
            records_by_case_id = {}
            for row in results:
                record = MeldebestaetigungRecord(
                    vorgangsnummer=row[0],
                    meldebestaetigung=row[1],
                    source_file=row[2],
                    typ_der_meldung=row[3],
                    indikationsbereich=row[4],
                    art_der_daten=row[5],
                    ergebnis_qc=row[6],
                    case_id=row[7],
                    gpas_domain=row[8],
                    processed_at=row[9],
                    is_done=row[10]
                )
                
                # Skip records without case_id
                if record.case_id is None:
                    continue
                
                if record.case_id not in records_by_case_id:
                    records_by_case_id[record.case_id] = {'genomic': None, 'clinical': None}
                
                # Assign to genomic or clinical based on art_der_daten (G/C)
                if record.art_der_daten.lower() == 'g':
                    records_by_case_id[record.case_id]['genomic'] = record
                elif record.art_der_daten.lower() == 'c':
                    records_by_case_id[record.case_id]['clinical'] = record
            
            # Create RecordPair objects
            pairs = []
            for case_id, records in records_by_case_id.items():
                genomic = records['genomic']
                clinical = records['clinical']
                
                # Determine completeness
                is_complete = genomic is not None and clinical is not None
                
                # Determine validity (complete + both have passing QC)
                is_valid = False
                if is_complete:
                    is_valid = genomic.ergebnis_qc == "1" and clinical.ergebnis_qc == "1"
                
                # Determine done status (both records marked done)
                is_done = False
                if is_complete:
                    is_done = genomic.is_done and clinical.is_done
                elif genomic is not None:
                    is_done = genomic.is_done
                elif clinical is not None:
                    is_done = clinical.is_done
                
                # Calculate priority group
                priority_group = self._calculate_priority_group(is_complete, is_done)
                
                pair = RecordPair(
                    case_id=case_id,
                    genomic=genomic,
                    clinical=clinical,
                    is_complete=is_complete,
                    is_valid=is_valid,
                    is_done=is_done,
                    priority_group=priority_group
                )
                pairs.append(pair)
            
            # Sort by priority group, then by Case ID
            pairs.sort(key=lambda p: (p.priority_group, p.case_id))
            
            return pairs
    
    def _calculate_priority_group(self, is_complete: bool, is_done: bool) -> int:
        """
        Calculate the priority group for a record pair.
        
        Priority groups:
        - Group 1: Complete pairs not done (highest priority)
        - Group 2: Incomplete pairs
        - Group 3: Complete pairs done (lowest priority)
        
        Args:
            is_complete: Whether both genomic and clinical records exist
            is_done: Whether both records are marked done
            
        Returns:
            Priority group number (1, 2, or 3)
        """
        if is_complete and not is_done:
            return 1  # Complete pairs not done
        elif not is_complete:
            return 2  # Incomplete pairs
        else:  # is_complete and is_done
            return 3  # Complete pairs done
    
    def update_pair_done_status(self, case_id: str, done: bool) -> bool:
        """
        Update the done status for both records in a pair.
        
        Args:
            case_id: The Case ID of the pair to update
            done: The new done status
            
        Returns:
            True if the update was successful, False otherwise
            
        Raises:
            ValueError: If the Case ID doesn't exist or the pair is incomplete
            Exception: If database operations fail
        """
        with MeldebestaetigungDatabase(self.db_path) as db:
            # Query records for this Case ID
            query = """
            SELECT 
                vorgangsnummer,
                meldebestaetigung,
                source_file,
                typ_der_meldung,
                indikationsbereich,
                art_der_daten,
                ergebnis_qc,
                case_id,
                gpas_domain,
                processed_at,
                is_done
            FROM meldebestaetigungen
            WHERE case_id = ?
            """
            
            results = db.conn.execute(query, [case_id]).fetchall()
            
            if not results:
                raise ValueError(f"No records found for Case ID: {case_id}")
            
            # Convert to records
            records = []
            for row in results:
                record = MeldebestaetigungRecord(
                    vorgangsnummer=row[0],
                    meldebestaetigung=row[1],
                    source_file=row[2],
                    typ_der_meldung=row[3],
                    indikationsbereich=row[4],
                    art_der_daten=row[5],
                    ergebnis_qc=row[6],
                    case_id=row[7],
                    gpas_domain=row[8],
                    processed_at=row[9],
                    is_done=row[10]
                )
                records.append(record)
            
            # Check if pair is complete (both genomic and clinical)
            has_genomic = any(r.art_der_daten.lower() == 'g' for r in records)
            has_clinical = any(r.art_der_daten.lower() == 'c' for r in records)
            
            if not (has_genomic and has_clinical):
                raise ValueError(f"Incomplete pair for Case ID: {case_id}")
            
            # Update done status for all records in the pair
            for record in records:
                record.is_done = done
                db.upsert_record(record)
            
            logger.info(f"Updated done status to {done} for Case ID: {case_id}")
            return True
    
    def get_pair_by_case_id(self, case_id: str) -> Optional[RecordPair]:
        """
        Retrieve a specific record pair by Case ID.
        
        Args:
            case_id: The Case ID to search for
            
        Returns:
            RecordPair if found, None otherwise
            
        Raises:
            Exception: If database operations fail
        """
        with MeldebestaetigungDatabase(self.db_path) as db:
            # Query records for this Case ID
            query = """
            SELECT 
                vorgangsnummer,
                meldebestaetigung,
                source_file,
                typ_der_meldung,
                indikationsbereich,
                art_der_daten,
                ergebnis_qc,
                case_id,
                gpas_domain,
                processed_at,
                is_done
            FROM meldebestaetigungen
            WHERE case_id = ?
            """
            
            results = db.conn.execute(query, [case_id]).fetchall()
            
            if not results:
                return None
            
            # Convert to records and group by type
            genomic = None
            clinical = None
            
            for row in results:
                record = MeldebestaetigungRecord(
                    vorgangsnummer=row[0],
                    meldebestaetigung=row[1],
                    source_file=row[2],
                    typ_der_meldung=row[3],
                    indikationsbereich=row[4],
                    art_der_daten=row[5],
                    ergebnis_qc=row[6],
                    case_id=row[7],
                    gpas_domain=row[8],
                    processed_at=row[9],
                    is_done=row[10]
                )
                
                # Handle single-letter codes (G/C)
                if record.art_der_daten.lower() == 'g':
                    genomic = record
                elif record.art_der_daten.lower() == 'c':
                    clinical = record
            
            # Determine completeness
            is_complete = genomic is not None and clinical is not None
            
            # Determine validity
            is_valid = False
            if is_complete:
                is_valid = genomic.ergebnis_qc == "1" and clinical.ergebnis_qc == "1"
            
            # Determine done status
            is_done = False
            if is_complete:
                is_done = genomic.is_done and clinical.is_done
            elif genomic is not None:
                is_done = genomic.is_done
            elif clinical is not None:
                is_done = clinical.is_done
            
            # Calculate priority group
            priority_group = self._calculate_priority_group(is_complete, is_done)
            
            return RecordPair(
                case_id=case_id,
                genomic=genomic,
                clinical=clinical,
                is_complete=is_complete,
                is_valid=is_valid,
                is_done=is_done,
                priority_group=priority_group
            )
