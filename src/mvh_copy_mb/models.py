"""
Pydantic models for API request and response validation.

This module defines data models for FastAPI endpoints, providing
type validation and serialization for web API interactions.
"""

from datetime import datetime, date
from typing import Optional

from pydantic import BaseModel, Field


class DoneStatusUpdate(BaseModel):
    """
    Request model for updating done status of a record pair.
    
    Attributes:
        done: The new done status (True = marked as done, False = not done)
    """
    done: bool = Field(
        ...,
        description="Whether the record pair should be marked as done"
    )


class RecordResponse(BaseModel):
    """
    Response model for individual Meldebestaetigung records.
    
    Attributes:
        vorgangsnummer: Pseudonymized identifier
        meldebestaetigung: IBE string (full content from second CSV column)
        case_id: Resolved case ID from gPAS (None if not found)
        art_der_daten: Type of data (genomic/clinical)
        typ_der_meldung: Type of report
        indikationsbereich: Medical indication area
        ergebnis_qc: QC result
        source_file: Name of source CSV file
        processed_at: Timestamp when record was processed
        is_done: Whether the record has been reviewed
        output_date: Leistungsdatum extracted from hash string (None if not parseable)
    """
    vorgangsnummer: str
    meldebestaetigung: str
    case_id: Optional[str]
    art_der_daten: str
    typ_der_meldung: str
    indikationsbereich: str
    ergebnis_qc: str
    source_file: str
    processed_at: datetime
    is_done: bool
    output_date: Optional[date] = None
    
    class Config:
        """Pydantic configuration."""
        from_attributes = True


class PairResponse(BaseModel):
    """
    Response model for record pairs grouped by Case ID.
    
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
    genomic: Optional[RecordResponse]
    clinical: Optional[RecordResponse]
    is_complete: bool
    is_valid: bool
    is_done: bool
    priority_group: int
    
    class Config:
        """Pydantic configuration."""
        from_attributes = True
