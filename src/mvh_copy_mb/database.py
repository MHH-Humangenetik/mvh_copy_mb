"""
Database module for storing and retrieving Meldebestaetigung records using DuckDB.

This module provides persistent storage for all processed Meldebestätigungen,
including metadata and gPAS resolution results.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb

logger = logging.getLogger(__name__)


@dataclass
class MeldebestaetigungRecord:
    """
    Represents a processed Meldebestaetigung record with all metadata.
    
    Attributes:
        vorgangsnummer: Pseudonymized identifier (unique)
        meldebestaetigung: Complete Meldebestaetigung string
        source_file: Name of source CSV file
        typ_der_meldung: Type of report (0=initial, etc.)
        indikationsbereich: Medical indication area
        art_der_daten: Type of data
        ergebnis_qc: QC result (1=passed, etc.)
        case_id: Resolved case ID from gPAS (None if not found)
        gpas_domain: gPAS domain that resolved the pseudonym (None if not found)
        processed_at: Timestamp when record was processed
        is_done: Whether the record has been reviewed and marked complete
    """
    vorgangsnummer: str
    meldebestaetigung: str
    source_file: str
    typ_der_meldung: str
    indikationsbereich: str
    art_der_daten: str
    ergebnis_qc: str
    case_id: Optional[str]
    gpas_domain: Optional[str]
    processed_at: datetime
    is_done: bool = False


class MeldebestaetigungDatabase:
    """
    Manages DuckDB database operations for Meldebestaetigung records.
    
    This class handles database connection lifecycle, schema creation,
    and CRUD operations for Meldebestaetigung records.
    """
    
    def __init__(self, db_path: Path):
        """
        Initialize database connection.
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = db_path
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
    
    def __enter__(self) -> 'MeldebestaetigungDatabase':
        """
        Context manager entry: open database connection and create schema.
        
        Returns:
            Self for use in with statement
            
        Raises:
            Exception: If database connection or schema creation fails
        """
        try:
            self.conn = duckdb.connect(str(self.db_path))
            logger.info(f"Connected to database at {self.db_path}")
            self._create_schema()
            return self
        except Exception as e:
            logger.error(f"Failed to initialize database at {self.db_path}: {e}", exc_info=True)
            raise
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit: close database connection.
        
        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        try:
            self.close()
        except Exception as e:
            logger.warning(f"Error while closing database connection: {e}", exc_info=True)
    
    def _create_schema(self) -> None:
        """
        Create the database schema if it doesn't exist.
        
        Creates the meldebestaetigungen table with all required columns
        and constraints.
        
        Raises:
            RuntimeError: If database connection is not established
            Exception: If schema creation fails
        """
        if self.conn is None:
            raise RuntimeError("Database connection not established")
        
        try:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS meldebestaetigungen (
                vorgangsnummer VARCHAR NOT NULL PRIMARY KEY,
                source_file VARCHAR NOT NULL,
                meldebestaetigung VARCHAR NOT NULL,
                typ_der_meldung VARCHAR NOT NULL,
                indikationsbereich VARCHAR NOT NULL,
                art_der_daten VARCHAR NOT NULL,
                ergebnis_qc VARCHAR NOT NULL,
                case_id VARCHAR,
                gpas_domain VARCHAR,
                processed_at TIMESTAMP NOT NULL,
                is_done BOOLEAN DEFAULT FALSE
            )
            """
            self.conn.execute(create_table_sql)
            logger.debug("Database schema created or verified")
        except Exception as e:
            logger.error(f"Failed to create database schema: {e}", exc_info=True)
            raise
    
    def upsert_record(self, record: MeldebestaetigungRecord) -> None:
        """
        Insert or update a Meldebestaetigung record.
        
        If a record with the same vorgangsnummer exists, it will be updated.
        Otherwise, a new record will be inserted.
        
        Args:
            record: The record to insert or update
            
        Raises:
            RuntimeError: If database connection is not established
        """
        if self.conn is None:
            raise RuntimeError("Database connection not established")
        
        try:
            upsert_sql = """
            INSERT OR REPLACE INTO meldebestaetigungen (
                vorgangsnummer,
                source_file,
                meldebestaetigung,
                typ_der_meldung,
                indikationsbereich,
                art_der_daten,
                ergebnis_qc,
                case_id,
                gpas_domain,
                processed_at,
                is_done
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            self.conn.execute(
                upsert_sql,
                [
                    record.vorgangsnummer,
                    record.source_file,
                    record.meldebestaetigung,
                    record.typ_der_meldung,
                    record.indikationsbereich,
                    record.art_der_daten,
                    record.ergebnis_qc,
                    record.case_id,
                    record.gpas_domain,
                    record.processed_at,
                    record.is_done,
                ]
            )
            self.conn.commit()
            logger.debug(f"Successfully upserted record for vorgangsnummer: {record.vorgangsnummer}")
        except Exception as e:
            logger.error(
                f"Failed to upsert record for vorgangsnummer {record.vorgangsnummer}: {e}",
                exc_info=True
            )
            raise
    
    def get_record(self, vorgangsnummer: str) -> Optional[MeldebestaetigungRecord]:
        """
        Retrieve a record by its vorgangsnummer.
        
        Args:
            vorgangsnummer: The unique identifier to search for
            
        Returns:
            The matching record if found, None otherwise
            
        Raises:
            RuntimeError: If database connection is not established
        """
        if self.conn is None:
            raise RuntimeError("Database connection not established")
        
        try:
            select_sql = """
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
            WHERE vorgangsnummer = ?
            """
            
            result = self.conn.execute(select_sql, [vorgangsnummer]).fetchone()
            
            if result is None:
                logger.debug(f"No record found for vorgangsnummer: {vorgangsnummer}")
                return None
            
            logger.debug(f"Successfully retrieved record for vorgangsnummer: {vorgangsnummer}")
            return MeldebestaetigungRecord(
                vorgangsnummer=result[0],
                meldebestaetigung=result[1],
                source_file=result[2],
                typ_der_meldung=result[3],
                indikationsbereich=result[4],
                art_der_daten=result[5],
                ergebnis_qc=result[6],
                case_id=result[7],
                gpas_domain=result[8],
                processed_at=result[9],
                is_done=result[10]
            )
        except Exception as e:
            logger.error(
                f"Failed to retrieve record for vorgangsnummer {vorgangsnummer}: {e}",
                exc_info=True
            )
            raise
    
    def close(self) -> None:
        """
        Close the database connection.
        
        This method can be called explicitly or will be called automatically
        when using the context manager.
        """
        if self.conn is not None:
            try:
                self.conn.close()
                logger.debug("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing database connection: {e}", exc_info=True)
            finally:
                self.conn = None
