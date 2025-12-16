"""
Integration tests for the end-to-end workflow with database storage.

This module tests the complete workflow from CSV processing through
gPAS lookup to database storage.
"""

import tempfile
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, MagicMock

import pytest

from mvh_copy_mb.cli import process_row, GpasClient
from mvh_copy_mb.database import MeldebestaetigungDatabase


def test_process_row_with_successful_gpas_resolution():
    """
    Integration test: Process a row with successful gPAS resolution.
    
    Verifies that:
    1. The row is processed correctly
    2. gPAS lookup succeeds
    3. The record is stored in the database with case_id and gpas_domain
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        root_dir = Path(tmpdir)
        db_path = root_dir / "test.duckdb"
        source_file = root_dir / "test.csv"
        source_file.touch()
        
        # Create mock gPAS client
        mock_gpas = Mock(spec=GpasClient)
        mock_gpas.get_original_value.return_value = "CASE123"
        mock_gpas.domains = ["domain1", "domain2"]
        
        # Mock the client service
        mock_client = MagicMock()
        mock_client.service.getValueFor.return_value = "CASE123"
        mock_gpas.client = mock_client
        
        # Create test row
        row = {
            'Vorgangsnummer': 'VORG123',
            'Meldebestaetigung': 'IBE+ID+CODE&DATE&LE&KDK&0&INDICATION&PROD&COST&DATATYPE&SEQ&1'
        }
        
        # Process with database
        with MeldebestaetigungDatabase(db_path) as db:
            process_row(row, source_file, root_dir, mock_gpas, db)
            
            # Verify record was stored
            record = db.get_record('VORG123')
            assert record is not None
            assert record.vorgangsnummer == 'VORG123'
            assert record.case_id == 'CASE123'
            assert record.gpas_domain in ['domain1', 'domain2']
            assert record.source_file == 'test.csv'
            assert record.typ_der_meldung == '0'
            assert record.indikationsbereich == 'INDICATION'
            assert record.art_der_daten == 'DATATYPE'
            assert record.ergebnis_qc == '1'


def test_process_row_with_failed_gpas_resolution():
    """
    Integration test: Process a row with failed gPAS resolution.
    
    Verifies that:
    1. The row is processed correctly
    2. gPAS lookup fails
    3. The record is stored in the database with NULL case_id and gpas_domain
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        root_dir = Path(tmpdir)
        db_path = root_dir / "test.duckdb"
        source_file = root_dir / "test.csv"
        source_file.touch()
        
        # Create mock gPAS client that returns None (failed resolution)
        mock_gpas = Mock(spec=GpasClient)
        mock_gpas.get_original_value.return_value = None
        mock_gpas.domains = ["domain1", "domain2"]
        
        # Create test row
        row = {
            'Vorgangsnummer': 'VORG456',
            'Meldebestaetigung': 'IBE+ID+CODE&DATE&LE&KDK&0&INDICATION&PROD&COST&DATATYPE&SEQ&1'
        }
        
        # Process with database
        with MeldebestaetigungDatabase(db_path) as db:
            process_row(row, source_file, root_dir, mock_gpas, db)
            
            # Verify record was stored with NULL values
            record = db.get_record('VORG456')
            assert record is not None
            assert record.vorgangsnummer == 'VORG456'
            assert record.case_id is None
            assert record.gpas_domain is None
            assert record.source_file == 'test.csv'


def test_multiple_processing_runs_with_upsert():
    """
    Integration test: Process the same record multiple times.
    
    Verifies that:
    1. Multiple processing runs work correctly
    2. Upsert behavior prevents duplicates
    3. The latest values are stored
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        root_dir = Path(tmpdir)
        db_path = root_dir / "test.duckdb"
        source_file1 = root_dir / "test1.csv"
        source_file2 = root_dir / "test2.csv"
        source_file1.touch()
        source_file2.touch()
        
        # Create mock gPAS client
        mock_gpas = Mock(spec=GpasClient)
        mock_gpas.get_original_value.return_value = "CASE789"
        mock_gpas.domains = ["domain1"]
        
        # Mock the client service
        mock_client = MagicMock()
        mock_client.service.getValueFor.return_value = "CASE789"
        mock_gpas.client = mock_client
        
        # Create test row (same vorgangsnummer)
        row = {
            'Vorgangsnummer': 'VORG789',
            'Meldebestaetigung': 'IBE+ID+CODE&DATE&LE&KDK&0&INDICATION&PROD&COST&DATATYPE&SEQ&1'
        }
        
        # First processing run
        with MeldebestaetigungDatabase(db_path) as db:
            process_row(row, source_file1, root_dir, mock_gpas, db)
            
            # Verify first record
            record1 = db.get_record('VORG789')
            assert record1 is not None
            assert record1.source_file == 'test1.csv'
            timestamp1 = record1.processed_at
        
        # Second processing run (same vorgangsnummer, different file)
        with MeldebestaetigungDatabase(db_path) as db:
            process_row(row, source_file2, root_dir, mock_gpas, db)
            
            # Verify record was updated, not duplicated
            record2 = db.get_record('VORG789')
            assert record2 is not None
            assert record2.source_file == 'test2.csv'
            assert record2.processed_at >= timestamp1
            
            # Verify only one record exists
            count = db.conn.execute(
                "SELECT COUNT(*) FROM meldebestaetigungen WHERE vorgangsnummer = ?",
                ['VORG789']
            ).fetchone()[0]
            assert count == 1


def test_database_error_handling_continues_processing():
    """
    Integration test: Verify processing continues after database errors.
    
    Verifies that:
    1. Database errors are logged
    2. Processing continues despite errors
    3. Subsequent records can still be processed
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        root_dir = Path(tmpdir)
        db_path = root_dir / "test.duckdb"
        source_file = root_dir / "test.csv"
        source_file.touch()
        
        # Create mock gPAS client
        mock_gpas = Mock(spec=GpasClient)
        mock_gpas.get_original_value.return_value = "CASE999"
        mock_gpas.domains = ["domain1"]
        
        # Mock the client service
        mock_client = MagicMock()
        mock_client.service.getValueFor.return_value = "CASE999"
        mock_gpas.client = mock_client
        
        # Create test row
        row = {
            'Vorgangsnummer': 'VORG999',
            'Meldebestaetigung': 'IBE+ID+CODE&DATE&LE&KDK&0&INDICATION&PROD&COST&DATATYPE&SEQ&1'
        }
        
        # Process with database
        with MeldebestaetigungDatabase(db_path) as db:
            # First, process successfully
            process_row(row, source_file, root_dir, mock_gpas, db)
            
            # Verify record was stored
            record = db.get_record('VORG999')
            assert record is not None
            
            # Now simulate a database error by closing the connection
            original_conn = db.conn
            db.conn = None
            
            # Try to process another row - should log error but not crash
            row2 = {
                'Vorgangsnummer': 'VORG888',
                'Meldebestaetigung': 'IBE+ID+CODE&DATE&LE&KDK&0&INDICATION&PROD&COST&DATATYPE&SEQ&1'
            }
            
            # This should not raise an exception
            process_row(row2, source_file, root_dir, mock_gpas, db)
            
            # Restore connection
            db.conn = original_conn
            
            # Verify we can still process after the error
            row3 = {
                'Vorgangsnummer': 'VORG777',
                'Meldebestaetigung': 'IBE+ID+CODE&DATE&LE&KDK&0&INDICATION&PROD&COST&DATATYPE&SEQ&1'
            }
            process_row(row3, source_file, root_dir, mock_gpas, db)
            
            # Verify the third record was stored
            record3 = db.get_record('VORG777')
            assert record3 is not None


def test_process_row_with_leistungsdatum_extraction():
    """
    Integration test: Process a row with Leistungsdatum extraction.
    
    Verifies that:
    1. The row is processed correctly
    2. Leistungsdatum is extracted from the hash string
    3. The record is stored in the database with output_date
    """
    from datetime import date
    
    with tempfile.TemporaryDirectory() as tmpdir:
        root_dir = Path(tmpdir)
        db_path = root_dir / "test.duckdb"
        source_file = root_dir / "test.csv"
        source_file.touch()
        
        # Create mock gPAS client
        mock_gpas = Mock(spec=GpasClient)
        mock_gpas.get_original_value.return_value = "CASE123"
        mock_gpas.domains = ["domain1", "domain2"]
        
        # Mock the client service
        mock_client = MagicMock()
        mock_client.service.getValueFor.return_value = "CASE123"
        mock_gpas.client = mock_client
        
        # Create test row with valid Leistungsdatum in hash string
        # Format: CODE&LEISTUNGSDATUM&LE&KDK&TYP&INDICATION&PROD&COST&DATATYPE&SEQ&QC
        # Leistungsdatum: 20240701001 (July 1, 2024 + counter 001)
        row = {
            'Vorgangsnummer': 'VORG_LEISTUNG',
            'Meldebestaetigung': 'IBE+ID+CODE&20240701001&LE&KDK&0&INDICATION&PROD&COST&DATATYPE&SEQ&1'
        }
        
        # Process with database
        with MeldebestaetigungDatabase(db_path) as db:
            process_row(row, source_file, root_dir, mock_gpas, db)
            
            # Verify record was stored with output_date
            record = db.get_record('VORG_LEISTUNG')
            assert record is not None
            assert record.vorgangsnummer == 'VORG_LEISTUNG'
            assert record.output_date == date(2024, 7, 1)
            assert record.case_id == 'CASE123'
            assert record.typ_der_meldung == '0'
            assert record.indikationsbereich == 'INDICATION'
            assert record.art_der_daten == 'DATATYPE'
            assert record.ergebnis_qc == '1'


def test_process_row_with_invalid_leistungsdatum():
    """
    Integration test: Process a row with invalid Leistungsdatum.
    
    Verifies that:
    1. The row is processed correctly
    2. Invalid Leistungsdatum results in NULL output_date
    3. Processing continues despite extraction failure
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        root_dir = Path(tmpdir)
        db_path = root_dir / "test.duckdb"
        source_file = root_dir / "test.csv"
        source_file.touch()
        
        # Create mock gPAS client
        mock_gpas = Mock(spec=GpasClient)
        mock_gpas.get_original_value.return_value = "CASE456"
        mock_gpas.domains = ["domain1"]
        
        # Mock the client service
        mock_client = MagicMock()
        mock_client.service.getValueFor.return_value = "CASE456"
        mock_gpas.client = mock_client
        
        # Create test row with invalid Leistungsdatum in hash string
        # Invalid date: 20241301001 (month 13 doesn't exist)
        row = {
            'Vorgangsnummer': 'VORG_INVALID',
            'Meldebestaetigung': 'IBE+ID+CODE&20241301001&LE&KDK&0&INDICATION&PROD&COST&DATATYPE&SEQ&1'
        }
        
        # Process with database
        with MeldebestaetigungDatabase(db_path) as db:
            process_row(row, source_file, root_dir, mock_gpas, db)
            
            # Verify record was stored with NULL output_date
            record = db.get_record('VORG_INVALID')
            assert record is not None
            assert record.vorgangsnummer == 'VORG_INVALID'
            assert record.output_date is None  # Should be NULL for invalid date
            assert record.case_id == 'CASE456'
            assert record.typ_der_meldung == '0'
            assert record.indikationsbereich == 'INDICATION'
            assert record.art_der_daten == 'DATATYPE'
            assert record.ergebnis_qc == '1'


def test_process_row_with_legacy_format_leistungsdatum():
    """
    Integration test: Process a row with legacy format (no valid Leistungsdatum).
    
    Verifies that:
    1. The row is processed correctly
    2. Legacy format results in NULL output_date
    3. Processing continues despite extraction failure
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        root_dir = Path(tmpdir)
        db_path = root_dir / "test.duckdb"
        source_file = root_dir / "test.csv"
        source_file.touch()
        
        # Create mock gPAS client
        mock_gpas = Mock(spec=GpasClient)
        mock_gpas.get_original_value.return_value = "CASE789"
        mock_gpas.domains = ["domain1"]
        
        # Mock the client service
        mock_client = MagicMock()
        mock_client.service.getValueFor.return_value = "CASE789"
        mock_gpas.client = mock_client
        
        # Create test row with legacy format (short date field)
        # Legacy format: DATE instead of JJJJMMTTZZZ
        row = {
            'Vorgangsnummer': 'VORG_LEGACY',
            'Meldebestaetigung': 'IBE+ID+CODE&DATE&LE&KDK&0&INDICATION&PROD&COST&DATATYPE&SEQ&1'
        }
        
        # Process with database
        with MeldebestaetigungDatabase(db_path) as db:
            process_row(row, source_file, root_dir, mock_gpas, db)
            
            # Verify record was stored with NULL output_date
            record = db.get_record('VORG_LEGACY')
            assert record is not None
            assert record.vorgangsnummer == 'VORG_LEGACY'
            assert record.output_date is None  # Should be NULL for legacy format
            assert record.case_id == 'CASE789'
            assert record.typ_der_meldung == '0'
            assert record.indikationsbereich == 'INDICATION'
            assert record.art_der_daten == 'DATATYPE'
            assert record.ergebnis_qc == '1'
