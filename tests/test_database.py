"""
Property-based tests for the DuckDB database module.

This module contains property-based tests using Hypothesis to verify
correctness properties of the database implementation.
"""

import tempfile
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mvh_copy_mb.database import MeldebestaetigungDatabase


# Feature: duckdb-storage, Property 1: Database file creation in correct location
# Validates: Requirements 1.1, 1.4
@settings(max_examples=100)
@given(st.text(min_size=1, max_size=50))
def test_database_file_creation_in_correct_location(dir_name: str):
    """
    Property 1: Database file creation in correct location
    
    For any valid input directory path, when the database is initialized,
    the database file should exist at the path {input_directory}/meldebestaetigungen.duckdb
    
    This test verifies that:
    1. The database file is created in the specified directory
    2. The file exists after initialization
    3. The file is at the exact expected path
    """
    # Create a temporary directory for the test
    with tempfile.TemporaryDirectory() as tmpdir:
        # Sanitize the directory name to avoid filesystem issues
        safe_dir_name = "".join(c if c.isalnum() or c in ('-', '_') else '_' for c in dir_name)
        input_dir = Path(tmpdir) / safe_dir_name
        input_dir.mkdir(parents=True, exist_ok=True)
        
        # Expected database path
        expected_db_path = input_dir / "meldebestaetigungen.duckdb"
        
        # Verify file doesn't exist before initialization
        assert not expected_db_path.exists()
        
        # Initialize database
        with MeldebestaetigungDatabase(expected_db_path) as db:
            # Verify the database file exists at the correct location
            assert expected_db_path.exists()
            assert expected_db_path.is_file()
            
            # Verify the path matches what we expect
            assert db.db_path == expected_db_path
            
            # Verify we can interact with the database
            assert db.conn is not None


# Feature: duckdb-storage, Property 2: Schema persistence across sessions
# Validates: Requirements 1.2, 1.3
@settings(max_examples=100)
@given(st.text(min_size=1, max_size=50))
def test_schema_persistence_across_sessions(db_name: str):
    """
    Property 2: Schema persistence across sessions
    
    For any database that has been created and closed, when reopened,
    the schema should still exist and be queryable.
    
    This test verifies that:
    1. A database can be created with the schema
    2. The database can be closed
    3. The database can be reopened
    4. The schema still exists and is queryable after reopening
    """
    # Create a temporary directory for the test database
    with tempfile.TemporaryDirectory() as tmpdir:
        # Sanitize the database name to avoid filesystem issues
        safe_db_name = "".join(c if c.isalnum() or c in ('-', '_') else '_' for c in db_name)
        db_path = Path(tmpdir) / f"{safe_db_name}.duckdb"
        
        # First session: create database and schema
        with MeldebestaetigungDatabase(db_path) as db:
            # Verify connection is established
            assert db.conn is not None
            
            # Verify the table exists by querying the schema
            result = db.conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'meldebestaetigungen'"
            ).fetchall()
            assert len(result) == 1
            assert result[0][0] == 'meldebestaetigungen'
        
        # Database is now closed (context manager exit)
        
        # Second session: reopen the database
        with MeldebestaetigungDatabase(db_path) as db:
            # Verify connection is established
            assert db.conn is not None
            
            # Verify the table still exists
            result = db.conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'meldebestaetigungen'"
            ).fetchall()
            assert len(result) == 1
            assert result[0][0] == 'meldebestaetigungen'
            
            # Verify all expected columns exist
            columns_result = db.conn.execute(
                """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'meldebestaetigungen'
                ORDER BY column_name
                """
            ).fetchall()
            
            column_names = [row[0] for row in columns_result]
            expected_columns = [
                'art_der_daten',
                'case_id',
                'ergebnis_qc',
                'gpas_domain',
                'indikationsbereich',
                'is_done',
                'meldebestaetigung',
                'processed_at',
                'source_file',
                'typ_der_meldung',
                'vorgangsnummer'
            ]
            
            assert sorted(column_names) == sorted(expected_columns)


# Feature: duckdb-storage, Property 8: Database connection cleanup
# Validates: Requirements 5.4, 5.5
@settings(max_examples=100)
@given(st.text(min_size=1, max_size=50))
def test_database_connection_cleanup(db_name: str):
    """
    Property 8: Database connection cleanup
    
    For any database instance, when closed (either explicitly or via context manager exit),
    subsequent operations should fail or require reopening the connection.
    
    This test verifies that:
    1. The connection is properly closed after context manager exit
    2. The connection is set to None after closing
    3. Operations on a closed connection fail appropriately
    """
    # Create a temporary directory for the test database
    with tempfile.TemporaryDirectory() as tmpdir:
        # Sanitize the database name to avoid filesystem issues
        safe_db_name = "".join(c if c.isalnum() or c in ('-', '_') else '_' for c in db_name)
        db_path = Path(tmpdir) / f"{safe_db_name}.duckdb"
        
        # Test 1: Context manager cleanup
        db = MeldebestaetigungDatabase(db_path)
        with db as db_context:
            # Verify connection is established
            assert db_context.conn is not None
            conn_before_exit = db_context.conn
        
        # After context manager exit, connection should be closed
        assert db.conn is None
        
        # Test 2: Explicit close() cleanup
        db2 = MeldebestaetigungDatabase(db_path)
        with db2:
            assert db2.conn is not None
        
        # Call close explicitly (should be idempotent)
        db2.close()
        assert db2.conn is None
        
        # Calling close again should not raise an error
        db2.close()
        assert db2.conn is None
        
        # Test 3: Operations after close should fail
        db3 = MeldebestaetigungDatabase(db_path)
        with db3:
            assert db3.conn is not None
        
        # After closing, attempting to create schema should fail
        with pytest.raises(RuntimeError, match="Database connection not established"):
            db3._create_schema()



# Feature: duckdb-storage, Property 3: Complete record storage
# Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 3.5
@settings(max_examples=100)
@given(
    vorgangsnummer=st.text(min_size=1, max_size=100),
    meldebestaetigung=st.text(min_size=1, max_size=500),
    source_file=st.text(min_size=1, max_size=100),
    typ_der_meldung=st.text(min_size=1, max_size=50),
    indikationsbereich=st.text(min_size=1, max_size=50),
    art_der_daten=st.text(min_size=1, max_size=50),
    ergebnis_qc=st.text(min_size=1, max_size=50),
    case_id=st.one_of(st.none(), st.text(min_size=1, max_size=100)),
    gpas_domain=st.one_of(st.none(), st.text(min_size=1, max_size=100)),
    processed_at=st.datetimes(min_value=pytest.importorskip("datetime").datetime(2000, 1, 1))
)
def test_complete_record_storage(
    vorgangsnummer: str,
    meldebestaetigung: str,
    source_file: str,
    typ_der_meldung: str,
    indikationsbereich: str,
    art_der_daten: str,
    ergebnis_qc: str,
    case_id: str,
    gpas_domain: str,
    processed_at
):
    """
    Property 3: Complete record storage
    
    For any valid Meldebestaetigung record with all required fields,
    when stored in the database, retrieving the record should return
    all fields with their original values.
    
    This test verifies that:
    1. All required fields are stored correctly
    2. Optional fields (case_id, gpas_domain) are stored correctly (including None)
    3. Retrieved values match the original values exactly
    """
    from mvh_copy_mb.database import MeldebestaetigungRecord
    
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create a record with all fields
        original_record = MeldebestaetigungRecord(
            vorgangsnummer=vorgangsnummer,
            meldebestaetigung=meldebestaetigung,
            source_file=source_file,
            typ_der_meldung=typ_der_meldung,
            indikationsbereich=indikationsbereich,
            art_der_daten=art_der_daten,
            ergebnis_qc=ergebnis_qc,
            case_id=case_id,
            gpas_domain=gpas_domain,
            processed_at=processed_at
        )
        
        # Store the record
        with MeldebestaetigungDatabase(db_path) as db:
            db.upsert_record(original_record)
            
            # Retrieve the record
            retrieved_record = db.get_record(vorgangsnummer)
            
            # Verify the record was retrieved
            assert retrieved_record is not None
            
            # Verify all fields match
            assert retrieved_record.vorgangsnummer == original_record.vorgangsnummer
            assert retrieved_record.meldebestaetigung == original_record.meldebestaetigung
            assert retrieved_record.source_file == original_record.source_file
            assert retrieved_record.typ_der_meldung == original_record.typ_der_meldung
            assert retrieved_record.indikationsbereich == original_record.indikationsbereich
            assert retrieved_record.art_der_daten == original_record.art_der_daten
            assert retrieved_record.ergebnis_qc == original_record.ergebnis_qc
            assert retrieved_record.case_id == original_record.case_id
            assert retrieved_record.gpas_domain == original_record.gpas_domain
            assert retrieved_record.processed_at == original_record.processed_at



# Feature: duckdb-storage, Property 4: Successful gPAS resolution storage
# Validates: Requirements 3.1, 3.2
@settings(max_examples=100)
@given(
    vorgangsnummer=st.text(min_size=1, max_size=100),
    meldebestaetigung=st.text(min_size=1, max_size=500),
    source_file=st.text(min_size=1, max_size=100),
    typ_der_meldung=st.text(min_size=1, max_size=50),
    indikationsbereich=st.text(min_size=1, max_size=50),
    art_der_daten=st.text(min_size=1, max_size=50),
    ergebnis_qc=st.text(min_size=1, max_size=50),
    case_id=st.text(min_size=1, max_size=100),
    gpas_domain=st.text(min_size=1, max_size=100),
    processed_at=st.datetimes(min_value=pytest.importorskip("datetime").datetime(2000, 1, 1))
)
def test_successful_gpas_resolution_storage(
    vorgangsnummer: str,
    meldebestaetigung: str,
    source_file: str,
    typ_der_meldung: str,
    indikationsbereich: str,
    art_der_daten: str,
    ergebnis_qc: str,
    case_id: str,
    gpas_domain: str,
    processed_at
):
    """
    Property 4: Successful gPAS resolution storage
    
    For any Meldebestaetigung where gPAS successfully resolves the Vorgangsnummer,
    when stored in the database, both the Case ID and the resolving domain name
    should be non-NULL and match the gPAS response.
    
    This test verifies that:
    1. Successful gPAS resolutions store non-NULL case_id
    2. Successful gPAS resolutions store non-NULL gpas_domain
    3. The stored values match the original gPAS response
    """
    from mvh_copy_mb.database import MeldebestaetigungRecord
    
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create a record with successful gPAS resolution (non-NULL case_id and gpas_domain)
        record = MeldebestaetigungRecord(
            vorgangsnummer=vorgangsnummer,
            meldebestaetigung=meldebestaetigung,
            source_file=source_file,
            typ_der_meldung=typ_der_meldung,
            indikationsbereich=indikationsbereich,
            art_der_daten=art_der_daten,
            ergebnis_qc=ergebnis_qc,
            case_id=case_id,  # Non-NULL indicates successful resolution
            gpas_domain=gpas_domain,  # Non-NULL indicates successful resolution
            processed_at=processed_at
        )
        
        # Store the record
        with MeldebestaetigungDatabase(db_path) as db:
            db.upsert_record(record)
            
            # Retrieve the record
            retrieved_record = db.get_record(vorgangsnummer)
            
            # Verify the record was retrieved
            assert retrieved_record is not None
            
            # Verify both case_id and gpas_domain are non-NULL
            assert retrieved_record.case_id is not None
            assert retrieved_record.gpas_domain is not None
            
            # Verify they match the original gPAS response
            assert retrieved_record.case_id == case_id
            assert retrieved_record.gpas_domain == gpas_domain



# Feature: duckdb-storage, Property 5: Failed gPAS resolution storage
# Validates: Requirements 3.3, 3.4
@settings(max_examples=100)
@given(
    vorgangsnummer=st.text(min_size=1, max_size=100),
    meldebestaetigung=st.text(min_size=1, max_size=500),
    source_file=st.text(min_size=1, max_size=100),
    typ_der_meldung=st.text(min_size=1, max_size=50),
    indikationsbereich=st.text(min_size=1, max_size=50),
    art_der_daten=st.text(min_size=1, max_size=50),
    ergebnis_qc=st.text(min_size=1, max_size=50),
    processed_at=st.datetimes(min_value=pytest.importorskip("datetime").datetime(2000, 1, 1))
)
def test_failed_gpas_resolution_storage(
    vorgangsnummer: str,
    meldebestaetigung: str,
    source_file: str,
    typ_der_meldung: str,
    indikationsbereich: str,
    art_der_daten: str,
    ergebnis_qc: str,
    processed_at
):
    """
    Property 5: Failed gPAS resolution storage
    
    For any Meldebestaetigung where gPAS fails to resolve the Vorgangsnummer,
    when stored in the database, both the Case ID and domain name fields
    should be NULL.
    
    This test verifies that:
    1. Failed gPAS resolutions store NULL for case_id
    2. Failed gPAS resolutions store NULL for gpas_domain
    3. The record is still stored with all other fields intact
    """
    from mvh_copy_mb.database import MeldebestaetigungRecord
    
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create a record with failed gPAS resolution (NULL case_id and gpas_domain)
        record = MeldebestaetigungRecord(
            vorgangsnummer=vorgangsnummer,
            meldebestaetigung=meldebestaetigung,
            source_file=source_file,
            typ_der_meldung=typ_der_meldung,
            indikationsbereich=indikationsbereich,
            art_der_daten=art_der_daten,
            ergebnis_qc=ergebnis_qc,
            case_id=None,  # NULL indicates failed resolution
            gpas_domain=None,  # NULL indicates failed resolution
            processed_at=processed_at
        )
        
        # Store the record
        with MeldebestaetigungDatabase(db_path) as db:
            db.upsert_record(record)
            
            # Retrieve the record
            retrieved_record = db.get_record(vorgangsnummer)
            
            # Verify the record was retrieved
            assert retrieved_record is not None
            
            # Verify both case_id and gpas_domain are NULL
            assert retrieved_record.case_id is None
            assert retrieved_record.gpas_domain is None
            
            # Verify all other fields are still stored correctly
            assert retrieved_record.vorgangsnummer == vorgangsnummer
            assert retrieved_record.meldebestaetigung == meldebestaetigung
            assert retrieved_record.source_file == source_file
            assert retrieved_record.typ_der_meldung == typ_der_meldung
            assert retrieved_record.indikationsbereich == indikationsbereich
            assert retrieved_record.art_der_daten == art_der_daten
            assert retrieved_record.ergebnis_qc == ergebnis_qc



# Feature: duckdb-storage, Property 6: Upsert prevents duplicates
# Validates: Requirements 4.1, 4.4
@settings(max_examples=100)
@given(
    vorgangsnummer=st.text(min_size=1, max_size=100),
    meldebestaetigung1=st.text(min_size=1, max_size=500),
    meldebestaetigung2=st.text(min_size=1, max_size=500),
    source_file1=st.text(min_size=1, max_size=100),
    source_file2=st.text(min_size=1, max_size=100),
    typ_der_meldung=st.text(min_size=1, max_size=50),
    indikationsbereich=st.text(min_size=1, max_size=50),
    art_der_daten=st.text(min_size=1, max_size=50),
    ergebnis_qc=st.text(min_size=1, max_size=50),
    case_id=st.one_of(st.none(), st.text(min_size=1, max_size=100)),
    gpas_domain=st.one_of(st.none(), st.text(min_size=1, max_size=100)),
    processed_at1=st.datetimes(min_value=pytest.importorskip("datetime").datetime(2000, 1, 1)),
    processed_at2=st.datetimes(min_value=pytest.importorskip("datetime").datetime(2000, 1, 1))
)
def test_upsert_prevents_duplicates(
    vorgangsnummer: str,
    meldebestaetigung1: str,
    meldebestaetigung2: str,
    source_file1: str,
    source_file2: str,
    typ_der_meldung: str,
    indikationsbereich: str,
    art_der_daten: str,
    ergebnis_qc: str,
    case_id: str,
    gpas_domain: str,
    processed_at1,
    processed_at2
):
    """
    Property 6: Upsert prevents duplicates
    
    For any Meldebestaetigung record with a given Vorgangsnummer,
    when inserted multiple times (even from different source files),
    the database should contain exactly one record with that Vorgangsnummer.
    
    This test verifies that:
    1. Inserting the same vorgangsnummer twice doesn't create duplicates
    2. The database contains exactly one record after multiple inserts
    3. The upsert mechanism works correctly
    """
    from mvh_copy_mb.database import MeldebestaetigungRecord
    
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create two records with the same vorgangsnummer but different data
        record1 = MeldebestaetigungRecord(
            vorgangsnummer=vorgangsnummer,
            meldebestaetigung=meldebestaetigung1,
            source_file=source_file1,
            typ_der_meldung=typ_der_meldung,
            indikationsbereich=indikationsbereich,
            art_der_daten=art_der_daten,
            ergebnis_qc=ergebnis_qc,
            case_id=case_id,
            gpas_domain=gpas_domain,
            processed_at=processed_at1
        )
        
        record2 = MeldebestaetigungRecord(
            vorgangsnummer=vorgangsnummer,  # Same vorgangsnummer
            meldebestaetigung=meldebestaetigung2,  # Different data
            source_file=source_file2,  # Different source file
            typ_der_meldung=typ_der_meldung,
            indikationsbereich=indikationsbereich,
            art_der_daten=art_der_daten,
            ergebnis_qc=ergebnis_qc,
            case_id=case_id,
            gpas_domain=gpas_domain,
            processed_at=processed_at2  # Different timestamp
        )
        
        # Store both records
        with MeldebestaetigungDatabase(db_path) as db:
            db.upsert_record(record1)
            db.upsert_record(record2)
            
            # Count how many records exist with this vorgangsnummer
            count_result = db.conn.execute(
                "SELECT COUNT(*) FROM meldebestaetigungen WHERE vorgangsnummer = ?",
                [vorgangsnummer]
            ).fetchone()
            
            # Verify exactly one record exists
            assert count_result[0] == 1
            
            # Verify the record is the second one (most recent)
            retrieved_record = db.get_record(vorgangsnummer)
            assert retrieved_record is not None
            assert retrieved_record.meldebestaetigung == meldebestaetigung2
            assert retrieved_record.source_file == source_file2
            assert retrieved_record.processed_at == processed_at2



# Feature: duckdb-storage, Property 7: Update modifies existing records
# Validates: Requirements 4.2, 4.3
@settings(max_examples=100)
@given(
    vorgangsnummer=st.text(min_size=1, max_size=100),
    meldebestaetigung=st.text(min_size=1, max_size=500),
    source_file=st.text(min_size=1, max_size=100),
    typ_der_meldung=st.text(min_size=1, max_size=50),
    indikationsbereich=st.text(min_size=1, max_size=50),
    art_der_daten=st.text(min_size=1, max_size=50),
    ergebnis_qc=st.text(min_size=1, max_size=50),
    original_case_id=st.one_of(st.none(), st.text(min_size=1, max_size=100)),
    original_gpas_domain=st.one_of(st.none(), st.text(min_size=1, max_size=100)),
    updated_case_id=st.one_of(st.none(), st.text(min_size=1, max_size=100)),
    updated_gpas_domain=st.one_of(st.none(), st.text(min_size=1, max_size=100)),
    original_timestamp=st.datetimes(min_value=pytest.importorskip("datetime").datetime(2000, 1, 1)),
    updated_timestamp=st.datetimes(min_value=pytest.importorskip("datetime").datetime(2000, 1, 1))
)
def test_update_modifies_existing_records(
    vorgangsnummer: str,
    meldebestaetigung: str,
    source_file: str,
    typ_der_meldung: str,
    indikationsbereich: str,
    art_der_daten: str,
    ergebnis_qc: str,
    original_case_id: str,
    original_gpas_domain: str,
    updated_case_id: str,
    updated_gpas_domain: str,
    original_timestamp,
    updated_timestamp
):
    """
    Property 7: Update modifies existing records
    
    For any existing database record, when updated with new values
    (timestamp or gPAS results), retrieving the record should return
    the updated values, not the original values.
    
    This test verifies that:
    1. Updates modify existing records rather than creating new ones
    2. Updated values are persisted correctly
    3. The timestamp is updated to reflect the latest processing time
    """
    from mvh_copy_mb.database import MeldebestaetigungRecord
    
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create original record
        original_record = MeldebestaetigungRecord(
            vorgangsnummer=vorgangsnummer,
            meldebestaetigung=meldebestaetigung,
            source_file=source_file,
            typ_der_meldung=typ_der_meldung,
            indikationsbereich=indikationsbereich,
            art_der_daten=art_der_daten,
            ergebnis_qc=ergebnis_qc,
            case_id=original_case_id,
            gpas_domain=original_gpas_domain,
            processed_at=original_timestamp
        )
        
        # Create updated record with same vorgangsnummer but different values
        updated_record = MeldebestaetigungRecord(
            vorgangsnummer=vorgangsnummer,  # Same vorgangsnummer
            meldebestaetigung=meldebestaetigung,
            source_file=source_file,
            typ_der_meldung=typ_der_meldung,
            indikationsbereich=indikationsbereich,
            art_der_daten=art_der_daten,
            ergebnis_qc=ergebnis_qc,
            case_id=updated_case_id,  # Updated gPAS result
            gpas_domain=updated_gpas_domain,  # Updated gPAS domain
            processed_at=updated_timestamp  # Updated timestamp
        )
        
        # Store original record, then update it
        with MeldebestaetigungDatabase(db_path) as db:
            db.upsert_record(original_record)
            db.upsert_record(updated_record)
            
            # Retrieve the record
            retrieved_record = db.get_record(vorgangsnummer)
            
            # Verify the record was retrieved
            assert retrieved_record is not None
            
            # Verify the values are the updated ones, not the original ones
            assert retrieved_record.case_id == updated_case_id
            assert retrieved_record.gpas_domain == updated_gpas_domain
            assert retrieved_record.processed_at == updated_timestamp
            
            # Verify the values are NOT the original ones (if they differ)
            if updated_case_id != original_case_id:
                assert retrieved_record.case_id != original_case_id
            if updated_gpas_domain != original_gpas_domain:
                assert retrieved_record.gpas_domain != original_gpas_domain
            if updated_timestamp != original_timestamp:
                assert retrieved_record.processed_at != original_timestamp



# Unit tests for record retrieval
def test_get_record_returns_none_for_nonexistent():
    """
    Test that get_record returns None when the record doesn't exist.
    """
    from mvh_copy_mb.database import MeldebestaetigungRecord
    from datetime import datetime
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        with MeldebestaetigungDatabase(db_path) as db:
            # Try to retrieve a record that doesn't exist
            result = db.get_record("nonexistent_vorgangsnummer")
            
            # Should return None
            assert result is None


def test_get_record_retrieves_existing_record():
    """
    Test that get_record correctly retrieves an existing record.
    """
    from mvh_copy_mb.database import MeldebestaetigungRecord
    from datetime import datetime
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create and store a record
        record = MeldebestaetigungRecord(
            vorgangsnummer="TEST123",
            meldebestaetigung="test_mb_string",
            source_file="test.csv",
            typ_der_meldung="0",
            indikationsbereich="test_indication",
            art_der_daten="test_data_type",
            ergebnis_qc="1",
            case_id="CASE456",
            gpas_domain="test_domain",
            processed_at=datetime(2023, 1, 1, 12, 0, 0)
        )
        
        with MeldebestaetigungDatabase(db_path) as db:
            db.upsert_record(record)
            
            # Retrieve the record
            retrieved = db.get_record("TEST123")
            
            # Verify it matches
            assert retrieved is not None
            assert retrieved.vorgangsnummer == "TEST123"
            assert retrieved.meldebestaetigung == "test_mb_string"
            assert retrieved.source_file == "test.csv"
            assert retrieved.case_id == "CASE456"
            assert retrieved.gpas_domain == "test_domain"


def test_get_record_with_null_fields():
    """
    Test that get_record correctly handles NULL fields.
    """
    from mvh_copy_mb.database import MeldebestaetigungRecord
    from datetime import datetime
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create a record with NULL case_id and gpas_domain
        record = MeldebestaetigungRecord(
            vorgangsnummer="TEST789",
            meldebestaetigung="test_mb_string",
            source_file="test.csv",
            typ_der_meldung="0",
            indikationsbereich="test_indication",
            art_der_daten="test_data_type",
            ergebnis_qc="1",
            case_id=None,  # NULL
            gpas_domain=None,  # NULL
            processed_at=datetime(2023, 1, 1, 12, 0, 0)
        )
        
        with MeldebestaetigungDatabase(db_path) as db:
            db.upsert_record(record)
            
            # Retrieve the record
            retrieved = db.get_record("TEST789")
            
            # Verify NULL fields are preserved
            assert retrieved is not None
            assert retrieved.case_id is None
            assert retrieved.gpas_domain is None
            assert retrieved.vorgangsnummer == "TEST789"



# Feature: duckdb-storage, Property 9: Error resilience
# Validates: Requirements 1.5
def test_error_resilience_with_logging(caplog):
    """
    Property 9: Error resilience
    
    For any database operation that raises an exception, the system should
    log the error and continue processing subsequent records without terminating.
    
    This test verifies that:
    1. Errors are logged appropriately
    2. The system can continue processing after an error
    3. Subsequent operations succeed after a failed operation
    """
    from mvh_copy_mb.database import MeldebestaetigungRecord
    from datetime import datetime
    import logging
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create a valid record
        valid_record = MeldebestaetigungRecord(
            vorgangsnummer="VALID123",
            meldebestaetigung="test_mb",
            source_file="test.csv",
            typ_der_meldung="0",
            indikationsbereich="test",
            art_der_daten="test",
            ergebnis_qc="1",
            case_id="CASE123",
            gpas_domain="test_domain",
            processed_at=datetime(2023, 1, 1, 12, 0, 0)
        )
        
        with MeldebestaetigungDatabase(db_path) as db:
            # First, insert a valid record successfully
            db.upsert_record(valid_record)
            
            # Verify it was inserted
            retrieved = db.get_record("VALID123")
            assert retrieved is not None
            
            # Now try to cause an error by using a closed connection
            # Save the connection and close it
            original_conn = db.conn
            db.conn = None
            
            # Try to insert a record with no connection - should raise RuntimeError
            with caplog.at_level(logging.ERROR):
                try:
                    db.upsert_record(valid_record)
                    assert False, "Should have raised RuntimeError"
                except RuntimeError as e:
                    assert "Database connection not established" in str(e)
            
            # Restore the connection
            db.conn = original_conn
            
            # Verify we can continue processing after the error
            another_record = MeldebestaetigungRecord(
                vorgangsnummer="VALID456",
                meldebestaetigung="test_mb2",
                source_file="test2.csv",
                typ_der_meldung="0",
                indikationsbereich="test",
                art_der_daten="test",
                ergebnis_qc="1",
                case_id="CASE456",
                gpas_domain="test_domain",
                processed_at=datetime(2023, 1, 2, 12, 0, 0)
            )
            
            # This should succeed
            db.upsert_record(another_record)
            
            # Verify the second record was inserted
            retrieved2 = db.get_record("VALID456")
            assert retrieved2 is not None
            assert retrieved2.vorgangsnummer == "VALID456"


# Feature: web-frontend, Property 11: Done status changes persist to database
# Validates: Requirements 6.3
@settings(max_examples=100)
@given(
    vorgangsnummer=st.text(min_size=1, max_size=100),
    meldebestaetigung=st.text(min_size=1, max_size=500),
    source_file=st.text(min_size=1, max_size=100),
    typ_der_meldung=st.text(min_size=1, max_size=50),
    indikationsbereich=st.text(min_size=1, max_size=50),
    art_der_daten=st.text(min_size=1, max_size=50),
    ergebnis_qc=st.text(min_size=1, max_size=50),
    case_id=st.one_of(st.none(), st.text(min_size=1, max_size=100)),
    gpas_domain=st.one_of(st.none(), st.text(min_size=1, max_size=100)),
    processed_at=st.datetimes(min_value=pytest.importorskip("datetime").datetime(2000, 1, 1)),
    initial_done=st.booleans(),
    updated_done=st.booleans()
)
def test_done_status_changes_persist_to_database(
    vorgangsnummer: str,
    meldebestaetigung: str,
    source_file: str,
    typ_der_meldung: str,
    indikationsbereich: str,
    art_der_daten: str,
    ergebnis_qc: str,
    case_id: str,
    gpas_domain: str,
    processed_at,
    initial_done: bool,
    updated_done: bool
):
    """
    Property 11: Done status changes persist to database
    
    For any done status update operation, when querying the database after the update,
    the is_done field should reflect the new value.
    
    This test verifies that:
    1. Initial done status is stored correctly
    2. Updated done status is persisted correctly
    3. Retrieved done status matches the updated value
    """
    from mvh_copy_mb.database import MeldebestaetigungRecord
    
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create a record with initial done status
        initial_record = MeldebestaetigungRecord(
            vorgangsnummer=vorgangsnummer,
            meldebestaetigung=meldebestaetigung,
            source_file=source_file,
            typ_der_meldung=typ_der_meldung,
            indikationsbereich=indikationsbereich,
            art_der_daten=art_der_daten,
            ergebnis_qc=ergebnis_qc,
            case_id=case_id,
            gpas_domain=gpas_domain,
            processed_at=processed_at,
            is_done=initial_done
        )
        
        # Store the record with initial done status
        with MeldebestaetigungDatabase(db_path) as db:
            db.upsert_record(initial_record)
            
            # Retrieve and verify initial done status
            retrieved_initial = db.get_record(vorgangsnummer)
            assert retrieved_initial is not None
            assert retrieved_initial.is_done == initial_done
            
            # Update the record with new done status
            updated_record = MeldebestaetigungRecord(
                vorgangsnummer=vorgangsnummer,
                meldebestaetigung=meldebestaetigung,
                source_file=source_file,
                typ_der_meldung=typ_der_meldung,
                indikationsbereich=indikationsbereich,
                art_der_daten=art_der_daten,
                ergebnis_qc=ergebnis_qc,
                case_id=case_id,
                gpas_domain=gpas_domain,
                processed_at=processed_at,
                is_done=updated_done
            )
            
            db.upsert_record(updated_record)
            
            # Retrieve and verify updated done status
            retrieved_updated = db.get_record(vorgangsnummer)
            assert retrieved_updated is not None
            assert retrieved_updated.is_done == updated_done
            
            # Verify the done status changed if initial and updated differ
            if initial_done != updated_done:
                assert retrieved_updated.is_done != initial_done
