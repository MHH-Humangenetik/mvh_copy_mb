"""
Unit tests for web API endpoints.

This module contains unit tests for FastAPI endpoints, focusing on
the done status update endpoint and error handling.
"""

import tempfile
from datetime import datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from mvh_copy_mb.database import MeldebestaetigungDatabase, MeldebestaetigungRecord
from mvh_copy_mb.web import app


@pytest.fixture
def test_db():
    """Create a temporary test database with sample data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create sample data
        with MeldebestaetigungDatabase(db_path) as db:
            # Complete pair (both genomic and clinical)
            genomic_complete = MeldebestaetigungRecord(
                vorgangsnummer="VN_G_COMPLETE",
                meldebestaetigung="mb_genomic_complete",
                source_file="source_complete.csv",
                typ_der_meldung="0",
                indikationsbereich="test",
                art_der_daten="genomic",
                ergebnis_qc="1",
                case_id="CASE_COMPLETE",
                gpas_domain="test_domain",
                processed_at=datetime(2023, 1, 1, 12, 0, 0),
                is_done=False
            )
            db.upsert_record(genomic_complete)
            
            clinical_complete = MeldebestaetigungRecord(
                vorgangsnummer="VN_C_COMPLETE",
                meldebestaetigung="mb_clinical_complete",
                source_file="source_complete.csv",
                typ_der_meldung="0",
                indikationsbereich="test",
                art_der_daten="clinical",
                ergebnis_qc="1",
                case_id="CASE_COMPLETE",
                gpas_domain="test_domain",
                processed_at=datetime(2023, 1, 1, 12, 0, 0),
                is_done=False
            )
            db.upsert_record(clinical_complete)
            
            # Incomplete pair (only genomic)
            genomic_incomplete = MeldebestaetigungRecord(
                vorgangsnummer="VN_G_INCOMPLETE",
                meldebestaetigung="mb_genomic_incomplete",
                source_file="source_incomplete.csv",
                typ_der_meldung="0",
                indikationsbereich="test",
                art_der_daten="genomic",
                ergebnis_qc="1",
                case_id="CASE_INCOMPLETE",
                gpas_domain="test_domain",
                processed_at=datetime(2023, 1, 1, 12, 0, 0),
                is_done=False
            )
            db.upsert_record(genomic_incomplete)
        
        yield db_path


def test_update_done_status_success(test_db, monkeypatch):
    """
    Test successful update of done status for a complete pair.
    
    Validates: Requirements 4.2, 4.3, 6.3
    """
    # Set the database path in environment
    monkeypatch.setenv('DB_PATH', str(test_db))
    
    # Create test client
    client = TestClient(app)
    
    # Update done status to True
    response = client.post(
        "/api/done/CASE_COMPLETE",
        data={"done": "true"}
    )
    
    # Should return 200 OK
    assert response.status_code == 200
    
    # Verify the database was updated
    with MeldebestaetigungDatabase(test_db) as db:
        genomic = db.get_record("VN_G_COMPLETE")
        clinical = db.get_record("VN_C_COMPLETE")
        
        assert genomic is not None
        assert clinical is not None
        assert genomic.is_done is True
        assert clinical.is_done is True


def test_update_done_status_incomplete_pair(test_db, monkeypatch):
    """
    Test rejection of done status update for incomplete pair.
    
    Validates: Requirements 4.2, 4.3
    """
    # Set the database path in environment
    monkeypatch.setenv('DB_PATH', str(test_db))
    
    # Create test client
    client = TestClient(app)
    
    # Attempt to update done status for incomplete pair
    response = client.post(
        "/api/done/CASE_INCOMPLETE",
        data={"done": "true"}
    )
    
    # Should return 400 Bad Request
    assert response.status_code == 400
    assert "incomplete" in response.json()["detail"].lower()


def test_update_done_status_nonexistent_case_id(test_db, monkeypatch):
    """
    Test 404 error for non-existent Case ID.
    
    Validates: Requirements 4.2, 4.3
    """
    # Set the database path in environment
    monkeypatch.setenv('DB_PATH', str(test_db))
    
    # Create test client
    client = TestClient(app)
    
    # Attempt to update done status for non-existent case
    response = client.post(
        "/api/done/CASE_NONEXISTENT",
        data={"done": "true"}
    )
    
    # Should return 404 Not Found
    assert response.status_code == 404


def test_update_done_status_database_unavailable(monkeypatch):
    """
    Test error handling when database is unavailable.
    
    Validates: Requirements 6.5
    """
    # Set a non-existent database path
    monkeypatch.setenv('DB_PATH', '/nonexistent/path/to/database.duckdb')
    
    # Create test client
    client = TestClient(app)
    
    # Attempt to update done status
    response = client.post(
        "/api/done/CASE_COMPLETE",
        data={"done": "true"}
    )
    
    # Should return 500 Internal Server Error
    assert response.status_code == 500
    assert "not found" in response.json()["detail"].lower()


def test_update_done_status_toggle(test_db, monkeypatch):
    """
    Test toggling done status from False to True and back to False.
    
    Validates: Requirements 4.3, 6.3
    """
    # Set the database path in environment
    monkeypatch.setenv('DB_PATH', str(test_db))
    
    # Create test client
    client = TestClient(app)
    
    # First update: set to True
    response1 = client.post(
        "/api/done/CASE_COMPLETE",
        data={"done": "true"}
    )
    assert response1.status_code == 200
    
    # Verify it's True
    with MeldebestaetigungDatabase(test_db) as db:
        genomic = db.get_record("VN_G_COMPLETE")
        assert genomic.is_done is True
    
    # Second update: set to False
    response2 = client.post(
        "/api/done/CASE_COMPLETE",
        data={"done": "false"}
    )
    assert response2.status_code == 200
    
    # Verify it's False
    with MeldebestaetigungDatabase(test_db) as db:
        genomic = db.get_record("VN_G_COMPLETE")
        clinical = db.get_record("VN_C_COMPLETE")
        assert genomic.is_done is False
        assert clinical.is_done is False
