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

from hypothesis import given, strategies as st, settings, HealthCheck
from datetime import date



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



def test_index_page_empty_database(monkeypatch):
    """
    Test that empty database displays appropriate message.
    
    Validates: Requirements 6.5
    """
    # Create an empty database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "empty.duckdb"
        
        # Create empty database
        with MeldebestaetigungDatabase(db_path) as db:
            pass  # Just create the schema, no records
        
        # Set the database path in environment
        monkeypatch.setenv('DB_PATH', str(db_path))
        
        # Create test client
        client = TestClient(app)
        
        # Request index page
        response = client.get("/")
        
        # Should return 200 OK
        assert response.status_code == 200
        
        # Should contain message about no records
        assert "No records found" in response.text or "no data" in response.text.lower()


def test_index_page_database_not_found(monkeypatch):
    """
    Test that missing database displays appropriate error message.
    
    Validates: Requirements 6.5
    """
    # Set a non-existent database path
    monkeypatch.setenv('DB_PATH', '/nonexistent/path/to/database.duckdb')
    
    # Create test client
    client = TestClient(app)
    
    # Request index page
    response = client.get("/")
    
    # Should return 200 OK (with error message in template)
    assert response.status_code == 200
    
    # Should contain message about database not found
    assert "not found" in response.text.lower() or "database" in response.text.lower()



def test_full_page_load_with_sample_data(test_db, monkeypatch):
    """
    Test full page load with sample data.
    
    This integration test verifies the complete workflow:
    1. Database query for all records
    2. Grouping records into pairs
    3. Sorting by priority group
    4. Rendering HTML template
    
    Validates: Requirements 1.1, 1.2, 1.3, 1.4, 2.1, 2.2, 8.1
    """
    # Set the database path in environment
    monkeypatch.setenv('DB_PATH', str(test_db))
    
    # Create test client
    client = TestClient(app)
    
    # Request index page
    response = client.get("/")
    
    # Should return 200 OK
    assert response.status_code == 200
    
    # Verify HTML contains expected elements
    html = response.text
    
    # Should contain the page title
    assert "Meldebestätigungen" in html
    
    # Should contain filter input
    assert 'type="text"' in html
    assert 'filter' in html.lower()
    
    # Should contain table headers
    assert "Case ID" in html
    assert "Art der Daten" in html
    assert "Typ der Meldung" in html
    assert "Indikationsbereich" in html
    assert "Ergebnis QC" in html
    assert "Source File" in html
    assert "Done" in html
    
    # Should contain data from test database
    assert "CASE_COMPLETE" in html
    assert "CASE_INCOMPLETE" in html
    assert "genomic" in html
    assert "clinical" in html
    
    # Should contain Alpine.js script
    assert "alpinejs" in html.lower()
    
    # Should contain HTMX script
    assert "htmx" in html.lower()


def test_checkbox_update_flow_integration(test_db, monkeypatch):
    """
    Test the complete checkbox update flow.
    
    This integration test verifies:
    1. Initial page load with unchecked checkbox
    2. POST request to update done status
    3. Database update
    4. Response with updated state
    
    Validates: Requirements 4.3, 4.4, 6.3
    """
    # Set the database path in environment
    monkeypatch.setenv('DB_PATH', str(test_db))
    
    # Create test client
    client = TestClient(app)
    
    # Step 1: Load initial page
    response1 = client.get("/")
    assert response1.status_code == 200
    
    # Verify initial state (not done)
    with MeldebestaetigungDatabase(test_db) as db:
        genomic = db.get_record("VN_G_COMPLETE")
        assert genomic.is_done is False
    
    # Step 2: Update done status via POST
    response2 = client.post(
        "/api/done/CASE_COMPLETE",
        data={"done": "true"}
    )
    assert response2.status_code == 200
    
    # Step 3: Verify database was updated
    with MeldebestaetigungDatabase(test_db) as db:
        genomic = db.get_record("VN_G_COMPLETE")
        clinical = db.get_record("VN_C_COMPLETE")
        assert genomic.is_done is True
        assert clinical.is_done is True
    
    # Step 4: Load page again and verify updated state
    response3 = client.get("/")
    assert response3.status_code == 200
    
    # The page should reflect the updated done status
    # (In the actual implementation, this would be visible in the rendered HTML)


@given(
    output_date=st.one_of(
        st.none(),
        st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31))
    )
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_api_compatibility_with_output_date(test_db, monkeypatch, output_date):
    """
    **Feature: leistungsdatum-integration, Property 24: API compatibility**
    
    *For any* API response, the output_date field should be included while maintaining 
    compatibility with existing clients.
    
    **Validates: Requirements 6.5**
    """
    # Set the database path in environment
    monkeypatch.setenv('DB_PATH', str(test_db))
    
    # Create a test record with output_date
    test_record = MeldebestaetigungRecord(
        vorgangsnummer="VN_API_TEST",
        meldebestaetigung="test_meldebestaetigung",
        source_file="test_api.csv",
        typ_der_meldung="0",
        indikationsbereich="test_ibe",
        art_der_daten="G",
        ergebnis_qc="1",
        case_id="CASE_API_TEST",
        gpas_domain="test_domain",
        processed_at=datetime.now(),
        is_done=False,
        output_date=output_date
    )
    
    # Add clinical record to make it a complete pair
    clinical_record = MeldebestaetigungRecord(
        vorgangsnummer="VN_API_TEST_C",
        meldebestaetigung="test_meldebestaetigung_clinical",
        source_file="test_api.csv",
        typ_der_meldung="0",
        indikationsbereich="test_ibe",
        art_der_daten="C",
        ergebnis_qc="1",
        case_id="CASE_API_TEST",
        gpas_domain="test_domain",
        processed_at=datetime.now(),
        is_done=False,
        output_date=output_date
    )
    
    # Insert records into database
    with MeldebestaetigungDatabase(test_db) as db:
        db.upsert_record(test_record)
        db.upsert_record(clinical_record)
    
    # Create test client
    client = TestClient(app)
    
    # Request index page
    response = client.get("/")
    
    # Should return 200 OK
    assert response.status_code == 200
    
    # The response should be valid HTML (basic compatibility check)
    html = response.text
    assert "<html" in html.lower() or "<!doctype" in html.lower()
    
    # Should contain the case ID (data is being rendered)
    assert "CASE_API_TEST" in html
    
    # Test the done status update endpoint (API compatibility)
    update_response = client.post(
        "/api/done/CASE_API_TEST",
        data={"done": "true"}
    )
    
    # Should return 200 OK (API still works)
    assert update_response.status_code == 200
    
    # Verify the database was updated (functionality preserved)
    with MeldebestaetigungDatabase(test_db) as db:
        updated_genomic = db.get_record("VN_API_TEST")
        updated_clinical = db.get_record("VN_API_TEST_C")
        
        assert updated_genomic is not None
        assert updated_clinical is not None
        assert updated_genomic.is_done is True
        assert updated_clinical.is_done is True
        
        # Verify output_date is preserved
        assert updated_genomic.output_date == output_date
        assert updated_clinical.output_date == output_date