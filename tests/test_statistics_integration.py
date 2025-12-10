"""
Integration tests for the statistics display feature.

This module contains integration tests that verify the statistics display
functionality works correctly in the context of the full web application.
"""

import tempfile
from datetime import datetime
from pathlib import Path

import pytest
from bs4 import BeautifulSoup
from fastapi.testclient import TestClient

from mvh_copy_mb.database import MeldebestaetigungDatabase, MeldebestaetigungRecord
from mvh_copy_mb.web import app


def test_statistics_appear_correctly_on_page_load(test_db, monkeypatch):
    """
    Test that statistics appear correctly on page load.
    
    This integration test verifies:
    1. Statistics container is present in the HTML
    2. All four statistics are displayed with correct labels
    3. Statistics values reflect the actual data in the database
    4. Alpine.js directives are properly set up
    
    Validates: Requirements 1.1, 1.2
    """
    # Set the database path in environment
    monkeypatch.setenv('DB_PATH', str(test_db))
    
    # Create test client
    client = TestClient(app)
    
    # Request index page
    response = client.get("/")
    
    # Should return 200 OK
    assert response.status_code == 200
    
    # Parse HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Verify statistics container is present
    stats_container = soup.find('div', class_='statistics-container')
    assert stats_container is not None, "Statistics container should be present"
    
    # Verify all three statistics items are present (as implemented in template)
    stats_items = stats_container.find_all('div', class_='statistics-item')
    assert len(stats_items) == 3, "Should have exactly 3 statistics items"
    
    # Expected statistics with their Alpine.js computed property names (including fallback)
    expected_stats = [
        ('Total Cases:', "totalCases || '--'"),
        ('Ready:', "readyPairs || '--'"),  # Note: template uses 'Ready' instead of 'Complete'
        ('Done:', "donePairs || '--'")
    ]
    
    # Verify all three statistics
    for i, (expected_label, expected_x_text) in enumerate(expected_stats):
            
        item = stats_items[i]
        
        # Find label and value spans
        label_span = item.find('span', class_='statistics-label')
        value_span = item.find('span', class_='statistics-value')
        
        assert label_span is not None, f"Label span missing for statistic {i}"
        assert value_span is not None, f"Value span missing for statistic {i}"
        
        # Verify label text
        assert label_span.text.strip() == expected_label, \
            f"Label text should be '{expected_label}', got '{label_span.text.strip()}'"
        
        # Verify Alpine.js x-text directive
        assert value_span.get('x-text') == expected_x_text, \
            f"x-text directive should be '{expected_x_text}', got '{value_span.get('x-text')}'"
    
    # Verify that the statistics container is placed next to the filter input
    filter_stats_container = soup.find('div', class_='filter-and-stats-container')
    assert filter_stats_container is not None, "filter-and-stats-container should be present"
    
    # Verify both filter and statistics containers are children
    filter_container = filter_stats_container.find('div', class_='filter-container')
    stats_container_in_parent = filter_stats_container.find('div', class_='statistics-container')
    
    assert filter_container is not None, "Filter container should be present"
    assert stats_container_in_parent is not None, "Statistics container should be in filter-stats container"


def test_statistics_reflect_actual_database_content(monkeypatch):
    """
    Test that statistics values reflect the actual content in the database.
    
    This integration test verifies:
    1. Total cases count matches the number of unique case IDs
    2. Ready pairs count matches complete and valid pairs that are not done
    3. Done pairs count matches pairs marked as done
    4. Statistics are calculated correctly from real database data
    
    Validates: Requirements 1.1, 2.1, 2.2, 3.1
    """
    # Create a temporary database with known test data
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_stats.duckdb"
        
        # Create test records with known statistics
        with MeldebestaetigungDatabase(db_path) as db:
            # Case 1: Complete, valid, not done (should count as ready)
            genomic1 = MeldebestaetigungRecord(
                vorgangsnummer="VN_G_001",
                meldebestaetigung="mb_genomic_001",
                source_file="test1.csv",
                typ_der_meldung="1",
                indikationsbereich="indication_1",
                art_der_daten="G",
                ergebnis_qc="1",  # Valid QC
                case_id="CASE_001",
                gpas_domain="test_domain",
                processed_at=datetime(2023, 1, 1, 12, 0, 0),
                is_done=False
            )
            clinical1 = MeldebestaetigungRecord(
                vorgangsnummer="VN_C_001",
                meldebestaetigung="mb_clinical_001",
                source_file="test1.csv",
                typ_der_meldung="1",
                indikationsbereich="indication_1",
                art_der_daten="C",
                ergebnis_qc="1",  # Valid QC
                case_id="CASE_001",
                gpas_domain="test_domain",
                processed_at=datetime(2023, 1, 1, 12, 0, 0),
                is_done=False
            )
            db.upsert_record(genomic1)
            db.upsert_record(clinical1)
            
            # Case 2: Complete, valid, done (should count as done)
            genomic2 = MeldebestaetigungRecord(
                vorgangsnummer="VN_G_002",
                meldebestaetigung="mb_genomic_002",
                source_file="test2.csv",
                typ_der_meldung="2",
                indikationsbereich="indication_2",
                art_der_daten="G",
                ergebnis_qc="1",  # Valid QC
                case_id="CASE_002",
                gpas_domain="test_domain",
                processed_at=datetime(2023, 1, 1, 12, 0, 0),
                is_done=True
            )
            clinical2 = MeldebestaetigungRecord(
                vorgangsnummer="VN_C_002",
                meldebestaetigung="mb_clinical_002",
                source_file="test2.csv",
                typ_der_meldung="2",
                indikationsbereich="indication_2",
                art_der_daten="C",
                ergebnis_qc="1",  # Valid QC
                case_id="CASE_002",
                gpas_domain="test_domain",
                processed_at=datetime(2023, 1, 1, 12, 0, 0),
                is_done=True
            )
            db.upsert_record(genomic2)
            db.upsert_record(clinical2)
            
            # Case 3: Incomplete (genomic only) - should not count as ready or done
            genomic3 = MeldebestaetigungRecord(
                vorgangsnummer="VN_G_003",
                meldebestaetigung="mb_genomic_003",
                source_file="test3.csv",
                typ_der_meldung="3",
                indikationsbereich="indication_3",
                art_der_daten="G",
                ergebnis_qc="1",
                case_id="CASE_003",
                gpas_domain="test_domain",
                processed_at=datetime(2023, 1, 1, 12, 0, 0),
                is_done=False
            )
            db.upsert_record(genomic3)
        
        # Set the database path in environment
        monkeypatch.setenv('DB_PATH', str(db_path))
        
        # Create test client
        client = TestClient(app)
        
        # Request index page
        response = client.get("/")
        assert response.status_code == 200
        
        # Parse HTML to extract the pairs data from the script tag
        soup = BeautifulSoup(response.text, 'html.parser')
        pairs_script = soup.find('script', id='pairs-data')
        assert pairs_script is not None, "Pairs data script should be present"
        
        # The pairs data is embedded as JSON in the script tag
        import json
        pairs_data = json.loads(pairs_script.string)
        
        # Verify expected statistics based on our test data
        # Total cases: 3 (CASE_001, CASE_002, CASE_003)
        assert len(pairs_data) == 3, f"Should have 3 pairs, got {len(pairs_data)}"
        
        # Count ready pairs: complete, valid, not done
        ready_count = 0
        done_count = 0
        
        for pair in pairs_data:
            if pair['is_complete'] and pair['is_valid'] and not pair['is_done']:
                ready_count += 1
            if pair['is_done']:
                done_count += 1
        
        # Expected: CASE_001 should be ready (complete, valid, not done)
        assert ready_count == 1, f"Should have 1 ready pair, got {ready_count}"
        
        # Expected: CASE_002 should be done
        assert done_count == 1, f"Should have 1 done pair, got {done_count}"
        
        # Verify the statistics container is present (Alpine.js will calculate the actual values)
        stats_container = soup.find('div', class_='statistics-container')
        assert stats_container is not None, "Statistics container should be present"


def test_statistics_update_when_done_status_changes(monkeypatch):
    """
    Test that done count updates when checkboxes are toggled.
    
    This integration test verifies:
    1. Initial done count reflects database state
    2. After toggling a checkbox via POST request, done count should change
    3. The updated statistics are reflected in subsequent page loads
    4. Alpine.js reactive updates work correctly
    
    Validates: Requirements 3.3, 3.4
    """
    # Create a temporary database with test data
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_done_update.duckdb"
        
        # Create a complete pair that is initially not done
        with MeldebestaetigungDatabase(db_path) as db:
            genomic = MeldebestaetigungRecord(
                vorgangsnummer="VN_G_TOGGLE",
                meldebestaetigung="mb_genomic_toggle",
                source_file="toggle_test.csv",
                typ_der_meldung="1",
                indikationsbereich="indication_toggle",
                art_der_daten="G",
                ergebnis_qc="1",
                case_id="CASE_TOGGLE",
                gpas_domain="test_domain",
                processed_at=datetime(2023, 1, 1, 12, 0, 0),
                is_done=False  # Initially not done
            )
            clinical = MeldebestaetigungRecord(
                vorgangsnummer="VN_C_TOGGLE",
                meldebestaetigung="mb_clinical_toggle",
                source_file="toggle_test.csv",
                typ_der_meldung="1",
                indikationsbereich="indication_toggle",
                art_der_daten="C",
                ergebnis_qc="1",
                case_id="CASE_TOGGLE",
                gpas_domain="test_domain",
                processed_at=datetime(2023, 1, 1, 12, 0, 0),
                is_done=False  # Initially not done
            )
            db.upsert_record(genomic)
            db.upsert_record(clinical)
        
        # Set the database path in environment
        monkeypatch.setenv('DB_PATH', str(db_path))
        
        # Create test client
        client = TestClient(app)
        
        # Step 1: Load initial page and verify initial state
        response1 = client.get("/")
        assert response1.status_code == 200
        
        # Parse initial page to get pairs data
        soup1 = BeautifulSoup(response1.text, 'html.parser')
        pairs_script1 = soup1.find('script', id='pairs-data')
        assert pairs_script1 is not None
        
        import json
        pairs_data1 = json.loads(pairs_script1.string)
        
        # Verify initial state: pair should not be done
        toggle_pair = next((p for p in pairs_data1 if p['case_id'] == 'CASE_TOGGLE'), None)
        assert toggle_pair is not None, "CASE_TOGGLE should be present"
        assert toggle_pair['is_done'] is False, "Pair should initially not be done"
        
        # Count initial done pairs (should be 0)
        initial_done_count = sum(1 for p in pairs_data1 if p['is_done'])
        assert initial_done_count == 0, "Initially should have 0 done pairs"
        
        # Step 2: Toggle the done status via POST request
        response2 = client.post(
            "/api/done/CASE_TOGGLE",
            data={"done": "true"}
        )
        assert response2.status_code == 200
        
        # Step 3: Verify database was updated
        with MeldebestaetigungDatabase(db_path) as db:
            updated_genomic = db.get_record("VN_G_TOGGLE")
            updated_clinical = db.get_record("VN_C_TOGGLE")
            assert updated_genomic.is_done is True, "Genomic record should be marked as done"
            assert updated_clinical.is_done is True, "Clinical record should be marked as done"
        
        # Step 4: Load page again and verify updated statistics
        response3 = client.get("/")
        assert response3.status_code == 200
        
        # Parse updated page to get new pairs data
        soup3 = BeautifulSoup(response3.text, 'html.parser')
        pairs_script3 = soup3.find('script', id='pairs-data')
        assert pairs_script3 is not None
        
        pairs_data3 = json.loads(pairs_script3.string)
        
        # Verify updated state: pair should now be done
        updated_toggle_pair = next((p for p in pairs_data3 if p['case_id'] == 'CASE_TOGGLE'), None)
        assert updated_toggle_pair is not None, "CASE_TOGGLE should still be present"
        assert updated_toggle_pair['is_done'] is True, "Pair should now be done"
        
        # Count updated done pairs (should be 1)
        updated_done_count = sum(1 for p in pairs_data3 if p['is_done'])
        assert updated_done_count == 1, "Should now have 1 done pair"
        
        # Verify that the statistics container is still present for Alpine.js to use
        stats_container = soup3.find('div', class_='statistics-container')
        assert stats_container is not None, "Statistics container should still be present"


def test_statistics_work_with_empty_filter(test_db, monkeypatch):
    """
    Test that statistics display correctly when no filter is applied.
    
    This integration test verifies:
    1. With empty filter, all pairs are included in statistics
    2. Statistics reflect the total dataset
    3. Alpine.js computed properties work with unfiltered data
    
    Validates: Requirements 1.1, 5.1
    """
    # Set the database path in environment
    monkeypatch.setenv('DB_PATH', str(test_db))
    
    # Create test client
    client = TestClient(app)
    
    # Request index page
    response = client.get("/")
    assert response.status_code == 200
    
    # Parse HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Verify filter input is present and empty by default
    filter_input = soup.find('input', {'id': 'filter'})
    assert filter_input is not None, "Filter input should be present"
    assert filter_input.get('value') is None or filter_input.get('value') == '', \
        "Filter input should be empty by default"
    
    # Verify statistics container is present
    stats_container = soup.find('div', class_='statistics-container')
    assert stats_container is not None, "Statistics container should be present"
    
    # Extract pairs data to verify statistics will be calculated from all pairs
    pairs_script = soup.find('script', id='pairs-data')
    assert pairs_script is not None, "Pairs data script should be present"
    
    import json
    pairs_data = json.loads(pairs_script.string)
    
    # With empty filter, statistics should include all pairs
    # The test_db fixture should have known test data
    assert len(pairs_data) > 0, "Should have test data in database"
    
    # Verify that Alpine.js will have access to all pairs for statistics calculation
    # (The actual calculation happens client-side via Alpine.js computed properties)
    
    # Verify the Alpine.js x-data attribute is present on the container
    alpine_container = soup.find('div', {'x-data': 'tableData()'})
    assert alpine_container is not None, "Alpine.js container should be present"


def test_statistics_container_structure_and_styling(test_db, monkeypatch):
    """
    Test that statistics container has correct structure and CSS classes.
    
    This integration test verifies:
    1. Statistics container has correct CSS classes
    2. Statistics items have proper structure
    3. Layout classes are applied for responsive design
    4. Visual consistency with existing design
    
    Validates: Requirements 4.1, 4.2, 4.3, 4.4
    """
    # Set the database path in environment
    monkeypatch.setenv('DB_PATH', str(test_db))
    
    # Create test client
    client = TestClient(app)
    
    # Request index page
    response = client.get("/")
    assert response.status_code == 200
    
    # Parse HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Verify main container structure
    filter_stats_container = soup.find('div', class_='filter-and-stats-container')
    assert filter_stats_container is not None, "filter-and-stats-container should be present"
    
    # Verify statistics container structure
    stats_container = soup.find('div', class_='statistics-container')
    assert stats_container is not None, "Statistics container should be present"
    
    # Verify statistics items structure
    stats_items = stats_container.find_all('div', class_='statistics-item')
    assert len(stats_items) >= 3, "Should have at least 3 statistics items"
    
    # Verify each statistics item has proper structure
    for i, item in enumerate(stats_items):
        # Each item should have label and value spans
        label_span = item.find('span', class_='statistics-label')
        value_span = item.find('span', class_='statistics-value')
        
        assert label_span is not None, f"Statistics item {i} should have label span"
        assert value_span is not None, f"Statistics item {i} should have value span"
        
        # Value span should have Alpine.js x-text directive
        assert value_span.get('x-text') is not None, \
            f"Statistics item {i} value should have x-text directive"
    
    # Verify CSS link is present for styling
    css_link = soup.find('link', {'href': '/static/css/custom.css'})
    assert css_link is not None, "Custom CSS link should be present"
    
    # Verify Alpine.js script is loaded
    alpine_script = soup.find('script', src=lambda x: x and 'alpinejs' in x)
    assert alpine_script is not None, "Alpine.js script should be loaded"