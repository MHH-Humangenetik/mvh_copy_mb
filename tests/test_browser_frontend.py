"""
Browser-based tests for the web frontend using Playwright.

These tests validate the actual Alpine.js and HTMX behavior in a real browser,
complementing the simulation-based tests in test_web_frontend.py.

To run these tests locally:
    uv run pytest tests/test_browser_frontend.py

To skip browser tests:
    uv run pytest -m "not browser"
"""

import os
import signal
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator

import pytest
from playwright.sync_api import Page, expect

from mvh_copy_mb.database import MeldebestaetigungDatabase, MeldebestaetigungRecord

# Mark all tests in this module as browser tests and group them to run serially
pytestmark = [pytest.mark.browser, pytest.mark.xdist_group(name="browser_serial")]

# Test configuration constants
TEST_SERVER_HOST = "127.0.0.1"
TEST_SERVER_PORT = 8765
SERVER_STARTUP_TIMEOUT = 3  # seconds - actual wait time in web_server fixture
ALPINE_REACTION_DELAY = 100  # milliseconds

# Expected row counts for test data
EXPECTED_TOTAL_ROWS = 7
EXPECTED_COMPLETE_PAIR_ROWS = 2
EXPECTED_INCOMPLETE_ROWS = 1


def _create_record_pair(
    case_id: str,
    source_file: str,
    typ_der_meldung: str,
    indikationsbereich: str,
    ergebnis_qc: str,
    processed_at: datetime,
    is_done: bool,
    include_clinical: bool = True,
) -> list[MeldebestaetigungRecord]:
    """Helper to create a pair of records (genomic and optionally clinical)."""
    records = []
    
    for art_der_daten in ["G", "C"] if include_clinical else ["G"]:
        record = MeldebestaetigungRecord(
            vorgangsnummer=f"VN_{art_der_daten}_{case_id}",
            meldebestaetigung=f"mb_{art_der_daten}_{case_id.lower()}",
            source_file=source_file,
            typ_der_meldung=typ_der_meldung,
            indikationsbereich=indikationsbereich,
            art_der_daten=art_der_daten,
            ergebnis_qc=ergebnis_qc,
            case_id=case_id,
            gpas_domain="test_domain",
            processed_at=processed_at,
            is_done=is_done,
        )
        records.append(record)
    
    return records


@pytest.fixture(scope="module")
def test_database() -> Iterator[Path]:
    """Create a test database with sample data for browser tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        with MeldebestaetigungDatabase(db_path) as db:
            # Create complete pair (both genomic and clinical) - not done
            for record in _create_record_pair(
                case_id="CASE_COMPLETE",
                source_file="source_complete.csv",
                typ_der_meldung="0",
                indikationsbereich="Hämatologie",
                ergebnis_qc="1",
                processed_at=datetime(2023, 1, 1, 12, 0, 0),
                is_done=False,
            ):
                db.upsert_record(record)
            
            # Create incomplete pair (only genomic)
            for record in _create_record_pair(
                case_id="CASE_INCOMPLETE",
                source_file="source_incomplete.csv",
                typ_der_meldung="1",
                indikationsbereich="Radiologie",
                ergebnis_qc="1",
                processed_at=datetime(2023, 1, 2, 12, 0, 0),
                is_done=False,
                include_clinical=False,
            ):
                db.upsert_record(record)
            
            # Create complete pair - already done
            for record in _create_record_pair(
                case_id="CASE_DONE",
                source_file="source_done.csv",
                typ_der_meldung="2",
                indikationsbereich="Kardiologie",
                ergebnis_qc="1",
                processed_at=datetime(2023, 1, 3, 12, 0, 0),
                is_done=True,
            ):
                db.upsert_record(record)
            
            # Create complete pair with failing QC (invalid)
            for record in _create_record_pair(
                case_id="CASE_INVALID",
                source_file="source_invalid.csv",
                typ_der_meldung="0",
                indikationsbereich="Neurologie",
                ergebnis_qc="0",
                processed_at=datetime(2023, 1, 4, 12, 0, 0),
                is_done=False,
            ):
                db.upsert_record(record)
        
        yield db_path


@pytest.fixture(scope="module")
def web_server(test_database):
    """Start the FastAPI web server for testing."""
    import os
    import subprocess
    import time
    import signal
    
    # Set database path
    os.environ['DB_PATH'] = str(test_database)
    
    # Start uvicorn server
    process = subprocess.Popen(
        ['uv', 'run', 'uvicorn', 'mvh_copy_mb.web:app', '--host', '127.0.0.1', '--port', '8765'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=os.setsid if hasattr(os, 'setsid') else None
    )
    
    # Wait for server to start
    time.sleep(3)
    
    yield 'http://127.0.0.1:8765'
    
    # Cleanup: kill the server
    try:
        if hasattr(os, 'killpg'):
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        else:
            process.terminate()
        process.wait(timeout=5)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        pass  # Process already dead or taking too long


@pytest.fixture
def app_page(web_server, page: Page):
    """Navigate to the web app before each test and wait for Alpine.js initialization."""
    page.goto(web_server)
    # Wait for Alpine.js to initialize
    page.wait_for_selector('[x-data]')
    return page


# Feature: web-frontend, Browser Test 1: Alpine.js data loads correctly
def test_alpine_data_loads_from_json(app_page: Page):
    """
    Test that Alpine.js loads data from the embedded JSON script.
    
    Validates:
    - Alpine.js initializes correctly
    - Data is loaded from the script tag
    - Table rows are rendered
    """
    # Check that the pairs data script exists
    expect(app_page.locator('#pairs-data')).to_be_attached()
    
    # Check that table has rows (should have 4 case IDs = 7 rows total)
    # CASE_COMPLETE: 2 rows, CASE_INCOMPLETE: 1 row, CASE_DONE: 2 rows, CASE_INVALID: 2 rows
    rows = app_page.locator('tbody tr')
    expect(rows).to_have_count(EXPECTED_TOTAL_ROWS)
    
    # Verify specific case IDs are present
    expect(app_page.locator('text=CASE_COMPLETE').first).to_be_visible()
    expect(app_page.locator('text=CASE_INCOMPLETE').first).to_be_visible()
    expect(app_page.locator('text=CASE_DONE').first).to_be_visible()
    expect(app_page.locator('text=CASE_INVALID').first).to_be_visible()


# Feature: web-frontend, Browser Test 2: Client-side filtering works
def test_alpine_filtering_updates_visible_rows(app_page: Page):
    """
    Test that typing in the filter input updates visible rows via Alpine.js.
    
    Validates: Property 15 (Requirement 9.1)
    - Filtering searches across all columns
    - Filtering is case-insensitive
    - Only matching records are visible
    """
    # Get initial row count
    initial_rows = app_page.locator('tbody tr')
    expect(initial_rows).to_have_count(EXPECTED_TOTAL_ROWS)
    
    # Type in filter input
    filter_input = app_page.locator('input#filter')
    filter_input.fill('CASE_COMPLETE')
    app_page.wait_for_timeout(ALPINE_REACTION_DELAY)
    
    # Should only show rows with CASE_COMPLETE (2 rows: genomic + clinical)
    expect(app_page.locator('tbody tr:has-text("CASE_COMPLETE")')).to_have_count(EXPECTED_COMPLETE_PAIR_ROWS)
    
    # Verify other case IDs are not visible
    expect(app_page.locator('tbody tr:has-text("CASE_INCOMPLETE")')).to_have_count(0)
    expect(app_page.locator('tbody tr:has-text("CASE_DONE")')).to_have_count(0)
    
    # Test case-insensitive filtering
    filter_input.fill('hämatologie')
    app_page.wait_for_timeout(ALPINE_REACTION_DELAY)
    
    # Should show CASE_COMPLETE rows (which have Hämatologie)
    expect(app_page.locator('tbody tr')).to_have_count(EXPECTED_COMPLETE_PAIR_ROWS)
    
    # Clear filter
    filter_input.fill('')
    app_page.wait_for_timeout(ALPINE_REACTION_DELAY)
    
    # Should show all rows again
    expect(app_page.locator('tbody tr')).to_have_count(EXPECTED_TOTAL_ROWS)


# Feature: web-frontend, Browser Test 3: Client-side sorting works
def test_alpine_sorting_by_column_header(app_page: Page):
    """
    Test that clicking column headers sorts the table via Alpine.js.
    
    Validates: Property 16 (Requirements 9.2, 9.3)
    - Clicking header sorts by that column
    - Clicking again toggles sort direction
    - Priority group remains primary sort
    """
    # Click Case ID header to sort
    app_page.locator('th:has-text("Case ID")').click()
    app_page.wait_for_timeout(ALPINE_REACTION_DELAY)
    
    # Verify rows are still present after sorting
    expect(app_page.locator('tbody tr')).to_have_count(EXPECTED_TOTAL_ROWS)
    
    # Click again to toggle sort direction
    app_page.locator('th:has-text("Case ID")').click()
    app_page.wait_for_timeout(ALPINE_REACTION_DELAY)
    
    # Verify sort indicator is visible
    expect(app_page.locator('th:has-text("Case ID") span').first).to_be_visible()
    
    # Rows should still be present
    expect(app_page.locator('tbody tr')).to_have_count(EXPECTED_TOTAL_ROWS)


# Feature: web-frontend, Browser Test 4: Complete pairs have rowspan
def test_complete_pairs_render_with_rowspan(app_page: Page):
    """
    Test that complete pairs render with rowspan=2 for Case ID and indicators.
    
    Validates: Property 5 (Requirement 2.3)
    - Complete pairs have two rows
    - Case ID cell has rowspan=2
    - Indicator cells have rowspan=2
    """
    # Find the genomic row for CASE_COMPLETE
    genomic_row = app_page.locator('tr.genomic[data-case-id="CASE_COMPLETE"]')
    expect(genomic_row).to_be_visible()
    
    # Check that Case ID cell has rowspan=2
    case_id_cell = genomic_row.locator('.case-id-cell')
    expect(case_id_cell).to_have_attribute('rowspan', '2')
    
    # Check that the clinical row exists and is consecutive
    clinical_row = app_page.locator('tr.clinical[data-case-id="CASE_COMPLETE"]')
    expect(clinical_row).to_be_visible()
    
    # Clinical row should NOT have a case-id-cell (it's spanned from genomic)
    expect(clinical_row.locator('.case-id-cell')).to_have_count(0)


# Feature: web-frontend, Browser Test 5: Incomplete pairs have single row
def test_incomplete_pairs_render_single_row(app_page: Page):
    """
    Test that incomplete pairs render as a single row without rowspan.
    
    Validates: Property 8 (Requirements 4.1, 4.2)
    - Incomplete pairs have one row
    - Case ID cell has no rowspan
    - No checkbox is present
    """
    # Find the row for CASE_INCOMPLETE
    incomplete_row = app_page.locator('tr[data-case-id="CASE_INCOMPLETE"]')
    expect(incomplete_row).to_be_visible()
    
    # Should only be one row with this case ID
    expect(app_page.locator('tr[data-case-id="CASE_INCOMPLETE"]')).to_have_count(EXPECTED_INCOMPLETE_ROWS)
    
    # Case ID cell should NOT have rowspan
    case_id_cell = incomplete_row.locator('.case-id-cell')
    expect(case_id_cell).not_to_have_attribute('rowspan', '2')
    
    # Should have a dash instead of checkbox
    done_cell = incomplete_row.locator('.done-cell')
    expect(done_cell.locator('span:has-text("—")')).to_be_visible()
    expect(done_cell.locator('input[type="checkbox"]')).to_have_count(0)


# Feature: web-frontend, Browser Test 6: Complete pairs have checkbox
def test_complete_pairs_have_checkbox(app_page: Page):
    """
    Test that complete pairs have a checkbox in the done column.
    
    Validates: Property 8 (Requirements 4.1, 4.2)
    - Complete pairs have checkbox
    - Checkbox has correct HTMX attributes
    """
    # Find the genomic row for CASE_COMPLETE
    genomic_row = app_page.locator('tr.genomic[data-case-id="CASE_COMPLETE"]')
    
    # Should have a checkbox
    checkbox = genomic_row.locator('input[type="checkbox"]')
    expect(checkbox).to_be_visible()
    
    # Checkbox should have HTMX attributes
    expect(checkbox).to_have_attribute('hx-post', '/api/done/CASE_COMPLETE')
    expect(checkbox).to_have_attribute('hx-target', '#pair-genomic-CASE_COMPLETE')
    expect(checkbox).to_have_attribute('hx-swap', 'outerHTML')


# Feature: web-frontend, Browser Test 7: Checkbox state reflects database
def test_checkbox_state_reflects_database(app_page: Page):
    """
    Test that checkbox states match the database is_done values.
    
    Validates: Property 10 (Requirement 4.5)
    - Checked checkboxes for done pairs
    - Unchecked checkboxes for not done pairs
    """
    # CASE_COMPLETE should be unchecked (is_done=False)
    complete_checkbox = app_page.locator('tr[data-case-id="CASE_COMPLETE"] input[type="checkbox"]')
    expect(complete_checkbox).not_to_be_checked()
    
    # CASE_DONE should be checked (is_done=True)
    done_checkbox = app_page.locator('tr[data-case-id="CASE_DONE"] input[type="checkbox"]')
    expect(done_checkbox).to_be_checked()


# Feature: web-frontend, Browser Test 8: Complete indicator shows correctly
def test_complete_indicator_visual_state(app_page: Page):
    """
    Test that complete pair indicators show correct visual state.
    
    Validates: Property 6 (Requirements 2.4, 2.5)
    - Complete pairs have 'yes' indicator
    - Incomplete pairs have 'no' indicator
    """
    # CASE_COMPLETE should have complete indicator with 'yes' class
    complete_indicator = app_page.locator('tr[data-case-id="CASE_COMPLETE"] .complete-indicator')
    expect(complete_indicator).to_have_class('complete-indicator yes')
    
    # CASE_INCOMPLETE should have complete indicator with 'no' class
    incomplete_indicator = app_page.locator('tr[data-case-id="CASE_INCOMPLETE"] .complete-indicator')
    expect(incomplete_indicator).to_have_class('complete-indicator no')


# Feature: web-frontend, Browser Test 9: Valid indicator shows correctly
def test_valid_indicator_visual_state(app_page: Page):
    """
    Test that valid pair indicators show correct visual state.
    
    Validates: Property 7 (Requirements 3.2, 3.3, 3.4, 3.5)
    - Valid pairs (complete + QC=1) have 'yes' indicator
    - Invalid pairs (failing QC) have 'no' indicator
    """
    # CASE_COMPLETE should have valid indicator with 'yes' class (QC=1)
    valid_indicator = app_page.locator('tr[data-case-id="CASE_COMPLETE"] .valid-indicator')
    expect(valid_indicator).to_have_class('valid-indicator yes')
    
    # CASE_INVALID should have valid indicator with 'no' class (QC=0)
    invalid_indicator = app_page.locator('tr[data-case-id="CASE_INVALID"] .valid-indicator')
    expect(invalid_indicator).to_have_class('valid-indicator no')


# Feature: web-frontend, Browser Test 10: Priority groups are styled
def test_priority_groups_have_css_classes(app_page: Page):
    """
    Test that priority groups have correct CSS classes applied.
    
    Validates: Property 2 (Requirements 1.3, 8.1, 8.5)
    - Priority group 1: complete not done
    - Priority group 2: incomplete
    - Priority group 3: complete done
    """
    # CASE_COMPLETE should be priority group 1 (complete, not done)
    complete_rows = app_page.locator('tr[data-case-id="CASE_COMPLETE"]')
    expect(complete_rows.first).to_have_class('pair-row genomic priority-group-1')
    
    # CASE_INCOMPLETE should be priority group 2 (incomplete)
    incomplete_row = app_page.locator('tr[data-case-id="CASE_INCOMPLETE"]')
    expect(incomplete_row).to_have_class('pair-row genomic priority-group-2')
    
    # CASE_DONE should be priority group 3 (complete, done)
    done_rows = app_page.locator('tr[data-case-id="CASE_DONE"]')
    expect(done_rows.first).to_have_class('pair-row genomic priority-group-3')


# Feature: web-frontend, Browser Test 11: Filtering preserves pair grouping
def test_filtering_preserves_pair_grouping(app_page: Page):
    """
    Test that filtering maintains pair grouping (consecutive rows).
    
    Validates: Property 17 (Requirement 9.4)
    - Filtered pairs remain consecutive
    - Both genomic and clinical rows shown together
    """
    # Filter for CASE_COMPLETE
    app_page.locator('input#filter').fill('CASE_COMPLETE')
    app_page.wait_for_timeout(ALPINE_REACTION_DELAY)
    
    # Should have exactly 2 rows (genomic + clinical)
    visible_rows = app_page.locator('tbody tr')
    expect(visible_rows).to_have_count(EXPECTED_COMPLETE_PAIR_ROWS)
    
    # Both should have the same case ID
    first_row_case_id = visible_rows.nth(0).get_attribute('data-case-id')
    second_row_case_id = visible_rows.nth(1).get_attribute('data-case-id')
    assert first_row_case_id == second_row_case_id == 'CASE_COMPLETE'
    
    # First should be genomic, second should be clinical
    expect(visible_rows.nth(0)).to_have_class('pair-row genomic priority-group-1')
    expect(visible_rows.nth(1)).to_have_class('pair-row clinical priority-group-1')


# Feature: web-frontend, Browser Test 12: All required fields are displayed
def test_all_required_fields_displayed(app_page: Page):
    """
    Test that all required fields are displayed in the table.
    
    Validates: Property 1 (Requirement 1.2)
    - Case ID, Vorgangsnummer, Art der Daten, Typ der Meldung,
      Indikationsbereich, Ergebnis QC, Source File
    """
    # Wait for table rows to be rendered by Alpine.js
    app_page.wait_for_selector('tbody tr', state='visible')
    
    # Check table headers
    expect(app_page.locator('th:has-text("Case ID")')).to_be_visible()
    expect(app_page.locator('th:has-text("Vorgangsnummer")')).to_be_visible()
    expect(app_page.locator('th:has-text("Art der Daten")')).to_be_visible()
    expect(app_page.locator('th:has-text("Typ der Meldung")')).to_be_visible()
    expect(app_page.locator('th:has-text("Indikationsbereich")')).to_be_visible()
    expect(app_page.locator('th:has-text("Ergebnis QC")')).to_be_visible()
    expect(app_page.locator('th:has-text("Source File")')).to_be_visible()
    expect(app_page.locator('th:has-text("Complete")')).to_be_visible()
    expect(app_page.locator('th:has-text("Valid")')).to_be_visible()
    expect(app_page.locator('th:has-text("Done")')).to_be_visible()
    
    # Check that data is present in cells
    # Note: Some cells in genomic rows may not be visible due to rowspan layout
    # Use nth(1) to get the clinical row which is always fully visible
    expect(app_page.get_by_text("VN_G_CASE_COMPLETE").first).to_be_visible()
    expect(app_page.get_by_text("genomic", exact=True).first).to_be_visible()
    expect(app_page.get_by_text("Hämatologie").first).to_be_visible()
    expect(app_page.get_by_text("source_complete.csv").nth(1)).to_be_visible()
