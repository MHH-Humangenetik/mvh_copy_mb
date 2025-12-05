"""
Tests for the "Done" checkbox functionality.

These tests verify that clicking the checkbox correctly updates the database
and refreshes the UI state via HTMX.
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

# Mark all tests in this module as browser tests
pytestmark = pytest.mark.browser

# Test configuration constants
TEST_SERVER_HOST = "127.0.0.1"
TEST_SERVER_PORT = 8766  # Different port to avoid conflicts
SERVER_STARTUP_TIMEOUT = 3
HTMX_SWAP_DELAY = 500  # milliseconds - wait for HTMX to complete swap


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
def test_database_checkbox() -> Iterator[Path]:
    """Create a test database with sample data for checkbox tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_checkbox.duckdb"
        
        with MeldebestaetigungDatabase(db_path) as db:
            # Create complete pair - not done (for testing check action)
            for record in _create_record_pair(
                case_id="CASE_CHECK",
                source_file="source_check.csv",
                typ_der_meldung="0",
                indikationsbereich="Hämatologie",
                ergebnis_qc="1",
                processed_at=datetime(2023, 1, 1, 12, 0, 0),
                is_done=False,
            ):
                db.upsert_record(record)
            
            # Create complete pair - already done (for testing uncheck action)
            for record in _create_record_pair(
                case_id="CASE_UNCHECK",
                source_file="source_uncheck.csv",
                typ_der_meldung="1",
                indikationsbereich="Kardiologie",
                ergebnis_qc="1",
                processed_at=datetime(2023, 1, 2, 12, 0, 0),
                is_done=True,
            ):
                db.upsert_record(record)
            
            # Create incomplete pair (should not have checkbox)
            for record in _create_record_pair(
                case_id="CASE_INCOMPLETE",
                source_file="source_incomplete.csv",
                typ_der_meldung="2",
                indikationsbereich="Radiologie",
                ergebnis_qc="1",
                processed_at=datetime(2023, 1, 3, 12, 0, 0),
                is_done=False,
                include_clinical=False,
            ):
                db.upsert_record(record)
        
        yield db_path


@pytest.fixture(scope="module")
def web_server_checkbox(test_database_checkbox):
    """Start the FastAPI web server for checkbox testing."""
    # Set database path
    os.environ['DB_PATH'] = str(test_database_checkbox)
    
    # Start uvicorn server
    process = subprocess.Popen(
        ['uv', 'run', 'uvicorn', 'mvh_copy_mb.web:app', '--host', TEST_SERVER_HOST, '--port', str(TEST_SERVER_PORT)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=os.setsid if hasattr(os, 'setsid') else None
    )
    
    # Wait for server to start
    time.sleep(SERVER_STARTUP_TIMEOUT)
    
    yield f'http://{TEST_SERVER_HOST}:{TEST_SERVER_PORT}'
    
    # Cleanup: kill the server
    try:
        if hasattr(os, 'killpg'):
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        else:
            process.terminate()
        process.wait(timeout=5)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        pass


@pytest.fixture
def checkbox_page(web_server_checkbox, page: Page):
    """Navigate to the web app before each test and wait for Alpine.js initialization."""
    page.goto(web_server_checkbox)
    page.wait_for_selector('[x-data]')
    return page


def test_checkbox_click_marks_as_done(checkbox_page: Page):
    """
    Test that clicking an unchecked checkbox marks the pair as done.
    
    Validates:
    - Checkbox changes from unchecked to checked
    - Database is updated (is_done=True)
    - Priority group changes from 1 to 3
    - Rows remain visible after update
    """
    # Find the checkbox for CASE_CHECK (should be unchecked)
    checkbox = checkbox_page.locator('tr[data-case-id="CASE_CHECK"] input[type="checkbox"]').first
    expect(checkbox).not_to_be_checked()
    
    # Verify initial priority group is 1 (complete, not done)
    genomic_row = checkbox_page.locator('tr.genomic[data-case-id="CASE_CHECK"]').first
    expect(genomic_row).to_have_class('pair-row genomic priority-group-1')
    
    # Click the checkbox
    checkbox.click()
    
    # Wait for HTMX to complete the swap
    checkbox_page.wait_for_timeout(HTMX_SWAP_DELAY)
    
    # Verify checkbox is now checked (select by ID to get HTMX-rendered row)
    checkbox_after = checkbox_page.locator('#pair-genomic-CASE_CHECK input[type="checkbox"]')
    expect(checkbox_after).to_be_checked()
    
    # Verify priority group changed to 3 (complete, done)
    genomic_row_after = checkbox_page.locator('#pair-genomic-CASE_CHECK')
    expect(genomic_row_after).to_have_class('pair-row genomic priority-group-3')
    
    # Verify both HTMX-rendered rows are visible
    expect(checkbox_page.locator('#pair-genomic-CASE_CHECK')).to_have_count(1)
    expect(checkbox_page.locator('#pair-clinical-CASE_CHECK')).to_have_count(1)


def test_checkbox_click_marks_as_not_done(checkbox_page: Page):
    """
    Test that clicking a checked checkbox marks the pair as not done.
    
    Validates:
    - Checkbox changes from checked to unchecked
    - Database is updated (is_done=False)
    - Priority group changes from 3 to 1
    - Rows remain visible after update
    """
    # Find the checkbox for CASE_UNCHECK (should be checked)
    checkbox = checkbox_page.locator('tr[data-case-id="CASE_UNCHECK"] input[type="checkbox"]').first
    expect(checkbox).to_be_checked()
    
    # Verify initial priority group is 3 (complete, done)
    genomic_row = checkbox_page.locator('tr.genomic[data-case-id="CASE_UNCHECK"]').first
    expect(genomic_row).to_have_class('pair-row genomic priority-group-3')
    
    # Click the checkbox
    checkbox.click()
    
    # Wait for HTMX to complete the swap
    checkbox_page.wait_for_timeout(HTMX_SWAP_DELAY)
    
    # Verify checkbox is now unchecked (select by ID to get HTMX-rendered row)
    checkbox_after = checkbox_page.locator('#pair-genomic-CASE_UNCHECK input[type="checkbox"]')
    expect(checkbox_after).not_to_be_checked()
    
    # Verify priority group changed to 1 (complete, not done)
    genomic_row_after = checkbox_page.locator('#pair-genomic-CASE_UNCHECK')
    expect(genomic_row_after).to_have_class('pair-row genomic priority-group-1')
    
    # Verify both HTMX-rendered rows are visible
    expect(checkbox_page.locator('#pair-genomic-CASE_UNCHECK')).to_have_count(1)
    expect(checkbox_page.locator('#pair-clinical-CASE_UNCHECK')).to_have_count(1)


def test_checkbox_preserves_all_data_fields(checkbox_page: Page):
    """
    Test that clicking checkbox preserves all data fields in the rows.
    
    Validates:
    - All fields remain unchanged after checkbox click
    - Only the done status and priority group change
    """
    # Get initial data from CASE_CHECK
    genomic_row = checkbox_page.locator('tr.genomic[data-case-id="CASE_CHECK"]').first
    clinical_row = checkbox_page.locator('tr.clinical[data-case-id="CASE_CHECK"]').first
    
    # Store initial values
    initial_vorgangsnummer_g = genomic_row.locator('td').nth(0).text_content()
    initial_indikationsbereich_g = genomic_row.locator('td').nth(3).text_content()
    initial_vorgangsnummer_c = clinical_row.locator('td').nth(0).text_content()
    
    # Click the checkbox
    checkbox = checkbox_page.locator('tr[data-case-id="CASE_CHECK"] input[type="checkbox"]').first
    checkbox.click()
    checkbox_page.wait_for_timeout(HTMX_SWAP_DELAY)
    
    # Get data after update from HTMX-rendered rows
    genomic_row_after = checkbox_page.locator('#pair-genomic-CASE_CHECK')
    clinical_row_after = checkbox_page.locator('#pair-clinical-CASE_CHECK')
    
    # Verify all fields are preserved
    expect(genomic_row_after.locator('td').nth(0)).to_have_text(initial_vorgangsnummer_g)
    expect(genomic_row_after.locator('td').nth(3)).to_have_text(initial_indikationsbereich_g)
    expect(clinical_row_after.locator('td').nth(0)).to_have_text(initial_vorgangsnummer_c)


def test_checkbox_updates_persist_on_page_reload(checkbox_page: Page):
    """
    Test that checkbox state persists after page reload.
    
    Validates:
    - Database update is permanent
    - Reloading page shows updated state
    """
    # Get initial state
    checkbox = checkbox_page.locator('tr[data-case-id="CASE_CHECK"] input[type="checkbox"]').first
    state_before_click = checkbox.is_checked()
    
    # Click to toggle it
    checkbox.click()
    checkbox_page.wait_for_timeout(HTMX_SWAP_DELAY)
    
    # Verify state changed
    checkbox_after_click = checkbox_page.locator('#pair-genomic-CASE_CHECK input[type="checkbox"]')
    state_after_click = checkbox_after_click.is_checked()
    assert state_after_click != state_before_click, "Checkbox state should have changed after click"
    
    # Reload the page
    checkbox_page.reload()
    checkbox_page.wait_for_selector('[x-data]')
    
    # Verify checkbox state persisted after reload
    checkbox_after_reload = checkbox_page.locator('tr[data-case-id="CASE_CHECK"] input[type="checkbox"]').first
    state_after_reload = checkbox_after_reload.is_checked()
    assert state_after_reload == state_after_click, "Checkbox state should persist after page reload"


def test_incomplete_pair_has_no_checkbox(checkbox_page: Page):
    """
    Test that incomplete pairs do not have a checkbox.
    
    Validates:
    - Incomplete pairs show dash instead of checkbox
    - No checkbox element exists for incomplete pairs
    """
    # Find the incomplete pair row
    incomplete_row = checkbox_page.locator('tr[data-case-id="CASE_INCOMPLETE"]')
    expect(incomplete_row).to_be_visible()
    
    # Should have dash, not checkbox
    done_cell = incomplete_row.locator('.done-cell')
    expect(done_cell.locator('span:has-text("—")')).to_be_visible()
    expect(done_cell.locator('input[type="checkbox"]')).to_have_count(0)


def test_multiple_checkbox_clicks_toggle_correctly(checkbox_page: Page):
    """
    Test that multiple clicks on the same checkbox toggle correctly.
    
    Validates:
    - First click: unchecked -> checked
    - Second click: checked -> unchecked
    - Third click: unchecked -> checked
    """
    checkbox = checkbox_page.locator('tr[data-case-id="CASE_CHECK"] input[type="checkbox"]').first
    
    # Get initial state
    initial_state = checkbox.is_checked()
    
    # First click
    checkbox.click()
    checkbox_page.wait_for_timeout(HTMX_SWAP_DELAY)
    checkbox_after_1 = checkbox_page.locator('#pair-genomic-CASE_CHECK input[type="checkbox"]')
    expect(checkbox_after_1).to_be_checked() if not initial_state else expect(checkbox_after_1).not_to_be_checked()
    
    # Second click
    checkbox_after_1.click()
    checkbox_page.wait_for_timeout(HTMX_SWAP_DELAY)
    checkbox_after_2 = checkbox_page.locator('#pair-genomic-CASE_CHECK input[type="checkbox"]')
    expect(checkbox_after_2).to_be_checked() if initial_state else expect(checkbox_after_2).not_to_be_checked()
    
    # Third click
    checkbox_after_2.click()
    checkbox_page.wait_for_timeout(HTMX_SWAP_DELAY)
    checkbox_after_3 = checkbox_page.locator('#pair-genomic-CASE_CHECK input[type="checkbox"]')
    expect(checkbox_after_3).to_be_checked() if not initial_state else expect(checkbox_after_3).not_to_be_checked()
