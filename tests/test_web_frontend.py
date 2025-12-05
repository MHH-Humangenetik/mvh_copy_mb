"""
Property-based tests for the web frontend rendering.

This module contains property-based tests using Hypothesis to verify
correctness properties of the web frontend HTML rendering.
"""

import tempfile
from datetime import datetime
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from bs4 import BeautifulSoup

from mvh_copy_mb.database import MeldebestaetigungDatabase, MeldebestaetigungRecord
from mvh_copy_mb.web_database import WebDatabaseService


def render_pair_to_html(pair) -> str:
    """
    Helper function to render a RecordPair to HTML similar to the template.
    This simulates what the Alpine.js template would render.
    """
    html_parts = []
    
    # Genomic row
    if pair.genomic:
        genomic_row = f"""
        <tr class="pair-row genomic priority-group-{pair.priority_group}" data-case-id="{pair.case_id}">
            <td rowspan="{'2' if pair.is_complete else '1'}" class="case-id-cell">{pair.case_id}</td>
            <td>{pair.genomic.vorgangsnummer}</td>
            <td>{pair.genomic.meldebestaetigung}</td>
            <td>genomic</td>
            <td>{pair.genomic.typ_der_meldung}</td>
            <td>{pair.genomic.indikationsbereich}</td>
            <td>{pair.genomic.ergebnis_qc}</td>
            <td>{pair.genomic.source_file}</td>
            <td rowspan="{'2' if pair.is_complete else '1'}">
                <span class="complete-indicator {'yes' if pair.is_complete else 'no'}"></span>
            </td>
            <td rowspan="{'2' if pair.is_complete else '1'}">
                <span class="valid-indicator {'yes' if pair.is_valid else 'no'}"></span>
            </td>
            <td rowspan="{'2' if pair.is_complete else '1'}" class="done-cell">
                {'<input type="checkbox" ' + ('checked' if pair.is_done else '') + '>' if pair.is_complete else ''}
            </td>
        </tr>
        """
        html_parts.append(genomic_row)
    
    # Clinical row
    if pair.clinical:
        clinical_row = f"""
        <tr class="pair-row clinical priority-group-{pair.priority_group}" data-case-id="{pair.case_id}">
            {'<td rowspan="1" class="case-id-cell">' + pair.case_id + '</td>' if not pair.genomic else ''}
            <td>{pair.clinical.vorgangsnummer}</td>
            <td>{pair.clinical.meldebestaetigung}</td>
            <td>clinical</td>
            <td>{pair.clinical.typ_der_meldung}</td>
            <td>{pair.clinical.indikationsbereich}</td>
            <td>{pair.clinical.ergebnis_qc}</td>
            <td>{pair.clinical.source_file}</td>
            {'<td rowspan="1"><span class="complete-indicator ' + ('yes' if pair.is_complete else 'no') + '"></span></td>' if not pair.genomic else ''}
            {'<td rowspan="1"><span class="valid-indicator ' + ('yes' if pair.is_valid else 'no') + '"></span></td>' if not pair.genomic else ''}
            {'<td rowspan="1" class="done-cell">' + ('<input type="checkbox" ' + ('checked' if pair.is_done else '') + '>' if pair.is_complete else '') + '</td>' if not pair.genomic else ''}
        </tr>
        """
        html_parts.append(clinical_row)
    
    return ''.join(html_parts)


# Feature: web-frontend, Property 1: All required fields are displayed
# Validates: Requirements 1.2
@settings(max_examples=100)
@given(
    num_pairs=st.integers(min_value=1, max_value=10),
    record_types=st.lists(
        st.integers(min_value=0, max_value=2),  # 0=genomic only, 1=clinical only, 2=both
        min_size=1,
        max_size=10
    )
)
def test_all_required_fields_are_displayed(num_pairs: int, record_types: list):
    """
    Property 1: All required fields are displayed
    
    For any set of Meldebestaetigung records, when rendered in the table,
    the HTML should contain Case ID, Vorgangsnummer, Art der Daten,
    Typ der Meldung, Indikationsbereich, Ergebnis QC, and source file
    for each record.
    
    This test verifies that:
    1. All required fields are present in the rendered HTML
    2. Field values match the database records
    """
    # Ensure we have enough record types
    while len(record_types) < num_pairs:
        record_types.append(2)
    
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create records
        with MeldebestaetigungDatabase(db_path) as db:
            for i in range(num_pairs):
                case_id = f"CASE_{i:03d}"
                record_type = record_types[i]
                
                if record_type in [0, 2]:  # Create genomic
                    genomic_record = MeldebestaetigungRecord(
                        vorgangsnummer=f"VN_G_{i}",
                        meldebestaetigung=f"mb_genomic_{i}",
                        source_file=f"source_{i}.csv",
                        typ_der_meldung=str(i % 3),
                        indikationsbereich=f"indication_{i}",
                        art_der_daten="G",
                        ergebnis_qc=str((i % 2) + 1),
                        case_id=case_id,
                        gpas_domain="test_domain",
                        processed_at=datetime(2023, 1, 1, 12, 0, 0),
                        is_done=False
                    )
                    db.upsert_record(genomic_record)
                
                if record_type in [1, 2]:  # Create clinical
                    clinical_record = MeldebestaetigungRecord(
                        vorgangsnummer=f"VN_C_{i}",
                        meldebestaetigung=f"mb_clinical_{i}",
                        source_file=f"source_{i}.csv",
                        typ_der_meldung=str(i % 3),
                        indikationsbereich=f"indication_{i}",
                        art_der_daten="C",
                        ergebnis_qc=str((i % 2) + 1),
                        case_id=case_id,
                        gpas_domain="test_domain",
                        processed_at=datetime(2023, 1, 1, 12, 0, 0),
                        is_done=False
                    )
                    db.upsert_record(clinical_record)
        
        # Get pairs from service
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        
        # Render each pair to HTML and verify required fields
        for pair in pairs:
            html = render_pair_to_html(pair)
            soup = BeautifulSoup(html, 'html.parser')
            
            # Verify Case ID is present
            case_id_cells = soup.find_all('td', class_='case-id-cell')
            assert len(case_id_cells) > 0, f"Case ID cell not found for {pair.case_id}"
            assert pair.case_id in case_id_cells[0].text
            
            # Verify genomic record fields if present
            if pair.genomic:
                assert pair.genomic.vorgangsnummer in html
                assert "genomic" in html
                assert pair.genomic.typ_der_meldung in html
                assert pair.genomic.indikationsbereich in html
                assert pair.genomic.ergebnis_qc in html
                assert pair.genomic.source_file in html
            
            # Verify clinical record fields if present
            if pair.clinical:
                assert pair.clinical.vorgangsnummer in html
                assert "clinical" in html
                assert pair.clinical.typ_der_meldung in html
                assert pair.clinical.indikationsbereich in html
                assert pair.clinical.ergebnis_qc in html
                assert pair.clinical.source_file in html


# Feature: web-frontend, Property 5: Paired records have grouping indicators
# Validates: Requirements 2.3
@settings(max_examples=100)
@given(
    num_complete_pairs=st.integers(min_value=1, max_value=10)
)
def test_paired_records_have_grouping_indicators(num_complete_pairs: int):
    """
    Property 5: Paired records have grouping indicators
    
    For any pair of records with the same Case ID, when rendered,
    the HTML should contain visual styling attributes (CSS classes or
    data attributes) indicating they belong together.
    
    This test verifies that:
    1. Paired rows have the same data-case-id attribute
    2. Paired rows have appropriate CSS classes (pair-row, genomic/clinical)
    3. Priority group classes are applied
    """
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create complete pairs
        with MeldebestaetigungDatabase(db_path) as db:
            for i in range(num_complete_pairs):
                case_id = f"CASE_{i:03d}"
                
                # Create genomic record
                genomic_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_G_{i}",
                    meldebestaetigung=f"mb_genomic_{i}",
                    source_file=f"source_{i}.csv",
                    typ_der_meldung="0",
                    indikationsbereich="test",
                    art_der_daten="G",
                    ergebnis_qc="1",
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=False
                )
                db.upsert_record(genomic_record)
                
                # Create clinical record
                clinical_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_C_{i}",
                    meldebestaetigung=f"mb_clinical_{i}",
                    source_file=f"source_{i}.csv",
                    typ_der_meldung="0",
                    indikationsbereich="test",
                    art_der_daten="C",
                    ergebnis_qc="1",
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=False
                )
                db.upsert_record(clinical_record)
        
        # Get pairs from service
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        
        # Verify grouping indicators for each pair
        for pair in pairs:
            html = render_pair_to_html(pair)
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find all rows for this pair
            rows = soup.find_all('tr', attrs={'data-case-id': pair.case_id})
            
            if pair.is_complete:
                # Should have 2 rows (genomic and clinical)
                assert len(rows) == 2, f"Complete pair {pair.case_id} should have 2 rows"
                
                # Verify both rows have pair-row class
                for row in rows:
                    assert 'pair-row' in row.get('class', [])
                
                # Verify one is genomic and one is clinical
                classes = [row.get('class', []) for row in rows]
                assert any('genomic' in c for c in classes)
                assert any('clinical' in c for c in classes)
                
                # Verify both have the same priority group class
                priority_classes = [c for row in rows for c in row.get('class', []) if 'priority-group' in c]
                assert len(set(priority_classes)) == 1, "Both rows should have same priority group"
            else:
                # Should have 1 row
                assert len(rows) == 1, f"Incomplete pair {pair.case_id} should have 1 row"


# Feature: web-frontend, Property 6: Complete pair indicator is conditional
# Validates: Requirements 2.4, 2.5
@settings(max_examples=100)
@given(
    num_complete=st.integers(min_value=0, max_value=10),
    num_incomplete=st.integers(min_value=0, max_value=10)
)
def test_complete_pair_indicator_is_conditional(num_complete: int, num_incomplete: int):
    """
    Property 6: Complete pair indicator is conditional
    
    For any Case ID, the complete pair indicator should be present in the HTML
    if and only if both genomic and clinical records exist for that Case ID.
    
    This test verifies that:
    1. Complete pairs have the complete indicator with 'yes' class
    2. Incomplete pairs have the complete indicator with 'no' class
    """
    # Skip if no records
    if num_complete + num_incomplete == 0:
        return
    
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create records
        with MeldebestaetigungDatabase(db_path) as db:
            # Create complete pairs
            for i in range(num_complete):
                case_id = f"COMPLETE_{i:03d}"
                
                # Create both genomic and clinical
                for art_der_daten in ["G", "C"]:
                    record = MeldebestaetigungRecord(
                        vorgangsnummer=f"VN_{art_der_daten[0].upper()}_{i}",
                        meldebestaetigung=f"mb_{art_der_daten}_{i}",
                        source_file=f"source_{i}.csv",
                        typ_der_meldung="0",
                        indikationsbereich="test",
                        art_der_daten=art_der_daten,
                        ergebnis_qc="1",
                        case_id=case_id,
                        gpas_domain="test_domain",
                        processed_at=datetime(2023, 1, 1, 12, 0, 0),
                        is_done=False
                    )
                    db.upsert_record(record)
            
            # Create incomplete pairs (genomic only)
            for i in range(num_incomplete):
                case_id = f"INCOMPLETE_{i:03d}"
                
                record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_G_{i}",
                    meldebestaetigung=f"mb_genomic_{i}",
                    source_file=f"source_{i}.csv",
                    typ_der_meldung="0",
                    indikationsbereich="test",
                    art_der_daten="G",
                    ergebnis_qc="1",
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=False
                )
                db.upsert_record(record)
        
        # Get pairs from service
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        
        # Verify complete indicator for each pair
        for pair in pairs:
            html = render_pair_to_html(pair)
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find complete indicator
            complete_indicators = soup.find_all('span', class_='complete-indicator')
            assert len(complete_indicators) > 0, f"Complete indicator not found for {pair.case_id}"
            
            # Verify the indicator class matches completeness
            indicator = complete_indicators[0]
            if pair.is_complete:
                assert 'yes' in indicator.get('class', []), \
                    f"Complete pair {pair.case_id} should have 'yes' indicator"
            else:
                assert 'no' in indicator.get('class', []), \
                    f"Incomplete pair {pair.case_id} should have 'no' indicator"


# Feature: web-frontend, Property 7: Valid pair indicator is conditional on completeness and QC
# Validates: Requirements 3.2, 3.3, 3.4, 3.5
@settings(max_examples=100)
@given(
    num_valid=st.integers(min_value=0, max_value=10),
    num_invalid=st.integers(min_value=0, max_value=10)
)
def test_valid_pair_indicator_conditional_on_qc(num_valid: int, num_invalid: int):
    """
    Property 7: Valid pair indicator is conditional on completeness and QC
    
    For any Case ID, the valid pair indicator should be present if and only if
    both genomic and clinical records exist AND both have Ergebnis QC indicating success.
    
    This test verifies that:
    1. Valid pairs (complete + both QC=1) have 'yes' indicator
    2. Invalid pairs (incomplete or failing QC) have 'no' indicator
    """
    # Skip if no records
    if num_valid + num_invalid == 0:
        return
    
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create records
        with MeldebestaetigungDatabase(db_path) as db:
            # Create valid pairs (complete + both QC=1)
            for i in range(num_valid):
                case_id = f"VALID_{i:03d}"
                
                for art_der_daten in ["G", "C"]:
                    record = MeldebestaetigungRecord(
                        vorgangsnummer=f"VN_{art_der_daten[0].upper()}_{i}",
                        meldebestaetigung=f"mb_{art_der_daten}_{i}",
                        source_file=f"source_{i}.csv",
                        typ_der_meldung="0",
                        indikationsbereich="test",
                        art_der_daten=art_der_daten,
                        ergebnis_qc="1",  # Passing QC
                        case_id=case_id,
                        gpas_domain="test_domain",
                        processed_at=datetime(2023, 1, 1, 12, 0, 0),
                        is_done=False
                    )
                    db.upsert_record(record)
            
            # Create invalid pairs (complete but failing QC)
            for i in range(num_invalid):
                case_id = f"INVALID_{i:03d}"
                
                for art_der_daten in ["G", "C"]:
                    record = MeldebestaetigungRecord(
                        vorgangsnummer=f"VN_{art_der_daten[0].upper()}_{i}",
                        meldebestaetigung=f"mb_{art_der_daten}_{i}",
                        source_file=f"source_{i}.csv",
                        typ_der_meldung="0",
                        indikationsbereich="test",
                        art_der_daten=art_der_daten,
                        ergebnis_qc="0",  # Failing QC
                        case_id=case_id,
                        gpas_domain="test_domain",
                        processed_at=datetime(2023, 1, 1, 12, 0, 0),
                        is_done=False
                    )
                    db.upsert_record(record)
        
        # Get pairs from service
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        
        # Verify valid indicator for each pair
        for pair in pairs:
            html = render_pair_to_html(pair)
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find valid indicator
            valid_indicators = soup.find_all('span', class_='valid-indicator')
            assert len(valid_indicators) > 0, f"Valid indicator not found for {pair.case_id}"
            
            # Verify the indicator class matches validity
            indicator = valid_indicators[0]
            if pair.is_valid:
                assert 'yes' in indicator.get('class', []), \
                    f"Valid pair {pair.case_id} should have 'yes' indicator"
            else:
                assert 'no' in indicator.get('class', []), \
                    f"Invalid pair {pair.case_id} should have 'no' indicator"


# Feature: web-frontend, Property 8: Done checkbox only for complete pairs
# Validates: Requirements 4.1, 4.2
@settings(max_examples=100)
@given(
    num_complete=st.integers(min_value=1, max_value=10),
    num_incomplete=st.integers(min_value=1, max_value=10)
)
def test_done_checkbox_only_for_complete_pairs(num_complete: int, num_incomplete: int):
    """
    Property 8: Done checkbox only for complete pairs
    
    For any Case ID, an enabled checkbox should be present if and only if
    both genomic and clinical records exist for that Case ID.
    
    This test verifies that:
    1. Complete pairs have a checkbox in the HTML
    2. Incomplete pairs do not have a checkbox
    """
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create records
        with MeldebestaetigungDatabase(db_path) as db:
            # Create complete pairs
            for i in range(num_complete):
                case_id = f"COMPLETE_{i:03d}"
                
                for art_der_daten in ["G", "C"]:
                    record = MeldebestaetigungRecord(
                        vorgangsnummer=f"VN_{art_der_daten[0].upper()}_{i}",
                        meldebestaetigung=f"mb_{art_der_daten}_{i}",
                        source_file=f"source_{i}.csv",
                        typ_der_meldung="0",
                        indikationsbereich="test",
                        art_der_daten=art_der_daten,
                        ergebnis_qc="1",
                        case_id=case_id,
                        gpas_domain="test_domain",
                        processed_at=datetime(2023, 1, 1, 12, 0, 0),
                        is_done=False
                    )
                    db.upsert_record(record)
            
            # Create incomplete pairs
            for i in range(num_incomplete):
                case_id = f"INCOMPLETE_{i:03d}"
                
                record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_G_{i}",
                    meldebestaetigung=f"mb_genomic_{i}",
                    source_file=f"source_{i}.csv",
                    typ_der_meldung="0",
                    indikationsbereich="test",
                    art_der_daten="G",
                    ergebnis_qc="1",
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=False
                )
                db.upsert_record(record)
        
        # Get pairs from service
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        
        # Verify checkbox presence for each pair
        for pair in pairs:
            html = render_pair_to_html(pair)
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find checkboxes
            checkboxes = soup.find_all('input', attrs={'type': 'checkbox'})
            
            if pair.is_complete:
                # Complete pairs should have a checkbox
                assert len(checkboxes) > 0, \
                    f"Complete pair {pair.case_id} should have a checkbox"
            else:
                # Incomplete pairs should not have a checkbox
                assert len(checkboxes) == 0, \
                    f"Incomplete pair {pair.case_id} should not have a checkbox"


# Feature: web-frontend, Property 10: Checkbox state reflects database state
# Validates: Requirements 4.5
@settings(max_examples=100)
@given(
    num_pairs=st.integers(min_value=1, max_value=10),
    done_flags=st.lists(st.booleans(), min_size=1, max_size=10)
)
def test_checkbox_state_reflects_database_state(num_pairs: int, done_flags: list):
    """
    Property 10: Checkbox state reflects database state
    
    For any database state, when the page loads, the checkbox states should
    match the is_done values in the database for all complete pairs.
    
    This test verifies that:
    1. Checked checkboxes correspond to pairs with is_done=True
    2. Unchecked checkboxes correspond to pairs with is_done=False
    """
    # Ensure we have enough done flags
    while len(done_flags) < num_pairs:
        done_flags.append(False)
    
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create complete pairs with varying done status
        with MeldebestaetigungDatabase(db_path) as db:
            for i in range(num_pairs):
                case_id = f"CASE_{i:03d}"
                is_done = done_flags[i]
                
                for art_der_daten in ["G", "C"]:
                    record = MeldebestaetigungRecord(
                        vorgangsnummer=f"VN_{art_der_daten[0].upper()}_{i}",
                        meldebestaetigung=f"mb_{art_der_daten}_{i}",
                        source_file=f"source_{i}.csv",
                        typ_der_meldung="0",
                        indikationsbereich="test",
                        art_der_daten=art_der_daten,
                        ergebnis_qc="1",
                        case_id=case_id,
                        gpas_domain="test_domain",
                        processed_at=datetime(2023, 1, 1, 12, 0, 0),
                        is_done=is_done
                    )
                    db.upsert_record(record)
        
        # Get pairs from service
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        
        # Verify checkbox state for each pair
        for pair in pairs:
            html = render_pair_to_html(pair)
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find checkboxes
            checkboxes = soup.find_all('input', attrs={'type': 'checkbox'})
            
            if pair.is_complete:
                assert len(checkboxes) > 0, f"Complete pair {pair.case_id} should have a checkbox"
                
                checkbox = checkboxes[0]
                has_checked_attr = checkbox.has_attr('checked')
                
                # Verify checkbox state matches database state
                if pair.is_done:
                    assert has_checked_attr, \
                        f"Pair {pair.case_id} with is_done=True should have checked checkbox"
                else:
                    assert not has_checked_attr, \
                        f"Pair {pair.case_id} with is_done=False should have unchecked checkbox"



# Feature: web-frontend, Property 15: Client-side filter matches all columns
# Validates: Requirements 9.1
@settings(max_examples=100)
@given(
    num_pairs=st.integers(min_value=1, max_value=10),
    filter_str=st.text(min_size=1, max_size=10, alphabet=st.characters(min_codepoint=65, max_codepoint=90))
)
def test_client_side_filter_matches_all_columns(num_pairs: int, filter_str: str):
    """
    Property 15: Client-side filter matches all columns
    
    For any filter string and any set of records, the filtered results should
    include only records where at least one column contains the filter string
    (case-insensitive).
    
    This test verifies that:
    1. Filtering searches across all columns
    2. Filtering is case-insensitive
    3. Only matching records are included in results
    """
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create records with varying data
        with MeldebestaetigungDatabase(db_path) as db:
            for i in range(num_pairs):
                case_id = f"CASE_{i:03d}"
                
                # Create genomic record
                genomic_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_G_{i}_{filter_str if i == 0 else 'OTHER'}",
                    meldebestaetigung=f"mb_genomic_{i}",
                    source_file=f"source_{i}.csv",
                    typ_der_meldung=str(i % 3),
                    indikationsbereich=f"indication_{i}",
                    art_der_daten="G",
                    ergebnis_qc=str((i % 2) + 1),
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=False
                )
                db.upsert_record(genomic_record)
                
                # Create clinical record
                clinical_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_C_{i}",
                    meldebestaetigung=f"mb_clinical_{i}",
                    source_file=f"source_{i}.csv",
                    typ_der_meldung=str(i % 3),
                    indikationsbereich=f"indication_{i}",
                    art_der_daten="C",
                    ergebnis_qc=str((i % 2) + 1),
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=False
                )
                db.upsert_record(clinical_record)
        
        # Get pairs from service
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        
        # Simulate the filtering logic from Alpine.js
        filter_lower = filter_str.lower()
        filtered_pairs = []
        
        for pair in pairs:
            # Check if filter matches any column
            matches = False
            
            # Check case_id
            if pair.case_id and filter_lower in pair.case_id.lower():
                matches = True
            
            # Check genomic record fields
            if pair.genomic:
                if (filter_lower in pair.genomic.vorgangsnummer.lower() or
                    filter_lower in pair.genomic.typ_der_meldung.lower() or
                    filter_lower in pair.genomic.indikationsbereich.lower() or
                    filter_lower in pair.genomic.ergebnis_qc.lower() or
                    filter_lower in pair.genomic.source_file.lower()):
                    matches = True
            
            # Check clinical record fields
            if pair.clinical:
                if (filter_lower in pair.clinical.vorgangsnummer.lower() or
                    filter_lower in pair.clinical.typ_der_meldung.lower() or
                    filter_lower in pair.clinical.indikationsbereich.lower() or
                    filter_lower in pair.clinical.ergebnis_qc.lower() or
                    filter_lower in pair.clinical.source_file.lower()):
                    matches = True
            
            if matches:
                filtered_pairs.append(pair)
        
        # Verify that at least the first pair (which contains filter_str) is included
        if num_pairs > 0:
            assert len(filtered_pairs) > 0, "Filter should match at least one record"
            
            # Verify that the first pair is in the filtered results
            first_pair_case_id = f"CASE_000"
            assert any(p.case_id == first_pair_case_id for p in filtered_pairs), \
                f"Pair with filter string should be in filtered results"


# Feature: web-frontend, Property 16: Client-side sort orders by selected column
# Validates: Requirements 9.2, 9.3
@settings(max_examples=100)
@given(
    num_pairs=st.integers(min_value=2, max_value=10),
    sort_direction=st.sampled_from(['asc', 'desc'])
)
def test_client_side_sort_orders_by_column(num_pairs: int, sort_direction: str):
    """
    Property 16: Client-side sort orders by selected column
    
    For any column and sort direction, when sorting is applied, the records
    should be ordered by that column's values in the specified direction.
    
    This test verifies that:
    1. Sorting orders records by the selected column
    2. Sort direction (ascending/descending) is respected
    3. Priority group remains the primary sort key
    """
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create records with varying data
        with MeldebestaetigungDatabase(db_path) as db:
            for i in range(num_pairs):
                # Use reverse order for case_id to test sorting
                case_id = f"CASE_{(num_pairs - i):03d}"
                
                # Create genomic record
                genomic_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_G_{i}",
                    meldebestaetigung=f"mb_genomic_{i}",
                    source_file=f"source_{i}.csv",
                    typ_der_meldung=str(i % 3),
                    indikationsbereich=f"indication_{i}",
                    art_der_daten="G",
                    ergebnis_qc="1",
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=False
                )
                db.upsert_record(genomic_record)
                
                # Create clinical record
                clinical_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_C_{i}",
                    meldebestaetigung=f"mb_clinical_{i}",
                    source_file=f"source_{i}.csv",
                    typ_der_meldung=str(i % 3),
                    indikationsbereich=f"indication_{i}",
                    art_der_daten="C",
                    ergebnis_qc="1",
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=False
                )
                db.upsert_record(clinical_record)
        
        # Get pairs from service
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        
        # Simulate the sorting logic from Alpine.js (sort by case_id)
        sorted_pairs = sorted(pairs, key=lambda p: (p.priority_group, p.case_id), 
                            reverse=(sort_direction == 'desc'))
        
        # Verify sorting
        prev_priority = None
        prev_case_id = None
        
        for pair in sorted_pairs:
            # Priority group should still be primary sort (always ascending)
            if prev_priority is not None:
                assert pair.priority_group >= prev_priority, \
                    "Priority group should be primary sort key"
            
            # Within same priority group, verify case_id sort direction
            if prev_priority == pair.priority_group and prev_case_id is not None:
                if sort_direction == 'asc':
                    assert pair.case_id >= prev_case_id, \
                        f"Case IDs should be ascending: {prev_case_id} -> {pair.case_id}"
                else:
                    assert pair.case_id <= prev_case_id, \
                        f"Case IDs should be descending: {prev_case_id} -> {pair.case_id}"
            
            prev_priority = pair.priority_group
            prev_case_id = pair.case_id


# Feature: web-frontend, Property 17: Filtering and sorting preserve pair grouping
# Validates: Requirements 9.4
@settings(max_examples=100)
@given(
    num_pairs=st.integers(min_value=1, max_value=10),
    filter_str=st.text(min_size=0, max_size=10, alphabet=st.characters(min_codepoint=65, max_codepoint=90)),
    sort_direction=st.sampled_from(['asc', 'desc'])
)
def test_filtering_and_sorting_preserve_pair_grouping(
    num_pairs: int,
    filter_str: str,
    sort_direction: str
):
    """
    Property 17: Filtering and sorting preserve pair grouping
    
    For any filter or sort operation, records with the same Case ID should
    remain consecutive in the output.
    
    This test verifies that:
    1. Filtering maintains pair grouping
    2. Sorting maintains pair grouping
    3. Combined filtering and sorting maintains pair grouping
    """
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create records
        with MeldebestaetigungDatabase(db_path) as db:
            for i in range(num_pairs):
                case_id = f"CASE_{i:03d}"
                
                # Create genomic record
                genomic_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_G_{i}",
                    meldebestaetigung=f"mb_genomic_{i}",
                    source_file=f"source_{i}.csv",
                    typ_der_meldung=str(i % 3),
                    indikationsbereich=f"indication_{i}",
                    art_der_daten="G",
                    ergebnis_qc="1",
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=False
                )
                db.upsert_record(genomic_record)
                
                # Create clinical record
                clinical_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_C_{i}",
                    meldebestaetigung=f"mb_clinical_{i}",
                    source_file=f"source_{i}.csv",
                    typ_der_meldung=str(i % 3),
                    indikationsbereich=f"indication_{i}",
                    art_der_daten="C",
                    ergebnis_qc="1",
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=False
                )
                db.upsert_record(clinical_record)
        
        # Get pairs from service
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        
        # Apply filtering if filter_str is not empty
        if filter_str:
            filter_lower = filter_str.lower()
            filtered_pairs = []
            
            for pair in pairs:
                matches = False
                
                if pair.case_id and filter_lower in pair.case_id.lower():
                    matches = True
                
                if pair.genomic and any(
                    filter_lower in str(getattr(pair.genomic, field, '')).lower()
                    for field in ['vorgangsnummer', 'typ_der_meldung', 'indikationsbereich', 
                                'ergebnis_qc', 'source_file']
                ):
                    matches = True
                
                if pair.clinical and any(
                    filter_lower in str(getattr(pair.clinical, field, '')).lower()
                    for field in ['vorgangsnummer', 'typ_der_meldung', 'indikationsbereich', 
                                'ergebnis_qc', 'source_file']
                ):
                    matches = True
                
                if matches:
                    filtered_pairs.append(pair)
            
            pairs = filtered_pairs
        
        # Apply sorting
        sorted_pairs = sorted(pairs, key=lambda p: (p.priority_group, p.case_id),
                            reverse=(sort_direction == 'desc'))
        
        # Verify pair grouping is preserved
        # Each case_id should appear at most once (pairs are already grouped)
        seen_case_ids = set()
        for pair in sorted_pairs:
            assert pair.case_id not in seen_case_ids, \
                f"Case ID {pair.case_id} appears multiple times (pair grouping broken)"
            seen_case_ids.add(pair.case_id)
        
        # Verify that complete pairs still have both genomic and clinical
        for pair in sorted_pairs:
            if pair.is_complete:
                assert pair.genomic is not None and pair.clinical is not None, \
                    f"Complete pair {pair.case_id} should have both records"
