"""Tests for HTML table row generation logic.

These tests verify the correct column structure for different pair types.
Each row MUST have exactly 11 columns:
1. Case ID
2. Vorgangsnummer
3. IBE String (Meldebestaetigung)
4. Art der Daten
5. Typ der Meldung
6. Indikationsbereich
7. Ergebnis QC
8. Source File
9. Complete
10. Valid
11. Done
"""

import tempfile
from datetime import datetime
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from mvh_copy_mb.database import MeldebestaetigungDatabase, MeldebestaetigungRecord
from mvh_copy_mb.web_database import WebDatabaseService


def render_pair_to_html(pair) -> str:
    """
    Helper function to render a RecordPair to HTML matching the template logic.
    This simulates what the Alpine.js template renders.
    """
    html_parts = []
    
    # Complete pair: both genomic and clinical
    if pair.genomic and pair.clinical:
        # Genomic row with rowspan
        genomic_row = f"""
        <tr class="pair-row genomic priority-group-{pair.priority_group}" data-case-id="{pair.case_id}">
            <td rowspan="2" class="case-id-cell">{pair.case_id}</td>
            <td>{pair.genomic.vorgangsnummer}</td>
            <td>{pair.genomic.meldebestaetigung}</td>
            <td>genomic</td>
            <td>{pair.genomic.typ_der_meldung}</td>
            <td>{pair.genomic.indikationsbereich}</td>
            <td>{pair.genomic.ergebnis_qc}</td>
            <td>{pair.genomic.source_file}</td>
            <td rowspan="2"><span class="complete-indicator {'yes' if pair.is_complete else 'no'}"></span></td>
            <td rowspan="2"><span class="valid-indicator {'yes' if pair.is_valid else 'no'}"></span></td>
            <td rowspan="2" class="done-cell"><input type="checkbox" {'checked' if pair.is_done else ''}></td>
        </tr>
        """
        html_parts.append(genomic_row)
        
        # Clinical row (no Case ID, no indicators - they span from genomic)
        clinical_row = f"""
        <tr class="pair-row clinical priority-group-{pair.priority_group}" data-case-id="{pair.case_id}">
            <td>{pair.clinical.vorgangsnummer}</td>
            <td>{pair.clinical.meldebestaetigung}</td>
            <td>clinical</td>
            <td>{pair.clinical.typ_der_meldung}</td>
            <td>{pair.clinical.indikationsbereich}</td>
            <td>{pair.clinical.ergebnis_qc}</td>
            <td>{pair.clinical.source_file}</td>
        </tr>
        """
        html_parts.append(clinical_row)
    
    # Genomic only: single row with all columns
    elif pair.genomic and not pair.clinical:
        genomic_row = f"""
        <tr class="pair-row genomic priority-group-{pair.priority_group}" data-case-id="{pair.case_id}">
            <td class="case-id-cell">{pair.case_id}</td>
            <td>{pair.genomic.vorgangsnummer}</td>
            <td>{pair.genomic.meldebestaetigung}</td>
            <td>genomic</td>
            <td>{pair.genomic.typ_der_meldung}</td>
            <td>{pair.genomic.indikationsbereich}</td>
            <td>{pair.genomic.ergebnis_qc}</td>
            <td>{pair.genomic.source_file}</td>
            <td><span class="complete-indicator {'yes' if pair.is_complete else 'no'}"></span></td>
            <td><span class="valid-indicator {'yes' if pair.is_valid else 'no'}"></span></td>
            <td class="done-cell"><span>—</span></td>
        </tr>
        """
        html_parts.append(genomic_row)
    
    # Clinical only: single row with all columns
    elif not pair.genomic and pair.clinical:
        clinical_row = f"""
        <tr class="pair-row clinical priority-group-{pair.priority_group}" data-case-id="{pair.case_id}">
            <td class="case-id-cell">{pair.case_id}</td>
            <td>{pair.clinical.vorgangsnummer}</td>
            <td>{pair.clinical.meldebestaetigung}</td>
            <td>clinical</td>
            <td>{pair.clinical.typ_der_meldung}</td>
            <td>{pair.clinical.indikationsbereich}</td>
            <td>{pair.clinical.ergebnis_qc}</td>
            <td>{pair.clinical.source_file}</td>
            <td><span class="complete-indicator {'yes' if pair.is_complete else 'no'}"></span></td>
            <td><span class="valid-indicator {'yes' if pair.is_valid else 'no'}"></span></td>
            <td class="done-cell"><span>—</span></td>
        </tr>
        """
        html_parts.append(clinical_row)
    
    return ''.join(html_parts)


def test_complete_pair_column_count():
    """Test that complete pair rows have correct column structure.
    
    Genomic row: 11 columns total
    - Case ID (rowspan=2)
    - 7 data columns (Vorgangsnummer, IBE String, Art der Daten, Typ, Indikation, QC, Source)
    - Complete (rowspan=2)
    - Valid (rowspan=2)
    - Done (rowspan=2)
    
    Clinical row: 7 columns total (4 are spanned from genomic)
    - NO Case ID (spanned from genomic)
    - 7 data columns
    - NO Complete (spanned from genomic)
    - NO Valid (spanned from genomic)
    - NO Done (spanned from genomic)
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        with MeldebestaetigungDatabase(db_path) as db:
            # Create complete pair
            for art_der_daten in ["G", "C"]:
                record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_{art_der_daten}",
                    meldebestaetigung=f"mb_{art_der_daten}",
                    source_file="test.csv",
                    typ_der_meldung="0",
                    indikationsbereich="R",
                    art_der_daten=art_der_daten,
                    ergebnis_qc="1",
                    case_id="TEST_001",
                    gpas_domain="test",
                    processed_at=datetime(2023, 1, 1),
                    is_done=False
                )
                db.upsert_record(record)
        
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        assert len(pairs) == 1
        
        pair = pairs[0]
        html = render_pair_to_html(pair)
        soup = BeautifulSoup(html, 'html.parser')
        
        rows = soup.find_all('tr')
        assert len(rows) == 2, "Complete pair should have 2 rows"
        
        # Genomic row: 11 columns
        genomic_row = rows[0]
        genomic_cols = genomic_row.find_all('td')
        assert len(genomic_cols) == 11, f"Genomic row should have 11 columns, got {len(genomic_cols)}"
        
        # Clinical row: 7 columns (4 spanned from genomic)
        clinical_row = rows[1]
        clinical_cols = clinical_row.find_all('td')
        assert len(clinical_cols) == 7, f"Clinical row should have 7 columns, got {len(clinical_cols)}"
        
        # Verify rowspan attributes on genomic row
        assert genomic_cols[0].get('rowspan') == '2', "Case ID should have rowspan=2"
        assert genomic_cols[8].get('rowspan') == '2', "Complete indicator should have rowspan=2"
        assert genomic_cols[9].get('rowspan') == '2', "Valid indicator should have rowspan=2"
        assert genomic_cols[10].get('rowspan') == '2', "Done cell should have rowspan=2"


def test_genomic_only_pair_has_one_row():
    """Test that a genomic-only pair generates 1 row with 11 columns and Case ID visible."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        with MeldebestaetigungDatabase(db_path) as db:
            record = MeldebestaetigungRecord(
                vorgangsnummer="GENOMIC_VN",
                meldebestaetigung="mb_genomic",
                source_file="genomic.csv",
                typ_der_meldung="0",
                indikationsbereich="R",
                art_der_daten="G",
                ergebnis_qc="1",
                case_id="TEST_002",
                gpas_domain="test",
                processed_at=datetime(2023, 1, 1),
                is_done=False
            )
            db.upsert_record(record)
        
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        assert len(pairs) == 1
        
        pair = pairs[0]
        html = render_pair_to_html(pair)
        soup = BeautifulSoup(html, 'html.parser')
        
        rows = soup.find_all('tr')
        assert len(rows) == 1, "Genomic-only pair should have 1 row"
        
        # Should have 11 columns
        cols = rows[0].find_all('td')
        assert len(cols) == 11, f"Genomic-only row should have 11 columns, got {len(cols)}"
        
        # Case ID should be visible (no rowspan)
        case_id_cell = cols[0]
        assert case_id_cell.get('class') == ['case-id-cell']
        assert case_id_cell.text.strip() == "TEST_002"
        assert case_id_cell.get('rowspan') is None, "Single row should not have rowspan"


def test_clinical_only_pair_has_one_row():
    """Test that a clinical-only pair generates 1 row with 11 columns and Case ID visible."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        with MeldebestaetigungDatabase(db_path) as db:
            record = MeldebestaetigungRecord(
                vorgangsnummer="CLINICAL_VN",
                meldebestaetigung="mb_clinical",
                source_file="clinical.csv",
                typ_der_meldung="0",
                indikationsbereich="H",
                art_der_daten="C",
                ergebnis_qc="1",
                case_id="TEST_003",
                gpas_domain="test",
                processed_at=datetime(2023, 1, 1),
                is_done=False
            )
            db.upsert_record(record)
        
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        assert len(pairs) == 1
        
        pair = pairs[0]
        html = render_pair_to_html(pair)
        soup = BeautifulSoup(html, 'html.parser')
        
        rows = soup.find_all('tr')
        assert len(rows) == 1, "Clinical-only pair should have 1 row"
        
        # Should have 11 columns
        cols = rows[0].find_all('td')
        assert len(cols) == 11, f"Clinical-only row should have 11 columns, got {len(cols)}"
        
        # Case ID should be visible
        case_id_cell = cols[0]
        assert case_id_cell.get('class') == ['case-id-cell']
        assert case_id_cell.text.strip() == "TEST_003"
        assert case_id_cell.get('rowspan') is None, "Single row should not have rowspan"


def test_complete_pair_case_id_rowspan():
    """Test that complete pair has Case ID with rowspan=2 in genomic row only."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        with MeldebestaetigungDatabase(db_path) as db:
            for art_der_daten in ["G", "C"]:
                record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_{art_der_daten}",
                    meldebestaetigung=f"mb_{art_der_daten}",
                    source_file="test.csv",
                    typ_der_meldung="0",
                    indikationsbereich="R",
                    art_der_daten=art_der_daten,
                    ergebnis_qc="1",
                    case_id="TEST_004",
                    gpas_domain="test",
                    processed_at=datetime(2023, 1, 1),
                    is_done=False
                )
                db.upsert_record(record)
        
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        pair = pairs[0]
        html = render_pair_to_html(pair)
        soup = BeautifulSoup(html, 'html.parser')
        
        rows = soup.find_all('tr')
        
        # Genomic row should have Case ID with rowspan=2
        genomic_cols = rows[0].find_all('td')
        assert genomic_cols[0].get('rowspan') == '2'
        assert genomic_cols[0].text.strip() == "TEST_004"
        
        # Clinical row should NOT have Case ID column
        clinical_cols = rows[1].find_all('td')
        # First column should be vorgangsnummer, not case_id
        assert "VN_C" in clinical_cols[0].text
        # Second column should be meldebestaetigung
        assert "mb_" in clinical_cols[1].text


def test_incomplete_pair_no_checkbox():
    """Test that incomplete pairs show a dash instead of checkbox in Done column."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        with MeldebestaetigungDatabase(db_path) as db:
            record = MeldebestaetigungRecord(
                vorgangsnummer="VN_G",
                meldebestaetigung="mb_genomic",
                source_file="test.csv",
                typ_der_meldung="0",
                indikationsbereich="R",
                art_der_daten="G",
                ergebnis_qc="1",
                case_id="TEST_005",
                gpas_domain="test",
                processed_at=datetime(2023, 1, 1),
                is_done=False
            )
            db.upsert_record(record)
        
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        pair = pairs[0]
        html = render_pair_to_html(pair)
        soup = BeautifulSoup(html, 'html.parser')
        
        # Should not have checkbox
        checkboxes = soup.find_all('input', attrs={'type': 'checkbox'})
        assert len(checkboxes) == 0, "Incomplete pair should not have checkbox"
        
        # Should have dash in Done column
        done_cell = soup.find('td', class_='done-cell')
        assert done_cell is not None
        assert '—' in done_cell.text


def test_complete_pair_has_checkbox():
    """Test that complete pairs have a checkbox in the Done column."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        with MeldebestaetigungDatabase(db_path) as db:
            for art_der_daten in ["G", "C"]:
                record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_{art_der_daten}",
                    meldebestaetigung=f"mb_{art_der_daten}",
                    source_file="test.csv",
                    typ_der_meldung="0",
                    indikationsbereich="R",
                    art_der_daten=art_der_daten,
                    ergebnis_qc="1",
                    case_id="TEST_006",
                    gpas_domain="test",
                    processed_at=datetime(2023, 1, 1),
                    is_done=True
                )
                db.upsert_record(record)
        
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        pair = pairs[0]
        html = render_pair_to_html(pair)
        soup = BeautifulSoup(html, 'html.parser')
        
        # Should have checkbox
        checkboxes = soup.find_all('input', attrs={'type': 'checkbox'})
        assert len(checkboxes) == 1, "Complete pair should have 1 checkbox"
        
        # Checkbox should be checked
        assert checkboxes[0].has_attr('checked'), "Checkbox should be checked when is_done=True"


def test_indicator_columns_have_rowspan():
    """Test that Complete/Valid/Done columns have rowspan=2 in complete pairs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        with MeldebestaetigungDatabase(db_path) as db:
            for art_der_daten in ["G", "C"]:
                record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_{art_der_daten}",
                    meldebestaetigung=f"mb_{art_der_daten}",
                    source_file="test.csv",
                    typ_der_meldung="0",
                    indikationsbereich="R",
                    art_der_daten=art_der_daten,
                    ergebnis_qc="1",
                    case_id="TEST_007",
                    gpas_domain="test",
                    processed_at=datetime(2023, 1, 1),
                    is_done=False
                )
                db.upsert_record(record)
        
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        pair = pairs[0]
        html = render_pair_to_html(pair)
        soup = BeautifulSoup(html, 'html.parser')
        
        rows = soup.find_all('tr')
        genomic_cols = rows[0].find_all('td')
        
        # Columns 8, 9, 10 (Complete, Valid, Done) should have rowspan=2
        assert genomic_cols[8].get('rowspan') == '2', "Complete indicator should have rowspan=2"
        assert genomic_cols[9].get('rowspan') == '2', "Valid indicator should have rowspan=2"
        assert genomic_cols[10].get('rowspan') == '2', "Done cell should have rowspan=2"
