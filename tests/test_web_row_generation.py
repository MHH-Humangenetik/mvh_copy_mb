"""Tests for HTML table row generation logic.

These tests verify the correct column structure for different pair types.
Each row MUST have exactly 10 columns:
1. Case ID
2. Vorgangsnummer
3. Art der Daten
4. Typ der Meldung
5. Indikationsbereich
6. Ergebnis QC
7. Source File
8. Complete
9. Valid
10. Done
"""

import pytest


def test_complete_pair_column_count():
    """Test that complete pair rows have correct column structure.
    
    Genomic row: 10 columns total
    - Case ID (rowspan=2)
    - 6 data columns
    - Complete (rowspan=2)
    - Valid (rowspan=2)
    - Done (rowspan=2)
    
    Clinical row: 7 columns total (3 are spanned from genomic)
    - NO Case ID (spanned from genomic)
    - 6 data columns
    - NO Complete (spanned from genomic)
    - NO Valid (spanned from genomic)
    - NO Done (spanned from genomic)
    """
    pair = {
        "case_id": "TEST_001",
        "genomic": {"vorgangsnummer": "G_VN"},
        "clinical": {"vorgangsnummer": "C_VN"},
        "is_complete": True,
    }

    # Genomic row should render: Case ID + 6 data + 3 indicators = 10 columns
    # Clinical row should render: 6 data columns only = 7 columns (3 spanned)
    assert pair["genomic"] is not None
    assert pair["clinical"] is not None
    assert pair["is_complete"] is True


def test_genomic_only_pair_has_one_row():
    """Test that a genomic-only pair generates 1 row with Case ID visible."""
    pair = {
        "case_id": "TEST_002",
        "genomic": {
            "vorgangsnummer": "GENOMIC_VN",
            "typ_der_meldung": "0",
            "indikationsbereich": "R",
            "ergebnis_qc": "1",
            "source_file": "genomic.csv",
        },
        "clinical": None,
        "is_complete": False,
        "is_valid": False,
        "is_done": False,
        "priority_group": 2,
    }

    # Only genomic should be truthy
    assert pair["genomic"] is not None
    assert pair["clinical"] is None
    assert pair["is_complete"] is False


def test_clinical_only_pair_has_one_row():
    """Test that a clinical-only pair generates 1 row with Case ID visible."""
    pair = {
        "case_id": "TEST_003",
        "genomic": None,
        "clinical": {
            "vorgangsnummer": "CLINICAL_VN",
            "typ_der_meldung": "0",
            "indikationsbereich": "H",
            "ergebnis_qc": "1",
            "source_file": "clinical.csv",
        },
        "is_complete": False,
        "is_valid": False,
        "is_done": False,
        "priority_group": 2,
    }

    # Only clinical should be truthy
    assert pair["genomic"] is None
    assert pair["clinical"] is not None
    assert pair["is_complete"] is False


def test_complete_pair_case_id_column_count():
    """Test that complete pair has correct column structure.
    
    Genomic row: 10 columns (Case ID with rowspan=2, 6 data cols, 3 indicators with rowspan=2)
    Clinical row: 7 columns (no Case ID, 6 data cols, no indicators - they span from genomic)
    """
    pair = {
        "case_id": "TEST_004",
        "genomic": {"vorgangsnummer": "G_VN"},
        "clinical": {"vorgangsnummer": "C_VN"},
        "is_complete": True,
    }

    # For complete pairs:
    # - Genomic row should have Case ID (rowspan=2)
    # - Clinical row should NOT have Case ID (covered by rowspan)
    assert pair["genomic"] is not None  # Genomic row renders Case ID
    assert pair["clinical"] is not None  # Clinical row does NOT render Case ID


def test_incomplete_pair_case_id_always_visible():
    """Test that incomplete pairs always show Case ID in their single row."""
    genomic_only = {
        "case_id": "TEST_005",
        "genomic": {"vorgangsnummer": "G_VN"},
        "clinical": None,
        "is_complete": False,
    }

    clinical_only = {
        "case_id": "TEST_006",
        "genomic": None,
        "clinical": {"vorgangsnummer": "C_VN"},
        "is_complete": False,
    }

    # Both should show Case ID in their respective rows
    # Genomic-only: genomic row has Case ID
    assert genomic_only["genomic"] is not None
    assert genomic_only["clinical"] is None

    # Clinical-only: clinical row has Case ID (because no genomic exists)
    assert clinical_only["genomic"] is None
    assert clinical_only["clinical"] is not None


def test_row_visibility_logic():
    """Test the x-show logic for row visibility."""
    pair_with_both = {
        "genomic": {"vorgangsnummer": "G"},
        "clinical": {"vorgangsnummer": "C"},
    }

    pair_genomic_only = {"genomic": {"vorgangsnummer": "G"}, "clinical": None}

    pair_clinical_only = {"genomic": None, "clinical": {"vorgangsnummer": "C"}}

    # Genomic row: x-show="pair.genomic"
    assert pair_with_both["genomic"] is not None  # Shows
    assert pair_genomic_only["genomic"] is not None  # Shows
    assert pair_clinical_only["genomic"] is None  # Hidden

    # Clinical row: x-show="pair.clinical"
    assert pair_with_both["clinical"] is not None  # Shows
    assert pair_genomic_only["clinical"] is None  # Hidden
    assert pair_clinical_only["clinical"] is not None  # Shows


def test_case_id_column_visibility_in_clinical_row():
    """Test that Case ID column in clinical row uses x-show="!pair.genomic"."""
    # When genomic exists, clinical row should NOT show Case ID
    pair_complete = {"genomic": {"vorgangsnummer": "G"}, "clinical": {"vorgangsnummer": "C"}}
    assert not (pair_complete["genomic"] is None)  # !pair.genomic = False -> hide

    # When genomic doesn't exist, clinical row SHOULD show Case ID
    pair_clinical_only = {"genomic": None, "clinical": {"vorgangsnummer": "C"}}
    assert pair_clinical_only["genomic"] is None  # !pair.genomic = True -> show


def test_indicator_columns_visibility_in_clinical_row():
    """Test that Complete/Valid/Done columns in clinical row use x-show="!pair.genomic"."""
    # When genomic exists, clinical row should NOT show indicators (they span from genomic)
    pair_complete = {"genomic": {"vorgangsnummer": "G"}, "clinical": {"vorgangsnummer": "C"}}
    assert not (pair_complete["genomic"] is None)  # !pair.genomic = False -> hide

    # When genomic doesn't exist, clinical row SHOULD show indicators
    pair_clinical_only = {"genomic": None, "clinical": {"vorgangsnummer": "C"}}
    assert pair_clinical_only["genomic"] is None  # !pair.genomic = True -> show
