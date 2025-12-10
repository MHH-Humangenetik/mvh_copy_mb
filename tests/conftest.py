"""
Pytest configuration and fixtures for the test suite.
"""
import tempfile
from datetime import datetime
from pathlib import Path

import pytest
from hypothesis import settings

from mvh_copy_mb.database import MeldebestaetigungDatabase, MeldebestaetigungRecord

# Configure Hypothesis settings for all tests
# Disable deadline to avoid flaky failures during parallel execution
settings.register_profile("default", deadline=None)
settings.load_profile("default")


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
                art_der_daten="G",
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
                art_der_daten="C",
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
                art_der_daten="G",
                ergebnis_qc="1",
                case_id="CASE_INCOMPLETE",
                gpas_domain="test_domain",
                processed_at=datetime(2023, 1, 1, 12, 0, 0),
                is_done=False
            )
            db.upsert_record(genomic_incomplete)
        
        yield db_path
