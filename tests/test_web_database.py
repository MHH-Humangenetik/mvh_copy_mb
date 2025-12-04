"""
Property-based tests for the web database service layer.

This module contains property-based tests using Hypothesis to verify
correctness properties of the web database service implementation.
"""

import tempfile
from datetime import datetime
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mvh_copy_mb.database import MeldebestaetigungDatabase, MeldebestaetigungRecord
from mvh_copy_mb.web_database import WebDatabaseService, RecordPair


# Feature: web-frontend, Property 4: Records with same Case ID are consecutive
# Validates: Requirements 2.1, 2.2
@settings(max_examples=100)
@given(
    # Generate a list of case IDs (some may be duplicates)
    case_ids=st.lists(
        st.text(min_size=1, max_size=50, alphabet=st.characters(min_codepoint=65, max_codepoint=90)),
        min_size=1,
        max_size=20
    )
)
def test_records_with_same_case_id_are_consecutive(case_ids: list):
    """
    Property 4: Records with same Case ID are consecutive
    
    For any two records sharing the same Case ID, when displayed,
    they should appear in consecutive rows with genomic before clinical.
    
    This test verifies that:
    1. Records with the same Case ID are grouped together
    2. Within each Case ID group, genomic appears before clinical
    3. The grouping is maintained in the output
    """
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create records for each case_id (both genomic and clinical)
        with MeldebestaetigungDatabase(db_path) as db:
            for i, case_id in enumerate(case_ids):
                # Create genomic record
                genomic_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_G_{case_id}_{i}",
                    meldebestaetigung=f"mb_{case_id}_genomic",
                    source_file=f"source_{i}.csv",
                    typ_der_meldung="0",
                    indikationsbereich="test",
                    art_der_daten="genomic",
                    ergebnis_qc="1",
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=False
                )
                db.upsert_record(genomic_record)
                
                # Create clinical record
                clinical_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_C_{case_id}_{i}",
                    meldebestaetigung=f"mb_{case_id}_clinical",
                    source_file=f"source_{i}.csv",
                    typ_der_meldung="0",
                    indikationsbereich="test",
                    art_der_daten="clinical",
                    ergebnis_qc="1",
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=False
                )
                db.upsert_record(clinical_record)
        
        # Get grouped records
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        
        # Verify that each pair has both genomic and clinical records
        for pair in pairs:
            # If the pair is complete, verify both records exist
            if pair.is_complete:
                assert pair.genomic is not None
                assert pair.clinical is not None
                assert pair.genomic.case_id == pair.case_id
                assert pair.clinical.case_id == pair.case_id
                assert pair.genomic.art_der_daten.lower() == "genomic"
                assert pair.clinical.art_der_daten.lower() == "clinical"
        
        # Verify that pairs are grouped by case_id (consecutive case_ids should be the same or different)
        # This means if we see case_id X, then case_id Y, we should never see case_id X again
        seen_case_ids = set()
        for pair in pairs:
            if pair.case_id in seen_case_ids:
                # If we've seen this case_id before, it should be the immediately previous one
                # This would indicate non-consecutive grouping, which violates the property
                # However, since we're grouping by case_id, each case_id should appear exactly once
                pytest.fail(f"Case ID {pair.case_id} appears multiple times in the output")
            seen_case_ids.add(pair.case_id)



# Feature: web-frontend, Property 12: Priority group 1 contains complete pairs not done
# Validates: Requirements 8.2
@settings(max_examples=100)
@given(
    num_pairs=st.integers(min_value=1, max_value=20),
    done_flags=st.lists(st.booleans(), min_size=1, max_size=20)
)
def test_priority_group_1_contains_complete_pairs_not_done(num_pairs: int, done_flags: list):
    """
    Property 12: Priority group 1 contains complete pairs not done
    
    For any record in priority group 1, it should have both genomic and clinical
    records present and is_done should be False for both.
    
    This test verifies that:
    1. All pairs in priority group 1 are complete (both genomic and clinical)
    2. All pairs in priority group 1 have is_done = False
    """
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Ensure we have enough done_flags
        while len(done_flags) < num_pairs:
            done_flags.append(False)
        
        # Create complete pairs with varying done status
        with MeldebestaetigungDatabase(db_path) as db:
            for i in range(num_pairs):
                case_id = f"CASE_{i:03d}"
                is_done = done_flags[i]
                
                # Create genomic record
                genomic_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_G_{i}",
                    meldebestaetigung=f"mb_genomic_{i}",
                    source_file=f"source_{i}.csv",
                    typ_der_meldung="0",
                    indikationsbereich="test",
                    art_der_daten="genomic",
                    ergebnis_qc="1",
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=is_done
                )
                db.upsert_record(genomic_record)
                
                # Create clinical record
                clinical_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_C_{i}",
                    meldebestaetigung=f"mb_clinical_{i}",
                    source_file=f"source_{i}.csv",
                    typ_der_meldung="0",
                    indikationsbereich="test",
                    art_der_daten="clinical",
                    ergebnis_qc="1",
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=is_done
                )
                db.upsert_record(clinical_record)
        
        # Get grouped records
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        
        # Filter to priority group 1
        group_1_pairs = [p for p in pairs if p.priority_group == 1]
        
        # Verify all pairs in group 1 are complete and not done
        for pair in group_1_pairs:
            assert pair.is_complete, f"Pair {pair.case_id} in group 1 is not complete"
            assert not pair.is_done, f"Pair {pair.case_id} in group 1 is marked done"
            assert pair.genomic is not None
            assert pair.clinical is not None
            assert not pair.genomic.is_done
            assert not pair.clinical.is_done


# Feature: web-frontend, Property 13: Priority group 2 contains incomplete pairs
# Validates: Requirements 8.3
@settings(max_examples=100)
@given(
    num_incomplete=st.integers(min_value=1, max_value=20),
    has_genomic_flags=st.lists(st.booleans(), min_size=1, max_size=20)
)
def test_priority_group_2_contains_incomplete_pairs(num_incomplete: int, has_genomic_flags: list):
    """
    Property 13: Priority group 2 contains incomplete pairs
    
    For any record in priority group 2, it should have either only genomic
    or only clinical record (not both).
    
    This test verifies that:
    1. All pairs in priority group 2 are incomplete
    2. Each incomplete pair has exactly one record (genomic OR clinical)
    """
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Ensure we have enough flags
        while len(has_genomic_flags) < num_incomplete:
            has_genomic_flags.append(True)
        
        # Create incomplete pairs
        with MeldebestaetigungDatabase(db_path) as db:
            for i in range(num_incomplete):
                case_id = f"CASE_{i:03d}"
                has_genomic = has_genomic_flags[i]
                
                if has_genomic:
                    # Create only genomic record
                    genomic_record = MeldebestaetigungRecord(
                        vorgangsnummer=f"VN_G_{i}",
                        meldebestaetigung=f"mb_genomic_{i}",
                        source_file=f"source_{i}.csv",
                        typ_der_meldung="0",
                        indikationsbereich="test",
                        art_der_daten="genomic",
                        ergebnis_qc="1",
                        case_id=case_id,
                        gpas_domain="test_domain",
                        processed_at=datetime(2023, 1, 1, 12, 0, 0),
                        is_done=False
                    )
                    db.upsert_record(genomic_record)
                else:
                    # Create only clinical record
                    clinical_record = MeldebestaetigungRecord(
                        vorgangsnummer=f"VN_C_{i}",
                        meldebestaetigung=f"mb_clinical_{i}",
                        source_file=f"source_{i}.csv",
                        typ_der_meldung="0",
                        indikationsbereich="test",
                        art_der_daten="clinical",
                        ergebnis_qc="1",
                        case_id=case_id,
                        gpas_domain="test_domain",
                        processed_at=datetime(2023, 1, 1, 12, 0, 0),
                        is_done=False
                    )
                    db.upsert_record(clinical_record)
        
        # Get grouped records
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        
        # Filter to priority group 2
        group_2_pairs = [p for p in pairs if p.priority_group == 2]
        
        # Verify all pairs in group 2 are incomplete
        for pair in group_2_pairs:
            assert not pair.is_complete, f"Pair {pair.case_id} in group 2 is complete"
            # Exactly one of genomic or clinical should be present
            assert (pair.genomic is None) != (pair.clinical is None), \
                f"Pair {pair.case_id} should have exactly one record"


# Feature: web-frontend, Property 14: Priority group 3 contains complete pairs done
# Validates: Requirements 8.4
@settings(max_examples=100)
@given(
    num_pairs=st.integers(min_value=1, max_value=20),
    done_flags=st.lists(st.booleans(), min_size=1, max_size=20)
)
def test_priority_group_3_contains_complete_pairs_done(num_pairs: int, done_flags: list):
    """
    Property 14: Priority group 3 contains complete pairs done
    
    For any record in priority group 3, it should have both genomic and clinical
    records present and is_done should be True for both.
    
    This test verifies that:
    1. All pairs in priority group 3 are complete (both genomic and clinical)
    2. All pairs in priority group 3 have is_done = True
    """
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Ensure we have enough done_flags
        while len(done_flags) < num_pairs:
            done_flags.append(True)
        
        # Create complete pairs with varying done status
        with MeldebestaetigungDatabase(db_path) as db:
            for i in range(num_pairs):
                case_id = f"CASE_{i:03d}"
                is_done = done_flags[i]
                
                # Create genomic record
                genomic_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_G_{i}",
                    meldebestaetigung=f"mb_genomic_{i}",
                    source_file=f"source_{i}.csv",
                    typ_der_meldung="0",
                    indikationsbereich="test",
                    art_der_daten="genomic",
                    ergebnis_qc="1",
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=is_done
                )
                db.upsert_record(genomic_record)
                
                # Create clinical record
                clinical_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_C_{i}",
                    meldebestaetigung=f"mb_clinical_{i}",
                    source_file=f"source_{i}.csv",
                    typ_der_meldung="0",
                    indikationsbereich="test",
                    art_der_daten="clinical",
                    ergebnis_qc="1",
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=is_done
                )
                db.upsert_record(clinical_record)
        
        # Get grouped records
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        
        # Filter to priority group 3
        group_3_pairs = [p for p in pairs if p.priority_group == 3]
        
        # Verify all pairs in group 3 are complete and done
        for pair in group_3_pairs:
            assert pair.is_complete, f"Pair {pair.case_id} in group 3 is not complete"
            assert pair.is_done, f"Pair {pair.case_id} in group 3 is not marked done"
            assert pair.genomic is not None
            assert pair.clinical is not None
            assert pair.genomic.is_done
            assert pair.clinical.is_done



# Feature: web-frontend, Property 9: Done status update affects both records in pair
# Validates: Requirements 4.3
@settings(max_examples=100)
@given(
    case_id=st.text(min_size=1, max_size=50, alphabet=st.characters(min_codepoint=65, max_codepoint=90)),
    initial_done=st.booleans(),
    updated_done=st.booleans()
)
def test_done_status_update_affects_both_records_in_pair(
    case_id: str,
    initial_done: bool,
    updated_done: bool
):
    """
    Property 9: Done status update affects both records in pair
    
    For any complete pair (Case ID with both genomic and clinical records),
    when the done status is updated, both records in the database should have
    their is_done field set to the same value.
    
    This test verifies that:
    1. Updating done status affects both genomic and clinical records
    2. Both records have the same is_done value after update
    3. The update persists in the database
    """
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create a complete pair with initial done status
        with MeldebestaetigungDatabase(db_path) as db:
            # Create genomic record
            genomic_record = MeldebestaetigungRecord(
                vorgangsnummer=f"VN_G_{case_id}",
                meldebestaetigung=f"mb_genomic_{case_id}",
                source_file="source.csv",
                typ_der_meldung="0",
                indikationsbereich="test",
                art_der_daten="genomic",
                ergebnis_qc="1",
                case_id=case_id,
                gpas_domain="test_domain",
                processed_at=datetime(2023, 1, 1, 12, 0, 0),
                is_done=initial_done
            )
            db.upsert_record(genomic_record)
            
            # Create clinical record
            clinical_record = MeldebestaetigungRecord(
                vorgangsnummer=f"VN_C_{case_id}",
                meldebestaetigung=f"mb_clinical_{case_id}",
                source_file="source.csv",
                typ_der_meldung="0",
                indikationsbereich="test",
                art_der_daten="clinical",
                ergebnis_qc="1",
                case_id=case_id,
                gpas_domain="test_domain",
                processed_at=datetime(2023, 1, 1, 12, 0, 0),
                is_done=initial_done
            )
            db.upsert_record(clinical_record)
        
        # Update the done status using the service
        service = WebDatabaseService(db_path)
        result = service.update_pair_done_status(case_id, updated_done)
        
        # Verify the update was successful
        assert result is True
        
        # Verify both records have the updated done status
        with MeldebestaetigungDatabase(db_path) as db:
            genomic_retrieved = db.get_record(f"VN_G_{case_id}")
            clinical_retrieved = db.get_record(f"VN_C_{case_id}")
            
            assert genomic_retrieved is not None
            assert clinical_retrieved is not None
            
            # Both should have the updated done status
            assert genomic_retrieved.is_done == updated_done
            assert clinical_retrieved.is_done == updated_done
            
            # Both should have the same done status
            assert genomic_retrieved.is_done == clinical_retrieved.is_done
