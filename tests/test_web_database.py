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
                    vorgangsnummer=f"VN_C_{case_id}_{i}",
                    meldebestaetigung=f"mb_{case_id}_clinical",
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
                assert pair.genomic.art_der_daten.lower() == "g"
                assert pair.clinical.art_der_daten.lower() == "c"
        
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
                    art_der_daten="G",
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
                    art_der_daten="C",
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
                        art_der_daten="G",
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
                        art_der_daten="C",
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
                    art_der_daten="G",
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
                    art_der_daten="C",
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
                art_der_daten="G",
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
                art_der_daten="C",
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



# Feature: web-frontend, Property 2: Records are sorted by priority then Case ID then data type
# Validates: Requirements 1.3, 8.1, 8.5
@settings(max_examples=100)
@given(
    num_complete_not_done=st.integers(min_value=0, max_value=10),
    num_incomplete=st.integers(min_value=0, max_value=10),
    num_complete_done=st.integers(min_value=0, max_value=10)
)
def test_records_sorted_by_priority_then_case_id(
    num_complete_not_done: int,
    num_incomplete: int,
    num_complete_done: int
):
    """
    Property 2: Records are sorted by priority then Case ID then data type
    
    For any set of Meldebestaetigung records, when displayed, they should be
    ordered first by priority group (1, 2, 3), then by Case ID, then by
    Art der Daten with genomic before clinical.
    
    This test verifies that:
    1. Priority group 1 records appear before group 2
    2. Priority group 2 records appear before group 3
    3. Within each priority group, records are sorted by Case ID
    4. For complete pairs, genomic and clinical are consecutive
    """
    # Skip if no records to test
    if num_complete_not_done + num_incomplete + num_complete_done == 0:
        return
    
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create records for each priority group
        with MeldebestaetigungDatabase(db_path) as db:
            record_idx = 0
            
            # Priority group 1: Complete pairs not done
            for i in range(num_complete_not_done):
                case_id = f"CASE_G1_{i:03d}"
                
                # Create genomic record
                genomic_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_G_{record_idx}",
                    meldebestaetigung=f"mb_genomic_{record_idx}",
                    source_file=f"source_{record_idx}.csv",
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
                record_idx += 1
                
                # Create clinical record
                clinical_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_C_{record_idx}",
                    meldebestaetigung=f"mb_clinical_{record_idx}",
                    source_file=f"source_{record_idx}.csv",
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
                record_idx += 1
            
            # Priority group 2: Incomplete pairs
            for i in range(num_incomplete):
                case_id = f"CASE_G2_{i:03d}"
                
                # Create only genomic record (incomplete)
                genomic_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_G_{record_idx}",
                    meldebestaetigung=f"mb_genomic_{record_idx}",
                    source_file=f"source_{record_idx}.csv",
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
                record_idx += 1
            
            # Priority group 3: Complete pairs done
            for i in range(num_complete_done):
                case_id = f"CASE_G3_{i:03d}"
                
                # Create genomic record
                genomic_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_G_{record_idx}",
                    meldebestaetigung=f"mb_genomic_{record_idx}",
                    source_file=f"source_{record_idx}.csv",
                    typ_der_meldung="0",
                    indikationsbereich="test",
                    art_der_daten="G",
                    ergebnis_qc="1",
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=True
                )
                db.upsert_record(genomic_record)
                record_idx += 1
                
                # Create clinical record
                clinical_record = MeldebestaetigungRecord(
                    vorgangsnummer=f"VN_C_{record_idx}",
                    meldebestaetigung=f"mb_clinical_{record_idx}",
                    source_file=f"source_{record_idx}.csv",
                    typ_der_meldung="0",
                    indikationsbereich="test",
                    art_der_daten="C",
                    ergebnis_qc="1",
                    case_id=case_id,
                    gpas_domain="test_domain",
                    processed_at=datetime(2023, 1, 1, 12, 0, 0),
                    is_done=True
                )
                db.upsert_record(clinical_record)
                record_idx += 1
        
        # Get grouped records
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        
        # Verify sorting by priority group
        prev_priority = 0
        prev_case_id = ""
        
        for pair in pairs:
            # Priority groups should be in ascending order (1, 2, 3)
            assert pair.priority_group >= prev_priority, \
                f"Priority groups not in order: {prev_priority} -> {pair.priority_group}"
            
            # Within the same priority group, case IDs should be sorted
            if pair.priority_group == prev_priority:
                assert pair.case_id >= prev_case_id, \
                    f"Case IDs not sorted within priority group {pair.priority_group}: {prev_case_id} -> {pair.case_id}"
            
            prev_priority = pair.priority_group
            prev_case_id = pair.case_id
        
        # Verify priority group membership
        group_1_pairs = [p for p in pairs if p.priority_group == 1]
        group_2_pairs = [p for p in pairs if p.priority_group == 2]
        group_3_pairs = [p for p in pairs if p.priority_group == 3]
        
        # All group 1 pairs should be complete and not done
        for pair in group_1_pairs:
            assert pair.is_complete and not pair.is_done
        
        # All group 2 pairs should be incomplete
        for pair in group_2_pairs:
            assert not pair.is_complete
        
        # All group 3 pairs should be complete and done
        for pair in group_3_pairs:
            assert pair.is_complete and pair.is_done



# Feature: web-frontend, Property 3: Displayed records match database state
# Validates: Requirements 1.4, 6.1
@settings(max_examples=100)
@given(
    num_case_ids=st.integers(min_value=1, max_value=15),
    # For each case_id, decide what records to create: 0=genomic only, 1=clinical only, 2=both
    record_types=st.lists(
        st.integers(min_value=0, max_value=2),
        min_size=1,
        max_size=15
    )
)
def test_displayed_records_match_database_state(
    num_case_ids: int,
    record_types: list
):
    """
    Property 3: Displayed records match database state
    
    For any database state, when the page loads, the displayed records
    should exactly match all records in the database.
    
    This test verifies that:
    1. All records in the database are included in the output
    2. No extra records are added
    3. Record data matches exactly
    4. Each case_id has at most one genomic and one clinical record (data integrity)
    """
    # Ensure we have enough record types
    while len(record_types) < num_case_ids:
        record_types.append(2)  # Default to both
    
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        
        # Create records in the database
        # Track what we create to ensure data integrity
        created_records = []
        record_idx = 0
        
        with MeldebestaetigungDatabase(db_path) as db:
            for i in range(num_case_ids):
                case_id = f"CASE_{i:03d}"
                record_type = record_types[i]
                
                # 0 = genomic only, 1 = clinical only, 2 = both
                if record_type in [0, 2]:  # Create genomic
                    genomic_record = MeldebestaetigungRecord(
                        vorgangsnummer=f"VN_G_{record_idx}",
                        meldebestaetigung=f"mb_genomic_{record_idx}",
                        source_file=f"source_{record_idx}.csv",
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
                    created_records.append(genomic_record)
                    record_idx += 1
                
                if record_type in [1, 2]:  # Create clinical
                    clinical_record = MeldebestaetigungRecord(
                        vorgangsnummer=f"VN_C_{record_idx}",
                        meldebestaetigung=f"mb_clinical_{record_idx}",
                        source_file=f"source_{record_idx}.csv",
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
                    created_records.append(clinical_record)
                    record_idx += 1
        
        # Get grouped records from the service
        service = WebDatabaseService(db_path)
        pairs = service.get_all_records_grouped()
        
        # Extract all individual records from pairs
        displayed_records = []
        for pair in pairs:
            if pair.genomic is not None:
                displayed_records.append(pair.genomic)
            if pair.clinical is not None:
                displayed_records.append(pair.clinical)
        
        # Verify count matches (all records with case_id should be displayed)
        records_with_case_id = [r for r in created_records if r.case_id is not None]
        assert len(displayed_records) == len(records_with_case_id), \
            f"Expected {len(records_with_case_id)} records, got {len(displayed_records)}"
        
        # Verify all created records are in the displayed records
        created_vorgangsnummern = {r.vorgangsnummer for r in records_with_case_id}
        displayed_vorgangsnummern = {r.vorgangsnummer for r in displayed_records}
        
        assert created_vorgangsnummern == displayed_vorgangsnummern, \
            f"Mismatch in vorgangsnummern: created={created_vorgangsnummern}, displayed={displayed_vorgangsnummern}"
        
        # Verify record data matches exactly
        for created_record in records_with_case_id:
            # Find the corresponding displayed record
            displayed_record = next(
                (r for r in displayed_records if r.vorgangsnummer == created_record.vorgangsnummer),
                None
            )
            
            assert displayed_record is not None, \
                f"Record {created_record.vorgangsnummer} not found in displayed records"
            
            # Verify all fields match
            assert displayed_record.vorgangsnummer == created_record.vorgangsnummer
            assert displayed_record.meldebestaetigung == created_record.meldebestaetigung
            assert displayed_record.source_file == created_record.source_file
            assert displayed_record.typ_der_meldung == created_record.typ_der_meldung
            assert displayed_record.indikationsbereich == created_record.indikationsbereich
            assert displayed_record.art_der_daten == created_record.art_der_daten
            assert displayed_record.ergebnis_qc == created_record.ergebnis_qc
            assert displayed_record.case_id == created_record.case_id
            assert displayed_record.gpas_domain == created_record.gpas_domain
            assert displayed_record.is_done == created_record.is_done
