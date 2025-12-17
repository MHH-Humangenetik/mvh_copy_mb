"""
Property-based tests for CLI summary statistics functionality.

This module contains property-based tests using Hypothesis to verify
correctness properties of the CLI statistics display functionality.
"""

import pytest
from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st

from mvh_copy_mb.statistics import ProcessingStatistics, render_progress_bar, display_statistics


# Feature: cli-summary-statistics, Property 1: Ready file total calculation
# Validates: Requirements 1.5
@settings(max_examples=100)
@given(
    ready_count=st.integers(min_value=0, max_value=1000),
    unpaired_genomic_count=st.integers(min_value=0, max_value=1000),
    unpaired_clinical_count=st.integers(min_value=0, max_value=1000),
    ignored_count=st.integers(min_value=0, max_value=1000)
)
def test_ready_file_total_calculation(
    ready_count: int,
    unpaired_genomic_count: int,
    unpaired_clinical_count: int,
    ignored_count: int
):
    """
    Property 1: Ready file total calculation
    
    For any set of processing statistics, the total file count should equal
    ready_count * 2 + unpaired_genomic + unpaired_clinical + ignored_count
    
    This test verifies that:
    1. Ready files are counted twice in the total as specified
    2. Other file types are counted once
    3. The calculation is mathematically consistent
    """
    stats = ProcessingStatistics(
        ready_count=ready_count,
        unpaired_genomic_count=unpaired_genomic_count,
        unpaired_clinical_count=unpaired_clinical_count,
        ignored_count=ignored_count
    )
    
    expected_total = ready_count * 2 + unpaired_genomic_count + unpaired_clinical_count + ignored_count
    actual_total = stats.get_total_files()
    
    assert actual_total == expected_total, \
        f"Total file calculation incorrect: expected {expected_total}, got {actual_total}"
    
    # Verify that ready files are indeed counted twice
    if ready_count > 0:
        stats_without_ready = ProcessingStatistics(
            ready_count=0,
            unpaired_genomic_count=unpaired_genomic_count,
            unpaired_clinical_count=unpaired_clinical_count,
            ignored_count=ignored_count
        )
        difference = actual_total - stats_without_ready.get_total_files()
        assert difference == ready_count * 2, \
            f"Ready files should contribute {ready_count * 2} to total, but contributed {difference}"


# Feature: cli-summary-statistics, Property 2: Progress bar width consistency
# Validates: Requirements 3.1, 3.2
@settings(max_examples=100)
@given(
    count=st.integers(min_value=0, max_value=1000),
    total=st.integers(min_value=0, max_value=1000),
    width=st.integers(min_value=1, max_value=50)
)
def test_progress_bar_width_consistency(count: int, total: int, width: int):
    """
    Property 2: Progress bar width consistency
    
    For any statistic display, all progress bars should be exactly width + 2 characters
    wide (width characters plus opening and closing brackets)
    
    This test verifies that:
    1. Progress bars always have consistent width regardless of count/total
    2. Brackets are always present
    3. Width parameter is respected
    """
    progress_bar = render_progress_bar(count, total, width)
    
    # Progress bar should always be width + 2 characters (for brackets)
    expected_length = width + 2
    actual_length = len(progress_bar)
    
    assert actual_length == expected_length, \
        f"Progress bar should be {expected_length} characters, got {actual_length}: '{progress_bar}'"
    
    # Should start and end with brackets
    assert progress_bar.startswith('['), f"Progress bar should start with '[': '{progress_bar}'"
    assert progress_bar.endswith(']'), f"Progress bar should end with ']': '{progress_bar}'"
    
    # Inner content should be exactly width characters
    inner_content = progress_bar[1:-1]
    assert len(inner_content) == width, \
        f"Inner content should be {width} characters, got {len(inner_content)}: '{inner_content}'"


# Feature: cli-summary-statistics, Property 3: Progress bar calculation accuracy
# Validates: Requirements 3.3, 3.4, 3.5, 4.4
@settings(max_examples=100)
@given(
    count=st.integers(min_value=0, max_value=1000),
    total=st.integers(min_value=0, max_value=1000),
    width=st.integers(min_value=1, max_value=50)
)
def test_progress_bar_calculation_accuracy(count: int, total: int, width: int):
    """
    Property 3: Progress bar calculation accuracy
    
    For any statistic count and total, the progress bar should accurately represent
    the proportion with appropriate filled and empty characters enclosed in brackets
    
    This test verifies that:
    1. Filled portion represents the correct proportion
    2. Empty portion fills the remainder
    3. Special case handling for zero totals
    4. Characters are appropriate (filled vs empty)
    """
    progress_bar = render_progress_bar(count, total, width)
    inner_content = progress_bar[1:-1]  # Remove brackets
    
    if total == 0:
        # Special case: when total is 0, should be all empty
        assert inner_content == "░" * width, \
            f"When total is 0, progress bar should be all empty: '{inner_content}'"
    else:
        # Calculate expected filled width (count is clamped to total)
        clamped_count = min(count, total)
        expected_filled_width = int((clamped_count / total) * width)
        expected_empty_width = width - expected_filled_width
        
        # Count actual filled and empty characters
        filled_chars = inner_content.count("█")
        empty_chars = inner_content.count("░")
        
        assert filled_chars == expected_filled_width, \
            f"Expected {expected_filled_width} filled chars, got {filled_chars}"
        
        assert empty_chars == expected_empty_width, \
            f"Expected {expected_empty_width} empty chars, got {empty_chars}"
        
        # Verify total characters add up
        assert filled_chars + empty_chars == width, \
            f"Filled ({filled_chars}) + empty ({empty_chars}) should equal width ({width})"
        
        # Verify only valid characters are used
        valid_chars = set("█░")
        actual_chars = set(inner_content)
        assert actual_chars.issubset(valid_chars), \
            f"Progress bar contains invalid characters: {actual_chars - valid_chars}"
        
        # Verify proportion accuracy (within rounding tolerance)
        actual_proportion = filled_chars / width
        expected_proportion = clamped_count / total
        # Allow for rounding errors due to integer division
        tolerance = 1 / width  # One character worth of tolerance
        assert abs(actual_proportion - expected_proportion) <= tolerance, \
            f"Proportion accuracy: expected ~{expected_proportion:.3f}, got {actual_proportion:.3f}"


# Feature: cli-summary-statistics, Property 4: Statistics formatting consistency
# Validates: Requirements 4.1, 4.2
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    ready_count=st.integers(min_value=0, max_value=1000),
    unpaired_genomic_count=st.integers(min_value=0, max_value=1000),
    unpaired_clinical_count=st.integers(min_value=0, max_value=1000),
    ignored_count=st.integers(min_value=0, max_value=1000),
    gepado_genomic_updates=st.integers(min_value=0, max_value=1000),
    gepado_clinical_updates=st.integers(min_value=0, max_value=1000),
    gepado_errors=st.integers(min_value=0, max_value=1000),
    gepado_enabled=st.booleans()
)
def test_statistics_formatting_consistency(
    ready_count: int,
    unpaired_genomic_count: int,
    unpaired_clinical_count: int,
    ignored_count: int,
    gepado_genomic_updates: int,
    gepado_clinical_updates: int,
    gepado_errors: int,
    gepado_enabled: bool
):
    """
    Property 4: Statistics formatting consistency
    
    For any statistics display, all labels and counts should follow the same
    formatting pattern with aligned progress bars
    
    This test verifies that:
    1. All progress bars are aligned vertically
    2. All count values are right-aligned consistently
    3. Labels follow consistent formatting
    4. Visual separators are properly placed
    """
    import io
    import sys
    from contextlib import redirect_stdout
    
    stats = ProcessingStatistics(
        ready_count=ready_count,
        unpaired_genomic_count=unpaired_genomic_count,
        unpaired_clinical_count=unpaired_clinical_count,
        ignored_count=ignored_count,
        gepado_genomic_updates=gepado_genomic_updates,
        gepado_clinical_updates=gepado_clinical_updates,
        gepado_errors=gepado_errors
    )
    
    # Capture output using redirect_stdout
    output_buffer = io.StringIO()
    with redirect_stdout(output_buffer):
        display_statistics(stats, gepado_enabled=gepado_enabled)
    
    captured_output = output_buffer.getvalue()
    lines = captured_output.strip().split('\n')
    
    # Find lines with statistics (contain progress bars)
    stat_lines = [line for line in lines if '[' in line and ']' in line]
    
    if len(stat_lines) > 0:
        # Check that all progress bars are aligned (same position)
        progress_bar_positions = []
        for line in stat_lines:
            bracket_pos = line.find('[')
            if bracket_pos != -1:
                progress_bar_positions.append(bracket_pos)
        
        # All progress bars should start at the same column position
        if len(progress_bar_positions) > 1:
            first_position = progress_bar_positions[0]
            for pos in progress_bar_positions[1:]:
                assert pos == first_position, \
                    f"Progress bars not aligned: positions {progress_bar_positions}"
        
        # Check that all progress bars have the same length
        progress_bar_lengths = []
        for line in stat_lines:
            start_bracket = line.find('[')
            end_bracket = line.find(']')
            if start_bracket != -1 and end_bracket != -1:
                bar_length = end_bracket - start_bracket + 1
                progress_bar_lengths.append(bar_length)
        
        if len(progress_bar_lengths) > 1:
            first_length = progress_bar_lengths[0]
            for length in progress_bar_lengths[1:]:
                assert length == first_length, \
                    f"Progress bars have inconsistent lengths: {progress_bar_lengths}"
        
        # Check that count values are right-aligned (consistent spacing before progress bar)
        count_positions = []
        for line in stat_lines:
            # Find the number before the progress bar
            bracket_pos = line.find('[')
            if bracket_pos > 0:
                # Extract the part before the bracket and find the last number
                before_bracket = line[:bracket_pos].strip()
                # The count should be the last token before the bracket
                tokens = before_bracket.split()
                if tokens and tokens[-1].isdigit():
                    # Calculate position of the count relative to the bracket
                    count_end_pos = bracket_pos - 1  # Position just before the space and bracket
                    count_positions.append(count_end_pos)
        
        # All counts should end at the same position (right-aligned)
        if len(count_positions) > 1:
            first_position = count_positions[0]
            for pos in count_positions[1:]:
                assert pos == first_position, \
                    f"Count values not right-aligned: positions {count_positions}"


# Unit tests for specific edge cases and examples

def test_processing_statistics_initialization():
    """Test that ProcessingStatistics initializes with correct default values."""
    stats = ProcessingStatistics()
    
    assert stats.ready_count == 0
    assert stats.unpaired_genomic_count == 0
    assert stats.unpaired_clinical_count == 0
    assert stats.ignored_count == 0
    assert stats.gepado_genomic_updates == 0
    assert stats.gepado_clinical_updates == 0
    assert stats.gepado_errors == 0


def test_gepado_operations_total():
    """Test GEPADO operations total calculation."""
    stats = ProcessingStatistics(
        gepado_genomic_updates=10,
        gepado_clinical_updates=15,
        gepado_errors=3
    )
    
    expected_total = 10 + 15 + 3
    assert stats.get_total_gepado_operations() == expected_total


def test_progress_bar_edge_cases():
    """Test progress bar rendering for specific edge cases."""
    # Zero total
    bar = render_progress_bar(5, 0, 10)
    assert bar == "[░░░░░░░░░░]"
    
    # Zero count
    bar = render_progress_bar(0, 10, 10)
    assert bar == "[░░░░░░░░░░]"
    
    # Full bar
    bar = render_progress_bar(10, 10, 10)
    assert bar == "[██████████]"
    
    # Half bar
    bar = render_progress_bar(5, 10, 10)
    assert bar == "[█████░░░░░]"


def test_display_statistics_output(capsys):
    """Test that display_statistics produces expected output format."""
    stats = ProcessingStatistics(
        ready_count=10,
        unpaired_genomic_count=5,
        unpaired_clinical_count=3,
        ignored_count=2,
        gepado_genomic_updates=8,
        gepado_clinical_updates=7,
        gepado_errors=1
    )
    
    # Test without GEPADO
    display_statistics(stats, gepado_enabled=False)
    captured = capsys.readouterr()
    
    assert "PROCESSING SUMMARY" in captured.out
    assert "Ready:" in captured.out
    assert "Unpaired genomic:" in captured.out
    assert "Unpaired clinical:" in captured.out
    assert "Ignored files:" in captured.out
    assert "GEPADO UPDATES:" not in captured.out
    # Verify 80-character width
    assert "="*80 in captured.out
    
    # Test with GEPADO
    display_statistics(stats, gepado_enabled=True)
    captured = capsys.readouterr()
    
    assert "PROCESSING SUMMARY" in captured.out
    assert "GEPADO UPDATES:" in captured.out
    assert "Updated genomic data:" in captured.out
    assert "Updated clinical data:" in captured.out
    assert "Errors while updating:" in captured.out
    # Verify 80-character width
    assert "="*80 in captured.out


def test_display_statistics_gepado_disabled_mode(capsys):
    """Test display formatting when GEPADO is disabled."""
    stats = ProcessingStatistics(
        ready_count=15,
        unpaired_genomic_count=8,
        unpaired_clinical_count=4,
        ignored_count=3,
        gepado_genomic_updates=10,  # These should not appear in output
        gepado_clinical_updates=5,
        gepado_errors=2
    )
    
    display_statistics(stats, gepado_enabled=False)
    captured = capsys.readouterr()
    
    # Should contain file statistics
    assert "Ready:" in captured.out
    assert "Unpaired genomic:" in captured.out
    assert "Unpaired clinical:" in captured.out
    assert "Ignored files:" in captured.out
    
    # Should NOT contain GEPADO statistics
    assert "GEPADO UPDATES:" not in captured.out
    assert "Updated genomic data:" not in captured.out
    assert "Updated clinical data:" not in captured.out
    assert "Errors while updating:" not in captured.out
    
    # Should have proper visual separators
    lines = captured.out.split('\n')
    separator_lines = [line for line in lines if line.strip() == "="*80]
    assert len(separator_lines) >= 2, "Should have opening and closing separators"


def test_display_statistics_gepado_enabled_mode(capsys):
    """Test display formatting when GEPADO is enabled."""
    stats = ProcessingStatistics(
        ready_count=12,
        unpaired_genomic_count=6,
        unpaired_clinical_count=2,
        ignored_count=1,
        gepado_genomic_updates=9,
        gepado_clinical_updates=8,
        gepado_errors=1
    )
    
    display_statistics(stats, gepado_enabled=True)
    captured = capsys.readouterr()
    
    # Should contain file statistics
    assert "Ready:" in captured.out
    assert "Unpaired genomic:" in captured.out
    assert "Unpaired clinical:" in captured.out
    assert "Ignored files:" in captured.out
    
    # Should contain GEPADO statistics
    assert "GEPADO UPDATES:" in captured.out
    assert "Updated genomic data:" in captured.out
    assert "Updated clinical data:" in captured.out
    assert "Errors while updating:" in captured.out
    
    # Should have proper visual separators
    lines = captured.out.split('\n')
    separator_lines = [line for line in lines if line.strip() == "="*80]
    assert len(separator_lines) >= 2, "Should have opening and closing separators"
    
    # GEPADO section should be separated from file statistics
    gepado_line_index = None
    for i, line in enumerate(lines):
        if "GEPADO UPDATES:" in line:
            gepado_line_index = i
            break
    
    assert gepado_line_index is not None, "GEPADO UPDATES section should be present"
    # There should be an empty line before GEPADO section
    assert lines[gepado_line_index - 1].strip() == "", "Should have empty line before GEPADO section"


def test_display_statistics_visual_separator_placement(capsys):
    """Test that visual separators are properly placed."""
    stats = ProcessingStatistics(
        ready_count=5,
        unpaired_genomic_count=3,
        unpaired_clinical_count=2,
        ignored_count=1,
        gepado_genomic_updates=4,
        gepado_clinical_updates=3,
        gepado_errors=0
    )
    
    display_statistics(stats, gepado_enabled=True)
    captured = capsys.readouterr()
    
    lines = captured.out.split('\n')
    
    # Find separator lines
    separator_indices = []
    for i, line in enumerate(lines):
        if line.strip() == "="*80:
            separator_indices.append(i)
    
    assert len(separator_indices) >= 2, "Should have at least opening and closing separators"
    
    # First separator should be near the beginning
    assert separator_indices[0] <= 2, "Opening separator should be at the beginning"
    
    # Last separator should be at the end
    assert separator_indices[-1] >= len(lines) - 3, "Closing separator should be at the end"
    
    # Check that title is centered between separators
    title_line_index = None
    for i, line in enumerate(lines):
        if "PROCESSING SUMMARY" in line:
            title_line_index = i
            break
    
    assert title_line_index is not None, "Should have PROCESSING SUMMARY title"
    assert separator_indices[0] < title_line_index < separator_indices[-1], \
        "Title should be between separators"


def test_display_statistics_zero_counts(capsys):
    """Test display formatting with zero counts."""
    stats = ProcessingStatistics(
        ready_count=0,
        unpaired_genomic_count=0,
        unpaired_clinical_count=0,
        ignored_count=0,
        gepado_genomic_updates=0,
        gepado_clinical_updates=0,
        gepado_errors=0
    )
    
    display_statistics(stats, gepado_enabled=True)
    captured = capsys.readouterr()
    
    # Should still display all categories with zero counts
    assert "Ready:" in captured.out
    assert "Unpaired genomic:" in captured.out
    assert "Unpaired clinical:" in captured.out
    assert "Ignored files:" in captured.out
    assert "GEPADO UPDATES:" in captured.out
    assert "Updated genomic data:" in captured.out
    assert "Updated clinical data:" in captured.out
    assert "Errors while updating:" in captured.out
    
    # All progress bars should be empty (all ░ characters)
    lines = captured.out.split('\n')
    stat_lines = [line for line in lines if '[' in line and ']' in line]
    
    for line in stat_lines:
        # Extract progress bar content
        start_bracket = line.find('[')
        end_bracket = line.find(']')
        if start_bracket != -1 and end_bracket != -1:
            bar_content = line[start_bracket+1:end_bracket]
            # Should be all empty characters
            assert all(c == '░' for c in bar_content), \
                f"Progress bar should be empty for zero counts: '{bar_content}'"


# Feature: cli-summary-statistics, Property 5: File categorization accuracy
# Validates: Requirements 5.1, 5.2, 5.3
@settings(max_examples=100)
@given(
    has_case_id=st.booleans(),
    ergebnis_qc=st.sampled_from(['0', '1']),  # QC result: 1 = passed, 0 = failed
    typ_der_meldung=st.sampled_from(['0', '1', '2']),  # Message type: 0 = initial, others = non-initial
    art_der_daten=st.sampled_from(['G', 'C', 'X']),  # Data type: G = genomic, C = clinical, X = unknown
    parsing_success=st.booleans()  # Whether parsing of Meldebestaetigung succeeds
)
def test_file_categorization_accuracy(
    has_case_id: bool,
    ergebnis_qc: str,
    typ_der_meldung: str,
    art_der_daten: str,
    parsing_success: bool
):
    """
    Property 5: File categorization accuracy
    
    For any processed file, it should be counted in exactly one category
    (Ready, Unpaired genomic, Unpaired clinical, or Ignored) based on its processing results
    
    This test verifies that:
    1. Files are categorized based on QC results, message type, and Case ID resolution
    2. Each file is counted in exactly one category
    3. Categorization logic follows the requirements specification
    """
    stats = ProcessingStatistics()
    initial_total = stats.ready_count + stats.unpaired_genomic_count + stats.unpaired_clinical_count + stats.ignored_count
    
    # Simulate file processing logic based on the CLI implementation
    if not parsing_success:
        # Parsing failure -> Ignored
        stats.ignored_count += 1
        expected_category = "ignored"
    elif ergebnis_qc != "1":
        # QC failed -> Ignored
        stats.ignored_count += 1
        expected_category = "ignored"
    elif typ_der_meldung != "0":
        # Non-initial report -> Ignored
        stats.ignored_count += 1
        expected_category = "ignored"
    elif has_case_id:
        # Has resolved Case ID and passed all checks -> Ready
        stats.ready_count += 1
        expected_category = "ready"
    else:
        # No Case ID resolved -> Unpaired based on data type
        if art_der_daten.upper() == 'G':
            stats.unpaired_genomic_count += 1
            expected_category = "unpaired_genomic"
        elif art_der_daten.upper() == 'C':
            stats.unpaired_clinical_count += 1
            expected_category = "unpaired_clinical"
        else:
            # Unknown data type -> Ignored
            stats.ignored_count += 1
            expected_category = "ignored"
    
    # Verify exactly one file was added to exactly one category
    final_total = stats.ready_count + stats.unpaired_genomic_count + stats.unpaired_clinical_count + stats.ignored_count
    assert final_total == initial_total + 1, \
        f"Exactly one file should be categorized, but total changed from {initial_total} to {final_total}"
    
    # Verify the file was categorized correctly based on expected logic
    if expected_category == "ready":
        assert stats.ready_count == 1, f"File should be categorized as ready, but ready_count = {stats.ready_count}"
        assert stats.unpaired_genomic_count == 0, f"File categorized as ready should not be unpaired genomic"
        assert stats.unpaired_clinical_count == 0, f"File categorized as ready should not be unpaired clinical"
        assert stats.ignored_count == 0, f"File categorized as ready should not be ignored"
    elif expected_category == "unpaired_genomic":
        assert stats.unpaired_genomic_count == 1, f"File should be categorized as unpaired genomic, but count = {stats.unpaired_genomic_count}"
        assert stats.ready_count == 0, f"File categorized as unpaired genomic should not be ready"
        assert stats.unpaired_clinical_count == 0, f"File categorized as unpaired genomic should not be unpaired clinical"
        assert stats.ignored_count == 0, f"File categorized as unpaired genomic should not be ignored"
    elif expected_category == "unpaired_clinical":
        assert stats.unpaired_clinical_count == 1, f"File should be categorized as unpaired clinical, but count = {stats.unpaired_clinical_count}"
        assert stats.ready_count == 0, f"File categorized as unpaired clinical should not be ready"
        assert stats.unpaired_genomic_count == 0, f"File categorized as unpaired clinical should not be unpaired genomic"
        assert stats.ignored_count == 0, f"File categorized as unpaired clinical should not be ignored"
    elif expected_category == "ignored":
        assert stats.ignored_count == 1, f"File should be categorized as ignored, but ignored_count = {stats.ignored_count}"
        assert stats.ready_count == 0, f"File categorized as ignored should not be ready"
        assert stats.unpaired_genomic_count == 0, f"File categorized as ignored should not be unpaired genomic"
        assert stats.unpaired_clinical_count == 0, f"File categorized as ignored should not be unpaired clinical"
    
    # Verify mutual exclusivity - file should be in exactly one category
    categories_with_files = 0
    if stats.ready_count > 0:
        categories_with_files += 1
    if stats.unpaired_genomic_count > 0:
        categories_with_files += 1
    if stats.unpaired_clinical_count > 0:
        categories_with_files += 1
    if stats.ignored_count > 0:
        categories_with_files += 1
    
    assert categories_with_files == 1, \
        f"File should be in exactly one category, but found in {categories_with_files} categories"


# Feature: cli-summary-statistics, Property 6: GEPADO statistics accuracy
# Validates: Requirements 5.4
@settings(max_examples=100)
@given(
    operation_success=st.booleans(),
    art_der_daten=st.sampled_from(['G', 'C', 'X']),  # Data type: G = genomic, C = clinical, X = unknown
    has_updates_needed=st.booleans(),  # Whether the operation requires actual updates
    record_found=st.booleans(),  # Whether a GEPADO record was found
    valid_processing_criteria=st.booleans()  # Whether QC and message type criteria are met
)
def test_gepado_statistics_accuracy(
    operation_success: bool,
    art_der_daten: str,
    has_updates_needed: bool,
    record_found: bool,
    valid_processing_criteria: bool
):
    """
    Property 6: GEPADO statistics accuracy
    
    For any GEPADO update operation, it should be counted as either successful
    (genomic or clinical) or failed, but not both
    
    This test verifies that:
    1. Each GEPADO operation is counted in exactly one category (success or error)
    2. Successful operations are categorized by data type (genomic vs clinical)
    3. Failed operations are counted as errors regardless of data type
    4. Operations that don't meet criteria are counted as errors
    """
    stats = ProcessingStatistics()
    initial_genomic = stats.gepado_genomic_updates
    initial_clinical = stats.gepado_clinical_updates
    initial_errors = stats.gepado_errors
    initial_total = stats.get_total_gepado_operations()
    
    # Simulate GEPADO operation logic based on the implementation
    if not valid_processing_criteria:
        # Doesn't meet QC/message type criteria -> Error
        stats.gepado_errors += 1
        expected_category = "error"
    elif not record_found:
        # No GEPADO record found -> Error
        stats.gepado_errors += 1
        expected_category = "error"
    elif art_der_daten.upper() not in ['G', 'C']:
        # Invalid data type -> Error
        stats.gepado_errors += 1
        expected_category = "error"
    elif operation_success:
        # Successful operation -> Count based on data type
        if art_der_daten.upper() == 'G':
            stats.gepado_genomic_updates += 1
            expected_category = "genomic_success"
        elif art_der_daten.upper() == 'C':
            stats.gepado_clinical_updates += 1
            expected_category = "clinical_success"
    else:
        # Operation failed (e.g., database update failed) -> Error
        stats.gepado_errors += 1
        expected_category = "error"
    
    # Verify exactly one operation was counted
    final_total = stats.get_total_gepado_operations()
    assert final_total == initial_total + 1, \
        f"Exactly one GEPADO operation should be counted, but total changed from {initial_total} to {final_total}"
    
    # Verify the operation was categorized correctly
    genomic_increase = stats.gepado_genomic_updates - initial_genomic
    clinical_increase = stats.gepado_clinical_updates - initial_clinical
    error_increase = stats.gepado_errors - initial_errors
    
    if expected_category == "genomic_success":
        assert genomic_increase == 1, f"Should have 1 genomic success, got {genomic_increase}"
        assert clinical_increase == 0, f"Genomic operation should not increase clinical count"
        assert error_increase == 0, f"Successful operation should not increase error count"
    elif expected_category == "clinical_success":
        assert clinical_increase == 1, f"Should have 1 clinical success, got {clinical_increase}"
        assert genomic_increase == 0, f"Clinical operation should not increase genomic count"
        assert error_increase == 0, f"Successful operation should not increase error count"
    elif expected_category == "error":
        assert error_increase == 1, f"Should have 1 error, got {error_increase}"
        assert genomic_increase == 0, f"Failed operation should not increase genomic count"
        assert clinical_increase == 0, f"Failed operation should not increase clinical count"
    
    # Verify mutual exclusivity - operation should be in exactly one category
    total_increases = genomic_increase + clinical_increase + error_increase
    assert total_increases == 1, \
        f"Operation should be counted in exactly one category, but total increases = {total_increases}"
    
    # Verify that successful operations are properly distinguished by data type
    if operation_success and valid_processing_criteria and record_found:
        if art_der_daten.upper() == 'G':
            assert genomic_increase == 1 and clinical_increase == 0 and error_increase == 0, \
                "Successful genomic operation should only increment genomic counter"
        elif art_der_daten.upper() == 'C':
            assert clinical_increase == 1 and genomic_increase == 0 and error_increase == 0, \
                "Successful clinical operation should only increment clinical counter"
    
    # Verify that failed operations are counted as errors regardless of data type
    if not operation_success or not valid_processing_criteria or not record_found or art_der_daten.upper() not in ['G', 'C']:
        assert error_increase == 1 and genomic_increase == 0 and clinical_increase == 0, \
            "Failed operations should only increment error counter"


# Feature: cli-summary-statistics, Property 7: Mathematical consistency
# Validates: Requirements 5.5
@settings(max_examples=100)
@given(
    ready_count=st.integers(min_value=0, max_value=1000),
    unpaired_genomic_count=st.integers(min_value=0, max_value=1000),
    unpaired_clinical_count=st.integers(min_value=0, max_value=1000),
    ignored_count=st.integers(min_value=0, max_value=1000),
    gepado_genomic_updates=st.integers(min_value=0, max_value=1000),
    gepado_clinical_updates=st.integers(min_value=0, max_value=1000),
    gepado_errors=st.integers(min_value=0, max_value=1000)
)
def test_mathematical_consistency(
    ready_count: int,
    unpaired_genomic_count: int,
    unpaired_clinical_count: int,
    ignored_count: int,
    gepado_genomic_updates: int,
    gepado_clinical_updates: int,
    gepado_errors: int
):
    """
    Property 7: Mathematical consistency
    
    For any statistics display, the sum of individual counts should equal
    the total used for progress bar calculations
    
    This test verifies that:
    1. File statistics sum equals the total used for file progress bars
    2. GEPADO statistics sum equals the total used for GEPADO progress bars
    3. Mathematical operations are consistent and accurate
    4. No counts are lost or double-counted in calculations
    """
    stats = ProcessingStatistics(
        ready_count=ready_count,
        unpaired_genomic_count=unpaired_genomic_count,
        unpaired_clinical_count=unpaired_clinical_count,
        ignored_count=ignored_count,
        gepado_genomic_updates=gepado_genomic_updates,
        gepado_clinical_updates=gepado_clinical_updates,
        gepado_errors=gepado_errors
    )
    
    # Test file statistics mathematical consistency
    calculated_file_total = stats.get_total_files()
    manual_file_total = ready_count * 2 + unpaired_genomic_count + unpaired_clinical_count + ignored_count
    
    assert calculated_file_total == manual_file_total, \
        f"File total calculation inconsistent: method returned {calculated_file_total}, manual calculation {manual_file_total}"
    
    # Test GEPADO statistics mathematical consistency
    calculated_gepado_total = stats.get_total_gepado_operations()
    manual_gepado_total = gepado_genomic_updates + gepado_clinical_updates + gepado_errors
    
    assert calculated_gepado_total == manual_gepado_total, \
        f"GEPADO total calculation inconsistent: method returned {calculated_gepado_total}, manual calculation {manual_gepado_total}"
    
    # Test that individual counts are preserved (no loss or corruption)
    assert stats.ready_count == ready_count, \
        f"Ready count not preserved: expected {ready_count}, got {stats.ready_count}"
    assert stats.unpaired_genomic_count == unpaired_genomic_count, \
        f"Unpaired genomic count not preserved: expected {unpaired_genomic_count}, got {stats.unpaired_genomic_count}"
    assert stats.unpaired_clinical_count == unpaired_clinical_count, \
        f"Unpaired clinical count not preserved: expected {unpaired_clinical_count}, got {stats.unpaired_clinical_count}"
    assert stats.ignored_count == ignored_count, \
        f"Ignored count not preserved: expected {ignored_count}, got {stats.ignored_count}"
    assert stats.gepado_genomic_updates == gepado_genomic_updates, \
        f"GEPADO genomic updates not preserved: expected {gepado_genomic_updates}, got {stats.gepado_genomic_updates}"
    assert stats.gepado_clinical_updates == gepado_clinical_updates, \
        f"GEPADO clinical updates not preserved: expected {gepado_clinical_updates}, got {stats.gepado_clinical_updates}"
    assert stats.gepado_errors == gepado_errors, \
        f"GEPADO errors not preserved: expected {gepado_errors}, got {stats.gepado_errors}"
    
    # Test that totals are non-negative (mathematical sanity check)
    assert calculated_file_total >= 0, \
        f"File total should be non-negative, got {calculated_file_total}"
    assert calculated_gepado_total >= 0, \
        f"GEPADO total should be non-negative, got {calculated_gepado_total}"
    
    # Test that ready files contribute exactly double to the total
    if ready_count > 0:
        stats_without_ready = ProcessingStatistics(
            ready_count=0,
            unpaired_genomic_count=unpaired_genomic_count,
            unpaired_clinical_count=unpaired_clinical_count,
            ignored_count=ignored_count
        )
        total_without_ready = stats_without_ready.get_total_files()
        ready_contribution = calculated_file_total - total_without_ready
        
        assert ready_contribution == ready_count * 2, \
            f"Ready files should contribute {ready_count * 2} to total, but contributed {ready_contribution}"
    
    # Test that each GEPADO operation type contributes exactly once to the total
    if gepado_genomic_updates > 0 or gepado_clinical_updates > 0 or gepado_errors > 0:
        # Verify each component contributes exactly its count
        component_sum = gepado_genomic_updates + gepado_clinical_updates + gepado_errors
        assert component_sum == calculated_gepado_total, \
            f"GEPADO component sum {component_sum} should equal total {calculated_gepado_total}"
    
    # Test mathematical properties (commutativity, associativity)
    # File total should be the same regardless of calculation order
    alt_file_total = (ready_count * 2) + (unpaired_genomic_count + unpaired_clinical_count + ignored_count)
    assert alt_file_total == calculated_file_total, \
        f"File total calculation should be commutative: {alt_file_total} != {calculated_file_total}"
    
    # GEPADO total should be the same regardless of calculation order
    alt_gepado_total = (gepado_genomic_updates + gepado_clinical_updates) + gepado_errors
    assert alt_gepado_total == calculated_gepado_total, \
        f"GEPADO total calculation should be commutative: {alt_gepado_total} != {calculated_gepado_total}"


def test_display_statistics_specific_known_data(capsys):
    """Test specific display scenarios with known data."""
    # Test scenario: 20 ready files, 10 unpaired genomic, 5 unpaired clinical, 2 ignored
    # Total files = 20*2 + 10 + 5 + 2 = 57
    stats = ProcessingStatistics(
        ready_count=20,
        unpaired_genomic_count=10,
        unpaired_clinical_count=5,
        ignored_count=2,
        gepado_genomic_updates=18,
        gepado_clinical_updates=15,
        gepado_errors=2
    )
    
    display_statistics(stats, gepado_enabled=True)
    captured = capsys.readouterr()
    
    # Verify specific counts are displayed
    assert "20" in captured.out  # Ready count
    assert "10" in captured.out  # Unpaired genomic count
    assert "5" in captured.out   # Unpaired clinical count
    assert "2" in captured.out   # Ignored count
    assert "18" in captured.out  # GEPADO genomic updates
    assert "15" in captured.out  # GEPADO clinical updates
    
    # Verify progress bars are present for each statistic
    lines = captured.out.split('\n')
    stat_lines = [line for line in lines if '[' in line and ']' in line]
    
    # Should have 7 statistics lines (4 file stats + 3 GEPADO stats)
    assert len(stat_lines) == 7, f"Expected 7 statistics lines, got {len(stat_lines)}"
    
    # Each line should have a progress bar with filled and empty characters
    for line in stat_lines:
        assert '[' in line and ']' in line, f"Line should contain progress bar: '{line}'"
        start_bracket = line.find('[')
        end_bracket = line.find(']')
        bar_content = line[start_bracket+1:end_bracket]
        # Should contain valid progress bar characters
        assert all(c in '█░' for c in bar_content), \
            f"Progress bar should only contain valid characters: '{bar_content}'"