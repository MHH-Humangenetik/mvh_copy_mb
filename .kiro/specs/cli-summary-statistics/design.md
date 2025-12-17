# CLI Summary Statistics Feature Design

## Overview

This feature adds comprehensive summary statistics display to the CLI tool that shows key metrics after processing Meldebestätigungen CSV files. The statistics will be displayed at the end of CLI execution with visual progress bars, providing users with a detailed overview of processing results including file counts, pairing status, and GEPADO update results. The implementation will track statistics during processing and display them in a formatted table with aligned progress bars.

## Architecture

The statistics display will be implemented as a statistics tracking system integrated into the existing CLI processing workflow. Statistics will be collected during file processing and displayed at the end of execution using a dedicated statistics formatter.

### Component Integration
- **Statistics Tracker**: Class to accumulate statistics during processing
- **Progress Bar Renderer**: Function to generate visual progress bars
- **Statistics Display**: Formatted output with aligned statistics and bars
- **CLI Integration**: Integration points in the main processing loop

## Components and Interfaces

### Statistics Tracker Class
```python
@dataclass
class ProcessingStatistics:
    """Tracks statistics during CLI processing."""
    ready_pairs_count: int = 0  # Complete pairs with both G and C data sharing same Case ID
    unpaired_genomic_count: int = 0  # Genomic files with Case IDs but no clinical counterpart
    unpaired_clinical_count: int = 0  # Clinical files with Case IDs but no genomic counterpart
    ignored_count: int = 0  # Files skipped due to QC failures, unresolved Case IDs, or errors
    
    # GEPADO statistics (only when GEPADO updates enabled)
    gepado_genomic_updates: int = 0  # Actual genomic data updates in GEPADO
    gepado_clinical_updates: int = 0  # Actual clinical data updates in GEPADO
    gepado_no_updates_needed: int = 0  # Records validated but no updates needed
    gepado_errors: int = 0  # Errors during GEPADO operations
    
    # Internal tracking for pairing logic
    _resolved_case_ids: dict = field(default_factory=dict)  # Case ID -> {genomic: bool, clinical: bool}
    
    def get_total_files(self) -> int:
        """Calculate total files processed (Ready pairs counted as two files each)."""
        return self.ready_pairs_count * 2 + self.unpaired_genomic_count + self.unpaired_clinical_count + self.ignored_count
    
    def get_total_gepado_operations(self) -> int:
        """Calculate total GEPADO operations attempted."""
        return self.gepado_genomic_updates + self.gepado_clinical_updates + self.gepado_no_updates_needed + self.gepado_errors
    
    def add_resolved_case_id(self, case_id: str, data_type: str) -> None:
        """Track a resolved Case ID and its data type for pairing logic."""
        if case_id not in self._resolved_case_ids:
            self._resolved_case_ids[case_id] = {'genomic': False, 'clinical': False}
        
        if data_type.upper() == 'G':
            self._resolved_case_ids[case_id]['genomic'] = True
        elif data_type.upper() == 'C':
            self._resolved_case_ids[case_id]['clinical'] = True
    
    def finalize_pairing_statistics(self) -> None:
        """Calculate final pairing statistics based on resolved Case IDs."""
        for case_id, types in self._resolved_case_ids.items():
            has_genomic = types['genomic']
            has_clinical = types['clinical']
            
            if has_genomic and has_clinical:
                # Complete pair
                self.ready_pairs_count += 1
            elif has_genomic and not has_clinical:
                # Unpaired genomic
                self.unpaired_genomic_count += 1
            elif has_clinical and not has_genomic:
                # Unpaired clinical
                self.unpaired_clinical_count += 1
```

### Progress Bar Renderer
```python
def render_progress_bar(count: int, total: int, width: int = 20) -> str:
    """
    Render a progress bar for the given count and total.
    
    Args:
        count: Current count value
        total: Maximum value for the progress bar
        width: Width of the progress bar in characters (excluding brackets)
        
    Returns:
        String representation of the progress bar with brackets
    """
    if total == 0:
        return "[" + "░" * width + "]"
    
    filled_width = int((count / total) * width)
    empty_width = width - filled_width
    
    return "[" + "█" * filled_width + "░" * empty_width + "]"
```

### Statistics Display Formatter
```python
def display_statistics(stats: ProcessingStatistics, gepado_enabled: bool = False) -> None:
    """
    Display formatted statistics with progress bars.
    
    Args:
        stats: ProcessingStatistics instance with collected data
        gepado_enabled: Whether GEPADO integration was enabled
    """
    total_files = stats.get_total_files()
    
    print("\n" + "="*50)
    print("PROCESSING SUMMARY")
    print("="*50)
    
    # File statistics
    print(f"Ready pairs:            {stats.ready_pairs_count:>6} {render_progress_bar(stats.ready_pairs_count * 2, total_files)}")
    print(f"Unpaired genomic:       {stats.unpaired_genomic_count:>6} {render_progress_bar(stats.unpaired_genomic_count, total_files)}")
    print(f"Unpaired clinical:      {stats.unpaired_clinical_count:>6} {render_progress_bar(stats.unpaired_clinical_count, total_files)}")
    print(f"Ignored files:          {stats.ignored_count:>6} {render_progress_bar(stats.ignored_count, total_files)}")
    
    # GEPADO statistics (if enabled)
    if gepado_enabled:
        total_gepado = stats.get_total_gepado_operations()
        print("\nGEPADO OPERATIONS:")
        print(f"Updated genomic data:   {stats.gepado_genomic_updates:>6} {render_progress_bar(stats.gepado_genomic_updates, total_gepado)}")
        print(f"Updated clinical data:  {stats.gepado_clinical_updates:>6} {render_progress_bar(stats.gepado_clinical_updates, total_gepado)}")
        print(f"No updates needed:      {stats.gepado_no_updates_needed:>6} {render_progress_bar(stats.gepado_no_updates_needed, total_gepado)}")
        print(f"Errors during ops:      {stats.gepado_errors:>6} {render_progress_bar(stats.gepado_errors, total_gepado)}")
    
    print("="*50)
```

## Data Models

### ProcessingStatistics
The main data model for tracking statistics during processing:

```python
@dataclass
class ProcessingStatistics:
    ready_pairs_count: int = 0
    unpaired_genomic_count: int = 0
    unpaired_clinical_count: int = 0
    ignored_count: int = 0
    gepado_genomic_updates: int = 0
    gepado_clinical_updates: int = 0
    gepado_no_updates_needed: int = 0
    gepado_errors: int = 0
    _resolved_case_ids: dict = field(default_factory=dict)
```

### Integration Points
The statistics will be integrated into the existing CLI workflow at these points:

1. **File Processing**: Track resolved Case IDs and data types during `process_row()`
2. **Pairing Logic**: Calculate final pairing statistics after all files are processed
3. **GEPADO Updates**: Track actual updates vs no-updates-needed in `validate_and_update_record()`
4. **CLI Completion**: Display statistics at the end of `main()`

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

After analyzing the acceptance criteria, several properties can be consolidated to eliminate redundancy:

**Property Reflection:**
After analyzing the updated requirements, several properties can be consolidated:
- Properties 1.2 and 1.3 both test unpaired file counting but for different data types - can be combined
- Properties 2.1 and 2.2 both test actual GEPADO updates but for different data types - can be combined  
- Properties 3.1 and 3.2 both test progress bar width consistency
- Properties 3.3 and 3.4 both test progress bar calculation logic but with different contexts
- Properties 5.1, 5.2, and 5.3 all test file categorization logic

**Consolidated Properties:**

Property 1: Pairing logic accuracy
*For any* set of files with resolved Case IDs, a Case ID should be counted as a ready pair if and only if it has both genomic (G) and clinical (C) files, matching the web interface logic
**Validates: Requirements 5.1**

Property 2: Unpaired file categorization
*For any* file with a resolved Case ID, it should be counted as unpaired genomic or unpaired clinical if it lacks a counterpart of the opposite data type
**Validates: Requirements 5.2**

Property 3: Total file calculation consistency
*For any* set of processing statistics, the total file count should equal ready_pairs_count * 2 + unpaired_genomic_count + unpaired_clinical_count + ignored_count
**Validates: Requirements 1.5, 5.5**

Property 4: GEPADO operation categorization
*For any* GEPADO operation, it should be counted in exactly one category: actual update (genomic or clinical), no update needed, or error
**Validates: Requirements 2.1, 2.2, 2.3, 2.4, 5.4**

Property 5: Progress bar width consistency
*For any* statistic display, all progress bars should be exactly 22 characters wide (20 characters plus opening and closing brackets)
**Validates: Requirements 3.1, 3.2**

Property 6: Progress bar calculation accuracy
*For any* statistic count and total, the progress bar should accurately represent the proportion with appropriate filled and empty characters
**Validates: Requirements 3.3, 3.4, 4.4**

Property 7: Statistics formatting consistency
*For any* statistics display, all labels and counts should follow the same formatting pattern with consistent alignment
**Validates: Requirements 4.2**

## Error Handling

### Statistics Tracking Errors
- **Invalid Counts**: Handle cases where statistics counters become negative or inconsistent
- **Division by Zero**: Handle cases where total counts are zero for progress bar calculations
- **Missing Statistics**: Handle cases where statistics object is not properly initialized

### Display Formatting Errors
- **Terminal Width**: Handle cases where terminal is too narrow for proper display
- **Character Encoding**: Handle cases where progress bar characters are not supported
- **Output Redirection**: Ensure statistics display works when output is redirected to files

### Fallback Behavior
- **Statistics Disabled**: Allow CLI to function normally if statistics tracking fails
- **Partial Statistics**: Display available statistics even if some counters are missing
- **Error Recovery**: Continue processing even if statistics tracking encounters errors

## Testing Strategy

### Dual Testing Approach
The testing strategy combines unit tests for specific examples and edge cases with property-based tests for universal behaviors.

**Unit Testing:**
- Test specific datasets with known expected statistics
- Test edge cases like empty datasets, all files ignored, GEPADO disabled
- Test display formatting with various count combinations
- Test integration with existing CLI workflow

**Property-Based Testing:**
- Use Hypothesis for Python property-based testing
- Configure each property test to run minimum 100 iterations
- Generate random processing results with varying file counts and GEPADO outcomes
- Test progress bar calculations across random count and total combinations

**Property-Based Testing Library:** Hypothesis (Python)
- Each property test will run a minimum of 100 iterations
- Each property-based test will be tagged with comments referencing the design document property
- Tag format: '**Feature: cli-summary-statistics, Property {number}: {property_text}**'

**Testing Components:**
- **Statistics Tracking**: Unit tests for ProcessingStatistics class methods
- **Progress Bar Rendering**: Property tests for progress bar calculations and formatting
- **Display Formatting**: Unit tests for statistics display layout and alignment
- **CLI Integration**: Integration tests for end-to-end statistics collection and display

### Test Data Generation
- Generate random file processing results with varying success/failure rates
- Generate random GEPADO update outcomes (success/failure for genomic/clinical)
- Generate edge cases (zero counts, maximum counts, mixed scenarios)
- Generate random count combinations to test mathematical consistency