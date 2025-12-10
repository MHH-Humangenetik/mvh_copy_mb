# Statistics Display Feature Design

## Overview

This feature adds a statistics display component to the web interface that shows key metrics about the Meldebestätigungen records. The statistics will be displayed next to the filter input, providing users with an at-a-glance overview of the data set including total cases, complete/valid pairs, and done pairs. The implementation will leverage the existing Alpine.js reactive data system to ensure statistics update automatically as users interact with the data.

## Architecture

The statistics display will be implemented as a client-side component using Alpine.js, integrated into the existing `tableData()` Alpine component. The statistics will be calculated reactively based on the filtered and sorted data, ensuring they always reflect the current view state.

### Component Integration
- **Frontend**: Alpine.js reactive computed properties for statistics calculation
- **Styling**: CSS classes following the existing Rosé Pine theme
- **Data Source**: Existing `pairs` data from the Alpine.js component
- **Reactivity**: Automatic updates when filter, sort, or done status changes

## Components and Interfaces

### Statistics Component Structure
```html
<div class="statistics-container">
  <div class="statistics-item">
    <span class="statistics-label">Total Cases:</span>
    <span class="statistics-value" x-text="totalCases">0</span>
  </div>
  <div class="statistics-item">
    <span class="statistics-label">Complete:</span>
    <span class="statistics-value" x-text="completePairs">0</span>
  </div>
  <div class="statistics-item">
    <span class="statistics-label">Valid:</span>
    <span class="statistics-value" x-text="validPairs">0</span>
  </div>
  <div class="statistics-item">
    <span class="statistics-label">Done:</span>
    <span class="statistics-value" x-text="donePairs">0</span>
  </div>
</div>
```

### Alpine.js Computed Properties
The following computed properties will be added to the existing `tableData()` component:

```javascript
get totalCases() {
  return this.filteredAndSorted.length;
}

get completePairs() {
  return this.filteredAndSorted.filter(pair => pair.is_complete).length;
}

get validPairs() {
  return this.filteredAndSorted.filter(pair => pair.is_valid).length;
}

get donePairs() {
  return this.filteredAndSorted.filter(pair => pair.is_done).length;
}
```

## Data Models

The statistics component will use the existing data models:

### RecordPair (from web_database.py)
```python
@dataclass
class RecordPair:
    case_id: str
    genomic: Optional[MeldebestaetigungRecord]
    clinical: Optional[MeldebestaetigungRecord]
    is_complete: bool  # Used for complete pairs count
    is_valid: bool     # Used for valid pairs count
    is_done: bool      # Used for done pairs count
    priority_group: int
```

### Statistics Data Flow
1. **Data Source**: `pairs` array from server-side rendering
2. **Filtering**: Applied via `filteredAndSorted` computed property
3. **Statistics Calculation**: Computed properties count filtered pairs
4. **Display**: Alpine.js `x-text` directives update DOM automatically
## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

After analyzing the acceptance criteria, several properties can be consolidated to eliminate redundancy:

**Property Reflection:**
- Properties 1.2, 2.3, 3.2, and 5.1 all test the same behavior: that filtering updates all statistics counts
- Properties 3.3, 3.4, and 5.2 all test the same behavior: that done status changes update the done count
- Several properties test the same filtering behavior across different statistics

**Consolidated Properties:**

Property 1: Filter updates all statistics
*For any* dataset and any filter string, applying the filter should update all statistics (total, complete, valid, done) to reflect only the pairs that match the filter criteria
**Validates: Requirements 1.2, 2.3, 3.2, 5.1**

Property 2: Complete pair definition
*For any* pair, it should be counted as complete if and only if it has both genomic and clinical records present
**Validates: Requirements 2.4**

Property 3: Done status reactivity
*For any* pair and any done status change, updating the done status should immediately update the done count to reflect the new state
**Validates: Requirements 3.3, 3.4, 5.2**

Property 4: Sort invariance
*For any* dataset and any sorting operation, the statistics counts should remain unchanged after sorting (since sorting doesn't change which items are included)
**Validates: Requirements 5.4**

## Error Handling

### Client-Side Error Handling
- **Invalid Data**: If pairs data is malformed, display zero counts with error indication
- **Missing Properties**: Handle cases where pair objects lack expected properties (is_complete, is_valid, is_done)
- **Filter Errors**: Gracefully handle filter input that causes JavaScript errors

### Fallback Behavior
- **JavaScript Disabled**: Statistics will not display, but core functionality remains
- **Alpine.js Load Failure**: Statistics container shows loading state or hides gracefully
- **Data Loading Errors**: Display "N/A" or "--" for statistics when data is unavailable

## Testing Strategy

### Dual Testing Approach
The testing strategy combines unit tests for specific examples and edge cases with property-based tests for universal behaviors.

**Unit Testing:**
- Test specific datasets with known expected counts
- Test edge cases like empty datasets, single records, incomplete pairs
- Test UI rendering with mock data
- Test integration with existing Alpine.js component

**Property-Based Testing:**
- Use Hypothesis for Python property-based testing
- Configure each property test to run minimum 100 iterations
- Generate random datasets with varying pair completeness, validity, and done status
- Test filter behavior across random filter strings and datasets

**Property-Based Testing Library:** Hypothesis (Python)
- Each property test will run a minimum of 100 iterations
- Each property-based test will be tagged with comments referencing the design document property
- Tag format: '**Feature: statistics-display, Property {number}: {property_text}**'

**Testing Components:**
- **Statistics Calculation Logic**: Unit tests for computed properties
- **Reactive Updates**: Property tests for filter and done status changes
- **UI Integration**: Unit tests for Alpine.js component integration
- **Data Validation**: Property tests for handling malformed data

### Test Data Generation
- Generate random pairs with binary completeness (complete: both genomic and clinical present, or incomplete: missing one or both)
- Generate random QC results (passing/failing)
- Generate random done status distributions
- Generate random filter strings including edge cases (empty, special characters)