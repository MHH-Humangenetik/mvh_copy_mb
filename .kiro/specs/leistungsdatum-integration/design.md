# Design Document

## Overview

The Leistungsdatum integration feature adds comprehensive support for service dates throughout the MVGenomSeq system. This involves extracting the Leistungsdatum from Meldebestätigung hash strings, storing it in both DuckDB and GEPADO databases, and displaying it in the web interface. The implementation ensures backward compatibility while providing robust date handling and validation.

The Leistungsdatum represents a critical piece of information for the Meldebestätigung system, as it indicates when the medical service was provided. According to the documentation, it appears as the second field in the hash string in JJJJMMTTZZZ format, where the first 8 characters represent the actual date and the last 3 characters are a counter.

## Architecture

The integration follows the existing modular architecture of the system:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Meldebestäti-  │    │   Database       │    │   Web Interface │
│  gung Processing│───▶│   Storage        │───▶│   Display       │
│                 │    │   (DuckDB)       │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Hash String   │    │   Schema         │    │   Table         │
│   Parsing       │    │   Migration      │    │   Columns       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │
         │                       │
         ▼                       ▼
┌─────────────────┐    ┌──────────────────┐
│   GEPADO        │    │   Data Models    │
│   Integration   │    │   Updates        │
└─────────────────┘    └──────────────────┘
```

## Components and Interfaces

### Hash String Parser
- **Purpose**: Extract and validate Leistungsdatum from Meldebestätigung hash strings
- **Input**: Raw hash string with '&' separated fields
- **Output**: Parsed date object or None if invalid
- **Interface**: `parse_leistungsdatum(hash_string: str) -> Optional[date]`

### Database Schema Manager
- **Purpose**: Handle DuckDB schema migration to add output_date column
- **Input**: Database connection and existing schema
- **Output**: Updated schema with output_date column
- **Interface**: `migrate_schema_for_output_date(conn: DuckDBConnection) -> None`

### GEPADO Synchronization
- **Purpose**: Update GEPADO records with MV_output_date field
- **Input**: HL7 case ID, extracted Leistungsdatum, existing GEPADO client
- **Output**: Success/failure status and validation results
- **Interface**: `sync_output_date_to_gepado(client: GepadoClient, hl7_case_id: str, output_date: date) -> bool`

### Web Interface Updates
- **Purpose**: Display output_date in table and support sorting
- **Input**: Database records with output_date field
- **Output**: HTML table with formatted date column
- **Interface**: Updated templates and API endpoints

## Data Models

### Updated MeldebestaetigungRecord
```python
@dataclass
class MeldebestaetigungRecord:
    vorgangsnummer: str
    meldebestaetigung: str
    source_file: str
    typ_der_meldung: str
    indikationsbereich: str
    art_der_daten: str
    ergebnis_qc: str
    case_id: Optional[str]
    gpas_domain: Optional[str]
    processed_at: datetime
    is_done: bool = False
    output_date: Optional[date] = None  # New field
```

### Updated RecordResponse
```python
class RecordResponse(BaseModel):
    vorgangsnummer: str
    meldebestaetigung: str
    case_id: Optional[str]
    art_der_daten: str
    typ_der_meldung: str
    indikationsbereich: str
    ergebnis_qc: str
    source_file: str
    processed_at: datetime
    is_done: bool
    output_date: Optional[date] = None  # New field
```

### Updated GepadoRecord
```python
@dataclass
class GepadoRecord:
    hl7_case_id: str
    vng: Optional[str] = None
    vnk: Optional[str] = None
    ibe_g: Optional[str] = None
    ibe_k: Optional[str] = None
    mv_output_date: Optional[date] = None  # New field
```

### Database Schema Changes
```sql
-- DuckDB schema addition
ALTER TABLE meldebestaetigungen ADD COLUMN output_date DATE;

-- GEPADO field mapping
-- MV_output_date field in av2_ordermanagement_addfields view
-- Maps to base table [transact].[t_case_addFieldsExt].MV_output_date
```
## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

Property 1: Hash string parsing extracts correct field
*For any* valid Meldebestätigung hash string with at least 2 fields, parsing should extract the second field (position 1) as the Leistungsdatum
**Validates: Requirements 1.1**

Property 2: Date format validation
*For any* 11-character string in JJJJMMTTZZZ format with valid date components, the parser should successfully extract the date portion
**Validates: Requirements 1.2**

Property 3: Date portion extraction
*For any* valid Leistungsdatum string, extracting the date portion should return exactly the first 8 characters
**Validates: Requirements 1.3**

Property 4: Invalid date handling
*For any* invalid date string, the system should return NULL and log an appropriate error
**Validates: Requirements 1.4**

Property 5: Malformed input resilience
*For any* malformed hash string, the system should handle the error gracefully without crashing
**Validates: Requirements 1.5**

Property 6: Database storage consistency
*For any* valid Leistungsdatum, storing it in the database should preserve the date value accurately
**Validates: Requirements 2.2**

Property 7: Query result completeness
*For any* database query, the result set should include the output_date field when records contain it
**Validates: Requirements 2.4**

Property 8: NULL storage for unparseable dates
*For any* unparseable Leistungsdatum, the database should store NULL in the output_date column
**Validates: Requirements 2.5**

Property 9: GEPADO update inclusion
*For any* GEPADO update operation, the MV_output_date field should be included when a valid date is available
**Validates: Requirements 3.1**

Property 10: GEPADO empty field updates
*For any* GEPADO record with empty MV_output_date, the system should update it with the extracted Leistungsdatum
**Validates: Requirements 3.2**

Property 11: GEPADO conflict detection
*For any* GEPADO record with different MV_output_date value, the system should log a data mismatch error
**Validates: Requirements 3.3**

Property 12: GEPADO validation logging
*For any* GEPADO record with matching MV_output_date value, the system should log successful validation
**Validates: Requirements 3.4**

Property 13: GEPADO error resilience
*For any* GEPADO update failure, the system should log the error and continue processing other records
**Validates: Requirements 3.5**

Property 14: Web display formatting
*For any* record with output_date, the web interface should display it with appropriate date formatting
**Validates: Requirements 4.1**

Property 15: Chronological sorting
*For any* set of records with output_date values, sorting by date should order them chronologically
**Validates: Requirements 4.3**



Property 17: Meldebestätigung processing data flow
*For any* Meldebestätigung processing, the Leistungsdatum should be extracted and passed to downstream components
**Validates: Requirements 5.3**

Property 18: GEPADO comparison validation
*For any* GEPADO record comparison, output_date validation logic should be included in the comparison
**Validates: Requirements 5.4**

Property 19: Migration data preservation
*For any* existing record during migration, existing output_date values should be preserved
**Validates: Requirements 5.5**

Property 20: NULL value handling
*For any* record without output_date, the system should handle NULL values gracefully without errors
**Validates: Requirements 6.1**

Property 21: Legacy format handling
*For any* old Meldebestätigung format, the system should attempt extraction but continue processing if it fails
**Validates: Requirements 6.3**

Property 22: Missing field resilience
*For any* GEPADO record lacking MV_output_date field, the system should handle the missing field without errors
**Validates: Requirements 6.4**

Property 23: API compatibility
*For any* API response, the output_date field should be included while maintaining compatibility with existing clients
**Validates: Requirements 6.5**

## Error Handling

### Hash String Parsing Errors
- **Invalid format**: Log warning and return None for Leistungsdatum
- **Missing fields**: Handle gracefully, continue processing other fields
- **Invalid date values**: Log error with specific date that failed parsing

### Database Errors
- **Schema migration failures**: Log error and attempt to continue with existing schema
- **Connection issues**: Retry with exponential backoff, fail gracefully if persistent
- **Data type mismatches**: Log error and store NULL for problematic values

### GEPADO Integration Errors
- **Connection failures**: Log error and skip GEPADO updates for current batch
- **Field mapping issues**: Log warning and continue with available fields
- **Update conflicts**: Log detailed mismatch information for manual review

### Web Interface Errors
- **NULL date display**: Show empty cell or configurable placeholder text
- **Sorting with mixed NULL/valid dates**: Handle NULL values consistently (e.g., sort to end)

## Testing Strategy

### Unit Testing Approach
Unit tests will focus on individual components and specific edge cases:

- **Hash string parser**: Test with various valid/invalid formats, edge cases like leap years
- **Database operations**: Test schema creation, migration, and CRUD operations
- **GEPADO integration**: Test field mapping, update logic, and error scenarios
- **Date formatting**: Test display formatting for different locales and NULL values

### Property-Based Testing Approach
Property-based tests will verify universal behaviors across many inputs using **Hypothesis** for Python:

- **Minimum 100 iterations** per property test to ensure thorough coverage
- **Smart generators** that create realistic Meldebestätigung hash strings with valid date ranges
- **Edge case generators** for boundary conditions like year boundaries, invalid dates
- **Error injection** to test resilience with malformed inputs

Each property-based test will be tagged with comments explicitly referencing the correctness property:
- Format: `**Feature: leistungsdatum-integration, Property {number}: {property_text}**`
- Each correctness property will be implemented by a single property-based test
- Tests will focus on core functional logic and important edge cases

The dual testing approach ensures comprehensive coverage: unit tests catch specific bugs and integration issues, while property tests verify general correctness across the input space.