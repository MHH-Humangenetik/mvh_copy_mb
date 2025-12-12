# Design Document

## Overview

The gepado integration feature extends the existing MVH Meldebestätigung processing CLI tool to synchronize data with the gepado laboratory information system. This integration adds the capability to extract HL7 case IDs from processed files, query the gepado MSSQL database for existing records, and update missing data fields while validating existing ones.

The feature is designed as an optional enhancement triggered by the `--update-gepado` CLI parameter, ensuring backward compatibility with existing workflows. Only valid records (QC=1, Typ der Meldung=0) are processed for gepado integration.

## Architecture

The gepado integration follows a modular architecture that extends the existing CLI processing pipeline:

```
CLI Processing Pipeline
├── Existing Flow
│   ├── CSV File Processing
│   ├── gPAS Pseudonym Resolution  
│   ├── DuckDB Storage
│   └── File Organization
└── New Gepado Integration (Optional)
    ├── HL7 ID Extraction
    ├── MSSQL Connection Management
    ├── Gepado Record Querying
    └── Data Validation & Updates
```

The integration is implemented as a separate module that can be imported and used within the existing `process_row` function, maintaining clean separation of concerns.

## Components and Interfaces

### GepadoClient Class
A new client class responsible for managing MSSQL database connections and operations:

```python
class GepadoClient:
    def __init__(self, host: str, database: str, username: str, password: str)
    def connect(self) -> pymssql.Connection
    def query_record(self, hl7_case_id: str) -> Optional[GepadoRecord]
    def update_record(self, hl7_case_id: str, updates: dict) -> bool
    def close(self)
```

### HL7 ID Extraction Module
Utility functions for extracting HL7 case IDs from Meldebestätigung data:

```python
def extract_hl7_case_id(meldebestaetigung: str) -> Optional[str]
def validate_hl7_format(case_id: str) -> bool
```

### Configuration Extension
Environment variable support for MSSQL connection parameters:
- `MSSQL_HOST`: Database server hostname
- `MSSQL_DATABASE`: Database name
- `MSSQL_USERNAME`: Authentication username  
- `MSSQL_PASSWORD`: Authentication password

### CLI Parameter Extension
New optional parameter added to existing CLI interface:
- `--update-gepado`: Boolean flag to enable gepado integration

## Data Models

### GepadoRecord
Data class representing a gepado database record:

```python
@dataclass
class GepadoRecord:
    hl7_case_id: str
    vng: Optional[str]  # Vorgangsnummer genomic
    vnk: Optional[str]  # Vorgangsnummer clinical
    ibe_g: Optional[str]  # IBE string genomic
    ibe_k: Optional[str]  # IBE string clinical
```

### Update Operations
Dictionary structure for database updates:

```python
UpdateDict = Dict[str, str]  # field_name -> new_value
```

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: HL7 ID Extraction Consistency
*For any* Meldebestätigung string containing a valid HUMGEN pattern, extracting the HL7 case ID should return the same numeric value when called multiple times, and successful extraction should trigger gepado queries with that ID
**Validates: Requirements 3.1, 3.2**

### Property 2: Environment Configuration Usage
*For any* set of MSSQL environment variables (host, database, username, password), the system should consistently use these values when initializing the gepado client
**Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5**

### Property 3: Record Processing Filter
*For any* Meldebestätigung record, gepado processing should only occur when both Ergebnis QC equals 1 and Typ der Meldung equals 0, and should be skipped otherwise
**Validates: Requirements 4.1, 4.2**

### Property 4: Data Field Mapping and Updates
*For any* Art der Daten value and empty gepado fields, the system should consistently map to the correct field names (genomic -> VNg/IBE_g, clinical -> VNk/IBE_k) and update only empty fields with corresponding Vorgangsnummer and IBE String values
**Validates: Requirements 4.3, 4.4, 4.5, 4.6**

### Property 5: Update Operation Idempotence
*For any* gepado record with empty fields, performing the same update operation multiple times should result in the same final state
**Validates: Requirements 4.3, 4.4, 4.5, 4.6**

### Property 6: HL7 Pattern Handling Edge Cases
*For any* Meldebestätigung string, when multiple HUMGEN patterns exist the first valid numeric ID should be extracted, when no patterns exist gepado processing should be skipped, and when extraction fails a warning should be logged
**Validates: Requirements 3.3, 3.4, 3.5**

### Property 7: Comprehensive Logging Behavior
*For any* gepado operation (connection, query, validation, update, error), the system should produce consistent and informative log messages containing relevant identifiers and operation details
**Validates: Requirements 1.4, 1.5, 4.7, 5.1, 5.2, 5.3, 5.4, 5.5**

## Error Handling

### Database Connection Errors
- Connection timeout: Log error and continue processing without gepado updates
- Authentication failure: Log error with sanitized message (no password exposure)
- Network connectivity issues: Retry once, then skip gepado processing

### Data Validation Errors  
- HL7 ID extraction failure: Log warning and skip gepado processing for that record
- Missing required fields: Log error with field names and continue
- Data type mismatches: Log error with expected vs actual types

### SQL Operation Errors
- Query execution failure: Log error with sanitized SQL and continue
- Update operation failure: Log error and mark record for manual review
- Transaction rollback: Log warning and retry once

## Testing Strategy

### Unit Testing Approach
Unit tests will focus on individual components and specific scenarios:

- **HL7 ID Extraction**: Test various HUMGEN pattern formats and edge cases
- **GepadoClient Initialization**: Test client creation with mocked database connections
- **Data Validation**: Test field comparison logic with matching/mismatched data
- **Configuration Loading**: Test environment variable parsing and validation

### Property-Based Testing Approach  
Property-based tests will use the Hypothesis library to verify universal properties across many inputs:

- **HL7 Pattern Extraction**: Generate random Meldebestätigung strings with embedded HUMGEN patterns
- **Database Operations**: Generate random record data to test update idempotence
- **Field Mapping**: Generate random Art der Daten values to verify consistent field selection
- **Validation Logic**: Generate random existing/new data combinations to test comparison consistency

Each property-based test will run a minimum of 100 iterations to ensure comprehensive coverage of the input space. Tests will be tagged with comments referencing the specific correctness property they implement using the format: `**Feature: gepado-integration, Property {number}: {property_text}**`

### Integration Testing
- End-to-end CLI execution with gepado integration enabled
- Database transaction testing with rollback scenarios  
- Error handling verification with simulated failures

### Test Configuration
- Mock all MSSQL database connections and operations for testing
- Use dependency injection to allow mocking of GepadoClient in tests
- Property-based tests configured for 100+ iterations per property