# Design Document: DuckDB Storage for Meldebestätigungen

## Overview

This design adds persistent storage capabilities to the MVH Meldebestaetigung processing system using DuckDB, an embedded analytical database. The database will track all processed records, including metadata extracted from Meldebestätigungen and gPAS resolution results. The implementation will be modular, maintainable, and integrate seamlessly with the existing CLI workflow.

## Architecture

### High-Level Architecture

The system will follow a layered architecture:

1. **CLI Layer** (`cli.py`): Orchestrates the processing workflow
2. **Database Layer** (new `database.py`): Manages all DuckDB operations
3. **External Services Layer**: gPAS client for pseudonym resolution

```
┌─────────────────────────────────────┐
│         CLI Layer (cli.py)          │
│  - File discovery & iteration       │
│  - Progress tracking                │
│  - Orchestration                    │
└──────────────┬──────────────────────┘
               │
               ├──────────────┐
               │              │
               ▼              ▼
┌──────────────────────┐  ┌─────────────────────┐
│  Database Layer      │  │  gPAS Client        │
│  (database.py)       │  │  (existing)         │
│  - Schema management │  │  - Pseudonym        │
│  - CRUD operations   │  │    resolution       │
│  - Connection mgmt   │  └─────────────────────┘
└──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  DuckDB File                        │
│  (meldebestaettigungen.duckdb)      │
└─────────────────────────────────────┘
```

### Database Location

The database file `meldebestaettigungen.duckdb` will be stored in the input directory alongside the CSV files being processed. This co-location ensures:
- Easy backup of both source data and processed results
- Clear association between data and its processing history
- Simplified deployment (no separate database server needed)

## Components and Interfaces

### 1. Database Manager Class

A new `MeldebestaetigungDatabase` class will encapsulate all database operations:

```python
class MeldebestaetigungDatabase:
    def __init__(self, db_path: Path)
    def __enter__(self) -> 'MeldebestaetigungDatabase'
    def __exit__(self, exc_type, exc_val, exc_tb)
    def _create_schema(self) -> None
    def upsert_record(self, record: MeldebestaetigungRecord) -> None
    def get_record(self, vorgangsnummer: str) -> Optional[MeldebestaetigungRecord]
    def close(self) -> None
```

**Responsibilities:**
- Database connection lifecycle management
- Schema creation and validation
- Record insertion and updates (upsert operations)
- Query operations for retrieving records

### 2. Data Model Class

A dataclass to represent Meldebestaetigung records:

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
```

### 3. CLI Integration

The existing `main()` function will be modified to:
1. Initialize the database at startup
2. Pass the database instance to processing functions
3. Store records after each successful parse and gPAS lookup
4. Close the database connection on exit

## Data Models

### Database Schema

**Table: `meldebestaettigungen`**

| Column Name          | Data Type    | Constraints                    | Description                                      |
|---------------------|--------------|--------------------------------|--------------------------------------------------|
| vorgangsnummer      | VARCHAR      | NOT NULL, PRIMARY KEY          | Pseudonymized identifier (unique)                |
| source_file         | VARCHAR      | NOT NULL                       | Name of source CSV file                          |
| meldebestaetigung   | VARCHAR      | NOT NULL                       | Complete Meldebestaetigung string                |
| typ_der_meldung     | VARCHAR      | NOT NULL                       | Type of report (0=initial, etc.)                 |
| indikationsbereich  | VARCHAR      | NOT NULL                       | Medical indication area                          |
| art_der_daten       | VARCHAR      | NOT NULL                       | Type of data                                     |
| ergebnis_qc         | VARCHAR      | NOT NULL                       | QC result (1=passed, etc.)                       |
| case_id             | VARCHAR      | NULL                           | Resolved case ID from gPAS (NULL if not found)   |
| gpas_domain         | VARCHAR      | NULL                           | gPAS domain that resolved the pseudonym          |
| processed_at        | TIMESTAMP    | NOT NULL                       | Timestamp when record was processed              |

**Primary Key:** `vorgangsnummer`

**Rationale:**
- Vorgangsnummer is unique across all files, making it suitable as the sole primary key
- When the same Vorgangsnummer appears in a new file, the record is updated with the latest information
- source_file tracks which file most recently provided this Vorgangsnummer
- VARCHAR types for flexibility with varying identifier formats
- NULL allowed for case_id and gpas_domain to represent resolution failures
- TIMESTAMP for precise processing time tracking

### Upsert Strategy

The system will use DuckDB's `INSERT OR REPLACE` functionality:
- On conflict (same vorgangsnummer), update all fields including source_file
- This handles reprocessing scenarios where the same Vorgangsnummer appears in different files
- Ensures the latest processing results are always stored
- The source_file field will reflect which file most recently contained this Vorgangsnummer

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*


### Property 1: Database file creation in correct location
*For any* valid input directory path, when the database is initialized, the database file should exist at the path `{input_directory}/meldebestaettigungen.duckdb`
**Validates: Requirements 1.1, 1.4**

### Property 2: Schema persistence across sessions
*For any* database that has been created and closed, when reopened, the schema should still exist and be queryable
**Validates: Requirements 1.2, 1.3**

### Property 3: Complete record storage
*For any* valid Meldebestaetigung record with all required fields (Vorgangsnummer, Meldebestaetigung string, metadata fields, source file, timestamp), when stored in the database, retrieving the record should return all fields with their original values
**Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 3.5**

### Property 4: Successful gPAS resolution storage
*For any* Meldebestaetigung where gPAS successfully resolves the Vorgangsnummer, when stored in the database, both the Case ID and the resolving domain name should be non-NULL and match the gPAS response
**Validates: Requirements 3.1, 3.2**

### Property 5: Failed gPAS resolution storage
*For any* Meldebestaetigung where gPAS fails to resolve the Vorgangsnummer, when stored in the database, both the Case ID and domain name fields should be NULL
**Validates: Requirements 3.3, 3.4**

### Property 6: Upsert prevents duplicates
*For any* Meldebestaetigung record with a given Vorgangsnummer, when inserted multiple times (even from different source files), the database should contain exactly one record with that Vorgangsnummer
**Validates: Requirements 4.1, 4.4**

### Property 7: Update modifies existing records
*For any* existing database record, when updated with new values (timestamp or gPAS results), retrieving the record should return the updated values, not the original values
**Validates: Requirements 4.2, 4.3**

### Property 8: Database connection cleanup
*For any* database instance, when closed (either explicitly or via context manager exit), subsequent operations should fail or require reopening the connection
**Validates: Requirements 5.4, 5.5**

### Property 9: Error resilience
*For any* database operation that raises an exception, the system should log the error and continue processing subsequent records without terminating
**Validates: Requirements 1.5**

## Error Handling

### Database Connection Errors

**Scenario:** Database file cannot be created or opened
- **Handling:** Log error with full path and permissions information
- **Recovery:** Raise exception to prevent processing without storage capability
- **User Impact:** Clear error message indicating database initialization failure

### Schema Creation Errors

**Scenario:** Schema creation fails due to SQL errors or permissions
- **Handling:** Log the SQL error and attempted schema
- **Recovery:** Raise exception as the system cannot function without proper schema
- **User Impact:** Error message with troubleshooting steps

### Record Insertion/Update Errors

**Scenario:** Individual record upsert fails (e.g., data type mismatch, constraint violation)
- **Handling:** Log the error with the problematic record details
- **Recovery:** Continue processing remaining records (fail gracefully)
- **User Impact:** Warning logged, processing continues, failed records reported at end

### Connection Cleanup Errors

**Scenario:** Database connection cannot be closed cleanly
- **Handling:** Log the error but don't raise exception
- **Recovery:** Best-effort cleanup, allow application to exit
- **User Impact:** Warning logged, no blocking of application exit

### Data Integrity Errors

**Scenario:** Retrieved data doesn't match expected schema or types
- **Handling:** Log the discrepancy with record identifier
- **Recovery:** Return None or raise specific exception depending on context
- **User Impact:** Warning logged, affected records skipped

## Testing Strategy

### Unit Testing Approach

The implementation will include focused unit tests for:

1. **Database initialization**
   - Test database file creation in specified directory
   - Test schema creation on first initialization

2. **Record operations**
   - Test single record insertion
   - Test record retrieval by composite key
   - Test upsert behavior (insert vs update)

3. **Edge cases**
   - Empty string handling in fields
   - NULL value handling for optional fields (case_id, gpas_domain)
   - Very long strings in VARCHAR fields
   - Special characters in identifiers

4. **Error scenarios**
   - Invalid database path
   - Missing required fields in record
   - Database file permissions issues

### Property-Based Testing Approach

Property-based testing will verify universal correctness properties using the **Hypothesis** library for Python. Each property test will:
- Run a minimum of 100 iterations with randomly generated inputs
- Be tagged with a comment referencing the specific correctness property from this design document
- Use the format: `# Feature: duckdb-storage, Property {number}: {property_text}`

**Property test coverage:**

1. **Property 1: Database file creation** - Generate random valid directory paths, verify database file location
2. **Property 2: Schema persistence** - Generate random database instances, verify schema survives close/reopen
3. **Property 3: Complete record storage** - Generate random valid records, verify round-trip storage/retrieval
4. **Property 4: Successful resolution storage** - Generate random successful gPAS responses, verify storage
5. **Property 5: Failed resolution storage** - Generate random failed gPAS scenarios, verify NULL storage
6. **Property 6: Upsert prevents duplicates** - Generate random records, insert twice, verify single entry
7. **Property 7: Update modifies records** - Generate random updates, verify changes persist
8. **Property 8: Connection cleanup** - Generate random database operations, verify cleanup
9. **Property 9: Error resilience** - Generate random error scenarios, verify continued processing

**Hypothesis strategies:**
- `text()` for generating Vorgangsnummer, Meldebestaetigung strings, filenames
- `sampled_from()` for metadata fields with known valid values
- `datetimes()` for timestamp generation
- `none() | text()` for optional fields (case_id, gpas_domain)
- Custom strategies for valid MeldebestaetigungRecord instances

### Integration Testing

Integration tests will verify:
- End-to-end workflow: CSV processing → gPAS lookup → database storage
- Database persistence across multiple processing runs
- Interaction between CLI, database layer, and gPAS client
- Archive functionality with database updates

### Test Data

Test data will include:
- Sample Meldebestaetigung CSV files with various metadata combinations
- Mock gPAS responses (both successful and failed resolutions)
- Edge cases: malformed Meldebestaetigung strings, missing fields
- Duplicate records for upsert testing

## Implementation Notes

### DuckDB Python Library

The implementation will use the `duckdb` Python package:
- Add `duckdb>=0.9.0` to project dependencies
- Use DuckDB's Python API for all database operations
- Leverage DuckDB's ACID properties for data consistency

### Context Manager Pattern

The `MeldebestaetigungDatabase` class will implement the context manager protocol:
```python
with MeldebestaetigungDatabase(db_path) as db:
    db.upsert_record(record)
# Connection automatically closed
```

This ensures proper resource cleanup even if exceptions occur.

### SQL Parameterization

All SQL queries will use parameterized statements to:
- Prevent SQL injection (though not a concern with local embedded DB)
- Handle special characters in data correctly
- Improve query performance through prepared statements

### Performance Considerations

- DuckDB is optimized for analytical queries and batch operations
- Single-row inserts are acceptable for this use case (processing CSV files)
- Consider batch inserts if processing very large CSV files (>10,000 rows)
- Database file size will grow with records; monitor disk space in production

### Migration Strategy

For existing deployments:
- First run will create the database and populate it with current processing
- No migration of historical data needed (database starts fresh)
- Future schema changes can use DuckDB's ALTER TABLE capabilities

## Dependencies

New dependencies to add to `pyproject.toml`:
```toml
dependencies = [
    # ... existing dependencies ...
    "duckdb>=0.9.0",
]
```

Development dependencies for testing:
```toml
[project.optional-dependencies]
dev = [
    "pytest>=7.4.0",
    "hypothesis>=6.90.0",
    "pytest-cov>=4.1.0",
]
```

## Future Enhancements

Potential future improvements (out of scope for this design):

1. **Query Interface**: Add CLI commands to query the database (e.g., `--query-stats`, `--list-failed`)
2. **Export Functionality**: Export database contents to CSV or other formats
3. **Analytics**: Built-in reporting on processing success rates, common failure patterns
4. **Incremental Processing**: Skip already-processed files based on database records
5. **Data Retention**: Automatic cleanup of old records based on age or count
6. **Multi-table Schema**: Separate tables for metadata, gPAS results, and processing logs
