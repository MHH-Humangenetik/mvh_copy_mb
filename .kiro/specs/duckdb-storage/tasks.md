# Implementation Plan

- [x] 1. Add DuckDB dependency and create database module structure
  - Add `duckdb>=0.9.0` to `pyproject.toml` dependencies
  - Create new file `src/mvh_copy_mb/database.py` for database operations
  - Add development dependencies: `pytest>=7.4.0`, `hypothesis>=6.90.0`, `pytest-cov>=4.1.0`
  - _Requirements: 5.1, 5.2_

- [x] 2. Implement data model and database schema
  - [x] 2.1 Create MeldebestaetigungRecord dataclass
    - Define dataclass with all required fields (vorgangsnummer, meldebestaetigung, source_file, metadata fields, case_id, gpas_domain, processed_at)
    - Add type hints for all fields
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 3.1, 3.2, 3.3, 3.4_

  - [x] 2.2 Implement database schema creation
    - Write SQL CREATE TABLE statement with Vorgangsnummer as primary key
    - Include all columns with appropriate data types and constraints
    - _Requirements: 1.2, 4.4_

  - [x] 2.3 Write property test for schema persistence
    - **Property 2: Schema persistence across sessions**
    - **Validates: Requirements 1.2, 1.3**

- [x] 3. Implement MeldebestaetigungDatabase class
  - [x] 3.1 Create database class with initialization and context manager
    - Implement `__init__` to accept database path
    - Implement `__enter__` and `__exit__` for context manager protocol
    - Implement `_create_schema` method to create table if not exists
    - Implement `close` method for explicit connection cleanup
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 5.4, 5.5_

  - [x] 3.2 Write property test for database file creation
    - **Property 1: Database file creation in correct location**
    - **Validates: Requirements 1.1, 1.4**

  - [x] 3.3 Write property test for connection cleanup
    - **Property 8: Database connection cleanup**
    - **Validates: Requirements 5.4, 5.5**

- [x] 4. Implement record storage operations
  - [x] 4.1 Implement upsert_record method
    - Write SQL INSERT OR REPLACE statement with parameterized queries
    - Handle all fields including optional case_id and gpas_domain
    - Convert MeldebestaetigungRecord to SQL parameters
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 3.1, 3.2, 3.3, 3.4, 3.5, 4.1, 4.2, 4.3_

  - [x] 4.2 Write property test for complete record storage
    - **Property 3: Complete record storage**
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 3.5**

  - [x] 4.3 Write property test for successful gPAS resolution storage
    - **Property 4: Successful gPAS resolution storage**
    - **Validates: Requirements 3.1, 3.2**

  - [x] 4.4 Write property test for failed gPAS resolution storage
    - **Property 5: Failed gPAS resolution storage**
    - **Validates: Requirements 3.3, 3.4**

  - [x] 4.5 Write property test for upsert preventing duplicates
    - **Property 6: Upsert prevents duplicates**
    - **Validates: Requirements 4.1, 4.4**

  - [x] 4.6 Write property test for record updates
    - **Property 7: Update modifies existing records**
    - **Validates: Requirements 4.2, 4.3**

- [x] 5. Implement record retrieval operations
  - [x] 5.1 Implement get_record method
    - Write SQL SELECT statement with Vorgangsnummer parameter
    - Convert SQL result to MeldebestaetigungRecord instance
    - Return None if record not found
    - _Requirements: 5.3_

  - [x] 5.2 Write unit tests for record retrieval
    - Test retrieval of existing records
    - Test retrieval of non-existent records returns None
    - Test retrieval with various field combinations
    - _Requirements: 5.3_

- [x] 6. Add error handling and logging
  - [x] 6.1 Implement error handling for database operations
    - Add try-except blocks around database operations
    - Log errors with appropriate context (record details, operation type)
    - Ensure errors don't terminate processing
    - _Requirements: 1.5_

  - [x] 6.2 Write property test for error resilience
    - **Property 9: Error resilience**
    - **Validates: Requirements 1.5**

- [x] 7. Integrate database with CLI workflow
  - [x] 7.1 Modify main() function to initialize database
    - Create database instance at start of processing
    - Pass database path based on input directory
    - Use context manager for automatic cleanup
    - _Requirements: 1.1, 1.4, 5.4, 5.5_

  - [x] 7.2 Modify process_row() to store records in database
    - Create MeldebestaetigungRecord from parsed data and gPAS results
    - Call database.upsert_record() after gPAS lookup
    - Handle database errors gracefully
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 3.1, 3.2, 3.3, 3.4, 4.1, 4.2, 4.3_

  - [x] 7.3 Write integration tests for end-to-end workflow
    - Test CSV processing with database storage
    - Test multiple processing runs with upsert behavior
    - Test with mock gPAS responses (success and failure)
    - _Requirements: All_

- [x] 8. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.
