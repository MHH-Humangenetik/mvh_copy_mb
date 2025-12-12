# Implementation Plan

- [x] 1. Set up project dependencies and environment configuration
  - Add pymssql dependency to pyproject.toml for MSSQL database connectivity
  - Update .env.example with MSSQL configuration variables (MSSQL_HOST, MSSQL_DATABASE, MSSQL_USERNAME, MSSQL_PASSWORD)
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

- [x] 2. Create HL7 case ID extraction module
  - [x] 2.1 Implement HL7 ID extraction functions
    - Write extract_hl7_case_id function to parse HUMGEN patterns from Meldebestätigung strings
    - Write validate_hl7_format function for ID format validation
    - Handle edge cases: multiple patterns, no patterns, invalid formats
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

  - [x] 2.2 Write property test for HL7 ID extraction consistency
    - **Property 1: HL7 ID Extraction Consistency**
    - **Validates: Requirements 3.1, 3.2**

  - [x] 2.3 Write property test for HL7 pattern edge cases
    - **Property 6: HL7 Pattern Handling Edge Cases**
    - **Validates: Requirements 3.3, 3.4, 3.5**

- [x] 3. Create gepado database client module
  - [x] 3.1 Implement GepadoRecord data class
    - Create dataclass with hl7_case_id, vng, vnk, ibe_g, ibe_k fields
    - Add type hints and optional field handling
    - _Requirements: 4.3, 4.4, 4.5, 4.6_

  - [x] 3.2 Implement GepadoClient class
    - Create client class with MSSQL connection management
    - Implement query_record method with parameterized SQL query
    - Implement update_record method for field updates
    - Add proper connection lifecycle management
    - _Requirements: 1.1, 2.1, 2.2, 2.3, 2.4, 2.5_

  - [x] 3.3 Write property test for environment configuration usage
    - **Property 2: Environment Configuration Usage**
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5**

  - [x] 3.4 Write unit tests for GepadoClient with mocked connections
    - Test client initialization with various configuration combinations
    - Test query and update operations with mocked database responses
    - Test error handling for connection and query failures
    - _Requirements: 1.1, 1.4, 1.5_

- [ ] 4. Implement data validation and update logic
  - [ ] 4.1 Create data field mapping functions
    - Implement logic to map Art der Daten to correct VN and IBE field names
    - Add validation for genomic vs clinical data type handling
    - _Requirements: 4.3, 4.4, 4.5, 4.6_

  - [ ] 4.2 Implement record validation and update logic
    - Create functions to compare existing gepado data with Meldebestätigung data
    - Implement conditional update logic for empty fields only
    - Add data mismatch detection and logging
    - _Requirements: 4.3, 4.4, 4.5, 4.6, 4.7_

  - [ ] 4.3 Write property test for data field mapping and updates
    - **Property 4: Data Field Mapping and Updates**
    - **Validates: Requirements 4.3, 4.4, 4.5, 4.6**

  - [ ] 4.4 Write property test for update operation idempotence
    - **Property 5: Update Operation Idempotence**
    - **Validates: Requirements 4.3, 4.4, 4.5, 4.6**

- [ ] 5. Add record processing filter
  - [ ] 5.1 Implement QC and Typ der Meldung validation
    - Add filtering logic to check Ergebnis QC = 1 and Typ der Meldung = 0
    - Ensure gepado processing is skipped for invalid records
    - _Requirements: 4.1, 4.2_

  - [ ] 5.2 Write property test for record processing filter
    - **Property 3: Record Processing Filter**
    - **Validates: Requirements 4.1, 4.2**

- [ ] 6. Integrate gepado functionality into CLI
  - [ ] 6.1 Add --update-gepado CLI parameter
    - Extend existing CLI with new optional boolean parameter
    - Update help text and parameter documentation
    - _Requirements: 1.1_

  - [ ] 6.2 Modify process_row function for gepado integration
    - Add conditional gepado processing based on --update-gepado flag
    - Integrate HL7 ID extraction, record querying, and updating
    - Ensure existing functionality remains unchanged when flag is not used
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

  - [ ] 6.3 Add comprehensive logging throughout gepado operations
    - Implement detailed logging for all gepado operations
    - Include HL7 case IDs, field names, and operation results in log messages
    - Add error logging with sanitized exception details
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

  - [ ] 6.4 Write property test for comprehensive logging behavior
    - **Property 7: Comprehensive Logging Behavior**
    - **Validates: Requirements 1.4, 1.5, 4.7, 5.1, 5.2, 5.3, 5.4, 5.5**

- [ ] 7. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 8. Create integration tests
  - Write end-to-end tests for CLI with gepado integration enabled
  - Test complete workflow from CSV processing to gepado updates
  - Verify error handling and logging in integration scenarios
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_