# Implementation Plan

- [x] 1. Create hash string parsing utilities for Leistungsdatum extraction
  - Implement function to parse Leistungsdatum from hash string second field
  - Add validation for JJJJMMTTZZZ format and date extraction logic
  - Include error handling for malformed inputs and invalid dates
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

- [x] 1.1 Write property test for hash string parsing
  - **Property 1: Hash string parsing extracts correct field**
  - **Validates: Requirements 1.1**

- [x] 1.2 Write property test for date format validation
  - **Property 2: Date format validation**
  - **Validates: Requirements 1.2**

- [x] 1.3 Write property test for date portion extraction
  - **Property 3: Date portion extraction**
  - **Validates: Requirements 1.3**

- [x] 1.4 Write property test for invalid date handling
  - **Property 4: Invalid date handling**
  - **Validates: Requirements 1.4**

- [x] 1.5 Write property test for malformed input resilience
  - **Property 5: Malformed input resilience**
  - **Validates: Requirements 1.5**

- [x] 2. Update database schema and models for output_date field
  - Add output_date column to DuckDB meldebestaetigungen table schema
  - Update MeldebestaetigungRecord dataclass to include output_date field
  - Implement schema migration logic for existing databases
  - _Requirements: 2.1, 2.2, 2.3, 5.1_

- [x] 2.1 Write property test for database storage consistency
  - **Property 6: Database storage consistency**
  - **Validates: Requirements 2.2**

- [x] 2.2 Write property test for query result completeness
  - **Property 7: Query result completeness**
  - **Validates: Requirements 2.4**

- [x] 2.3 Write property test for NULL storage handling
  - **Property 8: NULL storage for unparseable dates**
  - **Validates: Requirements 2.5**

- [x] 3. Integrate Leistungsdatum extraction into main processing pipeline
  - Update CLI processing to extract Leistungsdatum from hash strings
  - Modify database upsert operations to include output_date field
  - Add error handling and logging for extraction failures
  - _Requirements: 2.4, 2.5, 5.3, 6.1, 6.3_

- [x] 3.1 Write property test for NULL value handling
  - **Property 21: NULL value handling**
  - **Validates: Requirements 6.1**

- [x] 3.2 Write property test for legacy format handling
  - **Property 22: Legacy format handling**
  - **Validates: Requirements 6.3**

- [x] 4. Update GEPADO integration for MV_output_date field
  - Add MV_output_date field to GepadoRecord dataclass
  - Update GEPADO query logic to retrieve MV_output_date field
  - Implement comparison and update logic for output_date synchronization
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 5.4_

- [x] 4.1 Write property test for GEPADO update inclusion
  - **Property 9: GEPADO update inclusion**
  - **Validates: Requirements 3.1**

- [x] 4.2 Write property test for GEPADO empty field updates
  - **Property 10: GEPADO empty field updates**
  - **Validates: Requirements 3.2**

- [x] 4.3 Write property test for GEPADO conflict detection
  - **Property 11: GEPADO conflict detection**
  - **Validates: Requirements 3.3**

- [x] 4.4 Write property test for GEPADO validation logging
  - **Property 12: GEPADO validation logging**
  - **Validates: Requirements 3.4**

- [x] 4.5 Write property test for GEPADO error resilience
  - **Property 13: GEPADO error resilience**
  - **Validates: Requirements 3.5**

- [x] 4.6 Write property test for GEPADO comparison validation
  - **Property 19: GEPADO comparison validation**
  - **Validates: Requirements 5.4**

- [x] 4.7 Write property test for missing field resilience
  - **Property 23: Missing field resilience**
  - **Validates: Requirements 6.4**

- [x] 5. Update web interface models and endpoints for output_date
  - Add output_date field to RecordResponse and PairResponse models
  - Update web database queries to include output_date field
  - Modify API endpoints to return output_date in responses
  - _Requirements: 4.1, 4.5, 5.2, 6.5_

- [x] 5.1 Write property test for API compatibility
  - **Property 24: API compatibility**
  - **Validates: Requirements 6.5**

- [x] 6. Implement web frontend display for output_date column
  - Add output_date column to web table template
  - Implement date formatting for display
  - Add NULL value handling with appropriate placeholder text
  - _Requirements: 4.1, 4.2_

- [x] 6.1 Write property test for web display formatting
  - **Property 14: Web display formatting**
  - **Validates: Requirements 4.1**

- [x] 7. Add sorting functionality for output_date
  - Implement chronological sorting for output_date column
  - Handle NULL values appropriately in sorting
  - _Requirements: 4.3_

- [x] 7.1 Write property test for chronological sorting
  - **Property 15: Chronological sorting**
  - **Validates: Requirements 4.3**

- [x] 8. Implement database migration for existing installations
  - Create migration script to add output_date column to existing databases
  - Implement backward compatibility for records without output_date
  - Add migration data preservation logic for existing records
  - _Requirements: 2.3, 5.5, 6.2_

- [x] 8.1 Write property test for migration data preservation
  - **Property 16: Migration data preservation**
  - **Validates: Requirements 5.5**

- [x] 9. Checkpoint - Ensure all tests pass, ask the user if questions arise