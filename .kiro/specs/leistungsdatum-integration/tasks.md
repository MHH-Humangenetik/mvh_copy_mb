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
  - **Property 23: NULL value handling**
  - **Validates: Requirements 6.1**

- [x] 3.2 Write property test for legacy format handling
  - **Property 24: Legacy format handling**
  - **Validates: Requirements 6.3**

- [ ] 4. Update GEPADO integration for separate MV_servicedate fields
  - Replace MV_output_date field with MV_servicedate_k and MV_servicedate_g fields in GepadoRecord dataclass
  - Update GEPADO query logic to retrieve both MV_servicedate_k and MV_servicedate_g fields
  - Implement comparison and update logic for output_date synchronization based on data type (clinical/genetic)
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 5.4_

- [x] 4.1 Write property test for GEPADO clinical data update inclusion
  - **Property 9: GEPADO clinical data update inclusion**
  - **Validates: Requirements 3.1**

- [x] 4.2 Write property test for GEPADO genetic data update inclusion
  - **Property 10: GEPADO genetic data update inclusion**
  - **Validates: Requirements 3.2**

- [x] 4.3 Write property test for GEPADO clinical empty field updates
  - **Property 11: GEPADO clinical empty field updates**
  - **Validates: Requirements 3.3**

- [x] 4.4 Write property test for GEPADO genetic empty field updates
  - **Property 12: GEPADO genetic empty field updates**
  - **Validates: Requirements 3.4**

- [x] 4.5 Write property test for GEPADO clinical conflict detection
  - **Property 13: GEPADO clinical conflict detection**
  - **Validates: Requirements 3.5**

- [x] 4.6 Write property test for GEPADO genetic conflict detection
  - **Property 14: GEPADO genetic conflict detection**
  - **Validates: Requirements 3.6**

- [x] 4.7 Write property test for GEPADO clinical validation logging
  - **Property 15: GEPADO clinical validation logging**
  - **Validates: Requirements 3.7**

- [x] 4.8 Write property test for GEPADO genetic validation logging
  - **Property 16: GEPADO genetic validation logging**
  - **Validates: Requirements 3.8**

- [x] 4.9 Write property test for GEPADO error resilience
  - **Property 17: GEPADO error resilience**
  - **Validates: Requirements 3.9**

- [x] 4.10 Write property test for GEPADO comparison validation
  - **Property 21: GEPADO comparison validation**
  - **Validates: Requirements 5.4**

- [x] 4.11 Write property test for missing clinical field resilience
  - **Property 25: Missing clinical field resilience**
  - **Validates: Requirements 6.4**

- [x] 4.12 Write property test for missing genetic field resilience
  - **Property 26: Missing genetic field resilience**
  - **Validates: Requirements 6.5**

- [x] 5. Update web interface models and endpoints for output_date
  - Add output_date field to RecordResponse and PairResponse models
  - Update web database queries to include output_date field
  - Modify API endpoints to return output_date in responses
  - _Requirements: 4.1, 4.5, 5.2, 6.5_

- [x] 5.1 Write property test for API compatibility
  - **Property 27: API compatibility**
  - **Validates: Requirements 4.1**

- [x] 6. Implement web frontend display for output_date column
  - Add output_date column to web table template
  - Implement date formatting for display
  - Add NULL value handling with appropriate placeholder text
  - _Requirements: 4.1, 4.2_

- [x] 6.1 Write property test for web display formatting
  - **Property 18: Web display formatting**
  - **Validates: Requirements 4.1**

- [x] 7. Add sorting functionality for output_date
  - Implement chronological sorting for output_date column
  - Handle NULL values appropriately in sorting
  - _Requirements: 4.3_

- [x] 7.1 Write property test for chronological sorting
  - **Property 19: Chronological sorting**
  - **Validates: Requirements 4.3**

- [x] 8. Implement database migration for existing installations
  - Create migration script to add output_date column to existing databases
  - Implement backward compatibility for records without output_date
  - Add migration data preservation logic for existing records
  - _Requirements: 2.3, 5.5, 6.2_

- [x] 8.1 Write property test for migration data preservation
  - **Property 22: Migration data preservation**
  - **Validates: Requirements 5.5**

- [ ] 9. Update existing code to use new GEPADO field structure
  - Modify all existing GEPADO integration code to use MV_servicedate_k and MV_servicedate_g instead of MV_output_date
  - Update field mapping logic to select appropriate field based on art_der_daten value
  - Update all tests to reflect new field structure
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9_

- [ ] 10. Checkpoint - Ensure all tests pass, ask the user if questions arise