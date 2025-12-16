# Implementation Plan

- [x] 1. Create HL7 case ID validation utilities
  - Implement function to query sapVisitingType for a given hl7_case_id
  - Add validation logic to check if sapVisitingType equals 'GS'
  - Include error handling for database connection issues and missing records
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

- [ ]* 1.1 Write property test for sapVisitingType validation accuracy
  - **Property 1: sapVisitingType validation accuracy**
  - **Validates: Requirements 1.1**

- [ ]* 1.2 Write property test for correct case passthrough
  - **Property 2: Correct case passthrough**
  - **Validates: Requirements 1.2**

- [ ]* 1.3 Write property test for incorrect case detection
  - **Property 3: Incorrect case detection**
  - **Validates: Requirements 1.3**

- [x] 2. Implement patient GUID extraction and case search
  - Create function to extract guid_patient from av_ordermanagement for given hl7_case_id
  - Implement search logic to find all cases with same patient GUID and sapVisitingType 'GS'
  - Add handling for multiple, single, or no matching cases
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

- [ ]* 2.1 Write property test for patient GUID extraction consistency
  - **Property 4: Patient GUID extraction consistency**
  - **Validates: Requirements 2.1**

- [ ]* 2.2 Write property test for correct case search completeness
  - **Property 5: Correct case search completeness**
  - **Validates: Requirements 2.2**

- [ ]* 2.3 Write property test for single correct case selection
  - **Property 6: Single correct case selection**
  - **Validates: Requirements 2.3**

- [ ]* 2.4 Write property test for multiple cases error handling
  - **Property 7: Multiple cases error handling**
  - **Validates: Requirements 2.4**

- [ ]* 2.5 Write property test for no cases warning handling
  - **Property 8: No cases warning handling**
  - **Validates: Requirements 2.5**

- [x] 3. Create comprehensive logging system for corrections
  - Implement detailed logging for successful corrections with original and corrected IDs
  - Add logging for multiple candidate cases with all case IDs
  - Create warning logs for cases with no correction candidates
  - Include error logging for database failures and validation issues
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

- [ ]* 3.1 Write property test for correction logging completeness
  - **Property 9: Correction logging completeness**
  - **Validates: Requirements 3.1**

- [ ]* 3.2 Write property test for multiple candidates logging
  - **Property 10: Multiple candidates logging**
  - **Validates: Requirements 3.2**

- [x] 4. Integrate correction system with GEPADO operations
  - Create main correction orchestrator function that combines validation and search
  - Update GEPADO integration to use corrected hl7_case_id for updates only
  - Ensure local database storage and file naming continue using original hl7_case_id
  - Add clear logging to indicate when corrected case ID is being used
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [ ]* 4.1 Write property test for local storage preservation
  - **Property 11: Local storage preservation**
  - **Validates: Requirements 4.1**

- [ ]* 4.2 Write property test for file naming preservation
  - **Property 12: File naming preservation**
  - **Validates: Requirements 4.2**

- [ ]* 4.3 Write property test for GEPADO update correction
  - **Property 13: GEPADO update correction**
  - **Validates: Requirements 4.3**

- [ ] 6. Update existing GEPADO integration to use correction system
  - Modify validate_and_update_record function to use corrected hl7_case_id
  - Update all GEPADO query and update operations to use correction system
  - Ensure backward compatibility with existing GEPADO processing
  - Add integration tests to verify correction system works with existing workflows
  - _Requirements: 1.1, 1.2, 1.3, 2.1, 2.2, 2.3, 4.3, 4.4_

- [ ] 7. Checkpoint - Ensure all tests pass, ask the user if questions arise