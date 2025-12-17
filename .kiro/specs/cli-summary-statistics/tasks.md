# Implementation Plan

- [ ] 1. Create ProcessingStatistics class and progress bar renderer
- [x] 1.1 Update ProcessingStatistics dataclass for pairing logic
  - Modify dataclass to track ready pairs instead of individual ready files
  - Add internal Case ID tracking for pairing logic
  - Add GEPADO no-updates-needed counter
  - Add methods for pairing calculation and finalization
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 2.1, 2.2, 2.3, 2.4_

- [x] 1.2 Implement progress bar rendering function
  - Create render_progress_bar function with bracket formatting
  - Handle edge cases like zero totals and empty bars
  - _Requirements: 3.1, 3.2, 3.5, 4.4_

- [x] 1.3 Write property test for pairing logic accuracy
  - **Property 1: Pairing logic accuracy**
  - **Validates: Requirements 5.1**

- [x] 1.4 Write property test for unpaired file categorization
  - **Property 2: Unpaired file categorization**
  - **Validates: Requirements 5.2**

- [x] 1.5 Write property test for total file calculation consistency
  - **Property 3: Total file calculation consistency**
  - **Validates: Requirements 1.5, 5.5**

- [x] 2. Implement statistics display formatter
- [x] 2.1 Update display_statistics function for new statistics
  - Update labels to show "Ready pairs" instead of "Ready files"
  - Add "No updates needed" line to GEPADO statistics
  - Update progress bar calculations for new totals
  - _Requirements: 4.1, 4.2, 4.3_

- [x] 2.2 Write property test for GEPADO operation categorization
  - **Property 4: GEPADO operation categorization**
  - **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 5.4**

- [x] 2.3 Write property test for progress bar width consistency
  - **Property 5: Progress bar width consistency**
  - **Validates: Requirements 3.1, 3.2**

- [x] 2.4 Write property test for progress bar calculation accuracy
  - **Property 6: Progress bar calculation accuracy**
  - **Validates: Requirements 3.3, 3.4, 4.4**

- [x] 2.5 Write property test for statistics formatting consistency
  - **Property 7: Statistics formatting consistency**
  - **Validates: Requirements 4.2**

- [x] 3. Integrate statistics tracking into CLI workflow
- [x] 3.1 Update CLI workflow to use pairing logic
  - Modify process_row to track resolved Case IDs and data types
  - Remove direct ready/unpaired counting from process_row
  - Add finalize_pairing_statistics call after all files processed
  - _Requirements: 5.1, 5.2, 5.3_

- [x] 3.2 Update GEPADO statistics tracking for no-updates-needed
  - Modify validate_and_update_record to distinguish actual updates from validation-only
  - Track no-updates-needed cases separately from actual updates
  - _Requirements: 2.3, 5.4_

- [x] 4. Add statistics display to CLI main function
- [x] 4.1 Update main function for pairing workflow
  - Ensure finalize_pairing_statistics is called after all files processed
  - Update display_statistics call with new statistics structure
  - _Requirements: 1.1, 1.2, 1.3, 1.4_

- [x] 4.2 Write unit tests for updated display formatting
  - Test display with ready pairs instead of ready files
  - Test GEPADO statistics with no-updates-needed counter
  - Test visual separator and formatting consistency
  - _Requirements: 2.5, 4.3_

- [x] 5. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 6. Integration testing and validation
- [x] 6.1 Update existing integration tests for pairing logic
  - Modify test expectations to use ready pairs instead of ready files
  - Update test data to properly test pairing scenarios
  - Verify GEPADO no-updates-needed tracking
  - _Requirements: All requirements_

- [x] 6.2 Test complete CLI workflow with updated statistics
  - Verify pairing logic matches web interface behavior
  - Test GEPADO statistics distinguish updates from validation-only
  - Test with various pairing scenarios (complete pairs, unpaired files)
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

- [x] 7. Final Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.