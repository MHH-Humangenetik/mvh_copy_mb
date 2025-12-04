# Implementation Plan

- [x] 1. Update database schema to include done status column
  - Modify `_create_schema()` in database.py to include `is_done BOOLEAN DEFAULT FALSE` column
  - Update `MeldebestaetigungRecord` dataclass to include `is_done: bool` field
  - Update `upsert_record()` and `get_record()` methods to handle is_done field
  - _Requirements: 4.1, 4.2, 4.3, 4.5, 6.3_

- [x] 1.1 Write property test for done status persistence
  - **Property 11: Done status changes persist to database**
  - **Validates: Requirements 6.3**

- [x] 2. Create web database service layer
  - Implement `RecordPair` dataclass with case_id, genomic, clinical, is_complete, is_valid, is_done, priority_group fields
  - Implement `WebDatabaseService` class with methods for grouping records by Case ID
  - Add method to calculate priority groups (1: complete not done, 2: incomplete, 3: complete done)
  - Add method to determine pair completeness and validity
  - Add method to update done status for both records in a pair
  - _Requirements: 2.1, 2.2, 3.1, 3.2, 4.3, 8.1, 8.2, 8.3, 8.4_

- [x] 2.1 Write property test for record grouping by Case ID
  - **Property 4: Records with same Case ID are consecutive**
  - **Validates: Requirements 2.1, 2.2**

- [x] 2.2 Write property test for priority group calculation
  - **Property 12: Priority group 1 contains complete pairs not done**
  - **Property 13: Priority group 2 contains incomplete pairs**
  - **Property 14: Priority group 3 contains complete pairs done**
  - **Validates: Requirements 8.2, 8.3, 8.4**

- [x] 2.3 Write property test for done status update affecting both records
  - **Property 9: Done status update affects both records in pair**
  - **Validates: Requirements 4.3**

- [ ] 3. Set up FastAPI application structure
  - Create `web.py` with FastAPI app initialization
  - Configure Jinja2 templates directory
  - Set up static files serving for CSS and JS
  - Add CORS middleware if needed
  - Configure logging
  - _Requirements: 7.1, 7.2, 7.5, 10.1, 10.4_

- [ ] 4. Create Pydantic models for API
  - Create `DoneStatusUpdate` model for checkbox updates
  - Create `RecordResponse` model for individual records
  - Create `PairResponse` model for record pairs
  - Add validation and type hints
  - _Requirements: 7.4_

- [ ] 5. Implement main page endpoint
  - Create GET `/` route that queries database for all records
  - Group records into pairs using WebDatabaseService
  - Sort pairs by priority group, Case ID, and data type
  - Render index.html template with pairs data
  - Handle empty database case
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 6.1, 6.2_

- [ ] 5.1 Write property test for sorting order
  - **Property 2: Records are sorted by priority then Case ID then data type**
  - **Validates: Requirements 1.3, 8.1, 8.5**

- [ ] 5.2 Write property test for database state matching display
  - **Property 3: Displayed records match database state**
  - **Validates: Requirements 1.4, 6.1**

- [ ] 6. Implement done status update endpoint
  - Create POST `/api/done/{case_id}` route
  - Validate Case ID exists and pair is complete
  - Update done status for both records in pair
  - Return updated pair HTML partial for HTMX swap
  - Handle errors (invalid Case ID, incomplete pair, database errors)
  - _Requirements: 4.2, 4.3, 6.3_

- [ ] 6.1 Write unit tests for done status endpoint
  - Test successful update for complete pair
  - Test rejection for incomplete pair
  - Test 404 for non-existent Case ID
  - Test database error handling
  - _Requirements: 4.2, 4.3, 6.3_

- [ ] 7. Create HTML templates with Rosé Pine styling
  - Create `templates/index.html` with base layout
  - Add Rosé Pine CSS variables and theme styles
  - Include HTMX and Alpine.js from CDN
  - Add Milligram CSS base styles
  - Create table structure with sortable headers
  - Add filter input field
  - _Requirements: 1.1, 1.2, 9.1, 9.2, 10.1, 10.2, 10.3_

- [ ] 8. Implement table row rendering with pair grouping
  - Create `templates/partials/pair_row.html` for reusable pair rendering
  - Add visual styling for genomic/clinical pair grouping
  - Add complete pair indicator (when both records present)
  - Add valid pair indicator (when both present with passing QC)
  - Add done checkbox (enabled only for complete pairs)
  - Add HTMX attributes for checkbox updates
  - Apply priority group styling
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 3.2, 3.3, 3.4, 3.5, 4.1, 4.2, 8.1_

- [ ] 8.1 Write property test for required fields display
  - **Property 1: All required fields are displayed**
  - **Validates: Requirements 1.2**

- [ ] 8.2 Write property test for pair grouping indicators
  - **Property 5: Paired records have grouping indicators**
  - **Validates: Requirements 2.3**

- [ ] 8.3 Write property test for complete pair indicator
  - **Property 6: Complete pair indicator is conditional**
  - **Validates: Requirements 2.4, 2.5**

- [ ] 8.4 Write property test for valid pair indicator
  - **Property 7: Valid pair indicator is conditional on completeness and QC**
  - **Validates: Requirements 3.2, 3.3, 3.4, 3.5**

- [ ] 8.5 Write property test for checkbox presence
  - **Property 8: Done checkbox only for complete pairs**
  - **Validates: Requirements 4.1, 4.2**

- [ ] 8.6 Write property test for checkbox state matching database
  - **Property 10: Checkbox state reflects database state**
  - **Validates: Requirements 4.5**

- [ ] 9. Implement Alpine.js filtering functionality
  - Create Alpine.js component in index.html with pairs data
  - Implement filter logic that searches across all columns
  - Bind filter input to Alpine.js state
  - Ensure filtered results maintain pair grouping
  - _Requirements: 9.1, 9.4, 9.5, 10.2_

- [ ] 9.1 Write property test for client-side filtering
  - **Property 15: Client-side filter matches all columns**
  - **Validates: Requirements 9.1**

- [ ] 9.2 Write property test for filter preserving pair grouping
  - **Property 17: Filtering and sorting preserve pair grouping**
  - **Validates: Requirements 9.4**

- [ ] 10. Implement Alpine.js sorting functionality
  - Add sortBy method to Alpine.js component
  - Implement sort toggle (ascending/descending)
  - Add click handlers to table headers
  - Maintain priority group as primary sort
  - Ensure sorted results maintain pair grouping
  - _Requirements: 9.2, 9.3, 9.4, 9.5, 10.2_

- [ ] 10.1 Write property test for client-side sorting
  - **Property 16: Client-side sort orders by selected column**
  - **Validates: Requirements 9.2, 9.3**

- [ ] 11. Create custom CSS file with Rosé Pine theme
  - Create `static/css/custom.css` with Rosé Pine variables
  - Add dark mode support via prefers-color-scheme
  - Style pair grouping (borders, backgrounds)
  - Style priority groups with color coding
  - Style complete and valid indicators
  - Style table, headers, and cells
  - Style form inputs and buttons
  - _Requirements: 10.3_

- [ ] 12. Add error handling and user feedback
  - Add error page template for database connection failures
  - Add error messages for failed checkbox updates
  - Add loading indicators for HTMX requests
  - Add empty state message when no records exist
  - Log all errors with appropriate detail
  - _Requirements: 6.5, 7.5_

- [ ] 12.1 Write unit tests for error scenarios
  - Test database unavailable error page
  - Test invalid Case ID returns 404
  - Test incomplete pair update returns 400
  - Test empty database displays message
  - _Requirements: 6.5_

- [ ] 13. Add web server command to pyproject.toml
  - Add `web` entry to `[project.scripts]` section that runs Uvicorn with the FastAPI app
  - Configure appropriate host (0.0.0.0), port (8000), and reload settings
  - Command should be: `uv run web` to start the server
  - _Requirements: 7.2_

- [ ] 14. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 15. Create integration tests for end-to-end workflows
  - Test full page load with sample data
  - Test checkbox update flow (click → POST → database → HTML swap)
  - Test filtering with various search terms
  - Test sorting by different columns
  - Test priority group ordering
  - _Requirements: 1.1, 4.3, 9.1, 9.2_

- [ ] 15.1 Write integration test for HTMX checkbox interaction
  - Test checkbox click triggers POST request
  - Test database is updated correctly
  - Test HTML response is swapped correctly
  - _Requirements: 4.3, 4.4_

- [ ] 16. Add documentation and usage instructions
  - Document how to start the web server
  - Document URL and port configuration
  - Add screenshots or examples to README
  - Document Rosé Pine theme customization
  - _Requirements: 7.1, 7.2_

- [ ] 17. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.
