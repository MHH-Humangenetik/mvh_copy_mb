# Requirements Document

## Introduction

This feature adds a web-based frontend for viewing and managing Meldebestätigungen stored in the DuckDB database. The interface displays records in a tabular format with special grouping for genomic and clinical data pairs by Case ID, allowing users to mark records as "done" with persistent checkbox state stored in the database.

## Glossary

- **Meldebestaetigung**: A confirmation message containing metadata about medical data submissions
- **Case ID**: The original identifier resolved from a Vorgangsnummer via gPAS
- **Art der Daten**: Type of data - either "genomic" or "clinical"
- **Web Frontend**: Browser-based user interface for viewing and managing Meldebestätigungen
- **FastAPI**: Modern Python web framework for building APIs
- **Uvicorn**: ASGI web server for running FastAPI applications
- **HTMX**: Library for adding interactivity to HTML without heavy JavaScript
- **Alpine.js**: Lightweight JavaScript framework for client-side interactivity
- **Milligram**: Minimal CSS framework for styling
- **Done Status**: Boolean flag indicating whether a Meldebestaetigung has been reviewed and marked complete

## Requirements

### Requirement 1

**User Story:** As a data reviewer, I want to view all Meldebestätigungen in a web-based table, so that I can review processed records without using database tools.

#### Acceptance Criteria

1. WHEN a user accesses the web interface THEN the system SHALL display all Meldebestätigungen in a table format
2. WHEN displaying records THEN the system SHALL show Case ID, Vorgangsnummer, Art der Daten, Typ der Meldung, Indikationsbereich, Ergebnis QC, and source file
3. WHEN displaying records THEN the system SHALL order records by priority group first, then by Case ID, then by Art der Daten (genomic before clinical)
4. WHEN the table loads THEN the system SHALL retrieve all data from the DuckDB database
5. WHEN no records exist THEN the system SHALL display a message indicating no data is available

### Requirement 2

**User Story:** As a data reviewer, I want genomic and clinical records for the same Case ID visually grouped together, so that I can easily see paired data submissions.

#### Acceptance Criteria

1. WHEN two records share the same Case ID THEN the system SHALL display them in consecutive rows
2. WHEN displaying paired records THEN the system SHALL show the genomic record first followed by the clinical record
3. WHEN displaying paired records THEN the system SHALL apply visual styling to indicate they belong together
4. WHEN both genomic and clinical records exist for a Case ID THEN the system SHALL display a visual indicator showing the pair is complete
5. WHEN only one record type exists for a Case ID THEN the system SHALL display the single record without the complete pair indicator

### Requirement 3

**User Story:** As a data reviewer, I want to see which record pairs are valid and complete, so that I can prioritize my review workflow.

#### Acceptance Criteria

1. WHEN both genomic and clinical records exist for a Case ID THEN the system SHALL mark the pair as valid
2. WHEN both records in a pair have Ergebnis QC indicating success THEN the system SHALL mark the pair as complete and valid
3. WHEN displaying a valid complete pair THEN the system SHALL show a distinct visual indicator (such as a checkmark or colored badge)
4. WHEN a Case ID has only genomic or only clinical data THEN the system SHALL NOT show the valid pair indicator
5. WHEN a pair exists but one or both records have failing QC results THEN the system SHALL show a different indicator or no indicator

### Requirement 4

**User Story:** As a data reviewer, I want to mark complete record pairs as "done" using checkboxes, so that I can track which pairs I have reviewed.

#### Acceptance Criteria

1. WHEN displaying a complete pair (both genomic and clinical records present) THEN the system SHALL show a checkbox in the "done" column
2. WHEN displaying an incomplete pair (only one record type present) THEN the system SHALL NOT show a checkbox or SHALL show a disabled checkbox
3. WHEN a user clicks an enabled checkbox THEN the system SHALL update the done status for both records in the pair in the database immediately
4. WHEN a checkbox state changes THEN the system SHALL persist the change without requiring a page reload
5. WHEN the page loads THEN the system SHALL display checkboxes with their current state from the database

### Requirement 5

**User Story:** As a data reviewer, I want the web interface to be responsive and fast, so that I can efficiently review large numbers of records.

#### Acceptance Criteria

1. WHEN the page loads THEN the system SHALL render the table within 2 seconds for up to 1000 records
2. WHEN a user updates a checkbox THEN the system SHALL complete the database update within 500 milliseconds
3. WHEN displaying the table THEN the system SHALL use efficient rendering techniques to handle large datasets
4. THE system SHALL minimize JavaScript bundle size by using lightweight libraries
5. WHEN the user interacts with the interface THEN the system SHALL provide immediate visual feedback for all actions

### Requirement 6

**User Story:** As a system administrator, I want the web server to integrate with the existing DuckDB database, so that no data migration or duplication is required.

#### Acceptance Criteria

1. THE system SHALL read Meldebestaetigung records directly from the existing DuckDB database file
2. THE system SHALL use the same database schema defined in the duckdb-storage feature
3. WHEN updating done status THEN the system SHALL write changes to the DuckDB database
4. THE system SHALL handle concurrent access to the database safely
5. WHEN the database is unavailable THEN the system SHALL display an appropriate error message to the user

### Requirement 7

**User Story:** As a developer, I want the web application to follow modern Python web development practices, so that the codebase is maintainable and extensible.

#### Acceptance Criteria

1. THE system SHALL use FastAPI as the web framework
2. THE system SHALL use Uvicorn as the ASGI server
3. THE system SHALL separate API endpoints from database access logic
4. THE system SHALL use type hints and Pydantic models for request/response validation
5. THE system SHALL implement proper error handling and logging for all endpoints

### Requirement 8

**User Story:** As a data reviewer, I want records initially sorted by priority groups, so that I can focus on complete pairs that need review first.

#### Acceptance Criteria

1. WHEN the page loads THEN the system SHALL group records into three priority groups in this order: complete pairs not done, incomplete pairs, complete pairs done
2. WHEN displaying priority group 1 THEN the system SHALL show Case IDs where both genomic and clinical records exist and are not marked done
3. WHEN displaying priority group 2 THEN the system SHALL show Case IDs where only one record type (genomic or clinical) exists
4. WHEN displaying priority group 3 THEN the system SHALL show Case IDs where both records exist and are marked done
5. WHEN sorting within each priority group THEN the system SHALL order by Case ID and then by Art der Daten (genomic before clinical)

### Requirement 9

**User Story:** As a data reviewer, I want to filter and sort records in the table without page reloads, so that I can quickly find specific records or organize data by different criteria.

#### Acceptance Criteria

1. WHEN a user types in a filter input THEN the system SHALL filter table rows in real-time based on matching text in any column
2. WHEN a user clicks a column header THEN the system SHALL sort the table by that column in ascending order
3. WHEN a user clicks the same column header again THEN the system SHALL toggle the sort order to descending
4. WHEN filtering or sorting THEN the system SHALL maintain the visual grouping of genomic/clinical pairs by Case ID
5. WHEN filtering or sorting THEN the system SHALL perform the operation client-side without server requests

### Requirement 10

**User Story:** As a developer, I want the frontend to use minimal dependencies and simple technologies, so that the application is easy to deploy and maintain.

#### Acceptance Criteria

1. THE system SHALL use HTMX for interactive table updates without full page reloads
2. THE system SHALL use Alpine.js for client-side filtering and sorting functionality
3. THE system SHALL use Milligram CSS framework for styling
4. THE system SHALL serve static assets (CSS, JS) efficiently
5. THE system SHALL not require a separate build process for frontend assets
