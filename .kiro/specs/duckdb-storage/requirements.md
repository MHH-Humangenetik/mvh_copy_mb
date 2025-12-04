# Requirements Document

## Introduction

This feature adds persistent database storage using DuckDB to track all processed Meldebestätigungen and their resolved information from gPAS. The database will be stored as `meldebestaettigungen.duckdb` in the input directory and will maintain a record of all processed entries, including successful gPAS resolutions and failures.

## Glossary

- **Meldebestaetigung**: A confirmation message containing metadata about medical data submissions in a structured string format
- **gPAS**: The pseudonym resolution service that converts pseudonymized identifiers (Vorgangsnummer) to original case IDs
- **Vorgangsnummer**: A pseudonymized identifier that needs to be resolved via gPAS
- **DuckDB**: An embedded analytical database system used for local data storage
- **Input Directory**: The directory containing CSV files to be processed, where the database file will be stored
- **Case ID**: The original identifier resolved from a Vorgangsnummer via gPAS
- **Hash-String**: The structured metadata portion of a Meldebestaetigung containing fields separated by '&' characters

## Requirements

### Requirement 1

**User Story:** As a data processor, I want all processed Meldebestätigungen stored in a DuckDB database, so that I can track processing history and query resolved information.

#### Acceptance Criteria

1. WHEN the system starts processing THEN the system SHALL create or open a DuckDB database file named `meldebestaettigungen.duckdb` in the input directory
2. WHEN the database file does not exist THEN the system SHALL create a new database with the required schema
3. WHEN the database file exists THEN the system SHALL open the existing database and reuse the schema
4. THE system SHALL store the database file in the input directory specified by the user
5. WHEN database operations fail THEN the system SHALL log the error and continue processing without terminating

### Requirement 2

**User Story:** As a data analyst, I want each processed Meldebestaetigung record stored with all relevant metadata, so that I can analyze processing results and gPAS resolution outcomes.

#### Acceptance Criteria

1. WHEN a Meldebestaetigung is processed THEN the system SHALL store the Vorgangsnummer in the database
2. WHEN a Meldebestaetigung is processed THEN the system SHALL store the complete Meldebestaetigung string in the database
3. WHEN a Meldebestaetigung is processed THEN the system SHALL store all parsed metadata fields (Typ der Meldung, Indikationsbereich, Art der Daten, Ergebnis QC) in the database
4. WHEN a Meldebestaetigung is processed THEN the system SHALL store the source CSV filename in the database
5. WHEN a Meldebestaetigung is processed THEN the system SHALL store the processing timestamp in the database

### Requirement 3

**User Story:** As a data processor, I want gPAS resolution results stored in the database, so that I can track which pseudonyms were successfully resolved and which failed.

#### Acceptance Criteria

1. WHEN gPAS successfully resolves a Vorgangsnummer THEN the system SHALL store the resolved Case ID in the database
2. WHEN gPAS successfully resolves a Vorgangsnummer THEN the system SHALL store the gPAS domain name that provided the resolution in the database
3. WHEN gPAS fails to resolve a Vorgangsnummer THEN the system SHALL store NULL for the Case ID in the database
4. WHEN gPAS fails to resolve a Vorgangsnummer THEN the system SHALL store NULL for the domain name in the database
5. WHEN a database record is created THEN the system SHALL ensure all fields are stored with appropriate data types

### Requirement 4

**User Story:** As a system administrator, I want the database to handle duplicate processing attempts gracefully, so that reprocessing files does not create data integrity issues.

#### Acceptance Criteria

1. WHEN a Meldebestaetigung with the same Vorgangsnummer is processed multiple times THEN the system SHALL update the existing record rather than create duplicates
2. WHEN updating an existing record THEN the system SHALL update the processing timestamp to reflect the latest processing time
3. WHEN updating an existing record THEN the system SHALL update all fields including source file and gPAS resolution results
4. THE system SHALL use Vorgangsnummer as the primary key to identify unique records
5. WHEN inserting or updating records THEN the system SHALL maintain data consistency throughout the operation

### Requirement 5

**User Story:** As a developer, I want the database integration to be modular and maintainable, so that the codebase remains clean and testable.

#### Acceptance Criteria

1. THE system SHALL implement database operations in a separate database module or class
2. THE system SHALL separate database schema definition from business logic
3. THE system SHALL provide clear interfaces for storing and retrieving Meldebestaetigung records
4. THE system SHALL handle database connection lifecycle (open, close) appropriately
5. WHEN the application exits THEN the system SHALL close the database connection cleanly
