# Requirements Document

## Introduction

This feature adds the Leistungsdatum (service date) field to all data structures and systems within the MVGenomSeq project. According to the documentation, the Leistungsdatum represents the date of service provision and is a critical component of the Meldebestätigung hash string. The field must be integrated into DuckDB storage, GEPADO synchronization, and all related data models.

## Glossary

- **Leistungsdatum**: The date of service provision (JJJJMMTT format) as defined in the Meldebestätigung documentation
- **Hash-String**: The structured metadata portion of a Meldebestaetigung containing fields separated by '&' characters, which includes the Leistungsdatum
- **DuckDB**: The embedded analytical database system used for local data storage
- **GEPADO**: The laboratory information system with MSSQL database backend
- **MV_servicedate_k**: The SQL field name used in GEPADO for storing the service date for clinical data Meldebestätigungen
- **MV_servicedate_g**: The SQL field name used in GEPADO for storing the service date for genetic data Meldebestätigungen
- **output_date**: The simplified field name used in DuckDB for storing the service date
- **Meldebestaetigung**: A confirmation message containing metadata about medical data submissions
- **Hash String Processing**: The process of parsing and extracting fields from Meldebestätigung hash strings

## Requirements

### Requirement 1

**User Story:** As a data processor, I want the Leistungsdatum extracted from Meldebestätigung hash strings and stored in the database, so that service dates are preserved for all processed records.

#### Acceptance Criteria

1. WHEN processing a Meldebestätigung hash string THEN the system SHALL extract the Leistungsdatum from the second field (position 1) of the hash string
2. WHEN the Leistungsdatum is extracted THEN the system SHALL parse it as an 11-character string in JJJJMMTTZZZ format
3. WHEN storing the Leistungsdatum THEN the system SHALL extract only the date portion (first 8 characters JJJJMMTT) and discard the counter (ZZZ)
4. WHEN the extracted date is invalid THEN the system SHALL log an error and store NULL for the Leistungsdatum
5. WHEN the hash string format is invalid THEN the system SHALL handle the error gracefully and continue processing

### Requirement 2

**User Story:** As a database administrator, I want the Leistungsdatum stored in the DuckDB database schema, so that service dates are available for querying and analysis.

#### Acceptance Criteria

1. WHEN creating the DuckDB schema THEN the system SHALL add an output_date column of DATE type to the meldebestaetigungen table
2. WHEN inserting or updating records THEN the system SHALL store the parsed Leistungsdatum in the output_date column
3. WHEN the output_date column does not exist THEN the system SHALL add it using ALTER TABLE migration
4. WHEN querying records THEN the system SHALL include the output_date field in all result sets
5. WHEN the Leistungsdatum cannot be parsed THEN the system SHALL store NULL in the output_date column

### Requirement 3

**User Story:** As a laboratory technician, I want the Leistungsdatum synchronized with GEPADO records, so that service dates are consistent across all systems.

#### Acceptance Criteria

1. WHEN updating GEPADO records for clinical data THEN the system SHALL include the MV_servicedate_k field in update operations
2. WHEN updating GEPADO records for genetic data THEN the system SHALL include the MV_servicedate_g field in update operations
3. WHEN the GEPADO record has an empty MV_servicedate_k field for clinical data THEN the system SHALL update it with the extracted Leistungsdatum
4. WHEN the GEPADO record has an empty MV_servicedate_g field for genetic data THEN the system SHALL update it with the extracted Leistungsdatum
5. WHEN the GEPADO record has a different MV_servicedate_k value for clinical data THEN the system SHALL log a data mismatch error
6. WHEN the GEPADO record has a different MV_servicedate_g value for genetic data THEN the system SHALL log a data mismatch error
7. WHEN the GEPADO record has the same MV_servicedate_k value for clinical data THEN the system SHALL log successful validation
8. WHEN the GEPADO record has the same MV_servicedate_g value for genetic data THEN the system SHALL log successful validation
9. WHEN GEPADO updates fail THEN the system SHALL log the error and continue processing other records

### Requirement 4

**User Story:** As a web interface user, I want to see the service date displayed in the web frontend, so that I can review when each service was provided.

#### Acceptance Criteria

1. WHEN displaying records in the web table THEN the system SHALL show the output_date column with appropriate formatting
2. WHEN the output_date is NULL THEN the system SHALL display an empty cell or placeholder text
3. WHEN sorting by output_date THEN the system SHALL order records chronologically

### Requirement 5

**User Story:** As a developer, I want all data models updated to include the Leistungsdatum field, so that the codebase maintains consistency across all components.

#### Acceptance Criteria

1. WHEN defining the MeldebestaetigungRecord dataclass THEN the system SHALL include an output_date field of Optional[date] type
2. WHEN defining API response models THEN the system SHALL include the output_date field in RecordResponse and related models
3. WHEN processing Meldebestätigung records THEN the system SHALL extract and pass the Leistungsdatum to downstream components
4. WHEN creating GEPADO record comparisons THEN the system SHALL include output_date validation logic
5. WHEN updating existing records THEN the system SHALL preserve existing output_date values during migrations

### Requirement 6

**User Story:** As a system administrator, I want backward compatibility maintained during the Leistungsdatum integration, so that existing data and processes continue to function correctly.

#### Acceptance Criteria

1. WHEN processing existing records without output_date THEN the system SHALL handle NULL values gracefully
2. WHEN migrating the database schema THEN the system SHALL add the output_date column without breaking existing functionality
3. WHEN old Meldebestätigung formats are encountered THEN the system SHALL attempt extraction but continue processing if it fails
4. WHEN GEPADO records lack the MV_servicedate_k field THEN the system SHALL handle the missing field without errors
5. WHEN GEPADO records lack the MV_servicedate_g field THEN the system SHALL handle the missing field without errors
