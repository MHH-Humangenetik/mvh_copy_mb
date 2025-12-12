# Requirements Document

## Introduction

This feature adds integration with the gepado laboratory information system to update records with information gathered during Meldebestätigung processing. The system will extract HL7 case IDs from processed files and synchronize data with gepado's MSSQL database.

## Glossary

- **gepado**: Laboratory information system with MSSQL database backend
- **Meldebestätigung**: Medical notification confirmation files processed by the CLI
- **HL7 Case ID**: Unique identifier extracted from HUMGEN pattern in Meldebestätigung files
- **VNg**: Vorgangsnummer for genomic data type
- **VNk**: Vorgangsnummer for clinical data type  
- **IBE String**: Identifier string, either IBE_g (genomic) or IBE_k (clinical)
- **CLI Tool**: The mvh_copy_mb command-line interface
- **MSSQL Connection**: Database connection to gepado system

## Requirements

### Requirement 1

**User Story:** As a laboratory technician, I want to update gepado records with processed Meldebestätigung data, so that our laboratory information system stays synchronized with file processing results.

#### Acceptance Criteria

1. WHEN the CLI tool is invoked with --update-gepado parameter, THE CLI Tool SHALL establish connection to gepado MSSQL database
2. WHEN processing Meldebestätigung files, THE CLI Tool SHALL extract HL7 case IDs from HUMGEN pattern identifiers
3. WHEN HL7 case ID is extracted, THE CLI Tool SHALL query gepado database for existing record data
4. WHEN gepado record exists with matching data, THE CLI Tool SHALL log successful validation
5. WHEN gepado record exists with conflicting data, THE CLI Tool SHALL log error with details

### Requirement 2

**User Story:** As a system administrator, I want to configure gepado database connection through environment variables, so that database credentials are managed securely.

#### Acceptance Criteria

1. WHEN .env file contains MSSQL configuration, THE CLI Tool SHALL read database connection parameters
2. WHEN MSSQL_HOST environment variable is provided, THE CLI Tool SHALL use it for database host
3. WHEN MSSQL_DATABASE environment variable is provided, THE CLI Tool SHALL use it for database name
4. WHEN MSSQL_USERNAME environment variable is provided, THE CLI Tool SHALL use it for authentication
5. WHEN MSSQL_PASSWORD environment variable is provided, THE CLI Tool SHALL use it for authentication

### Requirement 3

**User Story:** As a data processor, I want the system to extract HL7 case IDs from Meldebestätigung identifiers, so that records can be matched with gepado entries.

#### Acceptance Criteria

1. WHEN Meldebestätigung contains HUMGEN pattern identifier, THE CLI Tool SHALL extract numeric portion after HUMGEN_\w_
2. WHEN HL7 case ID extraction succeeds, THE CLI Tool SHALL use extracted ID for gepado queries
3. WHEN HL7 case ID extraction fails, THE CLI Tool SHALL log warning and continue processing
4. WHEN multiple HUMGEN patterns exist, THE CLI Tool SHALL extract first valid numeric ID
5. WHEN no HUMGEN pattern exists, THE CLI Tool SHALL skip gepado update for that record

### Requirement 4

**User Story:** As a quality assurance specialist, I want the system to validate and update gepado records with Vorgangsnummer and IBE data, so that data consistency is maintained across systems.

#### Acceptance Criteria

1. WHEN Meldebestätigung has Ergebnis QC equal to 1 and Typ der Meldung equal to 0, THE CLI Tool SHALL proceed with gepado record processing
2. WHEN Meldebestätigung has Ergebnis QC not equal to 1 or Typ der Meldung not equal to 0, THE CLI Tool SHALL skip gepado update for that record and log that as a warning
3. WHEN gepado record has empty VNg field and Art der Daten is genomic, THE CLI Tool SHALL update VNg with Vorgangsnummer
4. WHEN gepado record has empty VNk field and Art der Daten is clinical, THE CLI Tool SHALL update VNk with Vorgangsnummer  
5. WHEN gepado record has empty IBE_g field and Art der Daten is genomic, THE CLI Tool SHALL update IBE_g with IBE String
6. WHEN gepado record has empty IBE_k field and Art der Daten is clinical, THE CLI Tool SHALL update IBE_k with IBE String
7. WHEN gepado record has populated fields matching current data, THE CLI Tool SHALL log successful validation

### Requirement 5

**User Story:** As a system operator, I want detailed logging of gepado integration operations, so that I can monitor and troubleshoot database synchronization.

#### Acceptance Criteria

1. WHEN gepado connection is established, THE CLI Tool SHALL log successful database connection
2. WHEN gepado query is executed, THE CLI Tool SHALL log query execution with HL7 case ID
3. WHEN data mismatch is detected, THE CLI Tool SHALL log error with expected and actual values
4. WHEN database update is performed, THE CLI Tool SHALL log successful update with affected fields
5. WHEN database operation fails, THE CLI Tool SHALL log error with exception details