# Requirements Document

## Introduction

This feature addresses the issue where Meldebestätigungen sometimes refer to the wrong hl7_case_id in GEPADO. The system needs to validate that the referenced case has the correct sapVisitingType ('GS' for genomic sequencing) and automatically find the correct case when needed. This correction only applies to GEPADO updates and does not affect local database storage or file naming.

## Glossary

- **hl7_case_id**: The HL7 case identifier used to reference cases in GEPADO
- **sapVisitingType**: A column in av_ordermanagement table that indicates the type of visit/case
- **GS**: The sapVisitingType value that indicates a genomic sequencing case
- **guid_patient**: The patient identifier used to link cases belonging to the same patient
- **av_ordermanagement**: The GEPADO table containing case management information
- **GEPADO**: The laboratory information system with MSSQL database backend
- **Meldebestätigung**: A confirmation message that may reference an incorrect hl7_case_id

## Requirements

### Requirement 1

**User Story:** As a laboratory technician, I want the system to validate hl7_case_id references before GEPADO updates, so that I can identify when Meldebestätigungen refer to wrong cases.

#### Acceptance Criteria

1. WHEN processing a Meldebestätigung for GEPADO updates THEN the system SHALL query the sapVisitingType for the referenced hl7_case_id
2. WHEN the sapVisitingType equals 'GS' THEN the system SHALL proceed with normal GEPADO processing
3. WHEN the sapVisitingType does not equal 'GS' THEN the system SHALL identify this as an incorrect reference
4. WHEN querying sapVisitingType fails THEN the system SHALL log an error and skip GEPADO processing
5. WHEN the hl7_case_id is not found in av_ordermanagement THEN the system SHALL log a warning and skip GEPADO processing

### Requirement 2

**User Story:** As a data processor, I want the system to automatically find the correct hl7_case_id when a wrong reference is detected, so that GEPADO updates target the appropriate genomic sequencing case.

#### Acceptance Criteria

1. WHEN an incorrect hl7_case_id is detected THEN the system SHALL query the guid_patient from av_ordermanagement for that case
2. WHEN the guid_patient is found THEN the system SHALL search for all cases with the same guid_patient and sapVisitingType equal to 'GS'
3. WHEN exactly one correct case is found THEN the system SHALL use that hl7_case_id for GEPADO updates
4. WHEN multiple correct cases are found THEN the system SHALL log an error with case details and skip GEPADO updates
5. WHEN no correct cases are found THEN the system SHALL log a warning and skip GEPADO updates

### Requirement 3

**User Story:** As a system administrator, I want comprehensive logging of hl7_case_id corrections, so that I can monitor and audit the correction process.

#### Acceptance Criteria

1. WHEN a correct hl7_case_id is found THEN the system SHALL log the original and corrected case IDs
2. WHEN multiple correct cases exist THEN the system SHALL log all candidate case IDs for manual review
3. WHEN no correct cases exist THEN the system SHALL log the patient GUID and original case ID
4. WHEN sapVisitingType validation passes THEN the system SHALL log successful validation
5. WHEN database queries fail THEN the system SHALL log detailed error information

### Requirement 4

**User Story:** As a developer, I want the hl7_case_id correction to be isolated to GEPADO operations, so that local database storage and file naming remain unchanged.

#### Acceptance Criteria

1. WHEN storing records in the local DuckDB database THEN the system SHALL use the original hl7_case_id from the Meldebestätigung
2. WHEN generating file names THEN the system SHALL use the original hl7_case_id from the Meldebestätigung
3. WHEN updating GEPADO records THEN the system SHALL use the corrected hl7_case_id if validation found a replacement
4. WHEN logging GEPADO operations THEN the system SHALL clearly indicate when a corrected hl7_case_id is being used
5. WHEN the correction process fails THEN the system SHALL continue with other processing using the original hl7_case_id

### Requirement 5

**User Story:** As a quality assurance specialist, I want the system to handle edge cases gracefully during hl7_case_id correction, so that processing continues reliably even when corrections cannot be made.

#### Acceptance Criteria

1. WHEN database connection issues occur during validation THEN the system SHALL retry once and fall back to original processing if retry fails
2. WHEN SQL queries return unexpected results THEN the system SHALL log the issue and skip GEPADO updates for that record
3. WHEN the correction process times out THEN the system SHALL log a timeout error and continue with original processing
4. WHEN memory or resource constraints occur THEN the system SHALL handle the error gracefully and continue processing other records
5. WHEN the GEPADO database is unavailable THEN the system SHALL skip all GEPADO operations but continue local processing