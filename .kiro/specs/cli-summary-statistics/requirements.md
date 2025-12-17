# Requirements Document

## Introduction

This feature adds summary statistics display to the CLI tool that shows key metrics after processing Meldebestätigungen CSV files. The statistics will be displayed at the end of the CLI execution with visual progress bars, providing users with a comprehensive overview of the processing results including file counts, pairing status, and GEPADO update results.

## Glossary

- **CLI Tool**: The command-line interface application for processing Meldebestätigungen CSV files
- **Ready Pairs**: Complete pairs of files where both genomic (G) and clinical (C) data exist with the same resolved Case ID
- **Unpaired Genomic**: Files with genomic data that have resolved Case IDs but lack corresponding clinical data with the same Case ID
- **Unpaired Clinical**: Files with clinical data that have resolved Case IDs but lack corresponding genomic data with the same Case ID  
- **Ignored Files**: Files that were skipped during processing due to QC failures, unresolved Case IDs, or other issues
- **GEPADO Updates**: Database update operations that actually modified data in the GEPADO system
- **GEPADO No Updates**: Database operations where validation passed but no data changes were needed
- **Progress Bar**: A visual representation showing the proportion of each statistic relative to a maximum value
- **Summary Statistics**: Aggregate metrics displayed after CLI processing completion
- **Case ID**: Unique identifier resolved from gPAS for linking genomic and clinical records

## Requirements

### Requirement 1

**User Story:** As a user, I want to see file processing statistics after CLI execution, so that I understand how many complete pairs and unpaired files were processed.

#### Acceptance Criteria

1. WHEN the CLI tool completes processing THEN the system SHALL display the count of Ready pairs (complete pairs with both genomic and clinical data sharing the same resolved Case ID)
2. WHEN the CLI tool completes processing THEN the system SHALL display the count of Unpaired genomic files (genomic files with resolved Case IDs but no matching clinical counterpart)
3. WHEN the CLI tool completes processing THEN the system SHALL display the count of Unpaired clinical files (clinical files with resolved Case IDs but no matching genomic counterpart)
4. WHEN the CLI tool completes processing THEN the system SHALL display the count of Ignored files (files skipped due to QC failures, unresolved Case IDs, or processing errors)
5. WHEN calculating totals for progress bars THEN the system SHALL count each Ready pair as two files (one genomic + one clinical)

### Requirement 2

**User Story:** As a user, I want to see GEPADO update statistics when GEPADO integration is enabled, so that I can track the success rate of database operations and distinguish between actual updates and validation-only operations.

#### Acceptance Criteria

1. WHEN GEPADO updates are enabled AND the CLI tool completes processing THEN the system SHALL display the count of genomic data records that were actually updated in GEPADO
2. WHEN GEPADO updates are enabled AND the CLI tool completes processing THEN the system SHALL display the count of clinical data records that were actually updated in GEPADO
3. WHEN GEPADO updates are enabled AND the CLI tool completes processing THEN the system SHALL display the count of records where validation passed but no updates were needed
4. WHEN GEPADO updates are enabled AND the CLI tool completes processing THEN the system SHALL display the count of errors encountered during GEPADO operations
5. WHEN GEPADO updates are disabled THEN the system SHALL not display GEPADO-related statistics

### Requirement 3

**User Story:** As a user, I want visual progress bars next to each statistic, so that I can quickly assess the proportional distribution of processing results.

#### Acceptance Criteria

1. WHEN displaying file statistics THEN the system SHALL show a 20-character wide progress bar next to each file count statistic
2. WHEN displaying GEPADO statistics THEN the system SHALL show a 20-character wide progress bar next to each GEPADO count statistic
3. WHEN calculating progress bars for file statistics THEN the system SHALL use the total number of processed files as the maximum value
4. WHEN calculating progress bars for GEPADO statistics THEN the system SHALL use the total number of records found in GEPADO as the maximum value
5. WHEN a statistic count is zero THEN the system SHALL display an empty progress bar

### Requirement 4

**User Story:** As a user, I want the statistics display to be visually aligned and formatted, so that I can easily read and compare the different metrics.

#### Acceptance Criteria

1. WHEN displaying statistics THEN the system SHALL align all progress bars vertically under each other
2. WHEN displaying statistics THEN the system SHALL use consistent formatting for statistic labels and counts
3. WHEN displaying statistics THEN the system SHALL use a clear visual separator between file statistics and GEPADO statistics
4. WHEN displaying progress bars THEN the system SHALL use appropriate characters to represent filled and empty portions

### Requirement 5

**User Story:** As a user, I want the statistics to accurately reflect the processing results using the same pairing logic as the web interface, so that I can trust the reported metrics for decision making.

#### Acceptance Criteria

1. WHEN counting Ready pairs THEN the system SHALL include only Case IDs that have both genomic (G) and clinical (C) files with resolved Case IDs, matching the web interface pairing logic
2. WHEN counting Unpaired files THEN the system SHALL include files with resolved Case IDs that lack a counterpart of the opposite data type (G missing C, or C missing G)
3. WHEN counting Ignored files THEN the system SHALL include files with QC failures, unresolved Case IDs, or processing errors
4. WHEN counting GEPADO operations THEN the system SHALL distinguish between actual updates, no-update-needed cases, and errors
5. WHEN calculating totals for progress bars THEN the system SHALL ensure mathematical consistency between counts and maximum values