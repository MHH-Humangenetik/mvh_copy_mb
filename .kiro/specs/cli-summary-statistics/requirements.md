# Requirements Document

## Introduction

This feature adds summary statistics display to the CLI tool that shows key metrics after processing Meldebestätigungen CSV files. The statistics will be displayed at the end of the CLI execution with visual progress bars, providing users with a comprehensive overview of the processing results including file counts, pairing status, and GEPADO update results.

## Glossary

- **CLI Tool**: The command-line interface application for processing Meldebestätigungen CSV files
- **Ready Files**: Files that have been successfully processed and have both genomic and clinical data with resolved Case IDs
- **Unpaired Genomic**: Files with genomic data that lack corresponding clinical data
- **Unpaired Clinical**: Files with clinical data that lack corresponding genomic data  
- **Ignored Files**: Files that were skipped during processing due to QC failures or other issues
- **GEPADO Updates**: Database update operations performed on the GEPADO system
- **Progress Bar**: A visual representation showing the proportion of each statistic relative to a maximum value
- **Summary Statistics**: Aggregate metrics displayed after CLI processing completion
- **Case ID**: Unique identifier resolved from gPAS for linking genomic and clinical records

## Requirements

### Requirement 1

**User Story:** As a user, I want to see file processing statistics after CLI execution, so that I understand how many files were processed in each category.

#### Acceptance Criteria

1. WHEN the CLI tool completes processing THEN the system SHALL display the count of Ready files (files with resolved Case IDs and complete data)
2. WHEN the CLI tool completes processing THEN the system SHALL display the count of Unpaired genomic files (genomic files without matching clinical data)
3. WHEN the CLI tool completes processing THEN the system SHALL display the count of Unpaired clinical files (clinical files without matching genomic data)
4. WHEN the CLI tool completes processing THEN the system SHALL display the count of Ignored files (files skipped due to QC failures or processing errors)
5. WHEN displaying file counts THEN the system SHALL count Ready files twice in the total as specified in the requirements

### Requirement 2

**User Story:** As a user, I want to see GEPADO update statistics when GEPADO integration is enabled, so that I can track the success rate of database updates.

#### Acceptance Criteria

1. WHEN GEPADO updates are enabled AND the CLI tool completes processing THEN the system SHALL display the count of successful genomic data updates in GEPADO
2. WHEN GEPADO updates are enabled AND the CLI tool completes processing THEN the system SHALL display the count of successful clinical data updates in GEPADO
3. WHEN GEPADO updates are enabled AND the CLI tool completes processing THEN the system SHALL display the count of errors encountered while updating GEPADO
4. WHEN GEPADO updates are disabled THEN the system SHALL not display GEPADO-related statistics

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

**User Story:** As a user, I want the statistics to accurately reflect the processing results, so that I can trust the reported metrics for decision making.

#### Acceptance Criteria

1. WHEN counting Ready files THEN the system SHALL include only files that have successfully resolved Case IDs and complete genomic/clinical pairing
2. WHEN counting Unpaired files THEN the system SHALL distinguish between genomic files missing clinical counterparts and clinical files missing genomic counterparts
3. WHEN counting Ignored files THEN the system SHALL include files with QC failures, processing errors, or unresolved Case IDs
4. WHEN counting GEPADO updates THEN the system SHALL track successful updates separately from failed attempts
5. WHEN calculating totals for progress bars THEN the system SHALL ensure mathematical consistency between counts and maximum values