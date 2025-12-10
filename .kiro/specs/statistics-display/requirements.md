# Requirements Document

## Introduction

This feature adds a statistics display component to the web interface that shows key metrics about the Meldebestätigungen records. The statistics will be displayed next to the filter input, providing users with an at-a-glance overview of the data set including total cases, complete/valid pairs, and done pairs.

## Glossary

- **Case**: A unique Case ID that may have associated genomic and/or clinical records
- **Pair**: A logical grouping of genomic and clinical records sharing the same Case ID
- **Complete Pair**: A pair that has both genomic and clinical records
- **Valid Pair**: A complete pair where both records have passing QC (ergebnis_qc = "1")
- **Done Pair**: A complete pair where both records are marked as done (is_done = true)
- **Statistics Component**: A UI element displaying aggregate metrics about the record set
- **Filter Input**: The existing search input field for filtering table rows
- **Web Interface**: The FastAPI-based web application displaying Meldebestätigungen records

## Requirements

### Requirement 1

**User Story:** As a user, I want to see the total number of cases at a glance, so that I understand the size of the dataset I'm working with.

#### Acceptance Criteria

1. WHEN the web interface loads THEN the system SHALL display the total count of unique Case IDs
2. WHEN the filter is applied THEN the system SHALL update the total count to reflect only filtered cases
3. WHEN no records exist THEN the system SHALL display zero as the total count

### Requirement 2

**User Story:** As a user, I want to see how many complete and valid pairs exist, so that I can assess data quality and completeness.

#### Acceptance Criteria

1. WHEN the web interface loads THEN the system SHALL display the count of complete pairs (both genomic and clinical records present)
2. WHEN the web interface loads THEN the system SHALL display the count of valid pairs (complete pairs with passing QC)
3. WHEN the filter is applied THEN the system SHALL update both counts to reflect only filtered pairs
4. WHEN a pair has only genomic or only clinical records THEN the system SHALL exclude it from the complete pair count

### Requirement 3

**User Story:** As a user, I want to see how many pairs are marked as done, so that I can track my progress through the review process.

#### Acceptance Criteria

1. WHEN the web interface loads THEN the system SHALL display the count of done pairs (complete pairs where both records are marked done)
2. WHEN the filter is applied THEN the system SHALL update the done count to reflect only filtered pairs
3. WHEN a user marks a pair as done THEN the system SHALL update the done count immediately
4. WHEN a user unmarks a pair as done THEN the system SHALL update the done count immediately

### Requirement 4

**User Story:** As a user, I want the statistics to be visually integrated with the filter input, so that I can easily see both filtering controls and data metrics in one place.

#### Acceptance Criteria

1. WHEN the web interface loads THEN the system SHALL display statistics next to the filter input
2. WHEN displaying statistics THEN the system SHALL use clear labels for each metric
3. WHEN displaying statistics THEN the system SHALL maintain the existing visual design (Rosé Pine theme)
4. WHEN the viewport is narrow THEN the system SHALL maintain readable statistics layout

### Requirement 5

**User Story:** As a user, I want the statistics to update reactively, so that I always see accurate counts as I interact with the data.

#### Acceptance Criteria

1. WHEN the filter value changes THEN the system SHALL recalculate statistics based on filtered data
2. WHEN a done status is toggled THEN the system SHALL recalculate the done count
3. WHEN statistics are recalculated THEN the system SHALL update the display without page reload
4. WHEN sorting is applied THEN the system SHALL maintain accurate statistics counts
