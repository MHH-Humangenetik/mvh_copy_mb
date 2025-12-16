# Design Document

## Overview

The HL7 Case ID Correction feature addresses a critical data quality issue where Meldebestätigungen sometimes reference incorrect hl7_case_id values in GEPADO. The system will validate that referenced cases have the correct sapVisitingType ('GS' for genomic sequencing) and automatically find the correct case when needed. This correction applies only to GEPADO operations, preserving the original hl7_case_id for local storage and file naming.

The correction process involves a two-step validation: first checking if the referenced case has the correct sapVisitingType, and if not, finding the correct genomic sequencing case for the same patient. This ensures that GEPADO updates target the appropriate records while maintaining data integrity in the local system.

## Architecture

The correction system integrates into the existing GEPADO processing pipeline:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Meldebestäti-  │    │   HL7 Case ID    │    │   GEPADO        │
│  gung Processing│───▶│   Validation     │───▶│   Updates       │
│                 │    │   & Correction   │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         │                       ▼                       │
         │              ┌──────────────────┐             │
         │              │   sapVisitingType│             │
         │              │   Check          │             │
         │              └──────────────────┘             │
         │                       │                       │
         │                       ▼                       │
         │              ┌──────────────────┐             │
         │              │   Patient GUID   │             │
         │              │   Lookup         │             │
         │              └──────────────────┘             │
         │                       │                       │
         │                       ▼                       │
         │              ┌──────────────────┐             │
         │              │   Correct Case   │             │
         │              │   Search         │             │
         │              └──────────────────┘             │
         │                                               │
         ▼                                               ▼
┌─────────────────┐                            ┌─────────────────┐
│   Local Storage │                            │   Corrected     │
│   (Original ID) │                            │   GEPADO Update │
└─────────────────┘                            └─────────────────┘
```

## Components and Interfaces

### HL7 Case ID Validator
- **Purpose**: Validate that hl7_case_id has correct sapVisitingType for genomic sequencing
- **Input**: hl7_case_id, GEPADO database connection
- **Output**: Boolean indicating if case is valid for genomic sequencing
- **Interface**: `validate_hl7_case_sapvisitingtype(client: GepadoClient, hl7_case_id: str) -> bool`

### Patient GUID Resolver
- **Purpose**: Extract patient GUID from av_ordermanagement for a given hl7_case_id
- **Input**: hl7_case_id, GEPADO database connection
- **Output**: Patient GUID or None if not found
- **Interface**: `get_patient_guid_for_case(client: GepadoClient, hl7_case_id: str) -> Optional[str]`

### Correct Case Finder
- **Purpose**: Find correct genomic sequencing case for a patient
- **Input**: Patient GUID, GEPADO database connection
- **Output**: Corrected hl7_case_id or None if correction fails
- **Interface**: `find_correct_genomic_case(client: GepadoClient, patient_guid: str) -> Optional[str]`

### HL7 Case ID Corrector (Main Component)
- **Purpose**: Orchestrate the complete validation and correction process
- **Input**: Original hl7_case_id, GEPADO database connection
- **Output**: Corrected hl7_case_id or original if no correction needed/possible
- **Interface**: `correct_hl7_case_id_for_gepado(client: GepadoClient, hl7_case_id: str) -> str`

## Data Models

### Case Validation Result
```python
@dataclass
class CaseValidationResult:
    original_case_id: str
    is_valid: bool
    corrected_case_id: Optional[str] = None
    patient_guid: Optional[str] = None
    error_message: Optional[str] = None
    correction_applied: bool = False
```

### Database Queries

#### sapVisitingType Validation Query
```sql
SELECT sapVisitingType, guid_patient 
FROM av_ordermanagement 
WHERE hl7fallid = %s
```

#### Correct Case Search Query
```sql
SELECT hl7fallid 
FROM av_ordermanagement 
WHERE guid_patient = %s AND sapVisitingType = 'GS'
```

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

Property 1: sapVisitingType validation accuracy
*For any* hl7_case_id in GEPADO, querying its sapVisitingType should return the correct value from av_ordermanagement
**Validates: Requirements 1.1**

Property 2: Correct case passthrough
*For any* hl7_case_id with sapVisitingType 'GS', the validation should pass and no correction should be attempted
**Validates: Requirements 1.2**

Property 3: Incorrect case detection
*For any* hl7_case_id with sapVisitingType not equal to 'GS', the system should identify it as incorrect
**Validates: Requirements 1.3**

Property 4: Patient GUID extraction consistency
*For any* valid hl7_case_id, extracting the patient GUID should return the same value consistently
**Validates: Requirements 2.1**

Property 5: Correct case search completeness
*For any* patient GUID, searching for cases with sapVisitingType 'GS' should return all matching cases
**Validates: Requirements 2.2**

Property 6: Single correct case selection
*For any* patient with exactly one genomic sequencing case, the correction should select that case
**Validates: Requirements 2.3**

Property 7: Multiple cases error handling
*For any* patient with multiple genomic sequencing cases, the system should log an error and not apply correction
**Validates: Requirements 2.4**

Property 8: No cases warning handling
*For any* patient with no genomic sequencing cases, the system should log a warning and not apply correction
**Validates: Requirements 2.5**

Property 9: Correction logging completeness
*For any* successful correction, the system should log both original and corrected case IDs
**Validates: Requirements 3.1**

Property 10: Multiple candidates logging
*For any* case with multiple correction candidates, the system should log all candidate case IDs
**Validates: Requirements 3.2**

Property 11: Local storage preservation
*For any* Meldebestätigung processing, the local database should store the original hl7_case_id regardless of correction
**Validates: Requirements 4.1**

Property 12: File naming preservation
*For any* file generation, the filename should use the original hl7_case_id regardless of correction
**Validates: Requirements 4.2**

Property 13: GEPADO update correction
*For any* GEPADO update operation, the system should use the corrected hl7_case_id when correction was successful
**Validates: Requirements 4.3**

Property 14: Database error resilience
*For any* database connection issue during validation, the system should handle the error gracefully and continue processing
**Validates: Requirements 5.1**

Property 15: Query error handling
*For any* SQL query that returns unexpected results, the system should log the issue and skip GEPADO updates
**Validates: Requirements 5.2**

## Error Handling

### Database Connection Errors
- **Connection timeout**: Retry once with exponential backoff, fall back to original processing
- **Authentication failure**: Log error and skip all GEPADO operations for current batch
- **Network issues**: Implement circuit breaker pattern to avoid repeated failures

### Query Execution Errors
- **Invalid hl7_case_id format**: Log warning and proceed with original case ID
- **Missing records**: Log appropriate level (warning for missing case, error for missing patient)
- **SQL syntax errors**: Log error with query details and skip GEPADO operations

### Data Validation Errors
- **Unexpected sapVisitingType values**: Log warning and treat as incorrect case
- **NULL patient GUID**: Log error and skip correction attempt
- **Malformed case IDs**: Log error with details and skip correction

### Resource Constraint Handling
- **Memory pressure**: Implement query result streaming for large patient case lists
- **CPU timeout**: Set reasonable query timeouts and handle timeout exceptions
- **Connection pool exhaustion**: Queue operations and process with available connections

## Testing Strategy

### Unit Testing Approach
Unit tests will focus on individual components and database interactions:

- **Database query validation**: Test with various hl7_case_id formats and edge cases
- **Patient GUID extraction**: Test with valid/invalid cases and missing records
- **Case search logic**: Test with different patient scenarios (0, 1, multiple cases)
- **Error handling**: Test database failures, timeouts, and malformed data

### Property-Based Testing Approach
Property-based tests will verify universal behaviors using **Hypothesis** for Python:

- **Minimum 100 iterations** per property test for thorough coverage
- **Database state generators** that create realistic av_ordermanagement scenarios
- **Case ID generators** with valid/invalid formats and sapVisitingType combinations
- **Error injection** to test resilience with database failures and malformed data

Each property-based test will be tagged with comments explicitly referencing the correctness property:
- Format: `**Feature: hl7-case-id-correction, Property {number}: {property_text}**`
- Each correctness property will be implemented by a single property-based test
- Tests will focus on database interactions and correction logic edge cases

The dual testing approach ensures comprehensive coverage: unit tests verify specific database operations and error scenarios, while property tests validate the correction logic across many realistic data combinations.