# Design Document: Web Frontend for Meldebestätigungen

## Overview

This design adds a web-based frontend for viewing and managing Meldebestätigungen stored in the DuckDB database. The interface provides a tabular view with intelligent grouping of genomic and clinical data pairs by Case ID, priority-based sorting, client-side filtering/sorting capabilities, and persistent "done" status tracking. The implementation uses FastAPI with Uvicorn for the backend, and HTMX + Alpine.js + Milligram for a lightweight, efficient frontend.

## Architecture

### High-Level Architecture

The system follows a three-tier architecture:

1. **Presentation Layer**: HTML templates with HTMX and Alpine.js
2. **Application Layer**: FastAPI web framework with REST endpoints
3. **Data Layer**: DuckDB database (existing from duckdb-storage feature)

```
┌─────────────────────────────────────────────┐
│         Browser (Client)                    │
│  - HTML + Milligram CSS                     │
│  - HTMX (AJAX interactions)                 │
│  - Alpine.js (filtering/sorting)            │
└──────────────┬──────────────────────────────┘
               │ HTTP/HTTPS
               ▼
┌─────────────────────────────────────────────┐
│      FastAPI Application (Backend)          │
│  - API Endpoints                            │
│  - Template Rendering (Jinja2)             │
│  - Request/Response Models (Pydantic)       │
└──────────────┬──────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────┐
│    Database Service Layer                   │
│  - MeldebestaetigungDatabase (existing)     │
│  - Query logic for web views                │
└──────────────┬──────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────┐
│         DuckDB Database                     │
│  (meldebestaetigungen.duckdb)              │
└─────────────────────────────────────────────┘
```

### Request Flow Examples

**Initial Page Load:**
1. Browser requests `/` (root endpoint)
2. FastAPI queries database for all records
3. Backend groups records by Case ID and priority
4. Jinja2 renders HTML table with data
5. Browser receives HTML with embedded Alpine.js data
6. Alpine.js initializes filtering/sorting state

**Checkbox Update:**
1. User clicks checkbox (HTMX intercepts)
2. HTMX sends POST to `/api/done/{case_id}`
3. FastAPI updates both records in database
4. FastAPI returns updated row HTML
5. HTMX swaps the updated HTML into the page
6. No full page reload required

## Components and Interfaces

### 1. FastAPI Application (`web.py`)

Main application entry point with route definitions:

```python
from fastapi import FastAPI, Request, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from pathlib import Path

app = FastAPI(title="Meldebestätigungen Viewer")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request)

@app.post("/api/done/{case_id}")
async def update_done_status(case_id: str, done: bool)

@app.get("/api/records")
async def get_records()
```

**Responsibilities:**
- Route handling and request validation
- Template rendering for HTML responses
- API endpoints for HTMX interactions
- Static file serving (CSS, JS)
- Error handling and logging

### 2. Database Service Layer (`web_database.py`)

Extended database operations for web-specific queries:

```python
from typing import List, Dict, Optional
from dataclasses import dataclass
from .database import MeldebestaetigungDatabase, MeldebestaetigungRecord

@dataclass
class RecordPair:
    case_id: str
    genomic: Optional[MeldebestaetigungRecord]
    clinical: Optional[MeldebestaetigungRecord]
    is_complete: bool
    is_valid: bool
    is_done: bool
    priority_group: int

class WebDatabaseService:
    def __init__(self, db_path: Path)
    def get_all_records_grouped(self) -> List[RecordPair]
    def update_pair_done_status(self, case_id: str, done: bool) -> bool
    def get_pair_by_case_id(self, case_id: str) -> Optional[RecordPair]
```

**Responsibilities:**
- Grouping records by Case ID
- Determining pair completeness and validity
- Priority group calculation
- Batch done status updates for pairs

### 3. Pydantic Models (`models.py`)

Request and response validation models:

```python
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class DoneStatusUpdate(BaseModel):
    done: bool

class RecordResponse(BaseModel):
    vorgangsnummer: str
    case_id: Optional[str]
    art_der_daten: str
    typ_der_meldung: str
    indikationsbereich: str
    ergebnis_qc: str
    source_file: str
    processed_at: datetime
    is_done: bool

class PairResponse(BaseModel):
    case_id: str
    genomic: Optional[RecordResponse]
    clinical: Optional[RecordResponse]
    is_complete: bool
    is_valid: bool
    is_done: bool
    priority_group: int
```

### 4. HTML Templates (`templates/`)

**`index.html`**: Main page template with Alpine.js integration

```html
<!DOCTYPE html>
<html lang="de">
<head>
    <title>Meldebestätigungen</title>
    <link rel="stylesheet" href="/static/css/milligram.min.css">
    <link rel="stylesheet" href="/static/css/custom.css">
    <script src="https://unpkg.com/htmx.org@1.9.10"></script>
    <script src="https://unpkg.com/alpinejs@3.13.3" defer></script>
</head>
<body>
    <div class="container" x-data="tableData()">
        <h1>Meldebestätigungen</h1>
        
        <!-- Filter input -->
        <input type="text" x-model="filter" placeholder="Filter...">
        
        <!-- Table -->
        <table>
            <thead>
                <tr>
                    <th @click="sortBy('case_id')">Case ID</th>
                    <th>Art der Daten</th>
                    <th @click="sortBy('typ_der_meldung')">Typ der Meldung</th>
                    <!-- ... more columns ... -->
                    <th>Done</th>
                </tr>
            </thead>
            <tbody>
                <template x-for="pair in filteredAndSorted" :key="pair.case_id">
                    <!-- Pair rows rendered here -->
                </template>
            </tbody>
        </table>
    </div>
</body>
</html>
```

**`partials/pair_row.html`**: Reusable template for HTMX updates

```html
<!-- Genomic row -->
<tr class="pair-row genomic" data-case-id="{{ pair.case_id }}">
    <td rowspan="2" class="case-id-cell">{{ pair.case_id }}</td>
    <td>genomic</td>
    <!-- ... data cells ... -->
    <td rowspan="2" class="done-cell">
        {% if pair.is_complete %}
        <input type="checkbox" 
               {% if pair.is_done %}checked{% endif %}
               hx-post="/api/done/{{ pair.case_id }}"
               hx-target="closest tr"
               hx-swap="outerHTML">
        {% endif %}
    </td>
</tr>
<!-- Clinical row -->
<tr class="pair-row clinical" data-case-id="{{ pair.case_id }}">
    <td>clinical</td>
    <!-- ... data cells ... -->
</tr>
```

### 5. Static Assets

**CSS (`static/css/custom.css`):**
- Pair grouping visual styles
- Valid pair indicators
- Priority group separators
- Responsive table layout

**JavaScript (`static/js/app.js`):**
- Alpine.js component initialization
- Filtering logic
- Sorting logic
- Table state management

## Data Models

### Database Schema Extension

The existing `meldebestaetigungen` table needs a new column for done status:

```sql
ALTER TABLE meldebestaetigungen 
ADD COLUMN is_done BOOLEAN DEFAULT FALSE;
```

**Updated Schema:**

| Column Name          | Data Type    | Constraints                    | Description                                      |
|---------------------|--------------|--------------------------------|--------------------------------------------------|
| vorgangsnummer      | VARCHAR      | NOT NULL, PRIMARY KEY          | Pseudonymized identifier (unique)                |
| source_file         | VARCHAR      | NOT NULL                       | Name of source CSV file                          |
| meldebestaetigung   | VARCHAR      | NOT NULL                       | Complete Meldebestaetigung string                |
| typ_der_meldung     | VARCHAR      | NOT NULL                       | Type of report (0=initial, etc.)                 |
| indikationsbereich  | VARCHAR      | NOT NULL                       | Medical indication area                          |
| art_der_daten       | VARCHAR      | NOT NULL                       | Type of data (genomic/clinical)                  |
| ergebnis_qc         | VARCHAR      | NOT NULL                       | QC result (1=passed, etc.)                       |
| case_id             | VARCHAR      | NULL                           | Resolved case ID from gPAS                       |
| gpas_domain         | VARCHAR      | NULL                           | gPAS domain that resolved the pseudonym          |
| processed_at        | TIMESTAMP    | NOT NULL                       | Timestamp when record was processed              |
| **is_done**         | **BOOLEAN**  | **NOT NULL, DEFAULT FALSE**    | **Whether the record has been reviewed**         |

### RecordPair Data Structure

The `RecordPair` class represents a logical grouping of genomic and clinical records:

```python
@dataclass
class RecordPair:
    case_id: str                                    # The shared Case ID
    genomic: Optional[MeldebestaetigungRecord]      # Genomic record (if exists)
    clinical: Optional[MeldebestaetigungRecord]     # Clinical record (if exists)
    is_complete: bool                               # Both genomic and clinical present
    is_valid: bool                                  # Complete + both have passing QC
    is_done: bool                                   # Both records marked done
    priority_group: int                             # 1, 2, or 3 for sorting
```

**Priority Group Logic:**
- **Group 1**: `is_complete == True AND is_done == False` (complete pairs not done)
- **Group 2**: `is_complete == False` (incomplete pairs)
- **Group 3**: `is_complete == True AND is_done == True` (complete pairs done)

**Validity Logic:**
- `is_valid = is_complete AND genomic.ergebnis_qc == "1" AND clinical.ergebnis_qc == "1"`

### Frontend Data Model (Alpine.js)

```javascript
function tableData() {
    return {
        pairs: [], // Loaded from server
        filter: '',
        sortColumn: 'priority_group',
        sortDirection: 'asc',
        
        get filteredAndSorted() {
            let filtered = this.pairs.filter(pair => {
                // Filter logic across all columns
            });
            
            return filtered.sort((a, b) => {
                // Sort by priority_group first, then sortColumn
            });
        },
        
        sortBy(column) {
            if (this.sortColumn === column) {
                this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
            } else {
                this.sortColumn = column;
                this.sortDirection = 'asc';
            }
        }
    }
}
```


## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*


### Property 1: All required fields are displayed
*For any* set of Meldebestaetigung records, when rendered in the table, the HTML should contain Case ID, Vorgangsnummer, Art der Daten, Typ der Meldung, Indikationsbereich, Ergebnis QC, and source file for each record
**Validates: Requirements 1.2**

### Property 2: Records are sorted by priority then Case ID then data type
*For any* set of Meldebestaetigung records, when displayed, they should be ordered first by priority group (1, 2, 3), then by Case ID, then by Art der Daten with genomic before clinical
**Validates: Requirements 1.3, 8.1, 8.5**

### Property 3: Displayed records match database state
*For any* database state, when the page loads, the displayed records should exactly match all records in the database
**Validates: Requirements 1.4, 6.1**

### Property 4: Records with same Case ID are consecutive
*For any* two records sharing the same Case ID, when displayed, they should appear in consecutive rows with genomic before clinical
**Validates: Requirements 2.1, 2.2**

### Property 5: Paired records have grouping indicators
*For any* pair of records with the same Case ID, when rendered, the HTML should contain visual styling attributes (CSS classes or data attributes) indicating they belong together
**Validates: Requirements 2.3**

### Property 6: Complete pair indicator is conditional
*For any* Case ID, the complete pair indicator should be present in the HTML if and only if both genomic and clinical records exist for that Case ID
**Validates: Requirements 2.4, 2.5**

### Property 7: Valid pair indicator is conditional on completeness and QC
*For any* Case ID, the valid pair indicator should be present if and only if both genomic and clinical records exist AND both have Ergebnis QC indicating success
**Validates: Requirements 3.2, 3.3, 3.4, 3.5**

### Property 8: Done checkbox only for complete pairs
*For any* Case ID, an enabled checkbox should be present if and only if both genomic and clinical records exist for that Case ID
**Validates: Requirements 4.1, 4.2**

### Property 9: Done status update affects both records in pair
*For any* complete pair (Case ID with both genomic and clinical records), when the done status is updated, both records in the database should have their is_done field set to the same value
**Validates: Requirements 4.3**

### Property 10: Checkbox state reflects database state
*For any* database state, when the page loads, the checkbox states should match the is_done values in the database for all complete pairs
**Validates: Requirements 4.5**

### Property 11: Done status changes persist to database
*For any* done status update operation, when querying the database after the update, the is_done field should reflect the new value
**Validates: Requirements 6.3**

### Property 12: Priority group 1 contains complete pairs not done
*For any* record in priority group 1, it should have both genomic and clinical records present and is_done should be False for both
**Validates: Requirements 8.2**

### Property 13: Priority group 2 contains incomplete pairs
*For any* record in priority group 2, it should have either only genomic or only clinical record (not both)
**Validates: Requirements 8.3**

### Property 14: Priority group 3 contains complete pairs done
*For any* record in priority group 3, it should have both genomic and clinical records present and is_done should be True for both
**Validates: Requirements 8.4**

### Property 15: Client-side filter matches all columns
*For any* filter string and any set of records, the filtered results should include only records where at least one column contains the filter string (case-insensitive)
**Validates: Requirements 9.1**

### Property 16: Client-side sort orders by selected column
*For any* column and sort direction, when sorting is applied, the records should be ordered by that column's values in the specified direction
**Validates: Requirements 9.2, 9.3**

### Property 17: Filtering and sorting preserve pair grouping
*For any* filter or sort operation, records with the same Case ID should remain consecutive in the output
**Validates: Requirements 9.4**

## Error Handling

### Database Connection Errors

**Scenario:** Database file cannot be opened or is unavailable
- **Handling:** Catch connection exceptions in FastAPI startup
- **Recovery:** Display error page with clear message
- **User Impact:** User sees "Database unavailable" message with troubleshooting steps
- **Logging:** Log full error with database path and permissions

### Database Query Errors

**Scenario:** Query execution fails (malformed SQL, schema mismatch)
- **Handling:** Catch query exceptions in database service layer
- **Recovery:** Return empty result set or raise HTTPException
- **User Impact:** User sees error message or empty table
- **Logging:** Log query and error details

### Done Status Update Errors

**Scenario:** Update operation fails (database locked, constraint violation)
- **Handling:** Catch update exceptions in API endpoint
- **Recovery:** Return HTTP 500 with error message
- **User Impact:** HTMX displays error, checkbox reverts to previous state
- **Logging:** Log Case ID and error details

### Template Rendering Errors

**Scenario:** Jinja2 template rendering fails (missing variable, syntax error)
- **Handling:** Catch rendering exceptions in route handlers
- **Recovery:** Return HTTP 500 with generic error page
- **User Impact:** User sees error page
- **Logging:** Log template name and error details

### Invalid Case ID Errors

**Scenario:** User attempts to update done status for non-existent Case ID
- **Handling:** Validate Case ID exists before update
- **Recovery:** Return HTTP 404 with error message
- **User Impact:** HTMX displays "Record not found" error
- **Logging:** Log attempted Case ID

### Incomplete Pair Update Errors

**Scenario:** User attempts to mark incomplete pair as done (should be prevented by UI)
- **Handling:** Validate pair completeness in API endpoint
- **Recovery:** Return HTTP 400 with error message
- **User Impact:** Error message displayed
- **Logging:** Log Case ID and validation failure

## Testing Strategy

### Unit Testing Approach

The implementation will include focused unit tests for:

1. **Database service layer**
   - Test record grouping by Case ID
   - Test priority group calculation
   - Test pair completeness detection
   - Test validity determination (QC checks)
   - Test done status updates for pairs

2. **API endpoints**
   - Test GET / returns HTML with records
   - Test POST /api/done/{case_id} updates database
   - Test error responses for invalid inputs
   - Test empty database handling

3. **Data model logic**
   - Test RecordPair construction from database records
   - Test priority group assignment
   - Test is_complete and is_valid flags

4. **Edge cases**
   - Empty database (no records)
   - Single record (no pairs)
   - All complete pairs
   - All incomplete pairs
   - Mixed QC results
   - NULL Case IDs

### Property-Based Testing Approach

Property-based testing will verify universal correctness properties using the **Hypothesis** library for Python. Each property test will:
- Run a minimum of 100 iterations with randomly generated inputs
- Be tagged with a comment referencing the specific correctness property from this design document
- Use the format: `# Feature: web-frontend, Property {number}: {property_text}`

**Property test coverage:**

1. **Property 1: All required fields displayed** - Generate random records, render, verify all fields present
2. **Property 2: Sorting order** - Generate random records, verify sort order matches specification
3. **Property 3: Database state match** - Generate random database state, verify displayed records match
4. **Property 4: Consecutive pairs** - Generate random pairs, verify they appear consecutively
5. **Property 5: Grouping indicators** - Generate random pairs, verify styling attributes present
6. **Property 6: Complete pair indicator** - Generate random complete/incomplete pairs, verify indicator presence
7. **Property 7: Valid pair indicator** - Generate random pairs with various QC values, verify indicator logic
8. **Property 8: Checkbox presence** - Generate random complete/incomplete pairs, verify checkbox presence
9. **Property 9: Done update affects both** - Generate random pairs, update done, verify both records updated
10. **Property 10: Checkbox state matches DB** - Generate random database states, verify checkbox rendering
11. **Property 11: Done persistence** - Generate random updates, verify database reflects changes
12. **Property 12: Priority group 1 membership** - Generate random records, verify group 1 criteria
13. **Property 13: Priority group 2 membership** - Generate random records, verify group 2 criteria
14. **Property 14: Priority group 3 membership** - Generate random records, verify group 3 criteria
15. **Property 15: Filter matching** - Generate random filter strings and records, verify filter logic
16. **Property 16: Sort ordering** - Generate random sort operations, verify ordering
17. **Property 17: Pair preservation** - Generate random filter/sort operations, verify pairs stay together

**Hypothesis strategies:**
- `text()` for generating Case IDs, Vorgangsnummer, filter strings
- `sampled_from(['genomic', 'clinical'])` for art_der_daten
- `sampled_from(['0', '1', '2'])` for typ_der_meldung, ergebnis_qc
- `booleans()` for is_done flags
- `lists()` for generating sets of records
- Custom strategies for generating valid RecordPair instances
- Custom strategies for generating complete/incomplete pairs

### Integration Testing

Integration tests will verify:
- End-to-end page load: database query → template rendering → HTML response
- HTMX checkbox interaction: click → POST request → database update → HTML swap
- Alpine.js filtering: input change → filtered display
- Alpine.js sorting: header click → sorted display
- Database migration: adding is_done column to existing database

### Frontend Testing

Frontend tests will verify:
- Alpine.js filtering logic with various filter strings
- Alpine.js sorting logic with various columns
- HTMX attribute presence and correctness
- CSS class application for pair grouping
- Responsive table layout

### Test Data

Test data will include:
- Complete pairs (both genomic and clinical)
- Incomplete pairs (only genomic or only clinical)
- Mixed QC results (passing and failing)
- Various Case ID formats
- NULL Case IDs (unresolved records)
- Large datasets (100+ records) for performance testing

## Implementation Notes

### Database Schema Migration

The existing database needs a new column. The migration will be handled in the database service layer:

```python
def migrate_schema(conn: duckdb.DuckDBPyConnection):
    """Add is_done column if it doesn't exist."""
    try:
        conn.execute("""
            ALTER TABLE meldebestaetigungen 
            ADD COLUMN IF NOT EXISTS is_done BOOLEAN DEFAULT FALSE
        """)
    except Exception as e:
        logger.warning(f"Migration may have already run: {e}")
```

### FastAPI Configuration

```python
app = FastAPI(
    title="Meldebestätigungen Viewer",
    description="Web interface for reviewing Meldebestätigungen",
    version="1.0.0"
)

# CORS configuration if needed
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
```

### Uvicorn Server Configuration

The web server will be started via a script entry in `pyproject.toml`:

```toml
[project.scripts]
mvh-copy-mb = "mvh_copy_mb.cli:main"
web = "uvicorn src.mvh_copy_mb.web:app --host 0.0.0.0 --port 8000 --reload"
```

Users can start the server with:
```bash
uv run web
```

For production deployment (without reload):
```bash
uv run uvicorn src.mvh_copy_mb.web:app --host 0.0.0.0 --port 8000
```

### HTMX Configuration

HTMX attributes for checkbox updates:

```html
<input type="checkbox" 
       {% if pair.is_done %}checked{% endif %}
       hx-post="/api/done/{{ pair.case_id }}"
       hx-vals='{"done": "{{ not pair.is_done }}"}'
       hx-target="closest tr"
       hx-swap="outerHTML"
       hx-indicator="#loading">
```

### Alpine.js Configuration

Alpine.js component for table state:

```javascript
function tableData() {
    return {
        pairs: {{ pairs_json | safe }},  // Injected from Jinja2
        filter: '',
        sortColumn: 'priority_group',
        sortDirection: 'asc',
        
        get filteredAndSorted() {
            let filtered = this.filter === '' 
                ? this.pairs 
                : this.pairs.filter(pair => 
                    Object.values(pair).some(val => 
                        String(val).toLowerCase().includes(this.filter.toLowerCase())
                    )
                );
            
            return filtered.sort((a, b) => {
                // Primary sort: priority_group
                if (a.priority_group !== b.priority_group) {
                    return a.priority_group - b.priority_group;
                }
                
                // Secondary sort: selected column
                let aVal = a[this.sortColumn];
                let bVal = b[this.sortColumn];
                
                if (this.sortDirection === 'asc') {
                    return aVal > bVal ? 1 : -1;
                } else {
                    return aVal < bVal ? 1 : -1;
                }
            });
        },
        
        sortBy(column) {
            if (this.sortColumn === column) {
                this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
            } else {
                this.sortColumn = column;
                this.sortDirection = 'asc';
            }
        }
    }
}
```

### CSS Styling for Pair Grouping

```css
/* Rosé Pine Palette variables (Dawn + Moon) */
:root {
    /* Dawn */
    --rp-dawn-base: #faf4ed;
    --rp-dawn-surface: #fffaf3;
    --rp-dawn-overlay: #f2e9e1;
    --rp-dawn-muted: #9893a5;
    --rp-dawn-subtle: #797593;
    --rp-dawn-text: #575279;
    --rp-dawn-love: #b4637a;
    --rp-dawn-gold: #ea9d34;
    --rp-dawn-rose: #d7827e;
    --rp-dawn-pine: #286983;
    --rp-dawn-foam: #56949f;
    --rp-dawn-iris: #907aa9;
    --rp-dawn-highlight-low: #f4ede8;
    --rp-dawn-highlight-med: #dfdad9;
    --rp-dawn-highlight-high: #cecacd;
    
    /* Moon */
    --rp-moon-base: #232136;
    --rp-moon-surface: #2a273f;
    --rp-moon-overlay: #393552;
    --rp-moon-muted: #6e6a86;
    --rp-moon-subtle: #908caa;
    --rp-moon-text: #e0def4;
    --rp-moon-love: #eb6f92;
    --rp-moon-gold: #f6c177;
    --rp-moon-rose: #ea9a97;
    --rp-moon-pine: #3e8fb0;
    --rp-moon-foam: #9ccfd8;
    --rp-moon-iris: #c4a7e7;
    --rp-moon-highlight-low: #2a283e;
    --rp-moon-highlight-med: #44415a;
    --rp-moon-highlight-high: #56526e;
    
    /* App theme tokens (light defaults = Dawn) */
    color-scheme: light;
    --bg: var(--rp-dawn-base);
    --text: var(--rp-dawn-text);
    --panel: var(--rp-dawn-surface);
    --overlay: var(--rp-dawn-overlay);
    --muted: var(--rp-dawn-muted);
    --subtle: var(--rp-dawn-subtle);
    --link: var(--rp-dawn-iris);
    --border: var(--rp-dawn-highlight-high);
    --accent: var(--rp-dawn-iris);
    --error: var(--rp-dawn-love);
    --warn: var(--rp-dawn-gold);
    --ok: var(--rp-dawn-pine);
    --info: var(--rp-dawn-foam);
    --code-bg: var(--rp-dawn-highlight-low);
}

@media (prefers-color-scheme: dark) {
    :root {
        color-scheme: dark;
        --bg: var(--rp-moon-base);
        --text: var(--rp-moon-text);
        --panel: var(--rp-moon-surface);
        --overlay: var(--rp-moon-overlay);
        --muted: var(--rp-moon-muted);
        --subtle: var(--rp-moon-subtle);
        --link: var(--rp-moon-iris);
        --border: var(--rp-moon-highlight-high);
        --accent: var(--rp-moon-iris);
        --error: var(--rp-moon-love);
        --warn: var(--rp-moon-gold);
        --ok: var(--rp-moon-pine);
        --info: var(--rp-moon-foam);
        --code-bg: var(--rp-moon-highlight-low);
    }
}

/* Base styles */
body {
    background-color: var(--bg);
    color: var(--text);
}

/* Pair grouping */
.pair-row.genomic {
    border-top: 2px solid var(--accent);
}

.pair-row.clinical {
    border-bottom: 2px solid var(--accent);
}

.pair-row.genomic + .pair-row.clinical {
    background-color: var(--panel);
}

/* Complete pair indicator */
.complete-indicator {
    color: var(--info);
    font-weight: bold;
}

/* Valid pair indicator */
.valid-indicator {
    color: var(--ok);
    font-size: 1.2em;
}

/* Priority group separators */
.priority-group-1 {
    background-color: var(--warn);
    opacity: 0.2;
}

.priority-group-2 {
    background-color: var(--error);
    opacity: 0.15;
}

.priority-group-3 {
    background-color: var(--ok);
    opacity: 0.15;
}

/* Done checkbox */
input[type="checkbox"]:disabled {
    opacity: 0.3;
    cursor: not-allowed;
}

/* Table styling */
table {
    background-color: var(--panel);
    border: 1px solid var(--border);
}

th {
    background-color: var(--overlay);
    color: var(--text);
    cursor: pointer;
}

th:hover {
    background-color: var(--border);
}

td {
    border-color: var(--border);
}

/* Links and buttons */
a {
    color: var(--link);
}

button {
    background-color: var(--accent);
    border-color: var(--accent);
}

button:hover {
    background-color: var(--link);
    border-color: var(--link);
}

/* Input fields */
input[type="text"] {
    background-color: var(--panel);
    border-color: var(--border);
    color: var(--text);
}

input[type="text"]:focus {
    border-color: var(--accent);
}
```

### Performance Considerations

- **Database queries**: Use single query with JOIN logic to fetch all records
- **Template rendering**: Jinja2 is fast for server-side rendering
- **Client-side operations**: Alpine.js filtering/sorting happens in-memory
- **HTMX updates**: Only update affected rows, not entire table
- **Static assets**: Serve from CDN (unpkg.com) or local cache

### Security Considerations

- **SQL injection**: Use parameterized queries (already implemented in database layer)
- **XSS**: Jinja2 auto-escapes HTML by default
- **CSRF**: Not critical for read-only operations, but consider for production
- **Authentication**: Out of scope for this feature, but consider adding
- **Database access**: Ensure database file has appropriate permissions

## Dependencies

New dependencies to add to `pyproject.toml`:

```toml
dependencies = [
    # ... existing dependencies ...
    "fastapi>=0.109.0",
    "uvicorn[standard]>=0.27.0",
    "jinja2>=3.1.3",
    "python-multipart>=0.0.6",  # For form data
]
```

Frontend dependencies (CDN, no installation needed):
- HTMX: https://unpkg.com/htmx.org@1.9.10
- Alpine.js: https://unpkg.com/alpinejs@3.13.3
- Milligram CSS: https://unpkg.com/milligram@1.4.1/dist/milligram.min.css

## Future Enhancements

Potential future improvements (out of scope for this design):

1. **Pagination**: Add pagination for large datasets (1000+ records)
2. **Export**: Export filtered/sorted data to CSV
3. **Authentication**: Add user login and role-based access
4. **Audit log**: Track who marked records as done and when
5. **Bulk operations**: Select multiple pairs and mark as done
6. **Advanced filtering**: Filter by specific columns, date ranges
7. **Real-time updates**: WebSocket support for multi-user scenarios
8. **Mobile optimization**: Responsive design for tablets and phones
9. **Dark mode**: Theme toggle for user preference
10. **Comments**: Add notes/comments to specific records
