"""
FastAPI web application for viewing and managing Meldebestätigungen.

This module provides a web-based interface for reviewing Meldebestätigungen
stored in the DuckDB database, with features for filtering, sorting, and
marking records as done.
"""

import logging
import os
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

import click
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Determine base directory (src/mvh_copy_mb/)
BASE_DIR = Path(__file__).resolve().parent

# Configure Jinja2 templates
# Templates will be in src/mvh_copy_mb/templates/
templates_dir = BASE_DIR / "templates"
templates_dir.mkdir(exist_ok=True)
templates = Jinja2Templates(directory=str(templates_dir))

# Configure static files directory
# Static files will be in src/mvh_copy_mb/static/
static_dir = BASE_DIR / "static"
static_dir.mkdir(exist_ok=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan event handler.
    
    Handles startup and shutdown events for the FastAPI application.
    """
    # Startup
    logger.info("Starting Meldebestätigungen Viewer application")
    logger.info(f"FastAPI version: {app.version}")
    logger.info(f"Templates directory: {templates_dir}")
    logger.info(f"Static files directory: {static_dir}")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Meldebestätigungen Viewer application")


# Initialize FastAPI application with lifespan handler
app = FastAPI(
    title="Meldebestätigungen Viewer",
    description="Web interface for reviewing and managing Meldebestätigungen",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Mount static files
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """
    Main page endpoint - displays all Meldebestätigungen in a table.
    
    Args:
        request: FastAPI request object
        
    Returns:
        HTML response with rendered template
        
    Raises:
        HTTPException: If database connection fails or other errors occur
    """
    logger.info("Index page requested")
    
    try:
        # Get database path from environment or use default
        db_path_str = os.getenv('DB_PATH', './data/meldebestaetigungen.duckdb')
        db_path = Path(db_path_str)
        
        # Check if database exists
        if not db_path.exists():
            logger.warning(f"Database not found at {db_path}")
            return templates.TemplateResponse(
                request=request,
                name="index.html",
                context={
                    "pairs": [],
                    "error_message": f"Database not found at {db_path}. Please process some CSV files first."
                }
            )
        
        # Import WebDatabaseService here to avoid circular imports
        from .web_database import WebDatabaseService
        
        # Query database for all records grouped by Case ID
        web_db = WebDatabaseService(db_path)
        pairs = web_db.get_all_records_grouped()
        
        logger.info(f"Retrieved {len(pairs)} record pairs from database")
        
        # Convert pairs to dictionaries for JSON serialization
        pairs_dict = []
        for pair in pairs:
            pair_dict = {
                'case_id': pair.case_id,
                'genomic': {
                    'vorgangsnummer': pair.genomic.vorgangsnummer,
                    'typ_der_meldung': pair.genomic.typ_der_meldung,
                    'indikationsbereich': pair.genomic.indikationsbereich,
                    'ergebnis_qc': pair.genomic.ergebnis_qc,
                    'source_file': pair.genomic.source_file,
                    'is_done': pair.genomic.is_done
                } if pair.genomic else None,
                'clinical': {
                    'vorgangsnummer': pair.clinical.vorgangsnummer,
                    'typ_der_meldung': pair.clinical.typ_der_meldung,
                    'indikationsbereich': pair.clinical.indikationsbereich,
                    'ergebnis_qc': pair.clinical.ergebnis_qc,
                    'source_file': pair.clinical.source_file,
                    'is_done': pair.clinical.is_done
                } if pair.clinical else None,
                'is_complete': pair.is_complete,
                'is_valid': pair.is_valid,
                'is_done': pair.is_done,
                'priority_group': pair.priority_group
            }
            pairs_dict.append(pair_dict)
        
        # Render template with pairs data
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context={
                "pairs": pairs_dict,
                "error_message": None if pairs else "No records found in database."
            }
        )
        
    except Exception as e:
        logger.error(f"Error loading index page: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load records from database: {str(e)}"
        )


@app.post("/api/done/{case_id}")
async def update_done_status(case_id: str, request: Request):
    """
    Update the done status for a record pair.
    
    Args:
        case_id: The Case ID of the pair to update
        request: FastAPI request object containing the form data
        
    Returns:
        HTML response with updated pair rows for HTMX swap
        
    Raises:
        HTTPException: If Case ID doesn't exist, pair is incomplete, or update fails
    """
    logger.info(f"Done status update requested for Case ID: {case_id}")
    
    try:
        # Get database path from environment or use default
        db_path_str = os.getenv('DB_PATH', './data/meldebestaetigungen.duckdb')
        db_path = Path(db_path_str)
        
        # Check if database exists
        if not db_path.exists():
            logger.error(f"Database not found at {db_path}")
            raise HTTPException(
                status_code=500,
                detail=f"Database not found at {db_path}"
            )
        
        # Parse form data to get the new done status
        form_data = await request.form()
        done_str = form_data.get('done', 'false')
        done = done_str.lower() in ('true', '1', 'yes', 'on')
        
        logger.info(f"Updating Case ID {case_id} to done={done}")
        
        # Import WebDatabaseService
        from .web_database import WebDatabaseService
        
        # Update the done status
        web_db = WebDatabaseService(db_path)
        
        try:
            result = web_db.update_pair_done_status(case_id, done)
            
            if not result:
                raise HTTPException(
                    status_code=500,
                    detail="Failed to update done status"
                )
            
            # Get the updated pair for rendering
            pair = web_db.get_pair_by_case_id(case_id)
            
            if pair is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Case ID {case_id} not found after update"
                )
            
            # TODO: Render the updated pair rows when template is created (task 8)
            # For now, return JSON response
            from fastapi.responses import JSONResponse
            return JSONResponse(
                content={
                    "success": True,
                    "case_id": case_id,
                    "is_done": done
                }
            )
            
        except ValueError as e:
            # Handle validation errors (incomplete pair, non-existent case_id)
            error_msg = str(e)
            logger.warning(f"Validation error updating Case ID {case_id}: {error_msg}")
            
            if "no records found" in error_msg.lower():
                raise HTTPException(status_code=404, detail=error_msg)
            elif "incomplete" in error_msg.lower():
                raise HTTPException(status_code=400, detail=error_msg)
            else:
                raise HTTPException(status_code=400, detail=error_msg)
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error updating done status for Case ID {case_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update done status: {str(e)}"
        )


@app.get("/health")
async def health_check():
    """
    Health check endpoint for monitoring.
    
    Returns:
        JSON response with application status
    """
    return {
        "status": "healthy",
        "application": "Meldebestätigungen Viewer",
        "version": app.version
    }


@click.command()
@click.option(
    '--host',
    envvar='WEB_HOST',
    default='0.0.0.0',
    show_default=True,
    help='Host to bind the web server to'
)
@click.option(
    '--port',
    envvar='WEB_PORT',
    type=int,
    default=8000,
    show_default=True,
    help='Port to bind the web server to'
)
@click.option(
    '--reload',
    envvar='WEB_RELOAD',
    is_flag=True,
    default=False,
    help='Enable auto-reload for development'
)
@click.option(
    '--log-level',
    envvar='WEB_LOG_LEVEL',
    default='info',
    show_default=True,
    type=click.Choice(['critical', 'error', 'warning', 'info', 'debug'], case_sensitive=False),
    help='Logging level for the web server'
)
def run_server(host: str, port: int, reload: bool, log_level: str):
    """
    Entry point for running the web server via CLI.
    
    This function is called when running `uv run web` command.
    It starts the Uvicorn server with appropriate configuration.
    
    Configuration can be provided via command-line options or environment variables:
    - WEB_HOST: Host to bind to (default: 0.0.0.0)
    - WEB_PORT: Port to bind to (default: 8000)
    - WEB_RELOAD: Enable auto-reload (default: false)
    - WEB_LOG_LEVEL: Logging level (default: info)
    """
    import uvicorn
    
    logger.info(f"Starting web server on {host}:{port}")
    logger.info(f"Reload mode: {reload}")
    logger.info(f"Log level: {log_level}")
    
    uvicorn.run(
        "mvh_copy_mb.web:app",
        host=host,
        port=port,
        reload=reload,
        log_level=log_level.lower()
    )
