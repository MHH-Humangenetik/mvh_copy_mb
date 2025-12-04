"""
FastAPI web application for viewing and managing Meldebestätigungen.

This module provides a web-based interface for reviewing Meldebestätigungen
stored in the DuckDB database, with features for filtering, sorting, and
marking records as done.
"""

import logging
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI application
app = FastAPI(
    title="Meldebestätigungen Viewer",
    description="Web interface for reviewing and managing Meldebestätigungen",
    version="1.0.0"
)

# Configure CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Determine base directory (src/mvh_copy_mb/)
BASE_DIR = Path(__file__).resolve().parent

# Configure Jinja2 templates
# Templates will be in src/mvh_copy_mb/templates/
templates_dir = BASE_DIR / "templates"
templates_dir.mkdir(exist_ok=True)
templates = Jinja2Templates(directory=str(templates_dir))

# Configure static files serving
# Static files will be in src/mvh_copy_mb/static/
static_dir = BASE_DIR / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

logger.info(f"Templates directory: {templates_dir}")
logger.info(f"Static files directory: {static_dir}")


@app.on_event("startup")
async def startup_event():
    """
    Application startup event handler.
    
    Logs application startup and verifies configuration.
    """
    logger.info("Starting Meldebestätigungen Viewer application")
    logger.info(f"FastAPI version: {app.version}")
    logger.info(f"Templates: {templates_dir}")
    logger.info(f"Static files: {static_dir}")


@app.on_event("shutdown")
async def shutdown_event():
    """
    Application shutdown event handler.
    
    Logs application shutdown and performs cleanup if needed.
    """
    logger.info("Shutting down Meldebestätigungen Viewer application")


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
    # TODO: Implement in task 5
    logger.info("Index page requested")
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "message": "Web frontend coming soon"}
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


def run_server():
    """
    Entry point for running the web server via CLI.
    
    This function is called when running `uv run web` command.
    It starts the Uvicorn server with appropriate configuration.
    """
    import uvicorn
    
    uvicorn.run(
        "mvh_copy_mb.web:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
