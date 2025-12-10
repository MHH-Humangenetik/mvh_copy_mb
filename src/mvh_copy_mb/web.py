"""
FastAPI web application for viewing and managing Meldebestätigungen.

This module provides a web-based interface for reviewing Meldebestätigungen
stored in the DuckDB database, with features for filtering, sorting, and
marking records as done.
"""

import logging
import os
import uuid
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager
from datetime import datetime

import click
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect
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

# Global sync system components (will be initialized in lifespan)
websocket_manager = None
event_broker = None
sync_service = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan event handler.
    
    Handles startup and shutdown events for the FastAPI application.
    """
    global websocket_manager, event_broker, sync_service
    
    # Startup
    logger.info("Starting Meldebestätigungen Viewer application")
    logger.info(f"FastAPI version: {app.version}")
    logger.info(f"Templates directory: {templates_dir}")
    logger.info(f"Static files directory: {static_dir}")
    
    # Initialize sync system components
    try:
        from .sync.config import SyncConfig
        from .websocket.manager import WebSocketManager
        from .events.broker import EventBrokerImpl
        from .sync.service import SyncServiceImpl
        from .sync.lock_manager import LockManagerImpl
        from .database import MeldebestaetigungDatabase
        
        # Create sync configuration
        config = SyncConfig()
        
        # Initialize components
        websocket_manager = WebSocketManager(config)
        event_broker = EventBrokerImpl()
        
        # Get database path for sync service
        db_path_str = os.getenv('DB_PATH', './data/meldebestaetigungen.duckdb')
        db_path = Path(db_path_str)
        database = MeldebestaetigungDatabase(db_path)
        
        # Initialize lock manager
        lock_manager = LockManagerImpl(config)
        
        # Initialize sync service
        sync_service = SyncServiceImpl(
            event_broker=event_broker,
            lock_manager=lock_manager,
            connection_manager=websocket_manager,
            database=database
        )
        
        # Start all services
        await websocket_manager.start()
        await sync_service.start()
        
        logger.info("Multi-user sync system initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize sync system: {e}", exc_info=True)
        # Continue without sync system for now
        websocket_manager = None
        event_broker = None
        sync_service = None
    
    yield
    
    # Shutdown
    logger.info("Shutting down Meldebestätigungen Viewer application")
    
    # Stop sync system components
    if sync_service:
        try:
            await sync_service.stop()
        except Exception as e:
            logger.error(f"Error stopping sync service: {e}")
    
    if websocket_manager:
        try:
            await websocket_manager.stop()
        except Exception as e:
            logger.error(f"Error stopping WebSocket manager: {e}")
    
    logger.info("Sync system shutdown complete")


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
                request,
                "index.html",
                {
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
        from dataclasses import asdict
        pairs_dict = []
        for pair in pairs:
            # Convert records to dicts and handle datetime serialization
            genomic_dict = None
            if pair.genomic:
                genomic_dict = asdict(pair.genomic)
                if genomic_dict.get('processed_at'):
                    genomic_dict['processed_at'] = genomic_dict['processed_at'].isoformat()
            
            clinical_dict = None
            if pair.clinical:
                clinical_dict = asdict(pair.clinical)
                if clinical_dict.get('processed_at'):
                    clinical_dict['processed_at'] = clinical_dict['processed_at'].isoformat()
            
            pair_dict = {
                'case_id': pair.case_id,
                'genomic': genomic_dict,
                'clinical': clinical_dict,
                'is_complete': pair.is_complete,
                'is_valid': pair.is_valid,
                'is_done': pair.is_done,
                'priority_group': pair.priority_group
            }
            pairs_dict.append(pair_dict)
        
        # Render template with pairs data
        return templates.TemplateResponse(
            request,
            "index.html",
            {
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
            
            # Trigger sync event if sync system is available
            if sync_service and event_broker:
                try:
                    # Create sync event data
                    sync_data = {
                        "case_id": case_id,
                        "is_done": done,
                        "is_complete": pair.is_complete,
                        "is_valid": pair.is_valid,
                        "priority_group": pair.priority_group,
                        "genomic": {
                            "vorgangsnummer": pair.genomic.vorgangsnummer,
                            "is_done": pair.genomic.is_done
                        } if pair.genomic else None,
                        "clinical": {
                            "vorgangsnummer": pair.clinical.vorgangsnummer,
                            "is_done": pair.clinical.is_done
                        } if pair.clinical else None
                    }
                    
                    # Get user ID from request (for now use a default)
                    user_id = request.headers.get("X-User-ID", "web_user")
                    
                    # Trigger sync event
                    await sync_service.handle_record_update(
                        record_id=case_id,
                        data=sync_data,
                        user_id=user_id,
                        version=1  # Simple versioning for now
                    )
                    
                    logger.info(f"Sync event triggered for Case ID {case_id}")
                    
                except Exception as sync_error:
                    # Log sync error but don't fail the request
                    logger.error(f"Failed to trigger sync event for {case_id}: {sync_error}")
            
            # Render the updated pair rows for HTMX swap
            return templates.TemplateResponse(
                request,
                "pair_rows.html",
                {"pair": pair}
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
    sync_status = "disabled"
    if websocket_manager and event_broker and sync_service:
        sync_status = "enabled"
        
    return {
        "status": "healthy",
        "application": "Meldebestätigungen Viewer",
        "version": app.version,
        "sync_system": sync_status
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Main WebSocket endpoint for real-time synchronization.
    
    Handles client connections, authentication, and message routing.
    """
    if not websocket_manager or not event_broker:
        logger.warning("WebSocket connection attempted but sync system not available")
        await websocket.close(code=1011, reason="Sync system unavailable")
        return
    
    # Accept the WebSocket connection
    await websocket.accept()
    
    # Generate connection ID and extract user info
    connection_id = str(uuid.uuid4())
    
    # For now, use a simple user identification scheme
    # In production, you'd extract this from authentication headers/tokens
    user_id = websocket.query_params.get("user_id", "anonymous")
    
    logger.info(f"WebSocket connection established: {connection_id} for user {user_id}")
    
    try:
        # Import sync models
        from .sync.models import ClientConnection
        
        # Create client connection object
        client_connection = ClientConnection(
            connection_id=connection_id,
            user_id=user_id,
            websocket=websocket,
            last_seen=datetime.now(),
            subscriptions=set()
        )
        
        # Add connection to manager
        await websocket_manager.add_connection(client_connection)
        
        # Subscribe client to all events (for now - could be more selective)
        await event_broker.subscribe_client(client_connection, {"*"})
        
        # Send welcome message
        welcome_message = {
            "type": "connection_established",
            "connection_id": connection_id,
            "user_id": user_id,
            "timestamp": datetime.now().isoformat()
        }
        await websocket.send_json(welcome_message)
        
        # Handle incoming messages
        while True:
            try:
                # Receive message from client
                data = await websocket.receive_json()
                
                # Update last seen timestamp
                await websocket_manager.update_last_seen(connection_id)
                
                # Handle different message types
                message_type = data.get("type")
                
                if message_type == "heartbeat":
                    # Respond to heartbeat
                    response = {
                        "type": "heartbeat_response",
                        "timestamp": datetime.now().isoformat()
                    }
                    await websocket.send_json(response)
                    
                elif message_type == "subscribe":
                    # Handle subscription updates
                    subscriptions = set(data.get("subscriptions", []))
                    await event_broker.subscribe_client(client_connection, subscriptions)
                    
                    response = {
                        "type": "subscription_updated",
                        "subscriptions": list(subscriptions),
                        "timestamp": datetime.now().isoformat()
                    }
                    await websocket.send_json(response)
                    
                elif message_type == "sync_request":
                    # Handle sync request for missed updates
                    last_sync = data.get("last_sync_timestamp")
                    if last_sync:
                        last_sync_dt = datetime.fromisoformat(last_sync.replace('Z', '+00:00'))
                        await sync_service.sync_client(connection_id, last_sync_dt)
                    else:
                        await sync_service.sync_client(connection_id)
                        
                else:
                    logger.warning(f"Unknown message type from {connection_id}: {message_type}")
                    
            except WebSocketDisconnect:
                logger.info(f"WebSocket client {connection_id} disconnected normally")
                break
            except Exception as e:
                logger.error(f"Error handling WebSocket message from {connection_id}: {e}")
                # Send error response
                error_response = {
                    "type": "error",
                    "message": "Failed to process message",
                    "timestamp": datetime.now().isoformat()
                }
                try:
                    await websocket.send_json(error_response)
                except:
                    # Connection might be broken
                    break
                    
    except WebSocketDisconnect:
        logger.info(f"WebSocket client {connection_id} disconnected during setup")
    except Exception as e:
        logger.error(f"Error in WebSocket connection {connection_id}: {e}", exc_info=True)
    finally:
        # Clean up connection
        if websocket_manager:
            try:
                await websocket_manager.remove_connection(connection_id)
            except Exception as e:
                logger.error(f"Error removing WebSocket connection {connection_id}: {e}")
        
        if event_broker:
            try:
                await event_broker.unsubscribe_client(connection_id)
            except Exception as e:
                logger.error(f"Error unsubscribing WebSocket client {connection_id}: {e}")
        
        logger.info(f"WebSocket connection {connection_id} cleanup complete")


@app.get("/api/sync/status")
async def get_sync_status():
    """
    Get the current status of the synchronization system.
    
    Returns:
        JSON response with sync system status and metrics
    """
    if not websocket_manager or not event_broker or not sync_service:
        return {
            "status": "disabled",
            "message": "Synchronization system is not available"
        }
    
    try:
        # Get metrics from all components
        connection_metrics = await websocket_manager.get_connection_metrics()
        broker_metrics = event_broker.get_metrics()
        buffer_stats = sync_service.get_buffer_stats()
        
        return {
            "status": "enabled",
            "connections": connection_metrics,
            "events": broker_metrics,
            "buffers": buffer_stats,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting sync status: {e}")
        return {
            "status": "error",
            "message": f"Failed to get sync status: {str(e)}"
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
