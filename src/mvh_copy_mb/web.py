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
audit_manager = None
audit_scheduler = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan event handler.
    
    Handles startup and shutdown events for the FastAPI application.
    """
    global websocket_manager, event_broker, sync_service, audit_manager, audit_scheduler
    
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
        from .sync.audit_manager import AuditTrailManager
        from .sync.audit_scheduler import AuditScheduler
        from .database import MeldebestaetigungDatabase
        
        # Create sync configuration
        config = SyncConfig()
        
        # Initialize audit trail manager
        audit_db_path_str = os.getenv('AUDIT_DB_PATH', './data/audit.duckdb')
        audit_db_path = Path(audit_db_path_str)
        audit_db_path.parent.mkdir(parents=True, exist_ok=True)
        
        audit_manager = AuditTrailManager(audit_db_path)
        audit_manager.__enter__()  # Initialize audit database
        
        # Initialize components
        websocket_manager = WebSocketManager(config)
        event_broker = EventBrokerImpl()
        
        # Get database path for sync service
        db_path_str = os.getenv('DB_PATH', './data/meldebestaetigungen.duckdb')
        db_path = Path(db_path_str)
        database = MeldebestaetigungDatabase(db_path)
        
        # Initialize lock manager
        lock_manager = LockManagerImpl(config)
        
        # Initialize sync service with audit manager
        sync_service = SyncServiceImpl(
            event_broker=event_broker,
            lock_manager=lock_manager,
            connection_manager=websocket_manager,
            database=database,
            audit_manager=audit_manager
        )
        
        # Initialize audit scheduler
        audit_scheduler = AuditScheduler(audit_manager)
        
        # Start all services
        await websocket_manager.start()
        await sync_service.start()
        await audit_scheduler.start()
        
        logger.info("Multi-user sync system with audit trail initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize sync system: {e}", exc_info=True)
        # Continue without sync system for now
        websocket_manager = None
        event_broker = None
        sync_service = None
        if audit_manager:
            try:
                audit_manager.__exit__(None, None, None)
            except Exception:
                pass
            audit_manager = None
        audit_scheduler = None
    
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
    
    if audit_scheduler:
        try:
            await audit_scheduler.stop()
        except Exception as e:
            logger.error(f"Error stopping audit scheduler: {e}")
    
    if audit_manager:
        try:
            audit_manager.__exit__(None, None, None)
        except Exception as e:
            logger.error(f"Error stopping audit manager: {e}")
    
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
        
        # Get user information for audit logging
        user_id = request.headers.get("X-User-ID", "web_user")
        session_id = request.headers.get("X-Session-ID")
        ip_address = request.client.host if request.client else None
        user_agent = request.headers.get("User-Agent")
        
        # Import WebDatabaseService
        from .web_database import WebDatabaseService
        
        # Update the done status
        web_db = WebDatabaseService(db_path)
        
        # Get current state for audit logging
        current_pair = web_db.get_pair_by_case_id(case_id)
        before_state = {
            "is_done": current_pair.is_done if current_pair else None,
            "genomic_done": current_pair.genomic.is_done if current_pair and current_pair.genomic else None,
            "clinical_done": current_pair.clinical.is_done if current_pair and current_pair.clinical else None
        } if current_pair else None
        
        try:
            result = web_db.update_pair_done_status(case_id, done)
            
            if not result:
                # Log failed update to audit trail
                if audit_manager:
                    audit_manager.log_record_status_change(
                        user_id=user_id,
                        record_id=case_id,
                        before_status=before_state.get("is_done") if before_state else None,
                        after_status=done,
                        session_id=session_id,
                        details={
                            "ip_address": ip_address,
                            "user_agent": user_agent,
                            "success": False,
                            "error": "Failed to update done status"
                        }
                    )
                
                raise HTTPException(
                    status_code=500,
                    detail="Failed to update done status"
                )
            
            # Get the updated pair for rendering
            pair = web_db.get_pair_by_case_id(case_id)
            
            # Log successful update to audit trail
            if audit_manager and pair:
                after_state = {
                    "is_done": pair.is_done,
                    "genomic_done": pair.genomic.is_done if pair.genomic else None,
                    "clinical_done": pair.clinical.is_done if pair.clinical else None
                }
                
                audit_manager.log_record_status_change(
                    user_id=user_id,
                    record_id=case_id,
                    before_status=before_state.get("is_done") if before_state else None,
                    after_status=done,
                    session_id=session_id,
                    details={
                        "ip_address": ip_address,
                        "user_agent": user_agent,
                        "before_state": before_state,
                        "after_state": after_state,
                        "success": True
                    }
                )
            
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
    session_id = websocket.query_params.get("session_id")
    
    logger.info(f"WebSocket connection established: {connection_id} for user {user_id}")
    
    # Log connection establishment to audit trail
    if audit_manager:
        from .sync.audit import AuditEventType
        audit_manager.log_connection_event(
            user_id=user_id,
            connection_id=connection_id,
            event_type=AuditEventType.CONNECTION_ESTABLISHED,
            session_id=session_id,
            details={
                "websocket_endpoint": "/ws",
                "query_params": dict(websocket.query_params)
            }
        )
    
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
                
                # Log normal disconnection to audit trail
                if audit_manager:
                    from .sync.audit import AuditEventType
                    audit_manager.log_connection_event(
                        user_id=user_id,
                        connection_id=connection_id,
                        event_type=AuditEventType.CONNECTION_LOST,
                        session_id=session_id,
                        details={"disconnect_reason": "normal"}
                    )
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
        
        # Log disconnection during setup to audit trail
        if audit_manager:
            from .sync.audit import AuditEventType
            audit_manager.log_connection_event(
                user_id=user_id,
                connection_id=connection_id,
                event_type=AuditEventType.CONNECTION_LOST,
                session_id=session_id,
                details={"disconnect_reason": "setup_failure"}
            )
    except Exception as e:
        logger.error(f"Error in WebSocket connection {connection_id}: {e}", exc_info=True)
        
        # Log connection error to audit trail
        if audit_manager:
            from .sync.audit import AuditEventType, AuditSeverity
            audit_manager.log_system_error(
                error_message=str(e),
                error_type="websocket_connection_error",
                details={
                    "connection_id": connection_id,
                    "user_id": user_id,
                    "session_id": session_id
                },
                severity=AuditSeverity.ERROR
            )
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


@app.get("/api/audit/events")
async def get_audit_events(
    request: Request,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    user_id: Optional[str] = None,
    event_types: Optional[str] = None,
    record_id: Optional[str] = None,
    session_id: Optional[str] = None,
    severity: Optional[str] = None,
    success: Optional[bool] = None,
    limit: int = 100,
    offset: int = 0
):
    """
    Query audit events with filtering options.
    
    Args:
        request: FastAPI request object
        start_time: Filter events after this time (ISO format)
        end_time: Filter events before this time (ISO format)
        user_id: Filter by user ID
        event_types: Comma-separated list of event types
        record_id: Filter by record ID
        session_id: Filter by session ID
        severity: Filter by severity level
        success: Filter by success status
        limit: Maximum number of results (max 1000)
        offset: Number of results to skip
        
    Returns:
        JSON response with audit events
    """
    if not audit_manager:
        raise HTTPException(
            status_code=503,
            detail="Audit trail system is not available"
        )
    
    try:
        # Parse datetime parameters
        start_dt = None
        end_dt = None
        
        if start_time:
            try:
                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid start_time format")
        
        if end_time:
            try:
                end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid end_time format")
        
        # Parse event types
        event_type_list = None
        if event_types:
            from .sync.audit import AuditEventType
            try:
                event_type_list = [AuditEventType(et.strip()) for et in event_types.split(",")]
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"Invalid event type: {e}")
        
        # Parse severity
        severity_enum = None
        if severity:
            from .sync.audit import AuditSeverity
            try:
                severity_enum = AuditSeverity(severity)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid severity: {severity}")
        
        # Limit the maximum number of results
        limit = min(limit, 1000)
        
        # Query audit events
        events = audit_manager.query_audit_events(
            start_time=start_dt,
            end_time=end_dt,
            user_id=user_id,
            event_types=event_type_list,
            record_id=record_id,
            session_id=session_id,
            severity=severity_enum,
            success=success,
            limit=limit,
            offset=offset
        )
        
        # Convert events to JSON-serializable format
        events_data = []
        for event in events:
            event_dict = {
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "severity": event.severity.value,
                "timestamp": event.timestamp.isoformat(),
                "user_id": event.user_id,
                "session_id": event.session_id,
                "connection_id": event.connection_id,
                "record_id": event.record_id,
                "action": event.action,
                "details": event.details,
                "ip_address": event.ip_address,
                "user_agent": event.user_agent,
                "duration_ms": event.duration_ms,
                "success": event.success,
                "error_message": event.error_message,
                "before_state": event.before_state,
                "after_state": event.after_state
            }
            events_data.append(event_dict)
        
        return {
            "events": events_data,
            "count": len(events_data),
            "limit": limit,
            "offset": offset,
            "has_more": len(events_data) == limit
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error querying audit events: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to query audit events")


@app.get("/api/audit/report")
async def get_audit_report(
    request: Request,
    start_time: str,
    end_time: str,
    group_by: str = "user_id"
):
    """
    Generate an audit report for a time period.
    
    Args:
        request: FastAPI request object
        start_time: Report start time (ISO format)
        end_time: Report end time (ISO format)
        group_by: Field to group by (user_id, event_type, severity)
        
    Returns:
        JSON response with audit report
    """
    if not audit_manager:
        raise HTTPException(
            status_code=503,
            detail="Audit trail system is not available"
        )
    
    try:
        # Parse datetime parameters
        try:
            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid datetime format")
        
        # Validate group_by parameter
        if group_by not in ["user_id", "event_type", "severity"]:
            raise HTTPException(status_code=400, detail="Invalid group_by parameter")
        
        # Generate report
        report = audit_manager.generate_audit_report(
            start_time=start_dt,
            end_time=end_dt,
            group_by=group_by
        )
        
        return report
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating audit report: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to generate audit report")


@app.get("/api/audit/sessions")
async def get_session_activity(
    request: Request,
    hours: int = 24
):
    """
    Get session activity report for the last N hours.
    
    Args:
        request: FastAPI request object
        hours: Number of hours to look back (max 168 = 1 week)
        
    Returns:
        JSON response with session activity report
    """
    if not audit_manager:
        raise HTTPException(
            status_code=503,
            detail="Audit trail system is not available"
        )
    
    try:
        # Limit hours to reasonable range
        hours = min(max(hours, 1), 168)  # 1 hour to 1 week
        
        # Generate session activity report
        report = audit_manager.get_session_activity_report(hours=hours)
        
        return report
        
    except Exception as e:
        logger.error(f"Error generating session activity report: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to generate session activity report")


@app.get("/api/audit/cleanup/preview")
async def get_audit_cleanup_preview(request: Request):
    """
    Get a preview of what would be cleaned up in audit trail cleanup.
    
    Returns:
        JSON response with cleanup preview statistics
    """
    if not audit_scheduler:
        raise HTTPException(
            status_code=503,
            detail="Audit scheduler is not available"
        )
    
    try:
        preview = await audit_scheduler.get_cleanup_preview()
        return preview
        
    except Exception as e:
        logger.error(f"Error getting audit cleanup preview: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get cleanup preview")


@app.post("/api/audit/cleanup")
async def run_audit_cleanup(request: Request, dry_run: bool = False):
    """
    Run audit trail cleanup manually.
    
    Args:
        request: FastAPI request object
        dry_run: If True, only show what would be cleaned up
        
    Returns:
        JSON response with cleanup statistics
    """
    if not audit_scheduler:
        raise HTTPException(
            status_code=503,
            detail="Audit scheduler is not available"
        )
    
    try:
        stats = await audit_scheduler.run_manual_cleanup(dry_run=dry_run)
        return stats
        
    except Exception as e:
        logger.error(f"Error running audit cleanup: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to run audit cleanup")


@app.get("/api/audit/scheduler/status")
async def get_audit_scheduler_status(request: Request):
    """
    Get current audit scheduler status.
    
    Returns:
        JSON response with scheduler status information
    """
    if not audit_scheduler:
        raise HTTPException(
            status_code=503,
            detail="Audit scheduler is not available"
        )
    
    try:
        status = audit_scheduler.get_status()
        return status
        
    except Exception as e:
        logger.error(f"Error getting audit scheduler status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get scheduler status")


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
