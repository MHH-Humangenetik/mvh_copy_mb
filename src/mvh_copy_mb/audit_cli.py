"""Command-line interface for audit trail management and reporting."""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List

from .sync.audit import AuditDatabase, AuditEventType, AuditSeverity
from .sync.audit_manager import AuditTrailManager


def parse_datetime(date_str: str) -> datetime:
    """Parse datetime string in various formats."""
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f"
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"Unable to parse datetime: {date_str}")


def format_event_for_display(event) -> str:
    """Format an audit event for human-readable display."""
    timestamp = event.timestamp.strftime("%Y-%m-%d %H:%M:%S")
    duration = f" ({event.duration_ms:.2f}ms)" if event.duration_ms else ""
    success_indicator = "✓" if event.success else "✗"
    
    return (
        f"{timestamp} [{event.severity.value.upper()}] {success_indicator} "
        f"{event.user_id} - {event.action}{duration}"
    )


def query_audit_events(args):
    """Query audit events with filtering options."""
    audit_db_path = Path(args.audit_db)
    
    # Parse datetime arguments
    start_time = parse_datetime(args.start_time) if args.start_time else None
    end_time = parse_datetime(args.end_time) if args.end_time else None
    
    # Parse event types
    event_types = None
    if args.event_types:
        try:
            event_types = [AuditEventType(et.strip()) for et in args.event_types.split(",")]
        except ValueError as e:
            print(f"Error: Invalid event type - {e}")
            return 1
    
    # Parse severity
    severity = None
    if args.severity:
        try:
            severity = AuditSeverity(args.severity)
        except ValueError:
            print(f"Error: Invalid severity level - {args.severity}")
            return 1
    
    try:
        with AuditDatabase(audit_db_path) as audit_db:
            events = audit_db.query_audit_events(
                start_time=start_time,
                end_time=end_time,
                user_id=args.user_id,
                event_types=event_types,
                record_id=args.record_id,
                session_id=args.session_id,
                severity=severity,
                success=args.success,
                limit=args.limit,
                offset=args.offset
            )
            
            if args.format == "json":
                # Output as JSON
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
                
                print(json.dumps(events_data, indent=2))
            else:
                # Output as human-readable text
                if not events:
                    print("No audit events found matching the criteria.")
                    return 0
                
                print(f"Found {len(events)} audit events:")
                print("-" * 80)
                
                for event in events:
                    print(format_event_for_display(event))
                    
                    if args.verbose:
                        if event.record_id:
                            print(f"  Record ID: {event.record_id}")
                        if event.session_id:
                            print(f"  Session ID: {event.session_id}")
                        if event.connection_id:
                            print(f"  Connection ID: {event.connection_id}")
                        if event.details:
                            print(f"  Details: {json.dumps(event.details, indent=4)}")
                        if event.error_message:
                            print(f"  Error: {event.error_message}")
                        print()
            
            return 0
            
    except Exception as e:
        print(f"Error querying audit events: {e}")
        return 1


def generate_audit_report(args):
    """Generate an audit report for a time period."""
    audit_db_path = Path(args.audit_db)
    
    # Parse datetime arguments
    try:
        start_time = parse_datetime(args.start_time)
        end_time = parse_datetime(args.end_time)
    except ValueError as e:
        print(f"Error parsing datetime: {e}")
        return 1
    
    try:
        with AuditDatabase(audit_db_path) as audit_db:
            report = audit_db.generate_audit_report(
                start_time=start_time,
                end_time=end_time,
                group_by=args.group_by
            )
            
            if args.format == "json":
                print(json.dumps(report, indent=2))
            else:
                # Human-readable report
                print(f"Audit Report: {report['period']['start_time']} to {report['period']['end_time']}")
                print("=" * 80)
                
                summary = report["summary"]
                print(f"Total Events: {summary['total_events']}")
                print(f"Unique Users: {summary['unique_users']}")
                print(f"Unique Sessions: {summary['unique_sessions']}")
                print(f"Error Count: {summary['error_count']} ({summary['error_rate']:.2%})")
                print(f"Average Duration: {summary['avg_duration_ms']:.2f}ms")
                print(f"Max Duration: {summary['max_duration_ms']:.2f}ms")
                print()
                
                # Group by statistics
                group_key = f"by_{args.group_by}"
                if group_key in report and report[group_key]:
                    print(f"Statistics by {args.group_by}:")
                    print("-" * 40)
                    for item in report[group_key][:10]:  # Top 10
                        print(f"{item[args.group_by]}: {item['event_count']} events "
                              f"({item['error_count']} errors, {item['error_rate']:.2%})")
                    print()
                
                # Event type distribution
                if report["event_types"]:
                    print("Event Type Distribution:")
                    print("-" * 40)
                    for event_type in report["event_types"][:10]:  # Top 10
                        print(f"{event_type['event_type']}: {event_type['count']} "
                              f"({event_type['errors']} errors, {event_type['error_rate']:.2%})")
                    print()
                
                # Top errors
                if report["top_errors"]:
                    print("Top Errors:")
                    print("-" * 40)
                    for error in report["top_errors"]:
                        print(f"{error['error_message']} ({error['event_type']}): {error['count']} times")
            
            return 0
            
    except Exception as e:
        print(f"Error generating audit report: {e}")
        return 1


def cleanup_audit_events(args):
    """Clean up old audit events based on retention policies."""
    audit_db_path = Path(args.audit_db)
    
    try:
        with AuditDatabase(audit_db_path) as audit_db:
            stats = audit_db.cleanup_old_events(dry_run=args.dry_run)
            
            if args.dry_run:
                print("Cleanup Dry Run Results:")
                print(f"Events to delete: {stats['events_to_delete']}")
                print(f"Events to archive: {stats['events_to_archive']}")
            else:
                print("Cleanup Results:")
                print(f"Events deleted: {stats['events_deleted']}")
                print(f"Events archived: {stats['events_archived']}")
            
            if args.verbose and stats["by_event_type"]:
                print("\nBy Event Type:")
                print("-" * 40)
                for event_type, type_stats in stats["by_event_type"].items():
                    if args.dry_run:
                        print(f"{event_type}: {type_stats['to_delete']} to delete, "
                              f"{type_stats['to_archive']} to archive")
                    else:
                        print(f"{event_type}: {type_stats['deleted']} deleted, "
                              f"{type_stats['archived']} archived")
            
            return 0
            
    except Exception as e:
        print(f"Error cleaning up audit events: {e}")
        return 1


def session_activity_report(args):
    """Generate a session activity report."""
    audit_db_path = Path(args.audit_db)
    
    try:
        with AuditTrailManager(audit_db_path) as audit_manager:
            report = audit_manager.get_session_activity_report(hours=args.hours)
            
            if args.format == "json":
                print(json.dumps(report, indent=2))
            else:
                print(f"Session Activity Report (Last {args.hours} hours)")
                print("=" * 50)
                print(f"Unique Users: {report['unique_users']}")
                print(f"Total Sessions: {report['total_sessions']}")
                print(f"Active Sessions: {report['active_sessions']}")
                print(f"Average Session Duration: {report['avg_session_duration_ms']:.2f}ms")
                print(f"Connection Events: {report['connection_events']}")
                print(f"Total Session Events: {report['session_events']}")
            
            return 0
            
    except Exception as e:
        print(f"Error generating session activity report: {e}")
        return 1


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Audit trail management and reporting CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--audit-db",
        default="audit.db",
        help="Path to audit database file (default: audit.db)"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Query command
    query_parser = subparsers.add_parser("query", help="Query audit events")
    query_parser.add_argument("--start-time", help="Start time (YYYY-MM-DD HH:MM:SS)")
    query_parser.add_argument("--end-time", help="End time (YYYY-MM-DD HH:MM:SS)")
    query_parser.add_argument("--user-id", help="Filter by user ID")
    query_parser.add_argument("--record-id", help="Filter by record ID")
    query_parser.add_argument("--session-id", help="Filter by session ID")
    query_parser.add_argument("--event-types", help="Comma-separated list of event types")
    query_parser.add_argument("--severity", choices=["info", "warning", "error", "critical"])
    query_parser.add_argument("--success", type=bool, help="Filter by success status")
    query_parser.add_argument("--limit", type=int, default=1000, help="Maximum results")
    query_parser.add_argument("--offset", type=int, default=0, help="Results offset")
    query_parser.add_argument("--format", choices=["text", "json"], default="text")
    query_parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    query_parser.set_defaults(func=query_audit_events)
    
    # Report command
    report_parser = subparsers.add_parser("report", help="Generate audit report")
    report_parser.add_argument("start_time", help="Report start time (YYYY-MM-DD HH:MM:SS)")
    report_parser.add_argument("end_time", help="Report end time (YYYY-MM-DD HH:MM:SS)")
    report_parser.add_argument("--group-by", choices=["user_id", "event_type", "severity"], 
                              default="user_id", help="Group results by field")
    report_parser.add_argument("--format", choices=["text", "json"], default="text")
    report_parser.set_defaults(func=generate_audit_report)
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser("cleanup", help="Clean up old audit events")
    cleanup_parser.add_argument("--dry-run", action="store_true", 
                               help="Show what would be cleaned up without doing it")
    cleanup_parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    cleanup_parser.set_defaults(func=cleanup_audit_events)
    
    # Session activity command
    session_parser = subparsers.add_parser("sessions", help="Session activity report")
    session_parser.add_argument("--hours", type=int, default=24, 
                               help="Hours to look back (default: 24)")
    session_parser.add_argument("--format", choices=["text", "json"], default="text")
    session_parser.set_defaults(func=session_activity_report)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())