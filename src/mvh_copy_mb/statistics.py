"""
CLI Summary Statistics Module

This module provides functionality for tracking and displaying processing statistics
during CLI execution, including file counts, pairing status, and GEPADO update results.
"""

from rich.console import Console
from dataclasses import dataclass
from typing import Optional


@dataclass
class ProcessingStatistics:
    """
    Tracks statistics during CLI processing.

    Attributes:
        ready_pairs_count: Complete pairs with both G and C data sharing same Case ID
        unpaired_genomic_count: Genomic files with Case IDs but no clinical counterpart
        unpaired_clinical_count: Clinical files with Case IDs but no genomic counterpart
        ignored_count: Files skipped due to QC failures, unresolved Case IDs, or errors
        gepado_genomic_updates: Actual genomic data updates in GEPADO
        gepado_clinical_updates: Actual clinical data updates in GEPADO
        gepado_no_updates_needed: Records validated but no updates needed
        gepado_errors: Errors during GEPADO operations
        _resolved_case_ids: Internal tracking for pairing logic
    """

    ready_pairs_count: int = 0
    unpaired_genomic_count: int = 0
    unpaired_clinical_count: int = 0
    ignored_count: int = 0
    gepado_genomic_updates: int = 0
    gepado_clinical_updates: int = 0
    gepado_no_updates_needed: int = 0
    gepado_errors: int = 0
    _resolved_case_ids: Optional[dict] = None

    def __post_init__(self):
        """Initialize internal tracking and validate statistics data after initialization."""
        if self._resolved_case_ids is None:
            self._resolved_case_ids = {}
        self._validate_counts()

    def _validate_counts(self) -> None:
        """
        Validate that all counts are non-negative integers.

        Raises:
            ValueError: If any count is negative or not an integer
        """
        counts = {
            "ready_pairs_count": self.ready_pairs_count,
            "unpaired_genomic_count": self.unpaired_genomic_count,
            "unpaired_clinical_count": self.unpaired_clinical_count,
            "ignored_count": self.ignored_count,
            "gepado_genomic_updates": self.gepado_genomic_updates,
            "gepado_clinical_updates": self.gepado_clinical_updates,
            "gepado_no_updates_needed": self.gepado_no_updates_needed,
            "gepado_errors": self.gepado_errors,
        }

        for name, value in counts.items():
            if not isinstance(value, int):
                raise ValueError(
                    f"{name} must be an integer, got {type(value).__name__}: {value}"
                )
            if value < 0:
                raise ValueError(f"{name} must be non-negative, got {value}")

    def increment_ready_pairs(self, count: int = 1) -> None:
        """
        Safely increment ready pairs count.

        Args:
            count: Number to increment by (default: 1)
        import os

        Raises:
            ValueError: If count is negative or not an integer
        """
        if not isinstance(count, int):
            raise ValueError(
                f"Increment count must be an integer, got {type(count).__name__}: {count}"
            )
        if count < 0:
            raise ValueError(f"Increment count must be non-negative, got {count}")
        self.ready_pairs_count += count

    def increment_unpaired_genomic(self, count: int = 1) -> None:
        """
        Safely increment unpaired genomic file count.

        Args:
            count: Number to increment by (default: 1)

        Raises:
            ValueError: If count is negative or not an integer
        """
        if not isinstance(count, int):
            raise ValueError(
                f"Increment count must be an integer, got {type(count).__name__}: {count}"
            )
        if count < 0:
            raise ValueError(f"Increment count must be non-negative, got {count}")
        self.unpaired_genomic_count += count

    def increment_unpaired_clinical(self, count: int = 1) -> None:
        """
        Safely increment unpaired clinical file count.

        Args:
            count: Number to increment by (default: 1)

        Raises:
            ValueError: If count is negative or not an integer
        """
        if not isinstance(count, int):
            raise ValueError(
                f"Increment count must be an integer, got {type(count).__name__}: {count}"
            )
        if count < 0:
            raise ValueError(f"Increment count must be non-negative, got {count}")
        self.unpaired_clinical_count += count

    def increment_ignored(self, count: int = 1) -> None:
        """
        Safely increment ignored file count.

        Args:
            count: Number to increment by (default: 1)

        Raises:
            ValueError: If count is negative or not an integer
        """
        if not isinstance(count, int):
            raise ValueError(
                f"Increment count must be an integer, got {type(count).__name__}: {count}"
            )
        if count < 0:
            raise ValueError(f"Increment count must be non-negative, got {count}")
        self.ignored_count += count

    def increment_gepado_genomic(self, count: int = 1) -> None:
        """
        Safely increment GEPADO genomic update count.

        Args:
            count: Number to increment by (default: 1)

        Raises:
            ValueError: If count is negative or not an integer
        """
        if not isinstance(count, int):
            raise ValueError(
                f"Increment count must be an integer, got {type(count).__name__}: {count}"
            )
        if count < 0:
            raise ValueError(f"Increment count must be non-negative, got {count}")
        self.gepado_genomic_updates += count

    def increment_gepado_clinical(self, count: int = 1) -> None:
        """
        Safely increment GEPADO clinical update count.

        Args:
            count: Number to increment by (default: 1)

        Raises:
            ValueError: If count is negative or not an integer
        """
        if not isinstance(count, int):
            raise ValueError(
                f"Increment count must be an integer, got {type(count).__name__}: {count}"
            )
        if count < 0:
            raise ValueError(f"Increment count must be non-negative, got {count}")
        self.gepado_clinical_updates += count

    def increment_gepado_no_updates_needed(self, count: int = 1) -> None:
        """
        Safely increment GEPADO no updates needed count.

        Args:
            count: Number to increment by (default: 1)

        Raises:
            ValueError: If count is negative or not an integer
        """
        if not isinstance(count, int):
            raise ValueError(
                f"Increment count must be an integer, got {type(count).__name__}: {count}"
            )
        if count < 0:
            raise ValueError(f"Increment count must be non-negative, got {count}")
        self.gepado_no_updates_needed += count

    def increment_gepado_errors(self, count: int = 1) -> None:
        """
        Safely increment GEPADO error count.

        Args:
            count: Number to increment by (default: 1)

        Raises:
            ValueError: If count is negative or not an integer
        """
        if not isinstance(count, int):
            raise ValueError(
                f"Increment count must be an integer, got {type(count).__name__}: {count}"
            )
        if count < 0:
            raise ValueError(f"Increment count must be non-negative, got {count}")
        self.gepado_errors += count

    def add_resolved_case_id(self, case_id: str, data_type: str) -> None:
        """
        Track a resolved Case ID and its data type for pairing logic.

        Args:
            case_id: The resolved Case ID
            data_type: The data type ('G' for genomic, 'C' for clinical)
        """
        # Ensure internal mapping initialized
        if self._resolved_case_ids is None:
            self._resolved_case_ids = {}

        if case_id not in self._resolved_case_ids:
            self._resolved_case_ids[case_id] = {"genomic": False, "clinical": False}

        if data_type.upper() == "G":
            self._resolved_case_ids[case_id]["genomic"] = True
        elif data_type.upper() == "C":
            self._resolved_case_ids[case_id]["clinical"] = True

    def finalize_pairing_statistics(self) -> None:
        """
        Calculate final pairing statistics based on resolved Case IDs.
        This should be called after all files have been processed.
        """
        # Reset pairing counts (but not ignored count which is tracked directly)
        self.ready_pairs_count = 0
        self.unpaired_genomic_count = 0
        self.unpaired_clinical_count = 0

        if not self._resolved_case_ids:
            return

        for case_id, types in self._resolved_case_ids.items():
            has_genomic = types["genomic"]
            has_clinical = types["clinical"]

            if has_genomic and has_clinical:
                # Complete pair
                self.ready_pairs_count += 1
            elif has_genomic and not has_clinical:
                # Unpaired genomic
                self.unpaired_genomic_count += 1
            elif has_clinical and not has_genomic:
                # Unpaired clinical
                self.unpaired_clinical_count += 1

    def get_total_files(self) -> int:
        """
        Calculate total files processed (Ready pairs counted as two files each).

        Returns:
            Total number of files processed, with ready pairs counted as two files

        Raises:
            ValueError: If any counts are invalid (should not happen with proper validation)
        """
        try:
            self._validate_counts()
            return (
                self.ready_pairs_count * 2
                + self.unpaired_genomic_count
                + self.unpaired_clinical_count
                + self.ignored_count
            )
        except ValueError as e:
            # Log error but return a safe fallback value
            Console(stderr=True).print(f"[yellow]Warning:[/] Invalid statistics data detected: {e}")
            return 0

    def get_total_gepado_operations(self) -> int:
        """
        Calculate total GEPADO operations attempted.

        Returns:
            Total number of GEPADO operations (updates + no-updates + errors)

        Raises:
            ValueError: If any counts are invalid (should not happen with proper validation)
        """
        try:
            self._validate_counts()
            return (
                self.gepado_genomic_updates
                + self.gepado_clinical_updates
                + self.gepado_no_updates_needed
                + self.gepado_errors
            )
        except ValueError as e:
            # Log error but return a safe fallback value
            Console(stderr=True).print(f"[yellow]Warning:[/] Invalid GEPADO statistics data detected: {e}")
            return 0


def render_progress_bar(count: int, total: int, width: int = 20) -> str:
    """
    Render a progress bar for the given count and total.

    Args:
        count: Current count value
        total: Maximum value for the progress bar
        width: Width of the progress bar in characters (excluding brackets)

    Returns:
        String representation of the progress bar with brackets

    Raises:
        ValueError: If width is non-positive or parameters are invalid
    """
    # Validate input parameters
    if not isinstance(count, int):
        try:
            count = int(count)
        except (ValueError, TypeError):
            Console(stderr=True).print(f"[yellow]Warning:[/] Invalid count value '{count}', using 0")
            count = 0

    if not isinstance(total, int):
        try:
            total = int(total)
        except (ValueError, TypeError):
            Console(stderr=True).print(f"[yellow]Warning:[/] Invalid total value '{total}', using 0")
            total = 0

    if not isinstance(width, int):
        try:
            width = int(width)
        except (ValueError, TypeError):
            Console(stderr=True).print(f"[yellow]Warning:[/] Invalid width value '{width}', using 20")
            width = 20

    # Validate width is positive
    if width <= 0:
        Console(stderr=True).print(f"[yellow]Warning:[/] Width must be positive, got {width}, using 20")
        width = 20

    # Ensure counts are non-negative
    if count < 0:
        Console(stderr=True).print(f"[yellow]Warning:[/] Count cannot be negative, got {count}, using 0")
        count = 0

    if total < 0:
        Console(stderr=True).print(f"[yellow]Warning:[/] Total cannot be negative, got {total}, using 0")
        total = 0

    # Handle division by zero case
    if total == 0:
        return "[" + "░" * width + "]"

    # Ensure count doesn't exceed total for progress calculation
    clamped_count = min(count, total)

    try:
        filled_width = int((clamped_count / total) * width)
        empty_width = width - filled_width

        # Ensure we don't exceed width due to rounding errors
        if filled_width > width:
            filled_width = width
            empty_width = 0
        elif filled_width < 0:
            filled_width = 0
            empty_width = width

        return "[" + "█" * filled_width + "░" * empty_width + "]"

    except (ZeroDivisionError, OverflowError, ValueError) as e:
        Console(stderr=True).print(f"[yellow]Warning:[/] Error calculating progress bar: {e}, returning empty bar")
        return "[" + "░" * width + "]"


def display_statistics(
    stats: Optional[ProcessingStatistics], gepado_enabled: bool = False
) -> None:
    """
    Display formatted statistics with progress bars.

    Args:
        stats: ProcessingStatistics instance with collected data (can be None)
        gepado_enabled: Whether GEPADO integration was enabled
    """
    # Handle missing statistics gracefully
    if stats is None:
        rich_console = Console()
        rich_console.print("\n" + "=" * 80)
        rich_console.print("PROCESSING SUMMARY".center(80))
        rich_console.print("=" * 80)
        rich_console.print("[yellow]Warning:[/] No statistics available to display")
        rich_console.print("=" * 80)
        return

    # Validate statistics object
    try:
        if not isinstance(stats, ProcessingStatistics):
            Console(stderr=True).print(f"[yellow]Warning:[/] Invalid statistics object type: {type(stats)}")
            return

        # Attempt to validate the statistics data
        stats._validate_counts()
    except ValueError as e:
        Console(stderr=True).print(f"[yellow]Warning:[/] Invalid statistics data: {e}")
        Console(stderr=True).print("Attempting to display available data...")
    except AttributeError:
        Console(stderr=True).print("[yellow]Warning:[/] Statistics object missing validation method")

    try:
        total_files = stats.get_total_files()

        # Calculate progress bar width to fit 80-character terminal
        # Format: "Label: count [progress_bar]"
        # Longest label is "Updated clinical data:" (21 chars) + count (6 chars) + space = 28 chars
        # Leave 5 chars margin, so progress bar can be 80 - 28 - 5 = 47 chars
        bar_width = 47

        # Detect terminal width for better compatibility
        try:
            import shutil

            terminal_width = shutil.get_terminal_size().columns
            if terminal_width < 80:
                # Adjust for narrow terminals
                bar_width = max(
                    10, terminal_width - 35
                )  # Minimum 10 chars for progress bar
                separator_width = terminal_width
            else:
                separator_width = 80
        except (OSError, AttributeError):
            # Fallback if terminal size detection fails
            separator_width = 80
            bar_width = 47

        # If stdout is a terminal, use rich for a prettier summary
        from rich.table import Table
        from rich import box
        from rich.progress_bar import ProgressBar

        console = Console()
        if console.is_terminal:
            # Build a table for file statistics using Rich ProgressBar for nicer visuals
            tbl = Table(title="PROCESSING SUMMARY", box=box.HEAVY_EDGE, pad_edge=True)
            tbl.add_column("Metric", justify="left")
            tbl.add_column("Count", justify="right")
            tbl.add_column("", justify="left")

            # Bar expects a positive total; fall back to 1 to show empty bar when total is 0
            total_for_bars = total_files if total_files > 0 else 1

            # Bar(size, begin, end, width=...)
            ready_bar = ProgressBar(
                total_for_bars, completed=stats.ready_pairs_count * 2, width=bar_width
            )
            genomic_bar = ProgressBar(
                total_for_bars, completed=stats.unpaired_genomic_count, width=bar_width
            )
            clinical_bar = ProgressBar(
                total_for_bars, completed=stats.unpaired_clinical_count, width=bar_width
            )
            ignored_bar = ProgressBar(
                total_for_bars, completed=stats.ignored_count, width=bar_width
            )

            tbl.add_row("Total files:", f"{total_files:>6}", "")
            tbl.add_row(
                "[green]Ready pairs:[/green]",
                f"[green]{stats.ready_pairs_count:>6}[/green]",
                ready_bar,
            )
            tbl.add_row(
                "[yellow]Unpaired genomic:[/yellow]",
                f"[yellow]{stats.unpaired_genomic_count:>6}[/yellow]",
                genomic_bar,
            )
            tbl.add_row(
                "[yellow]Unpaired clinical:[/yellow]",
                f"[yellow]{stats.unpaired_clinical_count:>6}[/yellow]",
                clinical_bar,
            )
            tbl.add_row(
                "[blue]Ignored files:[/blue]",
                f"[blue]{stats.ignored_count:>6}[/blue]",
                ignored_bar,
            )

            # GEPADO section included in same table when enabled
            if gepado_enabled:
                total_gepado = stats.get_total_gepado_operations()
                total_gepado_for_bars = total_gepado if total_gepado > 0 else 1

                genomic_updates_bar = ProgressBar(
                    total_gepado_for_bars,
                    completed=stats.gepado_genomic_updates,
                    width=bar_width,
                )
                clinical_updates_bar = ProgressBar(
                    total_gepado_for_bars,
                    completed=stats.gepado_clinical_updates,
                    width=bar_width,
                )
                no_updates_bar = ProgressBar(
                    total_gepado_for_bars,
                    completed=stats.gepado_no_updates_needed,
                    width=bar_width,
                )
                errors_bar = ProgressBar(
                    total_gepado_for_bars,
                    completed=stats.gepado_errors,
                    width=bar_width,
                )

                # Add a separator
                tbl.add_section()
                tbl.add_row("[bold]GEPADO[/bold]", "", "")
                tbl.add_row(
                    "[green]Updated genomic data:[/green]",
                    f"[green]{stats.gepado_genomic_updates:>6}[/green]",
                    genomic_updates_bar,
                )
                tbl.add_row(
                    "[green]Updated clinical data:[/green]",
                    f"[green]{stats.gepado_clinical_updates:>6}[/green]",
                    clinical_updates_bar,
                )
                tbl.add_row(
                    "[blue]No updates needed:[/blue]",
                    f"[blue]{stats.gepado_no_updates_needed:>6}[/blue]",
                    no_updates_bar,
                )
                tbl.add_row(
                    "[red]Errors during ops:[/red]",
                    f"[red]{stats.gepado_errors:>6}[/red]",
                    errors_bar,
                )
            console.print(tbl)

            return

        # Non-terminal fallback: keep original plain-text output for tests and non-interactive runs
        rich_console = Console()
        rich_console.print("\n" + "=" * separator_width)
        rich_console.print("PROCESSING SUMMARY".center(separator_width))
        rich_console.print("=" * separator_width)

        try:
            ready_bar = render_progress_bar(
                stats.ready_pairs_count * 2, total_files, bar_width
            )
            rich_console.print(f"Ready pairs:            {stats.ready_pairs_count:>6} {ready_bar}")
        except Exception as e:
            rich_console.print(f"Ready pairs:            {getattr(stats, 'ready_pairs_count', 0):>6} [Error: {e}]")

        try:
            genomic_bar = render_progress_bar(
                stats.unpaired_genomic_count, total_files, bar_width
            )
            rich_console.print(f"Unpaired genomic:       {stats.unpaired_genomic_count:>6} {genomic_bar}")
        except Exception as e:
            rich_console.print(f"Unpaired genomic:       {getattr(stats, 'unpaired_genomic_count', 0):>6} [Error: {e}]")

        try:
            clinical_bar = render_progress_bar(
                stats.unpaired_clinical_count, total_files, bar_width
            )
            rich_console.print(f"Unpaired clinical:      {stats.unpaired_clinical_count:>6} {clinical_bar}")
        except Exception as e:
            rich_console.print(f"Unpaired clinical:      {getattr(stats, 'unpaired_clinical_count', 0):>6} [Error: {e}]")

        try:
            ignored_bar = render_progress_bar(
                stats.ignored_count, total_files, bar_width
            )
            rich_console.print(f"Ignored files:          {stats.ignored_count:>6} {ignored_bar}")
        except Exception as e:
            rich_console.print(f"Ignored files:          {getattr(stats, 'ignored_count', 0):>6} [Error: {e}]")

        # GEPADO statistics (if enabled) with error handling
        if gepado_enabled:
            try:
                total_gepado = stats.get_total_gepado_operations()
                rich_console.print("\nGEPADO OPERATIONS:")

                try:
                    genomic_updates_bar = render_progress_bar(
                        stats.gepado_genomic_updates, total_gepado, bar_width
                    )
                    rich_console.print(f"Updated genomic data:   {stats.gepado_genomic_updates:>6} {genomic_updates_bar}")
                except Exception as e:
                    rich_console.print(f"Updated genomic data:   {getattr(stats, 'gepado_genomic_updates', 0):>6} [Error: {e}]")

                try:
                    clinical_updates_bar = render_progress_bar(
                        stats.gepado_clinical_updates, total_gepado, bar_width
                    )
                    rich_console.print(f"Updated clinical data:  {stats.gepado_clinical_updates:>6} {clinical_updates_bar}")
                except Exception as e:
                    rich_console.print(f"Updated clinical data:  {getattr(stats, 'gepado_clinical_updates', 0):>6} [Error: {e}]")

                try:
                    no_updates_bar = render_progress_bar(
                        stats.gepado_no_updates_needed, total_gepado, bar_width
                    )
                    rich_console.print(f"No updates needed:      {stats.gepado_no_updates_needed:>6} {no_updates_bar}")
                except Exception as e:
                    rich_console.print(f"No updates needed:      {getattr(stats, 'gepado_no_updates_needed', 0):>6} [Error: {e}]")

                try:
                    errors_bar = render_progress_bar(
                        stats.gepado_errors, total_gepado, bar_width
                    )
                    rich_console.print(f"Errors during ops:      {stats.gepado_errors:>6} {errors_bar}")
                except Exception as e:
                    rich_console.print(f"Errors during ops:      {getattr(stats, 'gepado_errors', 0):>6} [Error: {e}]")

            except Exception as e:
                rich_console.print(f"\nGEPADO OPERATIONS: [Error calculating totals: {e}]")

        rich_console.print("=" * separator_width)

    except Exception as e:
        # Ultimate fallback - display basic error message
        rich_console = Console()
        rich_console.print("\n" + "=" * 80)
        rich_console.print("PROCESSING SUMMARY".center(80))
        rich_console.print("=" * 80)
        rich_console.print(f"[red]Error displaying statistics:[/] {e}")
        rich_console.print("Statistics display failed - please check the data")
        rich_console.print("=" * 80)
