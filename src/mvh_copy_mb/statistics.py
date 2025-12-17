"""
CLI Summary Statistics Module

This module provides functionality for tracking and displaying processing statistics
during CLI execution, including file counts, pairing status, and GEPADO update results.
"""

from dataclasses import dataclass


@dataclass
class ProcessingStatistics:
    """
    Tracks statistics during CLI processing.
    
    Attributes:
        ready_count: Files with resolved Case IDs and complete pairing
        unpaired_genomic_count: Genomic files without clinical counterparts
        unpaired_clinical_count: Clinical files without genomic counterparts
        ignored_count: Files skipped due to QC failures or errors
        gepado_genomic_updates: Successful genomic updates in GEPADO
        gepado_clinical_updates: Successful clinical updates in GEPADO
        gepado_errors: Errors during GEPADO updates
    """
    ready_count: int = 0
    unpaired_genomic_count: int = 0
    unpaired_clinical_count: int = 0
    ignored_count: int = 0
    gepado_genomic_updates: int = 0
    gepado_clinical_updates: int = 0
    gepado_errors: int = 0
    
    def get_total_files(self) -> int:
        """
        Calculate total files processed (Ready counted twice as specified).
        
        Returns:
            Total number of files processed, with ready files counted twice
        """
        return self.ready_count * 2 + self.unpaired_genomic_count + self.unpaired_clinical_count + self.ignored_count
    
    def get_total_gepado_operations(self) -> int:
        """
        Calculate total GEPADO operations attempted.
        
        Returns:
            Total number of GEPADO operations (successful + failed)
        """
        return self.gepado_genomic_updates + self.gepado_clinical_updates + self.gepado_errors


def render_progress_bar(count: int, total: int, width: int = 20) -> str:
    """
    Render a progress bar for the given count and total.
    
    Args:
        count: Current count value
        total: Maximum value for the progress bar
        width: Width of the progress bar in characters (excluding brackets)
        
    Returns:
        String representation of the progress bar with brackets
    """
    if total == 0:
        return "[" + "░" * width + "]"
    
    # Ensure count doesn't exceed total for progress calculation
    clamped_count = min(count, total)
    filled_width = int((clamped_count / total) * width)
    empty_width = width - filled_width
    
    return "[" + "█" * filled_width + "░" * empty_width + "]"


def display_statistics(stats: ProcessingStatistics, gepado_enabled: bool = False) -> None:
    """
    Display formatted statistics with progress bars.
    
    Args:
        stats: ProcessingStatistics instance with collected data
        gepado_enabled: Whether GEPADO integration was enabled
    """
    total_files = stats.get_total_files()
    
    # Calculate progress bar width to fit 80-character terminal
    # Format: "Label: count [progress_bar]"
    # Longest label is "Updated clinical data:" (21 chars) + count (6 chars) + space = 28 chars
    # Leave 5 chars margin, so progress bar can be 80 - 28 - 5 = 47 chars
    bar_width = 47
    
    print("\n" + "="*80)
    print("PROCESSING SUMMARY".center(80))
    print("="*80)
    
    # File statistics
    print(f"Ready:                  {stats.ready_count:>6} {render_progress_bar(stats.ready_count * 2, total_files, bar_width)}")
    print(f"Unpaired genomic:       {stats.unpaired_genomic_count:>6} {render_progress_bar(stats.unpaired_genomic_count, total_files, bar_width)}")
    print(f"Unpaired clinical:      {stats.unpaired_clinical_count:>6} {render_progress_bar(stats.unpaired_clinical_count, total_files, bar_width)}")
    print(f"Ignored files:          {stats.ignored_count:>6} {render_progress_bar(stats.ignored_count, total_files, bar_width)}")
    
    # GEPADO statistics (if enabled)
    if gepado_enabled:
        total_gepado = stats.get_total_gepado_operations()
        print("\nGEPADO UPDATES:")
        print(f"Updated genomic data:   {stats.gepado_genomic_updates:>6} {render_progress_bar(stats.gepado_genomic_updates, total_gepado, bar_width)}")
        print(f"Updated clinical data:  {stats.gepado_clinical_updates:>6} {render_progress_bar(stats.gepado_clinical_updates, total_gepado, bar_width)}")
        print(f"Errors while updating:  {stats.gepado_errors:>6} {render_progress_bar(stats.gepado_errors, total_gepado, bar_width)}")
    
    print("="*80)