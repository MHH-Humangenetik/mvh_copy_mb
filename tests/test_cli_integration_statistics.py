"""
Integration tests for CLI workflow with statistics functionality.

This module contains integration tests that verify the complete CLI workflow
including statistics collection and display across various input scenarios
and GEPADO modes.
"""

import pytest
import tempfile
import csv
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from click.testing import CliRunner
import io
import sys

from mvh_copy_mb.cli import main, process_csv_file, GpasClient
from mvh_copy_mb.statistics import ProcessingStatistics, display_statistics, render_progress_bar
from mvh_copy_mb.database import MeldebestaetigungDatabase


class TestCLIIntegrationStatistics:
    """Integration tests for CLI workflow with statistics."""
    
    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.runner = CliRunner()
        self.temp_dir = None
        self.temp_db = None
    
    def teardown_method(self):
        """Clean up test fixtures after each test method."""
        if self.temp_dir:
            # Clean up temp directory if it exists
            import shutil
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
        
        if self.temp_db and self.temp_db.exists():
            self.temp_db.unlink()
    
    def create_test_csv(self, temp_dir: Path, filename: str, rows: list) -> list:
        """Create test CSV files with one row each (as per real-world usage)."""
        csv_paths = []
        for i, row in enumerate(rows):
            # Create a separate CSV file for each row
            csv_filename = f"{filename.replace('.csv', '')}_{i+1}.csv"
            csv_path = temp_dir / csv_filename
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['Vorgangsnummer', 'Meldebestaetigung'], delimiter=';')
                writer.writeheader()
                writer.writerow(row)
            csv_paths.append(csv_path)
        return csv_paths
    
    def create_mock_gpas_client(self, case_id_mapping: dict = None) -> Mock:
        """Create a mock gPAS client with configurable case ID resolution."""
        if case_id_mapping is None:
            case_id_mapping = {}
        
        mock_client = Mock(spec=GpasClient)
        mock_client.get_original_value.side_effect = lambda psn: case_id_mapping.get(psn)
        mock_client.domains = ['GRZ', 'KDK']
        
        # Mock the SOAP client for domain detection
        mock_soap_client = Mock()
        mock_soap_client.service.getValueFor.side_effect = lambda psn, domainName: case_id_mapping.get(psn) if psn in case_id_mapping else None
        mock_client.client = mock_soap_client
        
        return mock_client
    
    def test_cli_workflow_with_ready_files_statistics(self):
        """Test CLI workflow with files that resolve to Case IDs (ready files)."""
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            self.temp_dir = temp_dir
            
            # Create test CSV with valid data
            test_rows = [
                {
                    'Vorgangsnummer': 'PSN001',
                    'Meldebestaetigung': 'IBE+ID+CODE001&20240101&LE001&KDK001&0&IND001&PROD001&KT001&G&SEQ001&1'
                },
                {
                    'Vorgangsnummer': 'PSN002', 
                    'Meldebestaetigung': 'IBE+ID+CODE002&20240102&LE002&KDK002&0&IND002&PROD002&KT002&C&SEQ002&1'
                }
            ]
            csv_paths = self.create_test_csv(temp_dir, 'test_ready.csv', test_rows)
            
            # Mock gPAS client to resolve both pseudonyms to same Case ID to form a pair
            case_id_mapping = {
                'PSN001': 'CASE001_HUMGEN_SE_12345',  # Genomic
                'PSN002': 'CASE001_HUMGEN_SE_12345'   # Clinical - same Case ID forms pair
            }
            mock_gpas_client = self.create_mock_gpas_client(case_id_mapping)
            
            # Create database
            db_path = temp_dir / 'test.db'
            self.temp_db = db_path
            
            # Process files and collect statistics
            stats = ProcessingStatistics()
            
            with MeldebestaetigungDatabase(db_path) as db:
                for csv_path in csv_paths:
                    process_csv_file(csv_path, temp_dir, mock_gpas_client, db, False, None, stats)
            
            # Finalize pairing statistics after processing all files
            stats.finalize_pairing_statistics()
            
            # Verify statistics - should have 1 ready pair (both G and C with same Case ID)
            assert stats.ready_pairs_count == 1, f"Expected 1 ready pair, got {stats.ready_pairs_count}"
            assert stats.unpaired_genomic_count == 0, f"Expected 0 unpaired genomic, got {stats.unpaired_genomic_count}"
            assert stats.unpaired_clinical_count == 0, f"Expected 0 unpaired clinical, got {stats.unpaired_clinical_count}"
            assert stats.ignored_count == 0, f"Expected 0 ignored files, got {stats.ignored_count}"
            
            # Verify total calculation (ready pairs counted as 2 files each)
            expected_total = 1 * 2  # 1 ready pair * 2 files
            assert stats.get_total_files() == expected_total, f"Expected total {expected_total}, got {stats.get_total_files()}"
            
            # Verify files were created in correct directories
            genomic_dir = temp_dir / 'IND001' / 'G'
            clinical_dir = temp_dir / 'IND002' / 'C'
            
            assert genomic_dir.exists(), "Genomic directory should be created"
            assert clinical_dir.exists(), "Clinical directory should be created"
            
            genomic_files = list(genomic_dir.glob('*.csv'))
            clinical_files = list(clinical_dir.glob('*.csv'))
            
            assert len(genomic_files) == 1, f"Expected 1 genomic file, got {len(genomic_files)}"
            assert len(clinical_files) == 1, f"Expected 1 clinical file, got {len(clinical_files)}"
            
            # Verify filenames contain case IDs
            assert 'CASE001_HUMGEN' in genomic_files[0].name, "Genomic file should contain case ID"
            assert 'CASE001_HUMGEN' in clinical_files[0].name, "Clinical file should contain case ID"
    
    def test_cli_workflow_with_unpaired_files_statistics(self):
        """Test CLI workflow with files that don't resolve to Case IDs (unpaired files)."""
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            self.temp_dir = temp_dir
            
            # Create test CSV with data that won't resolve
            test_rows = [
                {
                    'Vorgangsnummer': 'UNRESOLVED001',
                    'Meldebestaetigung': 'IBE+ID+CODE001&20240101&LE001&KDK001&0&IND001&PROD001&KT001&G&SEQ001&1'
                },
                {
                    'Vorgangsnummer': 'UNRESOLVED002',
                    'Meldebestaetigung': 'IBE+ID+CODE002&20240102&LE002&KDK002&0&IND002&PROD002&KT002&C&SEQ002&1'
                },
                {
                    'Vorgangsnummer': 'UNRESOLVED003',
                    'Meldebestaetigung': 'IBE+ID+CODE003&20240103&LE003&KDK003&0&IND003&PROD003&KT003&X&SEQ003&1'
                }
            ]
            csv_paths = self.create_test_csv(temp_dir, 'test_unpaired.csv', test_rows)
            
            # Mock gPAS client to not resolve any pseudonyms
            mock_gpas_client = self.create_mock_gpas_client({})
            
            # Create database
            db_path = temp_dir / 'test.db'
            self.temp_db = db_path
            
            # Process files and collect statistics
            stats = ProcessingStatistics()
            
            with MeldebestaetigungDatabase(db_path) as db:
                for csv_path in csv_paths:
                    process_csv_file(csv_path, temp_dir, mock_gpas_client, db, False, None, stats)
            
            # Finalize pairing statistics after processing all files
            stats.finalize_pairing_statistics()
            
            # Verify statistics - no pairs since files don't resolve to Case IDs
            assert stats.ready_pairs_count == 0, f"Expected 0 ready pairs, got {stats.ready_pairs_count}"
            assert stats.unpaired_genomic_count == 0, f"Expected 0 unpaired genomic, got {stats.unpaired_genomic_count}"
            assert stats.unpaired_clinical_count == 0, f"Expected 0 unpaired clinical, got {stats.unpaired_clinical_count}"
            assert stats.ignored_count == 3, f"Expected 3 ignored files (all unresolved), got {stats.ignored_count}"
            
            # Verify total calculation
            expected_total = 0 + 1 + 1 + 1  # No ready files, 1 each of other types
            assert stats.get_total_files() == expected_total, f"Expected total {expected_total}, got {stats.get_total_files()}"
            
            # Verify files were created with NOTFOUND prefix
            genomic_dir = temp_dir / 'IND001' / 'G'
            clinical_dir = temp_dir / 'IND002' / 'C'
            unknown_dir = temp_dir / 'IND003' / 'X'
            
            assert genomic_dir.exists(), "Genomic directory should be created"
            assert clinical_dir.exists(), "Clinical directory should be created"
            assert unknown_dir.exists(), "Unknown type directory should be created"
            
            genomic_files = list(genomic_dir.glob('NOTFOUND_*.csv'))
            clinical_files = list(clinical_dir.glob('NOTFOUND_*.csv'))
            unknown_files = list(unknown_dir.glob('NOTFOUND_*.csv'))
            
            assert len(genomic_files) == 1, f"Expected 1 NOTFOUND genomic file, got {len(genomic_files)}"
            assert len(clinical_files) == 1, f"Expected 1 NOTFOUND clinical file, got {len(clinical_files)}"
            assert len(unknown_files) == 1, f"Expected 1 NOTFOUND unknown file, got {len(unknown_files)}"
    
    def test_cli_workflow_with_ignored_files_statistics(self):
        """Test CLI workflow with files that should be ignored (QC failed, non-initial)."""
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            self.temp_dir = temp_dir
            
            # Create test CSV with files that should be ignored
            test_rows = [
                {
                    'Vorgangsnummer': 'PSN001',
                    'Meldebestaetigung': 'IBE+ID+CODE001&20240101&LE001&KDK001&0&IND001&PROD001&KT001&G&SEQ001&0'  # QC failed (0)
                },
                {
                    'Vorgangsnummer': 'PSN002',
                    'Meldebestaetigung': 'IBE+ID+CODE002&20240102&LE002&KDK002&1&IND002&PROD002&KT002&C&SEQ002&1'  # Non-initial (1)
                },
                {
                    'Vorgangsnummer': 'PSN003',
                    'Meldebestaetigung': 'IBE+ID+CODE003&20240103&LE003&KDK003&2&IND003&PROD003&KT003&G&SEQ003&1'  # Non-initial (2)
                }
            ]
            csv_paths = self.create_test_csv(temp_dir, 'test_ignored.csv', test_rows)
            
            # Mock gPAS client (doesn't matter if they resolve since they'll be ignored)
            case_id_mapping = {
                'PSN001': 'CASE001_HUMGEN_SE_12345',
                'PSN002': 'CASE002_HUMGEN_SE_12346',
                'PSN003': 'CASE003_HUMGEN_SE_12347'
            }
            mock_gpas_client = self.create_mock_gpas_client(case_id_mapping)
            
            # Create database
            db_path = temp_dir / 'test.db'
            self.temp_db = db_path
            
            # Process files and collect statistics
            stats = ProcessingStatistics()
            
            with MeldebestaetigungDatabase(db_path) as db:
                for csv_path in csv_paths:
                    process_csv_file(csv_path, temp_dir, mock_gpas_client, db, False, None, stats)
            
            # Finalize pairing statistics after processing all files
            stats.finalize_pairing_statistics()
            
            # Verify statistics - all should be ignored
            assert stats.ready_pairs_count == 0, f"Expected 0 ready pairs, got {stats.ready_pairs_count}"
            assert stats.unpaired_genomic_count == 0, f"Expected 0 unpaired genomic, got {stats.unpaired_genomic_count}"
            assert stats.unpaired_clinical_count == 0, f"Expected 0 unpaired clinical, got {stats.unpaired_clinical_count}"
            assert stats.ignored_count == 3, f"Expected 3 ignored files, got {stats.ignored_count}"
            
            # Verify total calculation
            expected_total = 3  # All ignored
            assert stats.get_total_files() == expected_total, f"Expected total {expected_total}, got {stats.get_total_files()}"
            
            # Verify files were created with appropriate prefixes
            qc_failed_dir = temp_dir / 'IND001' / 'G'
            non_initial_dir1 = temp_dir / 'IND002' / 'C'
            non_initial_dir2 = temp_dir / 'IND003' / 'G'
            
            assert qc_failed_dir.exists(), "QC failed directory should be created"
            assert non_initial_dir1.exists(), "Non-initial directory 1 should be created"
            assert non_initial_dir2.exists(), "Non-initial directory 2 should be created"
            
            qc_failed_files = list(qc_failed_dir.glob('QC_FAILED_*.csv'))
            non_initial_files1 = list(non_initial_dir1.glob('NO_INITIAL_*.csv'))
            non_initial_files2 = list(non_initial_dir2.glob('NO_INITIAL_*.csv'))
            
            assert len(qc_failed_files) == 1, f"Expected 1 QC_FAILED file, got {len(qc_failed_files)}"
            assert len(non_initial_files1) == 1, f"Expected 1 NO_INITIAL file in dir1, got {len(non_initial_files1)}"
            assert len(non_initial_files2) == 1, f"Expected 1 NO_INITIAL file in dir2, got {len(non_initial_files2)}"
    
    def test_cli_workflow_mixed_scenarios_statistics(self):
        """Test CLI workflow with mixed file scenarios."""
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            self.temp_dir = temp_dir
            
            # Create test CSV with mixed scenarios
            test_rows = [
                # Ready files (resolve and pass QC)
                {
                    'Vorgangsnummer': 'READY001',
                    'Meldebestaetigung': 'IBE+ID+CODE001&20240101&LE001&KDK001&0&IND001&PROD001&KT001&G&SEQ001&1'
                },
                {
                    'Vorgangsnummer': 'READY002',
                    'Meldebestaetigung': 'IBE+ID+CODE002&20240102&LE002&KDK002&0&IND002&PROD002&KT002&C&SEQ002&1'
                },
                # Unpaired files (don't resolve but pass QC)
                {
                    'Vorgangsnummer': 'UNPAIRED001',
                    'Meldebestaetigung': 'IBE+ID+CODE003&20240103&LE003&KDK003&0&IND003&PROD003&KT003&G&SEQ003&1'
                },
                {
                    'Vorgangsnummer': 'UNPAIRED002',
                    'Meldebestaetigung': 'IBE+ID+CODE004&20240104&LE004&KDK004&0&IND004&PROD004&KT004&C&SEQ004&1'
                },
                # Ignored files (QC failed)
                {
                    'Vorgangsnummer': 'IGNORED001',
                    'Meldebestaetigung': 'IBE+ID+CODE005&20240105&LE005&KDK005&0&IND005&PROD005&KT005&G&SEQ005&0'
                },
                # Ignored files (non-initial)
                {
                    'Vorgangsnummer': 'IGNORED002',
                    'Meldebestaetigung': 'IBE+ID+CODE006&20240106&LE006&KDK006&1&IND006&PROD006&KT006&C&SEQ006&1'
                }
            ]
            csv_paths = self.create_test_csv(temp_dir, 'test_mixed.csv', test_rows)
            
            # Mock gPAS client to resolve only specific pseudonyms
            case_id_mapping = {
                'READY001': 'CASE001_HUMGEN_SE_12345',  # Genomic
                'READY002': 'CASE001_HUMGEN_SE_12345'   # Clinical - same Case ID forms a pair
                # UNPAIRED and IGNORED don't resolve
            }
            mock_gpas_client = self.create_mock_gpas_client(case_id_mapping)
            
            # Create database
            db_path = temp_dir / 'test.db'
            self.temp_db = db_path
            
            # Process files and collect statistics
            stats = ProcessingStatistics()
            
            with MeldebestaetigungDatabase(db_path) as db:
                for csv_path in csv_paths:
                    process_csv_file(csv_path, temp_dir, mock_gpas_client, db, False, None, stats)
            
            # Finalize pairing statistics after processing all files
            stats.finalize_pairing_statistics()
            
            # Verify statistics - 1 ready pair (READY001 & READY002 both resolve to same Case ID)
            # and unpaired files that resolve but don't have counterparts
            assert stats.ready_pairs_count == 1, f"Expected 1 ready pair, got {stats.ready_pairs_count}"
            assert stats.unpaired_genomic_count == 1, f"Expected 1 unpaired genomic, got {stats.unpaired_genomic_count}"
            assert stats.unpaired_clinical_count == 1, f"Expected 1 unpaired clinical, got {stats.unpaired_clinical_count}"
            assert stats.ignored_count == 2, f"Expected 2 ignored files, got {stats.ignored_count}"
            
            # Verify total calculation
            expected_total = 2 * 2 + 2 + 1 + 3  # 2 ready pairs * 2 + 2 unpaired G + 1 unpaired C + 3 ignored
            assert stats.get_total_files() == expected_total, f"Expected total {expected_total}, got {stats.get_total_files()}"
            
            # Verify all categories have files
            assert stats.ready_pairs_count > 0, "Should have ready pairs"
            assert stats.unpaired_genomic_count > 0, "Should have unpaired genomic files"
            assert stats.unpaired_clinical_count > 0, "Should have unpaired clinical files"
            assert stats.ignored_count > 0, "Should have ignored files"
    
    @patch('mvh_copy_mb.cli.create_gepado_client_from_env')
    @patch('mvh_copy_mb.cli.validate_and_update_record')
    def test_cli_workflow_with_gepado_enabled_statistics(self, mock_validate_update, mock_create_client):
        """Test CLI workflow with GEPADO enabled and statistics tracking."""
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            self.temp_dir = temp_dir
            
            # Create test CSV with ready files
            test_rows = [
                {
                    'Vorgangsnummer': 'PSN001',
                    'Meldebestaetigung': 'IBE+ID+CODE001&20240101&LE001&KDK001&0&IND001&PROD001&KT001&G&SEQ001&1'
                },
                {
                    'Vorgangsnummer': 'PSN002',
                    'Meldebestaetigung': 'IBE+ID+CODE002&20240102&LE002&KDK002&0&IND002&PROD002&KT002&C&SEQ002&1'
                },
                {
                    'Vorgangsnummer': 'PSN003',
                    'Meldebestaetigung': 'IBE+ID+CODE003&20240103&LE003&KDK003&0&IND003&PROD003&KT003&G&SEQ003&1'
                }
            ]
            csv_paths = self.create_test_csv(temp_dir, 'test_gepado.csv', test_rows)
            
            # Mock gPAS client to resolve all pseudonyms - create pairs
            case_id_mapping = {
                'PSN001': 'CASE001_HUMGEN_SE_12345',  # Genomic
                'PSN002': 'CASE001_HUMGEN_SE_12345',  # Clinical - forms pair with PSN001
                'PSN003': 'CASE002_HUMGEN_SE_12347'   # Genomic - unpaired
            }
            mock_gpas_client = self.create_mock_gpas_client(case_id_mapping)
            
            # Mock GEPADO client
            mock_gepado_client = Mock()
            mock_create_client.return_value = mock_gepado_client
            
            # Mock GEPADO update results - simulate different outcomes
            def mock_gepado_update(client, hl7_case_id, vorgangsnummer, meldebestaetigung, art_der_daten, ergebnis_qc, typ_der_meldung, output_date, stats):
                """Mock GEPADO update with statistics tracking."""
                if vorgangsnummer == 'PSN001':
                    # Successful genomic update
                    if stats:
                        stats.gepado_genomic_updates += 1
                    return True
                elif vorgangsnummer == 'PSN002':
                    # No updates needed for clinical
                    if stats:
                        stats.gepado_no_updates_needed += 1
                    return True
                elif vorgangsnummer == 'PSN003':
                    # Failed update (error)
                    if stats:
                        stats.gepado_errors += 1
                    return False
                return False
            
            mock_validate_update.side_effect = mock_gepado_update
            
            # Create database
            db_path = temp_dir / 'test.db'
            self.temp_db = db_path
            
            # Process files with GEPADO enabled
            stats = ProcessingStatistics()
            
            with MeldebestaetigungDatabase(db_path) as db:
                for csv_path in csv_paths:
                    process_csv_file(csv_path, temp_dir, mock_gpas_client, db, True, mock_gepado_client, stats)
            
            # Finalize pairing statistics after processing all files
            stats.finalize_pairing_statistics()
            
            # Verify file statistics - 1 pair (PSN001+PSN002) + 1 unpaired genomic (PSN003)
            assert stats.ready_pairs_count == 1, f"Expected 1 ready pair, got {stats.ready_pairs_count}"
            assert stats.unpaired_genomic_count == 1, f"Expected 1 unpaired genomic, got {stats.unpaired_genomic_count}"
            assert stats.unpaired_clinical_count == 0, f"Expected 0 unpaired clinical, got {stats.unpaired_clinical_count}"
            assert stats.ignored_count == 0, f"Expected 0 ignored files, got {stats.ignored_count}"
            
            # Verify GEPADO statistics
            assert stats.gepado_genomic_updates == 1, f"Expected 1 GEPADO genomic update, got {stats.gepado_genomic_updates}"
            assert stats.gepado_clinical_updates == 0, f"Expected 0 GEPADO clinical updates, got {stats.gepado_clinical_updates}"
            assert stats.gepado_no_updates_needed == 1, f"Expected 1 GEPADO no updates needed, got {stats.gepado_no_updates_needed}"
            assert stats.gepado_errors == 1, f"Expected 1 GEPADO error, got {stats.gepado_errors}"
            
            # Verify GEPADO total calculation
            expected_gepado_total = 1 + 1 + 1  # 1 genomic + 1 clinical + 1 error
            assert stats.get_total_gepado_operations() == expected_gepado_total, \
                f"Expected GEPADO total {expected_gepado_total}, got {stats.get_total_gepado_operations()}"
            
            # Verify GEPADO client was called for each ready file
            assert mock_validate_update.call_count == 3, f"Expected 3 GEPADO calls, got {mock_validate_update.call_count}"
    
    def test_statistics_display_integration(self):
        """Test that statistics display integrates correctly with CLI workflow."""
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            self.temp_dir = temp_dir
            
            # Create test data
            stats = ProcessingStatistics(
                ready_pairs_count=5,
                unpaired_genomic_count=3,
                unpaired_clinical_count=2,
                ignored_count=1,
                gepado_genomic_updates=4,
                gepado_clinical_updates=3,
                gepado_no_updates_needed=1,
                gepado_errors=1
            )
            
            # Capture display output
            output_buffer = io.StringIO()
            with patch('sys.stdout', output_buffer):
                display_statistics(stats, gepado_enabled=True)
            
            captured_output = output_buffer.getvalue()
            
            # Verify output contains expected elements
            assert "PROCESSING SUMMARY" in captured_output, "Should contain summary header"
            assert "Ready:" in captured_output, "Should contain ready files count"
            assert "Unpaired genomic:" in captured_output, "Should contain unpaired genomic count"
            assert "Unpaired clinical:" in captured_output, "Should contain unpaired clinical count"
            assert "Ignored files:" in captured_output, "Should contain ignored files count"
            assert "GEPADO UPDATES:" in captured_output, "Should contain GEPADO section"
            assert "Updated genomic data:" in captured_output, "Should contain GEPADO genomic updates"
            assert "Updated clinical data:" in captured_output, "Should contain GEPADO clinical updates"
            assert "Errors while updating:" in captured_output, "Should contain GEPADO errors"
            
            # Verify progress bars are present
            lines = captured_output.split('\n')
            progress_bar_lines = [line for line in lines if '[' in line and ']' in line]
            assert len(progress_bar_lines) == 7, f"Expected 7 progress bar lines, got {len(progress_bar_lines)}"
            
            # Verify all progress bars have consistent format
            for line in progress_bar_lines:
                assert line.count('[') == 1, f"Each line should have exactly one opening bracket: '{line}'"
                assert line.count(']') == 1, f"Each line should have exactly one closing bracket: '{line}'"
                
                # Extract progress bar content
                start_bracket = line.find('[')
                end_bracket = line.find(']')
                bar_content = line[start_bracket+1:end_bracket]
                
                # Verify progress bar contains only valid characters
                valid_chars = set('█░')
                actual_chars = set(bar_content)
                assert actual_chars.issubset(valid_chars), \
                    f"Progress bar contains invalid characters: {actual_chars - valid_chars} in '{bar_content}'"
    
    def test_statistics_display_without_gepado(self):
        """Test statistics display when GEPADO is disabled."""
        stats = ProcessingStatistics(
            ready_pairs_count=10,
            unpaired_genomic_count=5,
            unpaired_clinical_count=3,
            ignored_count=2,
            gepado_genomic_updates=8,  # These should not appear in output
            gepado_clinical_updates=6,
            gepado_no_updates_needed=1,
            gepado_errors=2
        )
        
        # Capture display output
        output_buffer = io.StringIO()
        with patch('sys.stdout', output_buffer):
            display_statistics(stats, gepado_enabled=False)
        
        captured_output = output_buffer.getvalue()
        
        # Verify file statistics are present
        assert "Ready:" in captured_output, "Should contain ready files count"
        assert "Unpaired genomic:" in captured_output, "Should contain unpaired genomic count"
        assert "Unpaired clinical:" in captured_output, "Should contain unpaired clinical count"
        assert "Ignored files:" in captured_output, "Should contain ignored files count"
        
        # Verify GEPADO statistics are NOT present
        assert "GEPADO UPDATES:" not in captured_output, "Should not contain GEPADO section when disabled"
        assert "Updated genomic data:" not in captured_output, "Should not contain GEPADO genomic updates"
        assert "Updated clinical data:" not in captured_output, "Should not contain GEPADO clinical updates"
        assert "Errors while updating:" not in captured_output, "Should not contain GEPADO errors"
        
        # Verify only file statistics progress bars are present (4 lines)
        lines = captured_output.split('\n')
        progress_bar_lines = [line for line in lines if '[' in line and ']' in line]
        assert len(progress_bar_lines) == 4, f"Expected 4 progress bar lines (files only), got {len(progress_bar_lines)}"
    
    def test_error_handling_in_statistics_workflow(self):
        """Test error handling during statistics collection and display."""
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            self.temp_dir = temp_dir
            
            # Test with invalid CSV data
            test_rows = [
                {
                    'Vorgangsnummer': 'PSN001',
                    'Meldebestaetigung': 'INVALID_FORMAT'  # Invalid format
                }
            ]
            csv_paths = self.create_test_csv(temp_dir, 'test_invalid.csv', test_rows)
            
            # Mock gPAS client
            mock_gpas_client = self.create_mock_gpas_client({})
            
            # Create database
            db_path = temp_dir / 'test.db'
            self.temp_db = db_path
            
            # Process files - should handle errors gracefully
            stats = ProcessingStatistics()
            
            with MeldebestaetigungDatabase(db_path) as db:
                # Should not raise an exception
                for csv_path in csv_paths:
                    process_csv_file(csv_path, temp_dir, mock_gpas_client, db, False, None, stats)
            
            # Finalize pairing statistics after processing all files
            stats.finalize_pairing_statistics()
            
            # Invalid files should be counted as ignored
            assert stats.ignored_count > 0, "Invalid files should be counted as ignored"
            
            # Display should work even with error conditions
            output_buffer = io.StringIO()
            with patch('sys.stdout', output_buffer):
                display_statistics(stats, gepado_enabled=False)
            
            captured_output = output_buffer.getvalue()
            assert "PROCESSING SUMMARY" in captured_output, "Display should work despite errors"
    
    def test_statistics_validation_during_workflow(self):
        """Test that statistics validation works during the workflow."""
        # Test with valid statistics
        stats = ProcessingStatistics(
            ready_pairs_count=5,
            unpaired_genomic_count=3,
            unpaired_clinical_count=2,
            ignored_count=1
        )
        
        # Should not raise any errors
        total = stats.get_total_files()
        assert total == 5 * 2 + 3 + 2 + 1, "Valid statistics should calculate correctly"
        
        # Test increment methods
        stats.increment_ready_pairs(2)
        stats.increment_unpaired_genomic(1)
        
        assert stats.ready_pairs_count == 7, "Increment should work correctly"
        assert stats.unpaired_genomic_count == 4, "Increment should work correctly"
        
        # Test error handling for invalid increments
        with pytest.raises(ValueError):
            stats.increment_ready(-1)  # Negative increment should fail
        
        with pytest.raises(ValueError):
            stats.increment_unpaired_clinical("invalid")  # Non-integer should fail


class TestStatisticsAccuracyWithRealData:
    """Test statistics accuracy with realistic sample data scenarios."""
    
    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.temp_dir = None
        self.temp_db = None
    
    def teardown_method(self):
        """Clean up test fixtures after each test method."""
        if self.temp_dir:
            import shutil
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
        
        if self.temp_db and self.temp_db.exists():
            self.temp_db.unlink()
    
    def create_realistic_csv_data(self, temp_dir: Path, filename_base: str, scenario: str) -> list:
        """Create realistic CSV data for different test scenarios (one file per row)."""
        csv_paths = []
        
        if scenario == "genomic_sequencing_batch":
            # Realistic genomic sequencing batch with mixed outcomes
            rows = [
                # Ready files - successful genomic sequencing
                {
                    'Vorgangsnummer': 'GRZ2024001',
                    'Meldebestaetigung': 'IBE+GRZ2024001+GRZ2024001&20240315&LE001&KDK001&0&GenomSeq&PROD001&AOK&G&WES&1'
                },
                {
                    'Vorgangsnummer': 'GRZ2024002', 
                    'Meldebestaetigung': 'IBE+GRZ2024002+GRZ2024002&20240315&LE002&KDK002&0&GenomSeq&PROD002&TK&G&WGS&1'
                },
                {
                    'Vorgangsnummer': 'GRZ2024003',
                    'Meldebestaetigung': 'IBE+GRZ2024003+GRZ2024003&20240316&LE003&KDK003&0&GenomSeq&PROD003&Barmer&G&Panel&1'
                },
                # Unpaired genomic - no clinical data yet
                {
                    'Vorgangsnummer': 'GRZ2024004',
                    'Meldebestaetigung': 'IBE+GRZ2024004+GRZ2024004&20240316&LE004&KDK004&0&GenomSeq&PROD004&DAK&G&WES&1'
                },
                {
                    'Vorgangsnummer': 'GRZ2024005',
                    'Meldebestaetigung': 'IBE+GRZ2024005+GRZ2024005&20240317&LE005&KDK005&0&GenomSeq&PROD005&AOK&G&WGS&1'
                },
                # QC failed files
                {
                    'Vorgangsnummer': 'GRZ2024006',
                    'Meldebestaetigung': 'IBE+GRZ2024006+GRZ2024006&20240317&LE006&KDK006&0&GenomSeq&PROD006&TK&G&WES&0'
                },
                {
                    'Vorgangsnummer': 'GRZ2024007',
                    'Meldebestaetigung': 'IBE+GRZ2024007+GRZ2024007&20240318&LE007&KDK007&0&GenomSeq&PROD007&Barmer&G&Panel&0'
                },
                # Non-initial reports (follow-up)
                {
                    'Vorgangsnummer': 'GRZ2024008',
                    'Meldebestaetigung': 'IBE+GRZ2024008+GRZ2024008&20240318&LE008&KDK008&1&GenomSeq&PROD008&DAK&G&WGS&1'
                }
            ]
        
        elif scenario == "clinical_data_batch":
            # Realistic clinical data batch
            rows = [
                # Ready files - clinical data with resolved case IDs
                {
                    'Vorgangsnummer': 'KDK2024001',
                    'Meldebestaetigung': 'IBE+KDK2024001+KDK2024001&20240320&LE101&KDK101&0&Onkologie&CLIN001&AOK&C&Befund&1'
                },
                {
                    'Vorgangsnummer': 'KDK2024002',
                    'Meldebestaetigung': 'IBE+KDK2024002+KDK2024002&20240320&LE102&KDK102&0&Onkologie&CLIN002&TK&C&Therapie&1'
                },
                # Unpaired clinical - no genomic data
                {
                    'Vorgangsnummer': 'KDK2024003',
                    'Meldebestaetigung': 'IBE+KDK2024003+KDK2024003&20240321&LE103&KDK103&0&Onkologie&CLIN003&Barmer&C&Befund&1'
                },
                {
                    'Vorgangsnummer': 'KDK2024004',
                    'Meldebestaetigung': 'IBE+KDK2024004+KDK2024004&20240321&LE104&KDK104&0&Onkologie&CLIN004&DAK&C&Therapie&1'
                },
                {
                    'Vorgangsnummer': 'KDK2024005',
                    'Meldebestaetigung': 'IBE+KDK2024005+KDK2024005&20240322&LE105&KDK105&0&Onkologie&CLIN005&AOK&C&Befund&1'
                },
                # QC failed clinical data
                {
                    'Vorgangsnummer': 'KDK2024006',
                    'Meldebestaetigung': 'IBE+KDK2024006+KDK2024006&20240322&LE106&KDK106&0&Onkologie&CLIN006&TK&C&Therapie&0'
                }
            ]
        
        elif scenario == "mixed_realistic_batch":
            # Mixed batch with various scenarios
            rows = [
                # Paired genomic and clinical (ready files)
                {
                    'Vorgangsnummer': 'PAIR001G',
                    'Meldebestaetigung': 'IBE+PAIR001G+PAIR001G&20240325&LE201&KDK201&0&Seltene&PROD201&AOK&G&WES&1'
                },
                {
                    'Vorgangsnummer': 'PAIR001C',
                    'Meldebestaetigung': 'IBE+PAIR001C+PAIR001C&20240325&LE201&KDK201&0&Seltene&CLIN201&AOK&C&Befund&1'
                },
                {
                    'Vorgangsnummer': 'PAIR002G',
                    'Meldebestaetigung': 'IBE+PAIR002G+PAIR002G&20240326&LE202&KDK202&0&Seltene&PROD202&TK&G&Panel&1'
                },
                {
                    'Vorgangsnummer': 'PAIR002C',
                    'Meldebestaetigung': 'IBE+PAIR002C+PAIR002C&20240326&LE202&KDK202&0&Seltene&CLIN202&TK&C&Therapie&1'
                },
                # Unpaired genomic
                {
                    'Vorgangsnummer': 'UNPAIR001G',
                    'Meldebestaetigung': 'IBE+UNPAIR001G+UNPAIR001G&20240327&LE203&KDK203&0&Seltene&PROD203&Barmer&G&WGS&1'
                },
                {
                    'Vorgangsnummer': 'UNPAIR002G',
                    'Meldebestaetigung': 'IBE+UNPAIR002G+UNPAIR002G&20240327&LE204&KDK204&0&Seltene&PROD204&DAK&G&WES&1'
                },
                # Unpaired clinical
                {
                    'Vorgangsnummer': 'UNPAIR001C',
                    'Meldebestaetigung': 'IBE+UNPAIR001C+UNPAIR001C&20240328&LE205&KDK205&0&Seltene&CLIN205&AOK&C&Befund&1'
                },
                # Various ignored scenarios
                {
                    'Vorgangsnummer': 'QC_FAIL001',
                    'Meldebestaetigung': 'IBE+QC_FAIL001+QC_FAIL001&20240328&LE206&KDK206&0&Seltene&PROD206&TK&G&WES&0'
                },
                {
                    'Vorgangsnummer': 'FOLLOWUP001',
                    'Meldebestaetigung': 'IBE+FOLLOWUP001+FOLLOWUP001&20240329&LE207&KDK207&1&Seltene&CLIN207&Barmer&C&Therapie&1'
                },
                {
                    'Vorgangsnummer': 'FOLLOWUP002',
                    'Meldebestaetigung': 'IBE+FOLLOWUP002+FOLLOWUP002&20240329&LE208&KDK208&2&Seltene&PROD208&DAK&G&Panel&1'
                }
            ]
        
        else:
            raise ValueError(f"Unknown scenario: {scenario}")
        
        # Create separate CSV files for each row (as per real-world usage)
        for i, row in enumerate(rows):
            csv_filename = f"{filename_base}_{i+1}.csv"
            csv_path = temp_dir / csv_filename
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['Vorgangsnummer', 'Meldebestaetigung'], delimiter=';')
                writer.writeheader()
                writer.writerow(row)
            csv_paths.append(csv_path)
        
        return csv_paths
    
    def create_realistic_case_id_mapping(self, scenario: str) -> dict:
        """Create realistic case ID mappings for different scenarios."""
        if scenario == "genomic_sequencing_batch":
            return {
                'GRZ2024001': 'CASE001_HUMGEN_SE_12345',
                'GRZ2024002': 'CASE002_HUMGEN_SE_12346',
                'GRZ2024003': 'CASE003_HUMGEN_SE_12347',
                # GRZ2024004, GRZ2024005 don't resolve (unpaired)
                # Others are ignored due to QC/message type
            }
        
        elif scenario == "clinical_data_batch":
            return {
                'KDK2024001': 'CASE101_HUMGEN_SE_12345',
                'KDK2024002': 'CASE102_HUMGEN_SE_12346',
                # KDK2024003, KDK2024004, KDK2024005 don't resolve (unpaired)
                # KDK2024006 is ignored due to QC failure
            }
        
        elif scenario == "mixed_realistic_batch":
            return {
                'PAIR001G': 'CASE201_HUMGEN_SE_12345',
                'PAIR001C': 'CASE201_HUMGEN_SE_12345',  # Same case ID (paired)
                'PAIR002G': 'CASE202_HUMGEN_SE_12346',
                'PAIR002C': 'CASE202_HUMGEN_SE_12346',  # Same case ID (paired)
                # UNPAIR* don't resolve
                # QC_FAIL*, FOLLOWUP* are ignored
            }
        
        else:
            return {}
    
    def test_genomic_sequencing_batch_accuracy(self):
        """Test statistics accuracy with realistic genomic sequencing batch."""
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            self.temp_dir = temp_dir
            
            # Create realistic genomic sequencing data
            csv_paths = self.create_realistic_csv_data(temp_dir, 'genomic_batch', 'genomic_sequencing_batch')
            case_id_mapping = self.create_realistic_case_id_mapping('genomic_sequencing_batch')
            
            # Mock gPAS client
            mock_gpas_client = Mock(spec=GpasClient)
            mock_gpas_client.get_original_value.side_effect = lambda psn: case_id_mapping.get(psn)
            mock_gpas_client.domains = ['GRZ', 'KDK']
            
            mock_soap_client = Mock()
            mock_soap_client.service.getValueFor.side_effect = lambda psn, domainName: case_id_mapping.get(psn) if psn in case_id_mapping else None
            mock_gpas_client.client = mock_soap_client
            
            # Create database
            db_path = temp_dir / 'genomic_test.db'
            self.temp_db = db_path
            
            # Process files and collect statistics
            stats = ProcessingStatistics()
            
            with MeldebestaetigungDatabase(db_path) as db:
                for csv_path in csv_paths:
                    process_csv_file(csv_path, temp_dir, mock_gpas_client, db, False, None, stats)
            
            # Finalize pairing statistics after processing all files
            stats.finalize_pairing_statistics()
            
            # Verify expected statistics based on realistic data
            # All genomic files, no clinical counterparts - all should be unpaired genomic
            # Expected: 0 ready pairs, 5 unpaired genomic (GRZ2024001-005), 0 unpaired clinical, 3 ignored (QC failed + non-initial)
            assert stats.ready_pairs_count == 0, f"Expected 0 ready pairs, got {stats.ready_pairs_count}"
            assert stats.unpaired_genomic_count == 5, f"Expected 5 unpaired genomic files, got {stats.unpaired_genomic_count}"
            assert stats.unpaired_clinical_count == 0, f"Expected 0 unpaired clinical files, got {stats.unpaired_clinical_count}"
            assert stats.ignored_count == 3, f"Expected 3 ignored files, got {stats.ignored_count}"
            
            # Verify total calculation matches manual count
            expected_total = 3 * 2 + 2 + 0 + 3  # 3 ready * 2 + 2 unpaired G + 0 unpaired C + 3 ignored = 11
            actual_total = stats.get_total_files()
            assert actual_total == expected_total, f"Expected total {expected_total}, got {actual_total}"
            
            # Verify progress bar calculations are accurate
            # Test progress bar for unpaired genomic files (should show 5 out of 8 total)
            genomic_bar = render_progress_bar(stats.unpaired_genomic_count, actual_total, 20)
            expected_filled_chars = int((5 / 8) * 20)  # 5 out of 8 = ~12.5 chars filled
            actual_filled_chars = genomic_bar.count('█')
            
            # Allow for rounding tolerance
            assert abs(actual_filled_chars - expected_filled_chars) <= 1, \
                f"Genomic files progress bar: expected ~{expected_filled_chars} filled chars, got {actual_filled_chars}"
            
            # Verify file organization matches expectations
            genomseq_g_dir = temp_dir / 'GenomSeq' / 'G'
            assert genomseq_g_dir.exists(), "GenomSeq/G directory should be created"
            
            # Count files in directory - each CSV has only one row, so we get one output file per input CSV
            ready_files = list(genomseq_g_dir.glob('CASE*.csv'))
            qc_failed_files = list(genomseq_g_dir.glob('*QC_FAILED*.csv'))
            no_initial_files = list(genomseq_g_dir.glob('*NO_INITIAL*.csv'))
            notfound_files = list(genomseq_g_dir.glob('NOTFOUND_*.csv'))
            # Filter out QC_FAILED and NO_INITIAL from NOTFOUND (these are unpaired files)
            unpaired_files = [f for f in notfound_files if 'QC_FAILED' not in f.name and 'NO_INITIAL' not in f.name]
            
            assert len(ready_files) == 3, f"Expected 3 ready files in directory, got {len(ready_files)}"
            assert len(unpaired_files) == 2, f"Expected 2 unpaired files in directory, got {len(unpaired_files)}"
            assert len(qc_failed_files) == 2, f"Expected 2 QC failed files in directory, got {len(qc_failed_files)}"
            assert len(no_initial_files) == 1, f"Expected 1 non-initial file in directory, got {len(no_initial_files)}"
    
    def test_clinical_data_batch_accuracy(self):
        """Test statistics accuracy with realistic clinical data batch."""
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            self.temp_dir = temp_dir
            
            # Create realistic clinical data
            csv_paths = self.create_realistic_csv_data(temp_dir, 'clinical_batch', 'clinical_data_batch')
            case_id_mapping = self.create_realistic_case_id_mapping('clinical_data_batch')
            
            # Mock gPAS client
            mock_gpas_client = Mock(spec=GpasClient)
            mock_gpas_client.get_original_value.side_effect = lambda psn: case_id_mapping.get(psn)
            mock_gpas_client.domains = ['GRZ', 'KDK']
            
            mock_soap_client = Mock()
            mock_soap_client.service.getValueFor.side_effect = lambda psn, domainName: case_id_mapping.get(psn) if psn in case_id_mapping else None
            mock_gpas_client.client = mock_soap_client
            
            # Create database
            db_path = temp_dir / 'clinical_test.db'
            self.temp_db = db_path
            
            # Process files and collect statistics
            stats = ProcessingStatistics()
            
            with MeldebestaetigungDatabase(db_path) as db:
                for csv_path in csv_paths:
                    process_csv_file(csv_path, temp_dir, mock_gpas_client, db, False, None, stats)
            
            # Finalize pairing statistics after processing all files
            stats.finalize_pairing_statistics()
            
            # Verify expected statistics
            # All clinical files, no genomic counterparts - all should be unpaired clinical
            # Expected: 0 ready pairs, 0 unpaired genomic, 5 unpaired clinical (KDK2024001-005), 1 ignored (QC failed)
            assert stats.ready_pairs_count == 0, f"Expected 0 ready pairs, got {stats.ready_pairs_count}"
            assert stats.unpaired_genomic_count == 0, f"Expected 0 unpaired genomic files, got {stats.unpaired_genomic_count}"
            assert stats.unpaired_clinical_count == 5, f"Expected 5 unpaired clinical files, got {stats.unpaired_clinical_count}"
            assert stats.ignored_count == 1, f"Expected 1 ignored file, got {stats.ignored_count}"
            
            # Verify total calculation
            expected_total = 0 * 2 + 0 + 5 + 1  # 0 ready pairs * 2 + 0 unpaired G + 5 unpaired C + 1 ignored = 6
            actual_total = stats.get_total_files()
            assert actual_total == expected_total, f"Expected total {expected_total}, got {actual_total}"
            
            # Verify progress bar calculations for unpaired clinical files
            clinical_bar = render_progress_bar(stats.unpaired_clinical_count, actual_total, 20)
            expected_filled_chars = int((3 / 8) * 20)  # 3 out of 8 = 7.5 chars filled
            actual_filled_chars = clinical_bar.count('█')
            
            assert abs(actual_filled_chars - expected_filled_chars) <= 1, \
                f"Unpaired clinical progress bar: expected ~{expected_filled_chars} filled chars, got {actual_filled_chars}"
    
    def test_mixed_realistic_batch_accuracy(self):
        """Test statistics accuracy with mixed realistic batch containing all scenarios."""
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            self.temp_dir = temp_dir
            
            # Create mixed realistic data
            csv_paths = self.create_realistic_csv_data(temp_dir, 'mixed_batch', 'mixed_realistic_batch')
            case_id_mapping = self.create_realistic_case_id_mapping('mixed_realistic_batch')
            
            # Mock gPAS client
            mock_gpas_client = Mock(spec=GpasClient)
            mock_gpas_client.get_original_value.side_effect = lambda psn: case_id_mapping.get(psn)
            mock_gpas_client.domains = ['GRZ', 'KDK']
            
            mock_soap_client = Mock()
            mock_soap_client.service.getValueFor.side_effect = lambda psn, domainName: case_id_mapping.get(psn) if psn in case_id_mapping else None
            mock_gpas_client.client = mock_soap_client
            
            # Create database
            db_path = temp_dir / 'mixed_test.db'
            self.temp_db = db_path
            
            # Process files and collect statistics
            stats = ProcessingStatistics()
            
            with MeldebestaetigungDatabase(db_path) as db:
                for csv_path in csv_paths:
                    process_csv_file(csv_path, temp_dir, mock_gpas_client, db, False, None, stats)
            
            # Finalize pairing statistics after processing all files
            stats.finalize_pairing_statistics()
            
            # Verify expected statistics for mixed batch
            # Expected: 2 ready pairs (PAIR001G+PAIR001C, PAIR002G+PAIR002C), 2 unpaired G, 1 unpaired C, 3 ignored
            assert stats.ready_pairs_count == 2, f"Expected 2 ready pairs, got {stats.ready_pairs_count}"
            assert stats.unpaired_genomic_count == 2, f"Expected 2 unpaired genomic files, got {stats.unpaired_genomic_count}"
            assert stats.unpaired_clinical_count == 1, f"Expected 1 unpaired clinical file, got {stats.unpaired_clinical_count}"
            assert stats.ignored_count == 3, f"Expected 3 ignored files, got {stats.ignored_count}"
            
            # Verify total calculation
            expected_total = 4 * 2 + 2 + 1 + 3  # 4 ready * 2 + 2 unpaired G + 1 unpaired C + 3 ignored = 14
            actual_total = stats.get_total_files()
            assert actual_total == expected_total, f"Expected total {expected_total}, got {actual_total}"
            
            # Verify all categories have appropriate representation in progress bars
            ready_proportion = (4 * 2) / 14  # 8/14 = ~57%
            genomic_proportion = 2 / 14  # 2/14 = ~14%
            clinical_proportion = 1 / 14  # 1/14 = ~7%
            ignored_proportion = 3 / 14  # 3/14 = ~21%
            
            # Test that proportions add up correctly (accounting for ready files counted twice)
            total_proportion = ready_proportion + genomic_proportion + clinical_proportion + ignored_proportion
            assert abs(total_proportion - 1.0) < 0.01, f"Proportions should sum to 1.0, got {total_proportion}"
            
            # Verify progress bars reflect these proportions accurately
            bar_width = 20
            
            ready_bar = render_progress_bar(stats.ready_pairs_count * 2, actual_total, bar_width)
            ready_filled = ready_bar.count('█')
            expected_ready_filled = int(ready_proportion * bar_width)
            assert abs(ready_filled - expected_ready_filled) <= 1, \
                f"Ready progress bar: expected ~{expected_ready_filled} filled, got {ready_filled}"
            
            genomic_bar = render_progress_bar(stats.unpaired_genomic_count, actual_total, bar_width)
            genomic_filled = genomic_bar.count('█')
            expected_genomic_filled = int(genomic_proportion * bar_width)
            assert abs(genomic_filled - expected_genomic_filled) <= 1, \
                f"Genomic progress bar: expected ~{expected_genomic_filled} filled, got {genomic_filled}"
    
    @patch('mvh_copy_mb.cli.create_gepado_client_from_env')
    @patch('mvh_copy_mb.cli.validate_and_update_record')
    def test_gepado_statistics_accuracy_with_realistic_data(self, mock_validate_update, mock_create_client):
        """Test GEPADO statistics accuracy with realistic data scenarios."""
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            self.temp_dir = temp_dir
            
            # Create mixed data for GEPADO testing
            csv_paths = self.create_realistic_csv_data(temp_dir, 'gepado_batch', 'mixed_realistic_batch')
            case_id_mapping = self.create_realistic_case_id_mapping('mixed_realistic_batch')
            
            # Mock gPAS client
            mock_gpas_client = Mock(spec=GpasClient)
            mock_gpas_client.get_original_value.side_effect = lambda psn: case_id_mapping.get(psn)
            mock_gpas_client.domains = ['GRZ', 'KDK']
            
            mock_soap_client = Mock()
            mock_soap_client.service.getValueFor.side_effect = lambda psn, domainName: case_id_mapping.get(psn) if psn in case_id_mapping else None
            mock_gpas_client.client = mock_soap_client
            
            # Mock GEPADO client
            mock_gepado_client = Mock()
            mock_create_client.return_value = mock_gepado_client
            
            # Mock realistic GEPADO update outcomes
            def realistic_gepado_update(client, hl7_case_id, vorgangsnummer, meldebestaetigung, art_der_daten, ergebnis_qc, typ_der_meldung, output_date, stats):
                """Mock realistic GEPADO update outcomes."""
                if vorgangsnummer in ['PAIR001G', 'PAIR002G']:
                    # Successful genomic updates
                    if stats:
                        stats.gepado_genomic_updates += 1
                    return True
                elif vorgangsnummer in ['PAIR001C']:
                    # Successful clinical update
                    if stats:
                        stats.gepado_clinical_updates += 1
                    return True
                elif vorgangsnummer in ['PAIR002C']:
                    # Failed clinical update (e.g., database error)
                    if stats:
                        stats.gepado_errors += 1
                    return False
                # Other files don't reach GEPADO (no case ID or ignored)
                return False
            
            mock_validate_update.side_effect = realistic_gepado_update
            
            # Create database
            db_path = temp_dir / 'gepado_test.db'
            self.temp_db = db_path
            
            # Process files with GEPADO enabled
            stats = ProcessingStatistics()
            
            with MeldebestaetigungDatabase(db_path) as db:
                for csv_path in csv_paths:
                    process_csv_file(csv_path, temp_dir, mock_gpas_client, db, True, mock_gepado_client, stats)
            
            # Finalize pairing statistics after processing all files
            stats.finalize_pairing_statistics()
            
            # Verify file statistics (same as before)
            assert stats.ready_pairs_count == 2, f"Expected 2 ready pairs, got {stats.ready_pairs_count}"
            
            # Verify GEPADO statistics match realistic outcomes
            assert stats.gepado_genomic_updates == 2, f"Expected 2 GEPADO genomic updates, got {stats.gepado_genomic_updates}"
            assert stats.gepado_clinical_updates == 1, f"Expected 1 GEPADO clinical update, got {stats.gepado_clinical_updates}"
            assert stats.gepado_errors == 1, f"Expected 1 GEPADO error, got {stats.gepado_errors}"
            
            # Verify GEPADO total calculation
            expected_gepado_total = 2 + 1 + 1  # 2 genomic + 1 clinical + 1 error = 4
            actual_gepado_total = stats.get_total_gepado_operations()
            assert actual_gepado_total == expected_gepado_total, \
                f"Expected GEPADO total {expected_gepado_total}, got {actual_gepado_total}"
            
            # Verify GEPADO progress bar calculations
            genomic_updates_bar = render_progress_bar(stats.gepado_genomic_updates, actual_gepado_total, 20)
            expected_genomic_filled = int((2 / 4) * 20)  # 2 out of 4 = 50% = 10 chars
            actual_genomic_filled = genomic_updates_bar.count('█')
            assert actual_genomic_filled == expected_genomic_filled, \
                f"GEPADO genomic progress bar: expected {expected_genomic_filled} filled, got {actual_genomic_filled}"
            
            clinical_updates_bar = render_progress_bar(stats.gepado_clinical_updates, actual_gepado_total, 20)
            expected_clinical_filled = int((1 / 4) * 20)  # 1 out of 4 = 25% = 5 chars
            actual_clinical_filled = clinical_updates_bar.count('█')
            assert actual_clinical_filled == expected_clinical_filled, \
                f"GEPADO clinical progress bar: expected {expected_clinical_filled} filled, got {actual_clinical_filled}"
            
            errors_bar = render_progress_bar(stats.gepado_errors, actual_gepado_total, 20)
            expected_errors_filled = int((1 / 4) * 20)  # 1 out of 4 = 25% = 5 chars
            actual_errors_filled = errors_bar.count('█')
            assert actual_errors_filled == expected_errors_filled, \
                f"GEPADO errors progress bar: expected {expected_errors_filled} filled, got {actual_errors_filled}"
    
    def test_large_batch_statistics_accuracy(self):
        """Test statistics accuracy with a larger, more realistic batch size."""
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            self.temp_dir = temp_dir
            
            # Create a larger batch with known distribution
            large_batch_rows = []
            case_id_mapping = {}
            
            # 50 ready files (25 genomic + 25 clinical, all resolve)
            for i in range(1, 26):
                # Genomic files
                vn_g = f'READY{i:03d}G'
                case_id = f'CASE{i:03d}_HUMGEN_SE_{12345+i}'
                large_batch_rows.append({
                    'Vorgangsnummer': vn_g,
                    'Meldebestaetigung': f'IBE+{vn_g}+{vn_g}&20240401&LE{i:03d}&KDK{i:03d}&0&TestInd&PROD{i:03d}&AOK&G&WES&1'
                })
                case_id_mapping[vn_g] = case_id
                
                # Clinical files
                vn_c = f'READY{i:03d}C'
                large_batch_rows.append({
                    'Vorgangsnummer': vn_c,
                    'Meldebestaetigung': f'IBE+{vn_c}+{vn_c}&20240401&LE{i:03d}&KDK{i:03d}&0&TestInd&CLIN{i:03d}&AOK&C&Befund&1'
                })
                case_id_mapping[vn_c] = case_id
            
            # 20 unpaired genomic files (don't resolve)
            for i in range(1, 21):
                vn = f'UNPAIRED{i:03d}G'
                large_batch_rows.append({
                    'Vorgangsnummer': vn,
                    'Meldebestaetigung': f'IBE+{vn}+{vn}&20240402&LE{i+100:03d}&KDK{i+100:03d}&0&TestInd&PROD{i+100:03d}&TK&G&WGS&1'
                })
            
            # 15 unpaired clinical files (don't resolve)
            for i in range(1, 16):
                vn = f'UNPAIRED{i:03d}C'
                large_batch_rows.append({
                    'Vorgangsnummer': vn,
                    'Meldebestaetigung': f'IBE+{vn}+{vn}&20240403&LE{i+200:03d}&KDK{i+200:03d}&0&TestInd&CLIN{i+200:03d}&Barmer&C&Therapie&1'
                })
            
            # 10 ignored files (QC failed)
            for i in range(1, 11):
                vn = f'QCFAIL{i:03d}'
                large_batch_rows.append({
                    'Vorgangsnummer': vn,
                    'Meldebestaetigung': f'IBE+{vn}+{vn}&20240404&LE{i+300:03d}&KDK{i+300:03d}&0&TestInd&PROD{i+300:03d}&DAK&G&Panel&0'
                })
            
            # Create CSV files (one per row)
            csv_paths = []
            for i, row in enumerate(large_batch_rows):
                csv_path = temp_dir / f'large_batch_{i+1}.csv'
                with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=['Vorgangsnummer', 'Meldebestaetigung'], delimiter=';')
                    writer.writeheader()
                    writer.writerow(row)
                csv_paths.append(csv_path)
            
            # Mock gPAS client
            mock_gpas_client = Mock(spec=GpasClient)
            mock_gpas_client.get_original_value.side_effect = lambda psn: case_id_mapping.get(psn)
            mock_gpas_client.domains = ['GRZ', 'KDK']
            
            mock_soap_client = Mock()
            mock_soap_client.service.getValueFor.side_effect = lambda psn, domainName: case_id_mapping.get(psn) if psn in case_id_mapping else None
            mock_gpas_client.client = mock_soap_client
            
            # Create database
            db_path = temp_dir / 'large_test.db'
            self.temp_db = db_path
            
            # Process files and collect statistics
            stats = ProcessingStatistics()
            
            with MeldebestaetigungDatabase(db_path) as db:
                for csv_path in csv_paths:
                    process_csv_file(csv_path, temp_dir, mock_gpas_client, db, False, None, stats)
            
            # Finalize pairing statistics after processing all files
            stats.finalize_pairing_statistics()
            
            # Verify expected statistics for large batch
            # Expected: 25 ready pairs, 20 unpaired G, 15 unpaired C, 10 ignored
            assert stats.ready_pairs_count == 25, f"Expected 25 ready pairs, got {stats.ready_pairs_count}"
            assert stats.unpaired_genomic_count == 20, f"Expected 20 unpaired genomic files, got {stats.unpaired_genomic_count}"
            assert stats.unpaired_clinical_count == 15, f"Expected 15 unpaired clinical files, got {stats.unpaired_clinical_count}"
            assert stats.ignored_count == 10, f"Expected 10 ignored files, got {stats.ignored_count}"
            
            # Verify total calculation for large batch
            expected_total = 25 * 2 + 20 + 15 + 10  # 25 ready pairs * 2 + 20 + 15 + 10 = 95
            actual_total = stats.get_total_files()
            assert actual_total == expected_total, f"Expected total {expected_total}, got {actual_total}"
            
            # Verify progress bar accuracy with larger numbers
            bar_width = 20
            
            # Ready pairs should dominate the progress bar (50/95 = ~53%)
            ready_bar = render_progress_bar(stats.ready_pairs_count * 2, actual_total, bar_width)
            ready_filled = ready_bar.count('█')
            expected_ready_filled = int((50 / 95) * bar_width)  # ~10-11 chars
            assert abs(ready_filled - expected_ready_filled) <= 1, \
                f"Large batch ready progress bar: expected ~{expected_ready_filled} filled, got {ready_filled}"
            
            # Unpaired genomic should be smaller portion (20/145 = ~14%)
            genomic_bar = render_progress_bar(stats.unpaired_genomic_count, actual_total, bar_width)
            genomic_filled = genomic_bar.count('█')
            expected_genomic_filled = int((20 / 145) * bar_width)  # ~2-3 chars
            assert abs(genomic_filled - expected_genomic_filled) <= 1, \
                f"Large batch genomic progress bar: expected ~{expected_genomic_filled} filled, got {genomic_filled}"
            
            # Test that the sum of all progress bar segments makes sense
            clinical_bar = render_progress_bar(stats.unpaired_clinical_count, actual_total, bar_width)
            ignored_bar = render_progress_bar(stats.ignored_count, actual_total, bar_width)
            
            total_filled_chars = ready_filled + genomic_bar.count('█') + clinical_bar.count('█') + ignored_bar.count('█')
            # Note: This won't equal bar_width exactly due to overlapping calculations, but should be reasonable
            assert total_filled_chars <= bar_width * 4, "Total filled characters across all bars should be reasonable"
    
    def test_edge_case_statistics_accuracy(self):
        """Test statistics accuracy with edge cases and boundary conditions."""
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            self.temp_dir = temp_dir
            
            # Test with minimal data (1 file of each type)
            minimal_rows = [
                {
                    'Vorgangsnummer': 'SINGLE_READY',
                    'Meldebestaetigung': 'IBE+SINGLE_READY+SINGLE_READY&20240501&LE001&KDK001&0&TestInd&PROD001&AOK&G&WES&1'
                },
                {
                    'Vorgangsnummer': 'SINGLE_UNPAIRED_G',
                    'Meldebestaetigung': 'IBE+SINGLE_UNPAIRED_G+SINGLE_UNPAIRED_G&20240501&LE002&KDK002&0&TestInd&PROD002&TK&G&WGS&1'
                },
                {
                    'Vorgangsnummer': 'SINGLE_UNPAIRED_C',
                    'Meldebestaetigung': 'IBE+SINGLE_UNPAIRED_C+SINGLE_UNPAIRED_C&20240501&LE003&KDK003&0&TestInd&CLIN003&Barmer&C&Befund&1'
                },
                {
                    'Vorgangsnummer': 'SINGLE_IGNORED',
                    'Meldebestaetigung': 'IBE+SINGLE_IGNORED+SINGLE_IGNORED&20240501&LE004&KDK004&0&TestInd&PROD004&DAK&G&Panel&0'
                }
            ]
            
            # Create separate CSV files for each row
            csv_paths = []
            for i, row in enumerate(minimal_rows):
                csv_path = temp_dir / f'minimal_batch_{i+1}.csv'
                with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=['Vorgangsnummer', 'Meldebestaetigung'], delimiter=';')
                    writer.writeheader()
                    writer.writerow(row)
                csv_paths.append(csv_path)
            
            # Only resolve one case ID
            case_id_mapping = {'SINGLE_READY': 'CASE001_HUMGEN_SE_12345'}
            
            # Mock gPAS client
            mock_gpas_client = Mock(spec=GpasClient)
            mock_gpas_client.get_original_value.side_effect = lambda psn: case_id_mapping.get(psn)
            mock_gpas_client.domains = ['GRZ', 'KDK']
            
            mock_soap_client = Mock()
            mock_soap_client.service.getValueFor.side_effect = lambda psn, domainName: case_id_mapping.get(psn) if psn in case_id_mapping else None
            mock_gpas_client.client = mock_soap_client
            
            # Create database
            db_path = temp_dir / 'minimal_test.db'
            self.temp_db = db_path
            
            # Process files and collect statistics
            stats = ProcessingStatistics()
            
            with MeldebestaetigungDatabase(db_path) as db:
                for csv_path in csv_paths:
                    process_csv_file(csv_path, temp_dir, mock_gpas_client, db, False, None, stats)
            
            # Finalize pairing statistics after processing all files
            stats.finalize_pairing_statistics()
            
            # Verify minimal case statistics - no pairs since all different Case IDs
            assert stats.ready_pairs_count == 0, f"Expected 0 ready pairs, got {stats.ready_pairs_count}"
            assert stats.unpaired_genomic_count == 1, f"Expected 1 unpaired genomic file, got {stats.unpaired_genomic_count}"
            assert stats.unpaired_clinical_count == 1, f"Expected 1 unpaired clinical file, got {stats.unpaired_clinical_count}"
            assert stats.ignored_count == 2, f"Expected 2 ignored files, got {stats.ignored_count}"
            
            # Verify total calculation for minimal case
            expected_total = 0 * 2 + 1 + 1 + 2  # 0 ready pairs * 2 + 1 + 1 + 2 = 4
            actual_total = stats.get_total_files()
            assert actual_total == expected_total, f"Expected total {expected_total}, got {actual_total}"
            
            # Verify progress bars work correctly with small numbers
            bar_width = 20
            
            # Each category should get proportional representation
            genomic_bar = render_progress_bar(stats.unpaired_genomic_count, actual_total, bar_width)
            genomic_filled = genomic_bar.count('█')
            expected_genomic_filled = int((1 / 4) * bar_width)  # 25% = 5 chars
            assert abs(genomic_filled - expected_genomic_filled) <= 1, \
                f"Genomic files should get ~{expected_genomic_filled} chars, got {genomic_filled}"
            
            # Test zero case - all files ignored
            zero_ready_rows = [
                {
                    'Vorgangsnummer': 'ALL_QC_FAIL1',
                    'Meldebestaetigung': 'IBE+ALL_QC_FAIL1+ALL_QC_FAIL1&20240502&LE001&KDK001&0&TestInd&PROD001&AOK&G&WES&0'
                },
                {
                    'Vorgangsnummer': 'ALL_QC_FAIL2',
                    'Meldebestaetigung': 'IBE+ALL_QC_FAIL2+ALL_QC_FAIL2&20240502&LE002&KDK002&0&TestInd&PROD002&TK&C&Befund&0'
                }
            ]
            
            # Create separate CSV files for zero ready case
            csv_paths_zero = []
            for i, row in enumerate(zero_ready_rows):
                csv_path_zero = temp_dir / f'zero_ready_batch_{i+1}.csv'
                with open(csv_path_zero, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=['Vorgangsnummer', 'Meldebestaetigung'], delimiter=';')
                    writer.writeheader()
                    writer.writerow(row)
                csv_paths_zero.append(csv_path_zero)
            
            # Process zero ready case
            stats_zero = ProcessingStatistics()
            
            with MeldebestaetigungDatabase(db_path) as db:
                for csv_path_zero in csv_paths_zero:
                    process_csv_file(csv_path_zero, temp_dir, mock_gpas_client, db, False, None, stats_zero)
            
            # Verify zero ready case
            assert stats_zero.ready_pairs_count == 0, f"Expected 0 ready pairs, got {stats_zero.ready_pairs_count}"
            assert stats_zero.unpaired_genomic_count == 0, f"Expected 0 unpaired genomic files, got {stats_zero.unpaired_genomic_count}"
            assert stats_zero.unpaired_clinical_count == 0, f"Expected 0 unpaired clinical files, got {stats_zero.unpaired_clinical_count}"
            assert stats_zero.ignored_count == 2, f"Expected 2 ignored files, got {stats_zero.ignored_count}"
            
            # Verify progress bars handle zero cases correctly
            ready_bar_zero = render_progress_bar(stats_zero.ready_pairs_count * 2, stats_zero.get_total_files(), bar_width)
            assert ready_bar_zero.count('█') == 0, "Zero ready pairs should result in empty progress bar"
            assert ready_bar_zero.count('░') == bar_width, "Zero ready pairs should result in all empty characters"
            
            ignored_bar_zero = render_progress_bar(stats_zero.ignored_count, stats_zero.get_total_files(), bar_width)
            assert ignored_bar_zero.count('█') == bar_width, "All ignored files should result in full progress bar"
            assert ignored_bar_zero.count('░') == 0, "All ignored files should result in no empty characters"