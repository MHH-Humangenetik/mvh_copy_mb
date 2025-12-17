"""
Gepado database client module for MSSQL integration.

This module provides functionality to connect to and interact with the gepado
laboratory information system database.
"""

from dataclasses import dataclass
from datetime import date
from typing import Optional, Dict, Any, TYPE_CHECKING
import logging
import pymssql
import os

if TYPE_CHECKING:
    from .statistics import ProcessingStatistics

logger = logging.getLogger(__name__)


@dataclass
class GepadoRecord:
    """
    Represents a record from the gepado database.
    
    Attributes:
        hl7_case_id: Unique HL7 case identifier
        vng: Vorgangsnummer for genomic data type (optional)
        vnk: Vorgangsnummer for clinical data type (optional)
        ibe_g: IBE string for genomic data type (optional)
        ibe_k: IBE string for clinical data type (optional)
        mv_servicedate_k: MV_servicedate_k field for clinical data Leistungsdatum (optional)
        mv_servicedate_g: MV_servicedate_g field for genetic data Leistungsdatum (optional)
    """
    hl7_case_id: str
    vng: Optional[str] = None
    vnk: Optional[str] = None
    ibe_g: Optional[str] = None
    ibe_k: Optional[str] = None
    mv_servicedate_k: Optional[date] = None
    mv_servicedate_g: Optional[date] = None


class GepadoClient:
    """
    Client for connecting to and interacting with the gepado MSSQL database.
    
    This class manages database connections and provides methods for querying
    and updating gepado records.
    """
    
    def __init__(self, host: str, database: str, username: str, password: str):
        """
        Initialize the GepadoClient with connection parameters.
        
        Args:
            host: MSSQL server hostname
            database: Database name
            username: Authentication username
            password: Authentication password
        """
        self.host = host
        self.database = database
        self.username = username
        self.password = password
        self._connection: Optional[pymssql.Connection] = None
    
    def connect(self) -> pymssql.Connection:
        """
        Establish connection to the gepado MSSQL database.
        
        Returns:
            pymssql.Connection: Active database connection
            
        Raises:
            pymssql.Error: If connection fails
        """
        try:
            self._connection = pymssql.connect(
                host=self.host,
                database=self.database,
                user=self.username,
                password=self.password,
                timeout=30,
                login_timeout=30,
                server="."
            )
            logger.info(f"Successfully connected to gepado database at {self.host}")
            return self._connection
        except pymssql.Error as e:
            logger.error(f"Failed to connect to gepado database: {e}")
            raise
    
    def query_record(self, hl7_case_id: str) -> Optional[GepadoRecord]:
        """
        Query gepado database for a record with the given HL7 case ID.
        
        Args:
            hl7_case_id: The HL7 case ID to search for
            
        Returns:
            GepadoRecord if found, None otherwise
            
        Raises:
            pymssql.Error: If query execution fails
        """
        if not self._connection:
            self.connect()
        
        try:
            cursor = self._connection.cursor()
            
            # Use the actual gepado database query structure
            query = """
                SELECT C.hl7fallid AS CaseID_HL7,
                       MV_VNg.value AS VNg,
                       MV_VNk.value AS VNk,
                       MV_IBE.value AS IBE_g,
                       MV_IBE2.value AS IBE_k,
                       MV_servicedate_k.value AS MV_servicedate_k,
                       MV_servicedate_g.value AS MV_servicedate_g
                FROM av_ordermanagement C
                LEFT JOIN av2_ordermanagement_addfields MV_VNg ON MV_VNg.masterguid = C.guid_ordermanagement AND MV_VNg.fieldname = 'MV_VNg'
                LEFT JOIN av2_ordermanagement_addfields MV_VNk ON MV_VNk.masterguid = C.guid_ordermanagement AND MV_VNk.fieldname = 'MV_VNk'
                LEFT JOIN av2_ordermanagement_addfields MV_IBE ON MV_IBE.masterguid = C.guid_ordermanagement AND MV_IBE.fieldname = 'MV_IBE'
                LEFT JOIN av2_ordermanagement_addfields MV_IBE2 ON MV_IBE2.masterguid = C.guid_ordermanagement AND MV_IBE2.fieldname = 'MV_IBE2'
                LEFT JOIN av2_ordermanagement_addfields MV_servicedate_k ON MV_servicedate_k.masterguid = C.guid_ordermanagement AND MV_servicedate_k.fieldname = 'MV_servicedate_k'
                LEFT JOIN av2_ordermanagement_addfields MV_servicedate_g ON MV_servicedate_g.masterguid = C.guid_ordermanagement AND MV_servicedate_g.fieldname = 'MV_servicedate_g'
                WHERE C.hl7fallid LIKE %s
            """
            
            cursor.execute(query, (hl7_case_id,))
            row = cursor.fetchone()
            
            if row:
                logger.info(f"Found gepado record for HL7 case ID: {hl7_case_id}")
                # Parse MV_servicedate_k if present
                mv_servicedate_k = None
                if row[5]:
                    try:
                        # Assume the date is stored as a string in YYYY-MM-DD format
                        from datetime import datetime
                        mv_servicedate_k = datetime.strptime(row[5], '%Y-%m-%d').date()
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid MV_servicedate_k format in GEPADO: {row[5]}")
                        mv_servicedate_k = None
                
                # Parse MV_servicedate_g if present
                mv_servicedate_g = None
                if row[6]:
                    try:
                        # Assume the date is stored as a string in YYYY-MM-DD format
                        from datetime import datetime
                        mv_servicedate_g = datetime.strptime(row[6], '%Y-%m-%d').date()
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid MV_servicedate_g format in GEPADO: {row[6]}")
                        mv_servicedate_g = None
                
                return GepadoRecord(
                    hl7_case_id=row[0],
                    vng=row[1] if row[1] else None,
                    vnk=row[2] if row[2] else None,
                    ibe_g=row[3] if row[3] else None,
                    ibe_k=row[4] if row[4] else None,
                    mv_servicedate_k=mv_servicedate_k,
                    mv_servicedate_g=mv_servicedate_g
                )
            else:
                logger.warning(f"No gepado record found for HL7 case ID: {hl7_case_id}")
                return None
                
        except pymssql.Error as e:
            logger.error(f"Failed to query gepado record for HL7 case ID {hl7_case_id}: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def update_record(self, hl7_case_id: str, updates: Dict[str, str]) -> bool:
        """
        Update gepado record with new field values.
        
        Args:
            hl7_case_id: The HL7 case ID of the record to update
            updates: Dictionary of field names to new values
            
        Returns:
            True if update was successful, False otherwise
            
        Raises:
            pymssql.Error: If update execution fails
        """
        if not self._connection:
            self.connect()
        
        if not updates:
            logger.info(f"No updates provided for HL7 case ID: {hl7_case_id}")
            return True
        
        try:
            cursor = self._connection.cursor()
            
            # First, get the guid_ordermanagement for this HL7 case ID
            guid_query = """
                SELECT guid_ordermanagement 
                FROM av_ordermanagement 
                WHERE hl7fallid LIKE %s
            """
            cursor.execute(guid_query, (hl7_case_id,))
            guid_row = cursor.fetchone()
            
            if not guid_row:
                logger.warning(f"No record found for HL7 case ID: {hl7_case_id}")
                return False
            
            guid_ordermanagement = guid_row[0]
            rows_affected = 0
            
            # Map field names to their corresponding column names in the base table [transact].[t_case_addFieldsExt]
            field_mapping = {
                'vng': 'MV_VNg',
                'vnk': 'MV_VNk', 
                'ibe_g': 'MV_IBE',
                'ibe_k': 'MV_IBE2',
                'mv_servicedate_k': 'MV_servicedate_k',
                'mv_servicedate_g': 'MV_servicedate_g'
            }
            
            for field_name, value in updates.items():
                # Validate field names to prevent SQL injection
                if field_name not in field_mapping:
                    logger.error(f"Invalid field name for update: {field_name}")
                    return False
                
                column_name = field_mapping[field_name]
                
                # Update the underlying base table [transact].[t_case_addFieldsExt] directly
                # The av2_ordermanagement_addfields view is read-only due to UNION statements
                # Use MERGE pattern to handle both INSERT and UPDATE cases
                merge_query = """
                    IF EXISTS (SELECT 1 FROM [transact].[t_case_addFieldsExt] WHERE masterGuid = %s)
                    BEGIN
                        UPDATE [transact].[t_case_addFieldsExt]
                        SET {} = %s
                        WHERE masterGuid = %s
                    END
                    ELSE
                    BEGIN
                        INSERT INTO [transact].[t_case_addFieldsExt] (masterGuid, {})
                        VALUES (%s, %s)
                    END
                """.format(column_name, column_name)
                
                cursor.execute(merge_query, (
                    guid_ordermanagement,  # EXISTS check
                    value, guid_ordermanagement,  # UPDATE
                    guid_ordermanagement, value  # INSERT
                ))
                rows_affected += cursor.rowcount
                
                logger.info(f"Updated/inserted {column_name} in base table for masterGuid {guid_ordermanagement}")
            
            if rows_affected > 0:
                self._connection.commit()
                logger.info(f"Successfully updated gepado record for HL7 case ID {hl7_case_id}: {updates}")
                return True
            else:
                logger.warning(f"No rows updated for HL7 case ID: {hl7_case_id}")
                return False
                
        except pymssql.Error as e:
            logger.error(f"Failed to update gepado record for HL7 case ID {hl7_case_id}: {e}")
            if self._connection:
                self._connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
    
    def close(self):
        """Close the database connection if it exists."""
        if self._connection:
            try:
                self._connection.close()
                logger.info("Closed gepado database connection")
            except pymssql.Error as e:
                logger.error(f"Error closing gepado database connection: {e}")
            finally:
                self._connection = None


def map_data_type_to_fields(art_der_daten: str) -> tuple[str, str, str]:
    """
    Map Art der Daten value to corresponding VN, IBE, and service date field names.
    
    Args:
        art_der_daten: Type of data ('G' for genomic, 'C' for clinical)
        
    Returns:
        Tuple of (vn_field_name, ibe_field_name, servicedate_field_name) for the data type
        
    Raises:
        ValueError: If art_der_daten is not 'G' or 'C'
    """
    art_der_daten_normalized = art_der_daten.upper().strip()
    
    if art_der_daten_normalized == 'G':
        return ('vng', 'ibe_g', 'mv_servicedate_g')
    elif art_der_daten_normalized == 'C':
        return ('vnk', 'ibe_k', 'mv_servicedate_k')
    else:
        raise ValueError(f"Invalid Art der Daten value: {art_der_daten}. Must be 'G' (genomic) or 'C' (clinical)")



def compare_record_data(existing_record: GepadoRecord, vorgangsnummer: str, ibe_string: str, art_der_daten: str, output_date: Optional[date] = None) -> tuple[Dict[str, str], Dict[str, tuple[str, str]]]:
    """
    Compare existing gepado data with Meldebestätigung data and determine updates needed.
    
    Args:
        existing_record: Current gepado record
        vorgangsnummer: Vorgangsnummer from Meldebestätigung
        ibe_string: IBE string from Meldebestätigung
        art_der_daten: Type of data ('G' or 'C')
        output_date: Leistungsdatum from Meldebestätigung (optional)
        
    Returns:
        Tuple of (updates_needed, mismatches_found)
        - updates_needed: Dict of field_name -> new_value for empty fields
        - mismatches_found: Dict of field_name -> (existing_value, new_value) for conflicts
        
    Raises:
        ValueError: If art_der_daten is invalid
    """
    vn_field, ibe_field, servicedate_field = map_data_type_to_fields(art_der_daten)
    
    updates_needed = {}
    mismatches_found = {}
    
    # Get current values from the record
    current_vn = getattr(existing_record, vn_field)
    current_ibe = getattr(existing_record, ibe_field)
    
    # Check VN field - strip whitespace but preserve case
    if current_vn is None or current_vn.strip() == '':
        # Field is empty, can be updated
        updates_needed[vn_field] = vorgangsnummer
        logger.info(f"Will update empty {vn_field} field with: {vorgangsnummer}")
    elif current_vn != vorgangsnummer:
        # Field has different value, log mismatch
        mismatches_found[vn_field] = (current_vn, vorgangsnummer)
        logger.error(f"Data mismatch in {vn_field}: existing='{current_vn}', new='{vorgangsnummer}'")
    else:
        # Field matches, log successful validation
        logger.info(f"Validated {vn_field} field: {current_vn}")
    
    # Check IBE field - strip whitespace but preserve case
    if current_ibe is None or current_ibe.strip() == '':
        # Field is empty, can be updated
        updates_needed[ibe_field] = ibe_string
        logger.info(f"Will update empty {ibe_field} field with: {ibe_string}")
    elif current_ibe != ibe_string:
        # Field has different value, log mismatch
        mismatches_found[ibe_field] = (current_ibe, ibe_string)
        logger.error(f"Data mismatch in {ibe_field}: existing='{current_ibe}', new='{ibe_string}'")
    else:
        # Field matches, log successful validation
        logger.info(f"Validated {ibe_field} field: {current_ibe}")
    
    # Check appropriate service date field based on data type
    current_servicedate = getattr(existing_record, servicedate_field)
    
    if output_date is None:
        # No new output_date to compare - skip like we would with empty string input
        logger.info("No output_date provided for comparison")
    elif current_servicedate is None:
        # Field is empty, can be updated
        updates_needed[servicedate_field] = output_date.strftime('%Y-%m-%d')
        logger.info(f"Will update empty {servicedate_field} field with: {output_date}")
    elif current_servicedate != output_date:
        # Field has different value, log mismatch
        mismatches_found[servicedate_field] = (current_servicedate, output_date)
        logger.error(f"Data mismatch in {servicedate_field}: existing='{current_servicedate}', new='{output_date}'")
    else:
        # Field matches, log successful validation
        logger.info(f"Validated {servicedate_field} field: {current_servicedate}")
    
    return updates_needed, mismatches_found



def should_process_record_for_gepado(ergebnis_qc: str, typ_der_meldung: str) -> bool:
    """
    Check if a record should be processed for gepado updates based on QC and message type.
    
    Args:
        ergebnis_qc: QC result value from Meldebestätigung
        typ_der_meldung: Type of report value from Meldebestätigung
        
    Returns:
        True if record should be processed (QC=1 and Typ=0), False otherwise
    """
    should_process = ergebnis_qc == '1' and typ_der_meldung == '0'
    
    if not should_process:
        logger.warning(f"Skipping gepado processing: QC={ergebnis_qc}, Typ={typ_der_meldung} (requires QC=1, Typ=0)")
    else:
        logger.info(f"Record passes gepado processing filter: QC={ergebnis_qc}, Typ={typ_der_meldung}")
    
    return should_process


def validate_and_update_record(client: GepadoClient, hl7_case_id: str, vorgangsnummer: str, ibe_string: str, art_der_daten: str, ergebnis_qc: str, typ_der_meldung: str, output_date: Optional[date] = None, stats: Optional["ProcessingStatistics"] = None) -> bool:
    """
    Validate record processing criteria and update gepado record if appropriate.
    
    This function integrates the HL7 case ID correction system to ensure GEPADO
    operations target the correct genomic sequencing cases. The original hl7_case_id
    is preserved for logging and error reporting, while the corrected case ID is
    used for all GEPADO database operations.
    
    Args:
        client: GepadoClient instance
        hl7_case_id: Original HL7 case ID from Meldebestätigung (used for local storage/file naming)
        vorgangsnummer: Vorgangsnummer from Meldebestätigung
        ibe_string: IBE string from Meldebestätigung
        art_der_daten: Type of data ('G' or 'C')
        ergebnis_qc: QC result value
        typ_der_meldung: Type of report value
        output_date: Leistungsdatum from Meldebestätigung (optional)
        
    Returns:
        True if processing was successful, False otherwise
    """
    # Check if record should be processed
    if not should_process_record_for_gepado(ergebnis_qc, typ_der_meldung):
        logger.warning(f"Skipping gepado update for HL7 case ID {hl7_case_id}")
        # Track as error since the record didn't meet processing criteria
        if stats:
            stats.gepado_errors += 1
        return False
    
    # Validate data type
    try:
        map_data_type_to_fields(art_der_daten)
    except ValueError:
        logger.error(f"Invalid Art der Daten '{art_der_daten}' for HL7 case ID {hl7_case_id}")
        # Track as error due to invalid data type
        if stats:
            stats.gepado_errors += 1
        return False
    
    try:
        # Apply HL7 case ID correction for GEPADO operations
        # Import inside function to avoid circular import
        from .hl7_case_id_correction import correct_hl7_case_id_for_gepado
        corrected_case_id = correct_hl7_case_id_for_gepado(client, hl7_case_id)
        
        # Log when corrected case ID is being used for GEPADO operations
        if corrected_case_id != hl7_case_id:
            logger.info(f"Using corrected HL7 case ID for GEPADO operations: {hl7_case_id} -> {corrected_case_id}")
        
        # Query existing record using corrected case ID
        existing_record = client.query_record(corrected_case_id)
        if not existing_record:
            logger.warning(f"No gepado record found for corrected HL7 case ID: {corrected_case_id} (original: {hl7_case_id})")
            # Track as error since no record was found
            if stats:
                stats.gepado_errors += 1
            return False
        
        # Compare data and determine updates (including output_date)
        updates_needed, mismatches_found = compare_record_data(
            existing_record, vorgangsnummer, ibe_string, art_der_daten, output_date
        )
        
        # Log any mismatches (reference original case ID for clarity)
        if mismatches_found:
            for field, (existing, new) in mismatches_found.items():
                if corrected_case_id != hl7_case_id:
                    logger.error(f"Data mismatch detected for corrected HL7 case ID {corrected_case_id} (original: {hl7_case_id}), field {field}: existing='{existing}', new='{new}'")
                else:
                    logger.error(f"Data mismatch detected for HL7 case ID {hl7_case_id}, field {field}: existing='{existing}', new='{new}'")
        
        # Perform updates if needed using corrected case ID
        success = True
        if updates_needed:
            success = client.update_record(corrected_case_id, updates_needed)
            if success:
                # Track successful update based on data type
                if stats:
                    if art_der_daten.upper() == 'G':
                        stats.gepado_genomic_updates += 1
                    elif art_der_daten.upper() == 'C':
                        stats.gepado_clinical_updates += 1
                
                if corrected_case_id != hl7_case_id:
                    logger.info(f"Successfully updated gepado record for corrected HL7 case ID {corrected_case_id} (original: {hl7_case_id}): {updates_needed}")
                else:
                    logger.info(f"Successfully updated gepado record for HL7 case ID {hl7_case_id}: {updates_needed}")
            else:
                # Track as error since update failed
                if stats:
                    stats.gepado_errors += 1
                # Error already logged in update_record(), just return False
                return False
        else:
            # No updates needed - still count as successful operation based on data type
            if stats:
                if art_der_daten.upper() == 'G':
                    stats.gepado_genomic_updates += 1
                elif art_der_daten.upper() == 'C':
                    stats.gepado_clinical_updates += 1
            
            if corrected_case_id != hl7_case_id:
                logger.info(f"No updates needed for gepado record with corrected HL7 case ID {corrected_case_id} (original: {hl7_case_id})")
            else:
                logger.info(f"No updates needed for gepado record with HL7 case ID {hl7_case_id}")
        
        return success
            
    except Exception as e:
        logger.error(f"Error processing gepado record for HL7 case ID {hl7_case_id}: {e}")
        # Track as error due to exception
        if stats:
            stats.gepado_errors += 1
        return False


def create_gepado_client_from_env() -> Optional[GepadoClient]:
    """
    Create a GepadoClient instance using environment variables.
    
    Expected environment variables:
    - MSSQL_HOST: Database server hostname
    - MSSQL_DATABASE: Database name
    - MSSQL_USERNAME: Authentication username
    - MSSQL_PASSWORD: Authentication password
    
    Returns:
        GepadoClient instance if all required env vars are present, None otherwise
    """
    host = os.getenv('MSSQL_HOST')
    database = os.getenv('MSSQL_DATABASE')
    username = os.getenv('MSSQL_USERNAME')
    password = os.getenv('MSSQL_PASSWORD')
    
    if not all([host, database, username, password]):
        missing_vars = []
        if not host:
            missing_vars.append('MSSQL_HOST')
        if not database:
            missing_vars.append('MSSQL_DATABASE')
        if not username:
            missing_vars.append('MSSQL_USERNAME')
        if not password:
            missing_vars.append('MSSQL_PASSWORD')
        
        logger.error(f"Missing required environment variables for gepado client: {', '.join(missing_vars)}")
        return None
    
    return GepadoClient(host=host, database=database, username=username, password=password)