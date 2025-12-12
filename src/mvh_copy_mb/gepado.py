"""
Gepado database client module for MSSQL integration.

This module provides functionality to connect to and interact with the gepado
laboratory information system database.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
import logging
import pymssql
import os

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
    """
    hl7_case_id: str
    vng: Optional[str] = None
    vnk: Optional[str] = None
    ibe_g: Optional[str] = None
    ibe_k: Optional[str] = None


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
                server=self.host,
                database=self.database,
                user=self.username,
                password=self.password,
                timeout=30,
                login_timeout=30
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
                       MV_IBE2.value AS IBE_k
                FROM av_ordermanagement C
                LEFT JOIN av2_ordermanagement_addfields MV_VNg ON MV_VNg.masterguid = C.guid_ordermanagement AND MV_VNg.fieldname = 'MV_VNg'
                LEFT JOIN av2_ordermanagement_addfields MV_VNk ON MV_VNk.masterguid = C.guid_ordermanagement AND MV_VNk.fieldname = 'MV_VNk'
                LEFT JOIN av2_ordermanagement_addfields MV_IBE ON MV_IBE.masterguid = C.guid_ordermanagement AND MV_IBE.fieldname = 'MV_IBE'
                LEFT JOIN av2_ordermanagement_addfields MV_IBE2 ON MV_IBE2.masterguid = C.guid_ordermanagement AND MV_IBE2.fieldname = 'MV_IBE2'
                WHERE C.hl7fallid LIKE %s
            """
            
            cursor.execute(query, (hl7_case_id,))
            row = cursor.fetchone()
            
            if row:
                logger.info(f"Found gepado record for HL7 case ID: {hl7_case_id}")
                return GepadoRecord(
                    hl7_case_id=row[0],
                    vng=row[1] if row[1] else None,
                    vnk=row[2] if row[2] else None,
                    ibe_g=row[3] if row[3] else None,
                    ibe_k=row[4] if row[4] else None
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
            
            # Map field names to their corresponding fieldname values in av2_ordermanagement_addfields
            field_mapping = {
                'vng': 'MV_VNg',
                'vnk': 'MV_VNk', 
                'ibe_g': 'MV_IBE',
                'ibe_k': 'MV_IBE2'
            }
            
            for field_name, value in updates.items():
                # Validate field names to prevent SQL injection
                if field_name not in field_mapping:
                    logger.error(f"Invalid field name for update: {field_name}")
                    return False
                
                fieldname = field_mapping[field_name]
                
                # Use MERGE or INSERT/UPDATE pattern for av2_ordermanagement_addfields
                update_query = """
                    IF EXISTS (SELECT 1 FROM av2_ordermanagement_addfields 
                              WHERE masterguid = %s AND fieldname = %s)
                    BEGIN
                        UPDATE av2_ordermanagement_addfields 
                        SET value = %s 
                        WHERE masterguid = %s AND fieldname = %s
                    END
                    ELSE
                    BEGIN
                        INSERT INTO av2_ordermanagement_addfields (masterguid, fieldname, value)
                        VALUES (%s, %s, %s)
                    END
                """
                
                cursor.execute(update_query, (
                    guid_ordermanagement, fieldname,  # EXISTS check
                    value, guid_ordermanagement, fieldname,  # UPDATE
                    guid_ordermanagement, fieldname, value  # INSERT
                ))
                rows_affected += cursor.rowcount
            
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
            raise
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