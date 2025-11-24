import csv
import shutil
import logging
from pathlib import Path
from typing import Optional, cast

import click
from tqdm import tqdm
from zeep import Client
from zeep.transports import Transport
from requests import Session
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)
logging.getLogger('zeep.wsdl.bindings.soap').setLevel(logging.ERROR)

class GpasClient:
    def __init__(self, endpoint: str, username: str, password: str, grz: str, kdk: str, verify_ssl: bool = True):
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.domains = [grz, kdk]
        self.verify_ssl = verify_ssl
        self.client = self._init_client()

    def _init_client(self) -> Optional[Client]:
        try:
            session = Session()
            session.auth = HTTPBasicAuth(self.username, self.password)
            session.verify = self.verify_ssl
            
            if not self.verify_ssl:
                # Suppress InsecureRequestWarning
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

            # Ensure the endpoint points to the WSDL
            wsdl_url = self.endpoint
            if not wsdl_url.endswith('?wsdl'):
                if 'gpasService' in wsdl_url:
                    wsdl_url = f"{wsdl_url}?wsdl"
                else:
                    wsdl_url = f"{wsdl_url.rstrip('/')}/gpas/gpasService?wsdl"
            
            logger.debug(f"Initializing gPAS SOAP client with WSDL: {wsdl_url}")
            client = Client(wsdl_url, transport=Transport(session=session))

            # Force the service address to match the WSDL URL (minus ?wsdl)
            # This fixes issues where the WSDL contains an internal IP or HTTP URL
            # or if the service is behind a proxy/gateway that the WSDL doesn't know about.
            service_url = wsdl_url.replace('?wsdl', '')
            if client.service:
                 client.service._binding_options['address'] = service_url
                 logger.debug(f"Forced service address to: {service_url}")

            return client
        except Exception as e:
            logger.error(f"Failed to initialize gPAS SOAP client: {e}")
            return None

    def get_original_value(self, pseudonym: str) -> Optional[str]:
        """
        Queries gPAS to resolve a pseudonym to its original value.
        Searches in both configured domains.
        """
        if not self.client:
            logger.error("gPAS client not initialized.")
            return None

        for domain in self.domains:
            logger.info(f"Looking up pseudonym '{pseudonym}' in domain '{domain}'")
            try:
                # Use getValueFor as specified in the gPAS manual (Section 7.3)
                # Arguments: psn (the pseudonym), domainName (the domain)
                response = self.client.service.getValueFor(psn=pseudonym, domainName=domain)
                logger.info(f"Response from '{domain}': {response}")
                
                if response:
                    # Zeep might return the value directly or an object
                    if hasattr(response, 'value'):
                        return response.value
                    return response
            
            except Exception as e:
                # Log warning to see why it fails
                logger.warning(f"Failed to resolve in domain '{domain}': {e}")

        return None

def parse_meldebestaetigung(mb_string: str) -> dict:
    """
    Parses the Meldebestaetigung string to extract metadata.
    Format: IBE+ID+HashString+...
    HashString: Code&Date&LE_ID&KDK_ID&Typ&Indikation&Produkt&Kostentraeger&ArtDaten&ArtSeq&QC
    """
    try:
        # Split by '+' to get the Hash-String (index 2)
        # Example: IBE+A123456789+A123456789&...
        parts = mb_string.split('+')
        if len(parts) < 3:
            logger.warning(f"Invalid Meldebestaetigung format (not enough '+' segments): {mb_string}")
            return {}
        
        hash_string = parts[2]
        
        # Split Hash-String by '&'
        # Indices:
        # 4: Typ der Meldung
        # 5: Indikationsbereich
        # 8: Art der Daten
        # 10: Ergebnis QC
        hash_parts = hash_string.split('&')
        if len(hash_parts) < 11:
            logger.warning(f"Invalid Hash-String format (not enough '&' segments): {hash_string}")
            return {}

        return {
            'Typ der Meldung': hash_parts[4],
            'Indikationsbereich': hash_parts[5],
            'Art der Daten': hash_parts[8],
            'Ergebnis QC': hash_parts[10]
        }
    except Exception as e:
        logger.error(f"Error parsing Meldebestaetigung '{mb_string}': {e}")
        return {}

def process_row(row: dict, source_file: Path, root_dir: Path, gpas_client: GpasClient):
    try:
        # Extract Vorgangsnummer directly
        vorgangsnummer = row.get('Vorgangsnummer')
        meldebestaetigung = row.get('Meldebestaetigung')

        if not vorgangsnummer or not meldebestaetigung:
            logger.warning(f"Missing Vorgangsnummer or Meldebestaetigung in row: {row}")
            return

        # Parse Meldebestaetigung for other fields
        mb_data = parse_meldebestaetigung(meldebestaetigung)
        
        indikationsbereich = mb_data.get('Indikationsbereich')
        art_der_daten = mb_data.get('Art der Daten')
        typ_der_meldung = mb_data.get('Typ der Meldung')
        ergebnis_qc = mb_data.get('Ergebnis QC')

        if not all([indikationsbereich, art_der_daten, typ_der_meldung, ergebnis_qc]):
            logger.warning(f"Could not extract all required fields from Meldebestaetigung: {meldebestaetigung}")
            return

        # Cast to string to satisfy type checker
        vorgangsnummer_str = cast(str, vorgangsnummer)
        indikationsbereich_str = cast(str, indikationsbereich)
        art_der_daten_str = cast(str, art_der_daten)

        # Create folder structure
        target_dir = root_dir / indikationsbereich_str / art_der_daten_str
        target_dir.mkdir(parents=True, exist_ok=True)

        # Determine filename prefix
        # Typ der Meldung: 0 = Erstmeldung
        # Ergebnis QC: 1 = bestanden
        prefix = ""
        if ergebnis_qc != "1":
            prefix = "QC_FAILED_"
        elif typ_der_meldung != "0":
            prefix = "NO_INITIAL_"

        # Resolve Case ID from gPAS
        case_id = gpas_client.get_original_value(vorgangsnummer_str)
        
        if case_id:
            # Requirement: "Files should be named by their case id then."
            # We copy the source CSV file and rename it to {prefix}{case_id}.csv
            new_filename = f"{prefix}{case_id}.csv"
            target_path = target_dir / new_filename
            
            shutil.copy2(source_file, target_path)
            logger.info(f"Copied {source_file.name} to {target_path}")
            
        else:
            # Fallback: Prepend "NOTFOUND_" to the original filename and copy
            new_filename = f"NOTFOUND_{prefix}{source_file.name}"
            target_path = target_dir / new_filename
            
            shutil.copy2(source_file, target_path)
            logger.warning(f"Could not resolve Case ID for Vorgangsnummer {vorgangsnummer}. Copied to {target_path}")

    except Exception as e:
        logger.error(f"Error processing row: {e}")

def process_csv_file(file_path: Path, root_dir: Path, gpas_client: GpasClient):
    try:
        # Detect delimiter - assuming ';' for German CSVs usually, but let's try to be robust
        with open(file_path, 'r', encoding='utf-8') as f:
            # Read a sample to sniff dialect
            sample = f.read(1024)
            f.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample)
            except csv.Error:
                # Fallback to semicolon if sniffing fails (common in German CSVs)
                dialect = csv.excel()
                dialect.delimiter = ';'
            
            reader = csv.DictReader(f, dialect=dialect)
            
            for row in reader:
                process_row(row, file_path, root_dir, gpas_client)
                
    except Exception as e:
        logger.error(f"Failed to process file {file_path}: {e}")

@click.command()
@click.option('--input-dir', envvar='INPUT_DIR', type=click.Path(exists=True, file_okay=False), required=True, help='Directory containing .csv files')
@click.option('--gpas-endpoint', envvar='GPAS_ENDPOINT', required=True, help='gPAS API Endpoint')
@click.option('--gpas-user', envvar='GPAS_USER', required=True, help='gPAS Username')
@click.option('--gpas-password', envvar='GPAS_PASSWORD', required=True, help='gPAS Password')
@click.option('--gpas-grz', envvar='GPAS_GRZ', required=True, help='First gPAS Domain')
@click.option('--gpas-kdk', envvar='GPAS_KDK', required=True, help='Second gPAS Domain')
@click.option('--gpas-verify-ssl', envvar='GPAS_VERIFY_SSL', type=bool, default=True, show_default=True, help='Verify SSL certificate')
@click.option('--log-level', envvar='LOG_LEVEL', default='INFO', show_default=True, help='Logging level')
@click.option('--log-file', envvar='LOG_FILE', default='mvh_copy_mb.log', show_default=True, help='Log file path')
@click.option('--archive-dir', envvar='ARCHIVE_DIR', type=click.Path(file_okay=False), help='Directory to move processed files to')
def main(input_dir, gpas_endpoint, gpas_user, gpas_password, gpas_grz, gpas_kdk, gpas_verify_ssl, log_level, log_file, archive_dir):
    """
    Process MVH Meldebestaetigung CSV files and organize them based on metadata, resolving pseudonyms via gPAS.
    """
    logging.basicConfig(
        filename=log_file,
        filemode='w',
        encoding='utf-8',
        level=log_level.upper(),
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    input_path = Path(input_dir)
    gpas_client = GpasClient(gpas_endpoint, gpas_user, gpas_password, gpas_grz, gpas_kdk, gpas_verify_ssl)

    if archive_dir:
        try:
            Path(archive_dir).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.error(f"Failed to create archive directory {archive_dir}: {e}")
            raise click.ClickException(f"Failed to create archive directory {archive_dir}: {e}")

    csv_files = list(input_path.glob('*.csv'))
    for csv_file in tqdm(csv_files, desc="Processing CSV files", unit="file", ncols=80):
        logger.info(f"Processing file: {csv_file.name}")
        process_csv_file(csv_file, input_path, gpas_client)

        if archive_dir:
            try:
                shutil.move(str(csv_file), archive_dir)
                logger.info(f"Moved {csv_file.name} to {archive_dir}")
            except Exception as e:
                logger.error(f"Failed to move {csv_file.name} to {archive_dir}: {e}")
                raise click.ClickException(f"Failed to move {csv_file.name} to {archive_dir}: {e}")

if __name__ == '__main__':
    main()
