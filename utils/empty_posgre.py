import os
import logging
import traceback
from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv(".env")

def force_reclaim_disk_space():
    """
    Forcefully drops and recreates schemas to return disk space to the OS.
    WARNING: Direct execution with no confirmation.
    """
    # Initialize connection
    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )

    schemas = ['iot', 'staging', 'production']
    
    logging.info("🔥 STARTING AUTOMATED DISK RECLAMATION")
    
    try:
        for schema in schemas:
            # 1. DROP CASCADE: Deletes schema, tables, and indexes physically from disk
            logging.info(f"💣 Dropping schema: {schema} (Reclaiming ~21GB total)")
            drop_query = f"DROP SCHEMA IF EXISTS {schema} CASCADE;"
            pc.execute_query(drop_query)
            
            # 2. CREATE SCHEMA: Re-initializes the empty structure
            logging.info(f"🏗️  Recreating empty schema: {schema}")
            create_query = f"CREATE SCHEMA {schema};"
            pc.execute_query(create_query)
            
            logging.info(f"✅ {schema.upper()} reset and space released.")

        logging.info("="*60)
        logging.info("🎉 SUCCESS: All data removed. Space returned to system.")
        logging.info("="*60)

    except Exception as e:
        logging.error(f"❌ Critical error during reclamation: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    # Execute immediately
    force_reclaim_disk_space()