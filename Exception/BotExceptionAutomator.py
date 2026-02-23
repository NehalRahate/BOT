"""
Bot Exception Handler - Automated Workflow (With Config File Support)
Monitors bot exceptions and retries failed inventory items every 10 minutes
"""

import psycopg2
from psycopg2 import sql
import time
from datetime import datetime
import logging
from typing import Set, List
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot_exception_handler.log'),
        logging.StreamHandler()
    ]
)

class BotExceptionHandler:
    def __init__(self, db_config):
        """
        Initialize the handler with database configuration
        
        Args:
            db_config (dict): Database connection parameters
        """
        self.db_config = db_config
        self.processed_inventory_ids: Set[str] = set()
        self.current_date = datetime.now().date()
        
    def get_connection(self):
        """Create and return a database connection"""
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise
    
    def check_date_change(self):
        """Check if date has changed and reset processed IDs if needed"""
        new_date = datetime.now().date()
        if new_date != self.current_date:
            logging.info(f"Date changed from {self.current_date} to {new_date}")
            self.current_date = new_date
            self.processed_inventory_ids.clear()
            logging.info("Processed inventory IDs reset for new day")
    
    def fetch_exception_inventory_ids(self, process_type: str) -> List[str]:
        """
        Fetch inventory IDs that have exceptions for a specific process type
        
        Args:
            process_type (str): Process type ('1' or '6')
            
        Returns:
            List[str]: List of inventory IDs
        """
        query = """
            SELECT DISTINCT inventoryid 
            FROM public.tbl_botlogs 
            WHERE message LIKE '%%Exception : file %%' 
            AND created_time::Date = Current_Date 
            AND process_type = %s 
            ORDER BY inventoryid
        """
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (process_type,))
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            
            inventory_ids = [str(row[0]) for row in results]
            logging.info(f"Found {len(inventory_ids)} inventory IDs with exceptions for process type {process_type}")
            return inventory_ids
            
        except Exception as e:
            logging.error(f"Error fetching exception inventory IDs for process type {process_type}: {e}")
            return []
    
    def update_inventory_status(self, inventory_ids: List[str]) -> int:
        """
        Update file status to 0 for given inventory IDs
        
        Args:
            inventory_ids (List[str]): List of inventory IDs to update
            
        Returns:
            int: Number of records updated
        """
        if not inventory_ids:
            return 0
        
        query = """
            UPDATE public.tbl_stginventoryuploaddata 
            SET filestatus = 0 
            WHERE inventoryid IN %s
        """

        delete_query = """
            DELETE FROM public.tbl_chartdocumenttransaction
            WHERE batchid IN %s
            AND name NOT LIKE '%%mr%%'
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            # UPDATE
            cursor.execute(query, (tuple(inventory_ids),))
            updated_count = cursor.rowcount

            #Delete
            cursor.execute(delete_query, (tuple(inventory_ids),))
            deleted_count = cursor.rowcount 

            conn.commit()

            cursor.close()
            conn.close()
            
            logging.info(f"Updated {updated_count} inventory records to filestatus=0")
            logging.info(f"Deleted {deleted_count} chart document records")
            return updated_count
            
        except Exception as e:
            logging.error(f"Error updating inventory status: {e}")
            return 0
 
    
    def process_exceptions(self):
        """
        Main processing logic - fetch exceptions and update inventory
        Only processes inventory IDs that haven't been processed before
        """
        # Check if date has changed
        self.check_date_change()
        
        logging.info("=" * 80)
        logging.info("Starting exception processing cycle")
        
        all_inventory_ids = []
        
        # Fetch inventory IDs for process type 1
        # type1_ids = self.fetch_exception_inventory_ids('1')          #SOS
        # all_inventory_ids.extend(type1_ids)
        
        # Fetch inventory IDs for process type 6
        type6_ids = self.fetch_exception_inventory_ids('6')
        all_inventory_ids.extend(type6_ids)
        
        # Remove duplicates
        unique_inventory_ids = list(set(all_inventory_ids))
        
        # Filter out already processed IDs
        new_inventory_ids = [
            inv_id for inv_id in unique_inventory_ids 
            if inv_id not in self.processed_inventory_ids
        ]
        
        if new_inventory_ids:
            logging.info(f"New inventory IDs to process: {len(new_inventory_ids)}")
            logging.info(f"Inventory IDs: {', '.join(new_inventory_ids)}")
            
            # Update the inventory status
            updated_count = self.update_inventory_status(new_inventory_ids)
            
            # Mark these IDs as processed
            self.processed_inventory_ids.update(new_inventory_ids)
            
            logging.info(f"Total processed inventory IDs today: {len(self.processed_inventory_ids)}")
        else:
            logging.info("No new inventory IDs to process")
        
        logging.info("Exception processing cycle completed")
        logging.info("=" * 80)
    
    def run(self, interval_minutes: int = 10):
        """
        Run the handler in a continuous loop
        
        Args:
            interval_minutes (int): Time interval between checks in minutes
        """
        logging.info(f"Bot Exception Handler started - Running every {interval_minutes} minutes")
        logging.info("Press Ctrl+C to stop")
        
        try:
            while True:
                self.process_exceptions()
                
                # Wait for the specified interval
                logging.info(f"Waiting {interval_minutes} minutes until next check...")
                time.sleep(interval_minutes * 60)
                
        except KeyboardInterrupt:
            logging.info("Handler stopped by user")
        except Exception as e:
            logging.error(f"Unexpected error in main loop: {e}")
            raise


def main():
    """Main entry point"""
    
    # Try to import config file
    try:
        import config
        db_config = config.DATABASE_CONFIG
        interval = config.CHECK_INTERVAL
        logging.info("Loaded configuration from config.py")
    except ImportError:
        logging.warning("config.py not found. Using default configuration.")
        logging.warning("Please create config.py from config_template.py")
        
        # Default configuration
        db_config = {
            'host': 'database-1-instance-1.cjcwia02c7gt.us-west-1.rds.amazonaws.com',
            'database': 'gebbs_unified_client_review_rendercare',
            'user': 'iCodeOneBot',
            'password': 'bgNMyu45FRBD$fg4f',
            'port': 5432
        }
        interval = 10
    
    # Validate configuration
    if db_config['database'] == 'gebbs_unified_client_review_rendercare':

        # Create handler instance
        handler = BotExceptionHandler(db_config)

        # Run the handler
        handler.run(interval_minutes=interval)
    
    else:    
        logging.error("Please update the database configuration in config.py")
        sys.exit(1)



if __name__ == "__main__":
    main()