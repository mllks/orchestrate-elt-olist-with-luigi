import luigi
import logging
import pandas as pd
import time
import sqlalchemy
from datetime import datetime
from pipeline.extract import Extract
from pipeline.load import Load
from pipeline.utils.db_conn import db_connection
from pipeline.utils.read_sql import read_sql_file
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define DIR
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_TRANSFORM_QUERY = os.getenv("DIR_TRANSFORM_QUERY")
DIR_LOG = os.getenv("DIR_LOG")

def _ensure_file_logging(log_dir: str):
    log_dir = log_dir or "./pipeline/temp/log"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "logs.log")

    root_logger = logging.getLogger()
    if root_logger.level == logging.NOTSET:
        root_logger.setLevel(logging.INFO)

    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler) and getattr(handler, "baseFilename", None) == os.path.abspath(log_file):
            return

    handler = logging.FileHandler(log_file)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    root_logger.addHandler(handler)

class Transform(luigi.Task):
    
    def requires(self):
        return Load()
    
    def run(self):
         
        _ensure_file_logging(DIR_TEMP_LOG)
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Establish connections to DWH
        try:
            _, dwh_engine = db_connection()
            logging.info(f"Connect to DWH - SUCCESS")
            
        except Exception:
            logging.info(f"Connect to DWH - FAILED")
            raise Exception("Failed to connect to Data Warehouse")
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Read query to be executed
        try:
            # Read transform query to final schema
            dim_customer_query = read_sql_file(f'{DIR_TRANSFORM_QUERY}/dim_customer.sql')
            if dim_customer_query is None: raise Exception("Failed to read dim_customer_query")
            dim_product_query = read_sql_file(f'{DIR_TRANSFORM_QUERY}/dim_product.sql')
            if dim_product_query is None: raise Exception("Failed to read dim_product_query")
            dim_seller_query = read_sql_file(f'{DIR_TRANSFORM_QUERY}/dim_seller.sql')
            if dim_seller_query is None: raise Exception("Failed to read dim_seller_query")
            
            fct_order_delivery_query = read_sql_file(f'{DIR_TRANSFORM_QUERY}/fct_order_delivery.sql')
            if fct_order_delivery_query is None: raise Exception("Failed to read fct_order_delivery_query")
            fct_order_item_query = read_sql_file(f'{DIR_TRANSFORM_QUERY}/fct_order_item.sql')
            if fct_order_item_query is None: raise Exception("Failed to read fct_order_item_query")
            fct_order_payment_query = read_sql_file(f'{DIR_TRANSFORM_QUERY}/fct_order_payment.sql')
            if fct_order_payment_query is None: raise Exception("Failed to read fct_order_payment_query")
            fct_order_reviews_query = read_sql_file(f'{DIR_TRANSFORM_QUERY}/fct_order_reviews.sql')
            if fct_order_reviews_query is None: raise Exception("Failed to read fct_order_reviews_query")
            
            logging.info("Read Transform Query - SUCCESS")
            
        except Exception as e:
            logging.error(f"Read Transform Query - FAILED: {str(e)}")
            raise Exception(f"Failed to read Transform Query: {str(e)}")        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Record start time for transform tables
        start_time = time.time()
        logging.info("==================================STARTING TRANSFROM DATA=======================================")  
               
        # Transform to dimensions and fact tables
        try:
            # Create session
            Session = sessionmaker(bind = dwh_engine)
            session = Session()
            
            # Transform to dimensions
            queries = [
                ('final.dim_customer', dim_customer_query),
                ('final.dim_product', dim_product_query),
                ('final.dim_seller', dim_seller_query),
                ('final.fct_order_delivery', fct_order_delivery_query),
                ('final.fct_order_item', fct_order_item_query),
                ('final.fct_order_payment', fct_order_payment_query),
                ('final.fct_order_reviews', fct_order_reviews_query)
            ]
            
            for name, q in queries:
                query = sqlalchemy.text(q)
                session.execute(query)
                logging.info(f"Transform to '{name}' - SUCCESS")
            
            # Commit transaction
            session.commit()
            
            # Close session
            session.close()

            logging.info(f"Transform to All Dimensions and Fact Tables - SUCCESS")
            
            # Record end time for loading tables
            end_time = time.time()  
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Transform'],
                'status' : ['Success'],
                'execution_time': [execution_time]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/transform-summary.csv", index = False)
            
        except Exception:
            logging.error(f"Transform to All Dimensions and Fact Tables - FAILED")
        
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Transform'],
                'status' : ['Failed'],
                'execution_time': [0]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/transform-summary.csv", index = False)
            
            logging.error("Transform Tables - FAILED")
            raise Exception('Failed Transforming Tables')   
        
        logging.info("==================================ENDING TRANSFROM DATA=======================================") 

    #----------------------------------------------------------------------------------------------------------------------------------------
    def output(self):
        return [luigi.LocalTarget(f'{DIR_TEMP_LOG}/logs.log'),
                luigi.LocalTarget(f'{DIR_TEMP_DATA}/transform-summary.csv')]
