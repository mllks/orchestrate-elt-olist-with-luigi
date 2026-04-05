import luigi
import logging
import pandas as pd
import time
import sqlalchemy
from datetime import datetime
from pipeline.extract import Extract
from pipeline.utils.db_conn import db_connection
from pipeline.utils.read_sql import read_sql_file
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
load_dotenv()

# Define DIR
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_LOAD_QUERY = os.getenv("DIR_LOAD_QUERY")
DIR_LOG = os.getenv("DIR_LOG")

class Load(luigi.Task):   
    
    def requires(self):
        return Extract()
    
    def run(self):
         
        # Configure logging
        logging.basicConfig(filename = f'{DIR_TEMP_LOG}/logs.log', 
                            level = logging.INFO, 
                            format = '%(asctime)s - %(levelname)s - %(message)s')
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Read query to be executed
        try:
            # Read query to truncate public schema in dwh
            truncate_query = read_sql_file(
                file_path = f'{DIR_LOAD_QUERY}/public-truncate_tables.sql'
            )
            
            # Read load query to staging schema
            product_category_name_translation_query = read_sql_file(f'{DIR_LOAD_QUERY}/stg.product_category_name_translation.sql')
            products_query = read_sql_file(f'{DIR_LOAD_QUERY}/stg-products.sql')
            geolocation_query = read_sql_file(f'{DIR_LOAD_QUERY}/stg-geolocation.sql')
            customers_query = read_sql_file(f'{DIR_LOAD_QUERY}/stg-customer.sql')
            sellers_query = read_sql_file(f'{DIR_LOAD_QUERY}/stg-sellers.sql')
            orders_query = read_sql_file(f'{DIR_LOAD_QUERY}/stg-orders.sql')
            order_items_query = read_sql_file(f'{DIR_LOAD_QUERY}/stg-order_items.sql')
            order_payments_query = read_sql_file(f'{DIR_LOAD_QUERY}/stg-order_payments.sql')
            order_reviews_query = read_sql_file(f'{DIR_LOAD_QUERY}/stg-order_reviews.sql')
            
            logging.info("Read Load Query - SUCCESS")
            
        except Exception:
            logging.error("Read Load Query - FAILED")
            raise Exception("Failed to read Load Query")

        #----------------------------------------------------------------------------------------------------------------------------------------
        # Read Data to be load
        try:
            input_paths = [t.path for t in self.input()]

            def _read_extracted_csv(table_name: str) -> pd.DataFrame:
                suffix = f"{table_name}.csv"
                matched = [p for p in input_paths if Path(p).name.endswith(suffix)]
                if not matched:
                    raise FileNotFoundError(f"Missing extracted file for '{table_name}': expected '*{suffix}'")
                return pd.read_csv(matched[0])

            product_category_name_translation = _read_extracted_csv("public.product_category_name_translation")
            products = _read_extracted_csv("public.products")
            geolocation = _read_extracted_csv("public.geolocation")
            customers = _read_extracted_csv("public.customers")
            sellers = _read_extracted_csv("public.sellers")
            orders = _read_extracted_csv("public.orders")
            order_items = _read_extracted_csv("public.order_items")
            order_payments = _read_extracted_csv("public.order_payments")
            order_reviews = _read_extracted_csv("public.order_reviews")
            
            logging.info(f"Read Extracted Data - SUCCESS")
            
        except Exception as e:
            logging.exception(f"Read Extracted Data  - FAILED: {e}")
            raise Exception("Failed to Read Extracted Data") from e
        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Establish connections to DWH
        try:
            _, dwh_engine = db_connection()
            logging.info(f"Connect to DWH - SUCCESS")
            
        except Exception:
            logging.info(f"Connect to DWH - FAILED")
            raise Exception("Failed to connect to Data Warehouse")
        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Truncate all tables before load
        # This puropose to avoid errors because duplicate key value violates unique constraint
        try:            
            # Split the SQL queries if multiple queries are present
            truncate_query = truncate_query.split(';')

            # Remove newline characters and leading/trailing whitespaces
            truncate_query = [query.strip() for query in truncate_query if query.strip()]
            
            # Create session
            Session = sessionmaker(bind = dwh_engine)
            session = Session()

            # Execute each query
            for query in truncate_query:
                query = sqlalchemy.text(query)
                session.execute(query)
                
            session.commit()
            
            # Close session
            session.close()

            logging.info(f"Truncate public Schema in DWH - SUCCESS")
        
        except Exception:
            logging.error(f"Truncate public Schema in DWH - FAILED")
            
            raise Exception("Failed to Truncate public Schema in DWH")
        
        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Record start time for loading tables
        start_time = time.time()  
        logging.info("==================================STARTING LOAD DATA=======================================")
        # Load to tables
        try:
            
            try:
                # Load to public schema
                product_category_name_translation.to_sql('product_category_name_translation', 
                    con = dwh_engine, 
                    if_exists = 'append', 
                    index = False, 
                    schema = 'public')
                products.to_sql('products', 
                    con = dwh_engine, 
                    if_exists = 'append', 
                    index = False, 
                    schema = 'public')
                geolocation.to_sql('geolocation', 
                    con = dwh_engine, 
                    if_exists = 'append', 
                    index = False, 
                    schema = 'public')
                customers.to_sql('customers', 
                    con = dwh_engine, 
                    if_exists = 'append', 
                    index = False, 
                    schema = 'public')
                sellers.to_sql('sellers', 
                    con = dwh_engine, 
                    if_exists = 'append', 
                    index = False, 
                    schema = 'public')
                
                orders.to_sql('orders', 
                    con = dwh_engine, 
                    if_exists = 'append', 
                    index = False, 
                    schema = 'public')
                order_items.to_sql('order_items', 
                    con = dwh_engine, 
                    if_exists = 'append', 
                    index = False, 
                    schema = 'public')
                order_payments.to_sql('order_payments', 
                    con = dwh_engine, 
                    if_exists = 'append', 
                    index = False, 
                    schema = 'public')
                order_reviews.to_sql('order_reviews', 
                    con = dwh_engine, 
                    if_exists = 'append', 
                    index = False, 
                    schema = 'public')
                
                logging.info(f"LOAD All Tables To DWH-Olist - SUCCESS")
                
            except Exception:
                logging.error(f"LOAD All Tables To DWH-Olist - FAILED")
                raise Exception('Failed Load Tables To DWH-Olist')
            
            
            #----------------------------------------------------------------------------------------------------------------------------------------
            # Load to staging schema
            try:
                # List query
                load_stg_queries = [
                    product_category_name_translation_query, 
                    products_query, 
                    geolocation_query, 
                    customers_query, 
                    sellers_query, 
                    orders_query, 
                    order_items_query, 
                    order_payments_query, 
                    order_reviews_query
                ]
                
                # Create session
                Session = sessionmaker(bind = dwh_engine)
                session = Session()

                # Execute each query
                for query in load_stg_queries:
                    query = sqlalchemy.text(query)
                    session.execute(query)
                    
                session.commit()
                
                # Close session
                session.close()
                
                logging.info("LOAD All Tables To DWH-Staging - SUCCESS")
                
            except Exception:
                logging.error("LOAD All Tables To DWH-Staging - FAILED")
                raise Exception('Failed Load Tables To DWH-Staging')
        
        
            # Record end time for loading tables
            end_time = time.time()  
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Load'],
                'status' : ['Success'],
                'execution_time': [execution_time]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/load-summary.csv", index = False)
            
                        
        #----------------------------------------------------------------------------------------------------------------------------------------
        except Exception:
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Load'],
                'status' : ['Failed'],
                'execution_time': [0]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/load-summary.csv", index = False)
            
            logging.error("LOAD All Tables To DWH - FAILED")
            raise Exception('Failed Load Tables To DWH')   
        
        logging.info("==================================ENDING LOAD DATA=======================================")
        
    #----------------------------------------------------------------------------------------------------------------------------------------
    def output(self):
        return [luigi.LocalTarget(f'{DIR_TEMP_LOG}/logs.log'),
                luigi.LocalTarget(f'{DIR_TEMP_DATA}/load-summary.csv')]
