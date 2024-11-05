# src/etl_lite/core/pipeline.py
from pathlib import Path
from typing import List
import logging
from clickhouse_driver import Client

class Pipeline:
    def __init__(self, connection: Client):
        self.connection = connection
        self.logger = logging.getLogger(__name__)

    def run(self, sql_path: Path):
        """Execute single SQL transformation"""
        from etl_lite.core.parser import parse_sql_file
        
        # Parse SQL file
        self.logger.info(f"Parsing SQL file: {sql_path}")
        metadata = parse_sql_file(sql_path)
        
        # Create target table
        if metadata.target['type'] == 'table':
            table_name = metadata.target['params']['name']
            columns = metadata.target['params']['columns']
            engine = metadata.target['params']['engine']
            
            self.logger.info(f"Creating target table: {table_name}")
            
            columns_def = ", ".join(
                f"{name} {type_}" 
                for name, type_ in columns.items()
            )
            
            create_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {columns_def}
                ) ENGINE = {engine}
            """
            self.connection.execute(create_query)
        
        # Execute main query
        self.logger.info("Executing main query")
        insert_query = f"INSERT INTO {table_name} {metadata.query}"
        self.connection.execute(insert_query)
        
        self.logger.info("Step completed successfully")