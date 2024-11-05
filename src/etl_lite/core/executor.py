# src/etl_lite/core/executor.py
from typing import Any, Dict
from pathlib import Path
from clickhouse_driver import Client
import logging


class Executor:
    def __init__(self, connection):
        self.connection = connection
        self.logger = logging.getLogger(__name__)

    def execute_step(self, sql_path: Path):
        """Execute single ETL step"""
        from etl_lite.core.parser import parse_sql_file
        
        # Parse SQL file
        self.logger.info(f"Parsing SQL file: {sql_path}")
        metadata = parse_sql_file(sql_path)
        
        # Create target table if needed
        if metadata.target['type'] == 'table':
            create_stmt = metadata.target['function'](**metadata.target['params']).get_create_statement()
            self.logger.info(f"Creating target table: {metadata.target['params']['name']}")
            self.connection.execute(create_stmt)
        
        # Execute main query
        self.logger.info("Executing main query")
        query = self._prepare_query(metadata.query, metadata.target['params']['name'])
        self.connection.execute(query)
        
        self.logger.info("Step completed successfully")