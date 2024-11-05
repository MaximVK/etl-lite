from clickhouse_driver import Client
from etl_lite.core.pipeline import Pipeline
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Initialize connection and pipeline
connection = Client(host='localhost')
pipeline = Pipeline(connection)

# Define steps
steps = [
    Path('sql/1.city_stats.sql')
]

current_dir = Path(__file__).parent

# Run pipeline
pipeline.run(current_dir / 'sql' / '1.city_stats.sql')