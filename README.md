# ETL Lite

A lightweight ETL framework for SQL and Python transformations.

## Features

- SQL and Python pipeline steps
- Automatic step ordering and dependency resolution
- Test and invariant validation
- Support for multiple databases (ClickHouse, Oracle, Pandas)
- Pipeline composition and reuse
- Execution results and statistics

## Installation

```bash
pip install etl-lite
```

## Quick Start

### SQL Pipeline

```sql
-- 01_validate.sql
-- @description: Validate input data
-- @target.table: reports.validated_data
SELECT FROM source.raw_data
WHERE amount > 0
```

### Python Pipeline

```python
from etl_lite.core.pipeline import Pipeline, pipeline_step
from etl_lite.core.metadata import target

@pipeline_step(
order=1,
target=target.table(
name="reports.transformed_data",
engine="ReplacingMergeTree",
order_by=["id"]
)
)
def transform_data(connection):
df = connection.select_into_df("""
SELECT FROM source.raw_data
""")
# Transform data
result_df = transform(df)
# Save results
connection.save_result(result_df)
```

```python
pipeline = Pipeline("example") \
.collect_sql_steps("sql") \
.collect_steps()
results = pipeline.execute()
```
