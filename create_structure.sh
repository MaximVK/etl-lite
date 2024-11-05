#!/bin/bash

# Root level files
touch .gitignore
touch pyproject.toml
touch README.md

# Source directories and files
mkdir -p src/etl_lite/{core,engines,clickhouse,pandas,oracle,utils}

# Core module
touch src/etl_lite/__init__.py
touch src/etl_lite/core/__init__.py
touch src/etl_lite/core/pipeline.py
touch src/etl_lite/core/step.py
touch src/etl_lite/core/metadata.py
touch src/etl_lite/core/results.py
touch src/etl_lite/core/graph.py

# Engines module
touch src/etl_lite/engines/__init__.py
touch src/etl_lite/engines/base.py
touch src/etl_lite/engines/connection.py

# ClickHouse module
touch src/etl_lite/clickhouse/__init__.py
touch src/etl_lite/clickhouse/connection.py
touch src/etl_lite/clickhouse/tests.py
touch src/etl_lite/clickhouse/invariants.py

# Pandas module
touch src/etl_lite/pandas/__init__.py
touch src/etl_lite/pandas/connection.py
touch src/etl_lite/pandas/tests.py
touch src/etl_lite/pandas/invariants.py

# Oracle module
touch src/etl_lite/oracle/__init__.py
touch src/etl_lite/oracle/connection.py
touch src/etl_lite/oracle/tests.py
touch src/etl_lite/oracle/invariants.py

# Utils module
touch src/etl_lite/utils/__init__.py
touch src/etl_lite/utils/sql_parser.py

# Test directories and files
mkdir -p tests/{core,engines,examples/{sql,python}}
touch tests/__init__.py
touch tests/conftest.py
touch tests/core/test_pipeline.py
touch tests/core/test_step.py
touch tests/core/test_metadata.py
touch tests/engines/test_clickhouse.py

# Example directories and files
mkdir -p examples/{simple_pipeline/{sql,python},mixed_pipeline/{sql,steps}}
touch examples/simple_pipeline/sql/01_validate.sql
touch examples/simple_pipeline/sql/02_transform.sql
touch examples/mixed_pipeline/steps/transform.py
touch examples/mixed_pipeline/pipeline.py

echo "Project structure created successfully!"