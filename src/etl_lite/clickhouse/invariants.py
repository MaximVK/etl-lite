from etl_lite.sql import invariants as sql_invariants
from typing import Any

# Override generic SQL implementation
def sum(connection: Any, name: str, column: str, tolerance: str):
    """ClickHouse-specific sum implementation"""
    query = f"SELECT sum({column}) FROM {{table}} SETTINGS optimize_aggregation_in_order=1"
    result = connection.execute(query)
    return float(result[0][0])

# Add ClickHouse-specific invariant
def array_sum(connection: Any, name: str, column: str, tolerance: str):
    """ClickHouse-specific array sum invariant"""
    query = f"SELECT sum(arraySum({column})) FROM {{table}}"
    result = connection.execute(query)
    return float(result[0][0])

# Inherit other functions from SQL invariants
count = sql_invariants.count
custom = sql_invariants.custom
