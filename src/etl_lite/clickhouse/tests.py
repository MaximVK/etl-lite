# src/etl_lite/clickhouse/tests.py
from etl_lite.sql import tests as sql_tests
from typing import List, Any

# Override generic SQL implementation
def no_duplicates(connection: Any, name: str, columns: List[str]):
    """ClickHouse-specific duplicate check"""
    cols = ", ".join(columns)
    query = f"""
        SELECT count(*) = count(distinct({cols}))
        FROM {{table}}
        SETTINGS optimize_aggregation_in_order=1
    """
    result = connection.execute(query)
    return bool(result[0][0])

# Add ClickHouse-specific test
def array_length(connection: Any, name: str, column: str, min_length: int):
    """ClickHouse-specific array length test"""
    query = f"""
        SELECT min(length({column})) >= {min_length}
        FROM {{table}}
    """
    result = connection.execute(query)
    return bool(result[0][0])

# Inherit other functions from SQL tests
range = sql_tests.range
