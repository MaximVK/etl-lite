from typing import Any

def sum(connection: Any, name: str, column: str, tolerance: str):
    """Generic SQL sum invariant"""
    query = f"SELECT sum({column}) FROM {{table}}"
    result = connection.execute(query)
    return float(result[0][0])

def count(connection: Any, name: str, tolerance: str):
    """Generic SQL count invariant"""
    query = "SELECT count(*) FROM {table}"
    result = connection.execute(query)
    return int(result[0][0])

def custom(connection: Any, name: str, query: str, tolerance: str):
    """Custom invariant check"""
    result = connection.execute(query)
    return result[0][0]
