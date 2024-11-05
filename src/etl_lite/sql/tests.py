from typing import List, Any

def no_duplicates(connection: Any, name: str, columns: List[str]):
    """Generic SQL duplicate check"""
    cols = ", ".join(columns)
    query = f"""
        SELECT count(*) = count(distinct({cols}))
        FROM {{table}}
    """
    result = connection.execute(query)
    return bool(result[0][0])

def range(connection: Any, name: str, column: str, min: float, max: float):
    """Generic SQL range check"""
    query = f"""
        SELECT min({column}) >= {min} AND max({column}) <= {max}
        FROM {{table}}
    """
    result = connection.execute(query)
    return bool(result[0][0])
