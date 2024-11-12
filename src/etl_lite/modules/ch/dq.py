from typing import Callable
from functools import partial
from src.etl_lite.core.model import (
    DataQualityTest,
    Number,
    RAG,
    DbTable,
    ValidateFunc,
    Query
)


def exact_match(x: Number, y: Number) -> RAG:
    return 'green' if x == y else 'red'


def custom_test(name: str, sql: Query, validate: ValidateFunc = partial(exact_match, y=0)) -> DataQualityTest:
    return DataQualityTest(name, sql, validate)


def no_duplicates(name: str, table: DbTable, columns: list[str], ) -> DataQualityTest:
    columns = ', '.join(columns)
    sql = f"""
    SELECT COUNT() AS CNT 
    FROM (
        SELECT {columns} 
        FROM {table} 
        GROUP BY {columns} 
        HAVING count(*) > 1 
        LIMIT 100
    )"""

    return DataQualityTest(name, sql, partial(exact_match, 0))

