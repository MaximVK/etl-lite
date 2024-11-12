from typing import Callable
from functools import partial
from typing import Tuple
from src.etl_lite.core.model import (
    DataQualityTest,
    Number,
    RAG,
    DbTable,
    ClickhouseTargetTable,
    MergeTreeEngine,
    ValidateFunc,
    Query
)

def merge_tree(table_name: str,
               order_by: list[str],
               partition_by: str,
               primary_key: list[str],
               description: str
               ) -> ClickhouseTargetTable | None:

    schema, table = table_name.split(".")
    db_table = DbTable(schema, table)
    engine = MergeTreeEngine(
        order_by=order_by,
        partition_by=partition_by,
        primary_key=primary_key
    )
    return ClickhouseTargetTable[db_table, engine]


def log(table_name: str, description: str) -> ClickhouseTargetTable | None:
    schema, table = table_name.split(".")
    db_table = DbTable(schema, table)
    return ClickhouseTargetTable[db_table, ]
