from typing import Any, Dict, List, Tuple, Union, Optional, Literal, Callable, TypeVar, TypeVarTuple, Concatenate

from pathlib import Path
import datetime
from dataclasses import dataclass
from typing import NamedTuple

# Basic types
type Number = int | float
type RAG = Literal['red', 'green', 'amber']


class DbTable(NamedTuple):
    schema: str
    table: str

    def __str__(self) -> str:
        return f"{self.schema}.{self.table}"

type Query = str
type DbType = int | float | str | bool | datetime.datetime | datetime.date
type CHArrayType = list[DbType] | dict[str, CHArrayType]
type CHType = DbType | CHArrayType | dict[str, DbType]
type QueryParameters = dict[str, CHType]
type ParameterizedQuery = tuple[Query, list[str]]  # query, parameters
type ParsedQuery = tuple[ParameterizedQuery | Query, list[DbTable]] # query, input tables


# Clickhouse engine types
@dataclass(frozen=True)
class LogEngine:
    pass

@dataclass(frozen=True)
class MergeTreeEngine:
    order_by: list[str]
    partition_by: str | None = None
    primary_key: list[str] | None = None
    settings: dict | None = None

type ClickhouseTableEngine = LogEngine | MergeTreeEngine
type ClickhouseTargetTable = tuple[DbTable, ClickhouseTableEngine]
type ExcelFile = Path
type CSVFile = Path
type Target = ClickhouseTargetTable | ExcelFile | CSVFile
type ValidateFunc = Callable[[Number], RAG]

# Processing strategies
type TargetStrategy = Literal['replace', 'append']
type ProcessingStrategy = Literal['one_go', 'incremental']


# Tests
# Not perfect, as I can't contraint P to be CHType
type ApplyParametersFunc[**P] = Callable[Concatenate[DbTable, P], Query | None]


@dataclass(frozen=True)
class DataQualityTestTemplate:
    name: str
    get_query: ApplyParametersFunc


@dataclass(frozen=True)
class DataQualityTest:
    name: str
    query: Query
    validate_func: ValidateFunc


# Invariants, the same as test, but different scope
type DataInvariantTemplate =  DataQualityTestTemplate
type DataInvariant = DataQualityTest


@dataclass(frozen=True)
class SqlPipelineStep:
    step_number: int
    main_query: Query | ParameterizedQuery
    output_table: Target
    target_strategy: TargetStrategy = 'replace'
    processing_strategy: ProcessingStrategy = 'one_go'
    tests: List[DataQualityTest] | None = None
    invariants: List[DataInvariant] | None = None
    invariant_actions: List[str] | None = None # TODO: define  invariant actions: activate, pause, suspend


# draft, to be defined later
@dataclass(frozen=True)
class PythonPipelineStep:
    step_number: int
    main_func: Callable[[], None]
    output_table: Target
    target_strategy: TargetStrategy = 'replace'
    processing_strategy: ProcessingStrategy = 'one_go'
    tests: List[DataQualityTest] | None = None
    invariants: List[DataInvariant] | None = None
    invariant_actions: List[str] | None = None # TODO: define  invariant actions: activate, pause, suspend


type PipelineStep = SqlPipelineStep | PythonPipelineStep

@dataclass(frozen=True)
class ETLPipeline:
    description: str | None = None
    steps: List[PipelineStep]
    

# API



