from typing import Any, Dict, List, Tuple, Union, Optional, Literal, Callable, TypeVar, TypeVarTuple, ParamSpec, Unpack, Concatenate

from pathlib import Path
import datetime
from dataclasses import dataclass


type DbTable = tuple[str, str]
type Query = str
type DbType = int | float | str | bool
type CHType = DbType | list[DbType] | dict[str, DbType]
type QueryParameters = dict[str, CHType]
type ParameterizedQuery = tuple[Query, list[str]]  # query, parameters
type ParsedQuery = tuple[ParameterizedQuery | Query, list[DbTable]] # query, input tables

# Not perfect, as I can't contraint P to be CHType
type TestFunc[**P] = Callable[Concatenate[DbTable, P], Query]

