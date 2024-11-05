from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Callable
from enum import Enum

class EngineType(Enum):
    MERGE_TREE = "MergeTree"
    REPLACING_MERGE_TREE = "ReplacingMergeTree"
    COLLAPSING_MERGE_TREE = "CollapsingMergeTree"
    LOG = "Log"

@dataclass(frozen=True)
class TableDefinition:
    """Definition of a target table"""
    name: str
    engine: EngineType
    order_by: Optional[List[str]] = None
    partition_by: Optional[List[str]] = None
    primary_key: Optional[List[str]] = None
    settings: Optional[Dict[str, Any]] = None

class target:
    """Factory for target definitions"""
    
    @staticmethod
    def table(
        name: str,
        engine: str,
        order_by: Optional[List[str]] = None,
        partition_by: Optional[List[str]] = None,
        primary_key: Optional[List[str]] = None,
        settings: Optional[Dict[str, Any]] = None
    ) -> TableDefinition:
        """Create a table target definition"""
        return TableDefinition(
            name=name,
            engine=EngineType(engine),
            order_by=order_by,
            partition_by=partition_by,
            primary_key=primary_key,
            settings=settings
        )

@dataclass(frozen=True)
class StepMetadata:
    """Metadata for a pipeline step"""
    name: str
    order: int
    target: TableDefinition
    description: Optional[str] = None
    tests: Optional[List[Callable]] = None
    invariants: Optional[List[Callable]] = None

def pipeline_step(
    order: int,
    target: TableDefinition,
    description: Optional[str] = None,
    tests: Optional[List[Callable]] = None,
    invariants: Optional[List[Callable]] = None
) -> Callable:
    """Decorator for pipeline steps"""
    def decorator(func: Callable) -> Callable:
        # Store metadata on the function
        func._is_pipeline_step = True
        func._metadata = StepMetadata(
            name=func.__name__,
            order=order,
            target=target,
            description=description or func.__doc__,
            tests=tests,
            invariants=invariants
        )
        return func
    return decorator

# Example usage:
if __name__ == "__main__":
    @pipeline_step(
        order=1,
        target=target.table(
            name="reports.client_volume",
            engine="ReplacingMergeTree",
            order_by=["client_id", "year"]
        ),
        description="Process client volume data"
    )
    def process_client_volume(connection):
        """Process client volume data"""
        pass
    
    # Access metadata
    assert process_client_volume._is_pipeline_step
    assert process_client_volume._metadata.name == "process_client_volume"
    assert process_client_volume._metadata.order == 1
    assert process_client_volume._metadata.target.name == "reports.client_volume"
    