from typing import Dict, Any, List, Optional
from dataclasses import dataclass

@dataclass
class TableTarget:
    """Table target configuration"""
    name: str
    engine: str
    order_by: List[str]
    columns: Dict[str, str]  # column_name -> column_type
    partition_by: Optional[str] = None
    settings: Dict[str, Any] = None

    def get_create_statement(self) -> str:
        """Generate CREATE TABLE statement"""
        columns_def = ", ".join(
            f"{name} {type_}" 
            for name, type_ in self.columns.items()
        )
        
        order_by = ", ".join(self.order_by)
        
        stmt = f"""
            CREATE TABLE IF NOT EXISTS {self.name} (
                {columns_def}
            ) ENGINE = {self.engine}
            ORDER BY ({order_by})
        """
        
        if self.partition_by:
            stmt += f"\nPARTITION BY {self.partition_by}"
            
        if self.settings:
            settings_str = ", ".join(
                f"{k} = {v}" 
                for k, v in self.settings.items()
            )
            stmt += f"\nSETTINGS {settings_str}"
            
        return stmt

def table(
    name: str,
    engine: str,
    order_by: List[str],
    columns: Dict[str, str],
    partition_by: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None
) -> TableTarget:
    """Table target configuration"""
    return TableTarget(
        name=name,
        engine=engine,
        order_by=order_by,
        columns=columns,
        partition_by=partition_by,
        settings=settings
    )