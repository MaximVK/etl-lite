from dataclasses import dataclass
from typing import Dict, List, Any, Optional
from pathlib import Path
import re
import yaml
import importlib

@dataclass
class SQLMetadata:
    """Parsed SQL metadata and query"""
    meta: Dict[str, Any]        # meta.description, meta.engine etc
    target: Dict[str, Any]      # target configuration
    strategy: Dict[str, Any]    # strategy.chunk, strategy.incremental etc
    invariants: List[Dict[str, Any]]
    tests: List[Dict[str, Any]]
    query: str

    @property
    def engine(self) -> str:
        """Get engine name from metadata"""
        return self.meta.get('engine', {}).get('params', {}).get('type', 'sql')

    @property
    def engine_settings(self) -> Dict[str, Any]:
        """Get engine settings from metadata"""
        return self.meta.get('engine', {}).get('params', {}).get('settings', {})

class ParsingError(Exception):
    """Base class for parsing errors"""
    pass

def parse_sql_file(path: Path, default_engine: str = 'sql') -> SQLMetadata:
    """Parse SQL file with metadata comments
    
    Args:
        path: Path to SQL file
        default_engine: Default database engine if not specified in metadata
    
    Returns:
        SQLMetadata object containing parsed metadata and query
    
    Raises:
        ParsingError: If parsing fails
        FileNotFoundError: If file doesn't exist
    """
    with open(path) as f:
        content = f.read()
    
    # Split into metadata and query parts
    parts = re.split(r'--\s*@main\b', content)
    if len(parts) != 2:
        raise ParsingError(f"SQL file must contain '-- @main' separator: {path}")
    
    metadata_text, query = parts
    
    # Initialize metadata containers
    metadata = {
        'meta': {},
        'target': None,
        'strategy': {},
        'invariants': [],
        'tests': []
    }
    
    # Split metadata text into blocks
    blocks = re.split(r'--\s*@', metadata_text)
    
    # First pass: process meta.engine if present
    engine = default_engine
    for block in blocks[1:]:  # skip first empty block
        if not block.strip():
            continue
        
        lines = block.strip().split('\n')
        if not lines:
            continue
        
        first_line = lines[0]
        if first_line.startswith('meta.engine'):
            process_metadata_block(first_line, lines[1:], metadata, default_engine)
            engine = (metadata['meta'].get('engine', {})
                     .get('params', {})
                     .get('type', default_engine))
            break
    
    # Second pass: process all blocks with determined engine
    for block in blocks[1:]:
        if not block.strip():
            continue
        
        lines = block.strip().split('\n')
        if not lines:
            continue
            
        first_line = lines[0]
        remaining_lines = lines[1:]
        
        try:
            process_metadata_block(first_line, remaining_lines, metadata, engine)
        except Exception as e:
            raise ParsingError(f"Error processing metadata block '{first_line}': {str(e)}")
    
    # Validate required metadata
    if not metadata['target']:
        raise ParsingError(f"SQL file must contain target definition: {path}")
    
    return SQLMetadata(
        meta=metadata['meta'],
        target=metadata['target'],
        strategy=metadata['strategy'],
        invariants=metadata['invariants'],
        tests=metadata['tests'],
        query=query.strip()
    )

def process_metadata_block(first_line: str, remaining_lines: List[str], metadata: Dict, engine: str):
    """Process single metadata block
    
    Args:
        first_line: First line containing metadata type and description
        remaining_lines: Remaining lines containing YAML content
        metadata: Metadata dictionary to update
        engine: Database engine name
    """
    # Parse block type and description
    match = re.match(r'(\w+)\.(\w+)(?:\s*:\s*(.*))?', first_line)
    if not match:
        raise ParsingError(f"Invalid metadata block format: {first_line}")
    
    category, func_name, description = match.groups()
    
    # Process YAML lines and handle SQL blocks
    processed_lines = []
    in_sql_block = False
    sql_lines = []
    
    for line in remaining_lines:
        # Skip empty lines
        if not line.strip():
            continue
            
        # Remove comment marker if present
        if line.lstrip().startswith('--'):
            line = re.sub(r'^\s*--\s?', '', line)
        
        # Check for YAML pipe symbol
        if line.rstrip() == 'query: |':
            in_sql_block = True
            processed_lines.append(line)
            continue
            
        if in_sql_block:
            sql_lines.append(line)
        else:
            processed_lines.append(line)
    
    # Parse YAML content
    yaml_text = '\n'.join(processed_lines)
    try:
        params = yaml.safe_load(yaml_text) or {}
    except yaml.YAMLError as e:
        raise ParsingError(f"Invalid YAML in metadata block: {str(e)}")
    
    # If we had a SQL block, add it to params
    if sql_lines:
        # Find minimum indentation to properly dedent SQL
        non_empty_lines = [line for line in sql_lines if line.strip()]
        if non_empty_lines:
            min_indent = min(len(line) - len(line.lstrip()) for line in non_empty_lines)
            sql_text = '\n'.join(line[min_indent:] for line in sql_lines)
            params['query'] = sql_text.strip()
    
    # For meta.engine, we don't need to get the function
    if category == 'meta' and func_name == 'engine':
        params['type'] = params.get('type', engine)
        block = {
            'type': func_name,
            'params': params,
            'description': description
        }
    else:
        # Get function implementation
        try:
            func = get_function(category, func_name, engine)
        except (ImportError, AttributeError) as e:
            raise ParsingError(f"Unknown function {category}.{func_name} for engine {engine}: {str(e)}")
        
        block = {
            'type': func_name,
            'params': params,
            'function': func,
            'description': description
        }
    
    # Add block to appropriate category
    if category == 'meta':
        metadata['meta'][func_name] = block
    elif category == 'target':
        if metadata['target']:
            raise ParsingError("Multiple target definitions found")
        metadata['target'] = block
    elif category == 'strategy':
        metadata['strategy'][func_name] = block
    elif category == 'invariant':
        metadata['invariants'].append(block)
    elif category == 'test':
        metadata['tests'].append(block)
    else:
        raise ParsingError(f"Unknown metadata category: {category}")

def get_function(category: str, name: str, engine: str = 'sql') -> Any:
    """Get function implementation for specific engine
    
    Args:
        category: Metadata category (meta, target, strategy, invariant, test)
        name: Function name
        engine: Database engine name
    
    Returns:
        Function implementation
    
    Raises:
        ImportError: If module not found
        AttributeError: If function not found
    """
    try:
        # Try engine-specific implementation first
        module = importlib.import_module(f'etl_lite.{engine}.{category}s')
    except ImportError:
        # Fall back to generic SQL implementation
        module = importlib.import_module(f'etl_lite.sql.{category}s')
    
    if not hasattr(module, name):
        raise AttributeError(f"Function {name} not found in {module.__name__}")
    
    return getattr(module, name)

# Example usage
if __name__ == "__main__":
    sql_file = """
-- @meta.engine: clickhouse
--   type: clickhouse
--   version: 23.8
--   settings:
--     max_memory_usage: 20000000000
--     max_bytes_before_external_group_by: 20000000000

-- @meta.description: Calculates daily client trading volume
--   This is a multi-line description
--   that provides more details about the step

-- @target.table: Output configuration
--   name: reports.client_volume
--   engine: ReplacingMergeTree
--   order_by: [client_id, trade_date]
--   partition_by: toYYYYMM(trade_date)

-- @strategy.incremental: Process only new data
--   column: trade_date
--   start: latest
--   window: 3 day

-- @invariant.sum: Total volume should stay the same
--   name: total_volume
--   column: amount
--   tolerance: relative(0.001)

-- @test.no_duplicates: Ensure no duplicate records
--   name: unique_clients
--   columns: [client_id, trade_date]

-- @main
SELECT client_id, trade_date, sum(amount)
FROM trades
GROUP BY client_id, trade_date
"""
    
    with open('test.sql', 'w') as f:
        f.write(sql_file)
    
    metadata = parse_sql_file(Path('test.sql'))
    print(f"Engine: {metadata.engine}")
    print(f"Engine settings: {metadata.engine_settings}")
    print(metadata)