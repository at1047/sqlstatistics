import re
from dataclasses import dataclass
from typing import List
from datetime import datetime

@dataclass
class TableStats:
    table_name: str
    scan_count: int
    logical_reads: int
    physical_reads: int
    read_ahead_reads: int
    lob_logical_reads: int
    lob_physical_reads: int
    lob_read_ahead_reads: int

@dataclass
class QueryStats:
    tables: List[TableStats]
    rows_affected: List[int]
    completion_time: datetime

def parse_stats_file(file_path: str) -> List[QueryStats]:
    with open(file_path, 'r') as f:
        content = f.read()

    # Split the content into individual query blocks
    query_blocks = content.split('\n\n')
    all_stats = []

    for block in query_blocks:
        if not block.strip():
            continue

        tables = []
        rows_affected = []
        completion_time = None

        # Parse each line in the block
        for line in block.strip().split('\n'):
            if line.startswith('Table'):
                # Parse table statistics
                match = re.match(r"Table '([^']+)'. Scan count (\d+), logical reads (\d+), physical reads (\d+), read-ahead reads (\d+), lob logical reads (\d+), lob physical reads (\d+), lob read-ahead reads (\d+)", line)
                if match:
                    table_stats = TableStats(
                        table_name=match.group(1),
                        scan_count=int(match.group(2)),
                        logical_reads=int(match.group(3)),
                        physical_reads=int(match.group(4)),
                        read_ahead_reads=int(match.group(5)),
                        lob_logical_reads=int(match.group(6)),
                        lob_physical_reads=int(match.group(7)),
                        lob_read_ahead_reads=int(match.group(8))
                    )
                    tables.append(table_stats)
            
            elif line.startswith('(') and 'rows affected' in line:
                # Parse rows affected
                match = re.match(r"\((\d+) rows affected\)", line)
                if match:
                    rows_affected.append(int(match.group(1)))
            
            elif line.startswith('Completion time'):
                # Parse completion time
                time_str = line.split('Completion time: ')[1]
                completion_time = datetime.fromisoformat(time_str)

        if tables or rows_affected or completion_time:
            query_stats = QueryStats(
                tables=tables,
                rows_affected=rows_affected,
                completion_time=completion_time
            )
            all_stats.append(query_stats)

    return all_stats

def parse_stats_text(text: str) -> List[QueryStats]:
    # Split the content into individual query blocks
    query_blocks = text.split('\n\n')
    all_stats = []

    for block in query_blocks:
        if not block.strip():
            continue

        tables = []
        rows_affected = []
        completion_time = None

        # Parse each line in the block
        for line in block.strip().split('\n'):
            if line.startswith('Table'):
                # Parse table statistics
                match = re.match(r"Table '([^']+)'. Scan count (\d+), logical reads (\d+), physical reads (\d+), read-ahead reads (\d+), lob logical reads (\d+), lob physical reads (\d+), lob read-ahead reads (\d+)", line)
                if match:
                    table_stats = TableStats(
                        table_name=match.group(1),
                        scan_count=int(match.group(2)),
                        logical_reads=int(match.group(3)),
                        physical_reads=int(match.group(4)),
                        read_ahead_reads=int(match.group(5)),
                        lob_logical_reads=int(match.group(6)),
                        lob_physical_reads=int(match.group(7)),
                        lob_read_ahead_reads=int(match.group(8))
                    )
                    tables.append(table_stats)
            
            elif line.startswith('(') and 'rows affected' in line:
                # Parse rows affected
                match = re.match(r"\((\d+) rows affected\)", line)
                if match:
                    rows_affected.append(int(match.group(1)))
            
            elif line.startswith('Completion time'):
                # Parse completion time
                time_str = line.split('Completion time: ')[1]
                try:
                    completion_time = datetime.fromisoformat(time_str)
                except ValueError:
                    completion_time = None

        if tables or rows_affected or completion_time:
            query_stats = QueryStats(
                tables=tables,
                rows_affected=rows_affected,
                completion_time=completion_time
            )
            all_stats.append(query_stats)

    return all_stats

if __name__ == "__main__":
    # Example usage
    stats = parse_stats_file("fast statistics io.txt")
    
    # Print the parsed statistics
    total_logical_reads = 0
    for i, query in enumerate(stats, 1):
        print(f"\nQuery {i}:")
        query_logical_reads = 0
        for table in query.tables:
            print(f"\nTable: {table.table_name}")
            print(f"  Scan count: {table.scan_count}")
            print(f"  Logical reads: {table.logical_reads}")
            print(f"  Physical reads: {table.physical_reads}")
            query_logical_reads += table.logical_reads
        
        if query.rows_affected:
            print(f"\nRows affected: {query.rows_affected}")
        
        if query.completion_time:
            print(f"Completion time: {query.completion_time}")
        
        print(f"\nTotal logical reads for this query: {query_logical_reads}")
        total_logical_reads += query_logical_reads
    
    print(f"\n{'='*50}")
    print(f"Total logical reads across all queries: {total_logical_reads}")
    print(f"{'='*50}") 