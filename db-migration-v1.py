import psycopg2
import configparser
import os
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import io
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.panel import Panel
from rich.table import Table

console = Console()

def read_db_config(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    return {
        'dbname': config['postgresql']['database'],
        'user': config['postgresql']['user'],
        'password': config['postgresql']['password'],
        'host': config['postgresql']['host'],
        'port': config['postgresql']['port']
    }

def get_table_schema(conn, table_name):
    """Get the CREATE TABLE statement for a given table"""
    with conn.cursor() as cur:
        # Get column definitions
        cur.execute("""
            SELECT column_name, data_type, character_maximum_length,
                   is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        
        columns = []
        for col in cur.fetchall():
            name, data_type, max_length, is_nullable, default = col
            column_def = f"{name} {data_type}"
            if max_length:
                column_def += f"({max_length})"
            if default:
                column_def += f" DEFAULT {default}"
            if is_nullable == 'NO':
                column_def += " NOT NULL"
            columns.append(column_def)

        # Get primary key constraint
        cur.execute("""
            SELECT c.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage AS ccu 
                ON ccu.constraint_name = tc.constraint_name
            JOIN information_schema.columns AS c 
                ON c.table_name = tc.table_name 
                AND c.column_name = ccu.column_name
            WHERE tc.constraint_type = 'PRIMARY KEY' 
                AND tc.table_name = %s;
        """, (table_name,))
        
        pk_columns = [row[0] for row in cur.fetchall()]
        if pk_columns:
            columns.append(f"PRIMARY KEY ({', '.join(pk_columns)})")

        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    " + ",\n    ".join(columns) + "\n)"

def ensure_table_exists(source_conn, target_conn, table_name):
    """Ensure the table exists in the target database with the same schema as source"""
    create_table_sql = get_table_schema(source_conn, table_name)
    with target_conn.cursor() as target_cur:
        target_cur.execute(create_table_sql)
        target_conn.commit()


def get_database_size(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pg_database_size(current_database()) as size
        """)
        size_bytes = cur.fetchone()[0]
        
        # Convert to appropriate unit
        units = ['B', 'KB', 'MB', 'GB', 'TB']
        index = 0
        size = float(size_bytes)
        while size >= 1024 and index < len(units) - 1:
            size /= 1024
            index += 1
        return f"{size:.2f} {units[index]}"

def get_table_size(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT pg_total_relation_size('{table_name}') as size
        """)
        size_bytes = cur.fetchone()[0]
        
        units = ['B', 'KB', 'MB', 'GB', 'TB']
        index = 0
        size = float(size_bytes)
        while size >= 1024 and index < len(units) - 1:
            size /= 1024
            index += 1
        return f"{size:.2f} {units[index]}"

def get_table_stats(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cur.fetchone()[0]

def print_migration_summary(tables_info, source_size, target_size):
    table = Table(title="Migration Summary", show_header=True, header_style="bold magenta")
    table.add_column("Table Name", style="cyan")
    table.add_column("Source Rows", justify="right", style="green")
    table.add_column("Target Rows", justify="right", style="blue")
    table.add_column("Source Size", justify="right", style="green")
    table.add_column("Target Size", justify="right", style="blue")
    table.add_column("Status", style="yellow")
    
    total_source = total_target = 0
    
    for info in tables_info:
        table.add_row(
            info['table'],
            str(info['source_rows']),
            str(info['target_rows']),
            info['source_size'],
            info['target_size'],
            "✅ Success" if info['success'] else "❌ Failed"
        )
        total_source += info['source_rows']
        total_target += info['target_rows']
    
    table.add_row(
        "Total", 
        str(total_source), 
        str(total_target),
        source_size,
        target_size,
        "✨ Complete",
        style="bold"
    )
    console.print("\n")
    console.print(table)

def copy_table_data(source_conn, target_conn, table_name, progress, task_id):
    try:
        # First ensure the table exists in the target database
        ensure_table_exists(source_conn, target_conn, table_name)

        with source_conn.cursor() as source_cur, target_conn.cursor() as target_cur:
            source_rows = get_table_stats(source_conn, table_name)
            progress.update(task_id, total=source_rows)
            
            source_size = get_table_size(source_conn, table_name)
            
            # Create temp table for staging the data
            temp_table = f"temp_{table_name}"
            source_cur.execute("""
                SELECT column_name, data_type, character_maximum_length 
                FROM information_schema.columns 
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            
            columns_def = ', '.join(
                f"{col[0]} {col[1]}" + (f"({col[2]})" if col[2] else '')
                for col in source_cur.fetchall()
            )
            
            # Drop temp table if it exists and create new one
            target_cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
            target_cur.execute(f"CREATE TEMP TABLE {temp_table} ({columns_def})")
            
            # Copy data using CSV format
            output = io.StringIO()
            source_cur.copy_expert(f"COPY {table_name} TO STDOUT WITH CSV", output)
            output.seek(0)
            target_cur.copy_expert(f"COPY {temp_table} FROM STDIN WITH CSV", output)
            
            # Insert only new records
            target_cur.execute(f"""
                INSERT INTO {table_name}
                SELECT * FROM {temp_table} t
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} m
                    WHERE m.timestamp = t.timestamp
                )
            """)
            
            rows_inserted = target_cur.rowcount
            target_conn.commit()
            
            progress.update(task_id, advance=source_rows)
            target_rows = get_table_stats(target_conn, table_name)
            target_size = get_table_size(target_conn, table_name)
            
            return {
                'table': table_name,
                'source_rows': source_rows,
                'target_rows': target_rows,
                'source_size': source_size,
                'target_size': target_size,
                'success': True
            }
            
    except Exception as e:
        console.print(f"[red]Error copying {table_name}: {str(e)}[/red]")
        target_conn.rollback()
        return {
            'table': table_name,
            'source_rows': source_rows if 'source_rows' in locals() else 0,
            'target_rows': 0,
            'source_size': source_size if 'source_size' in locals() else '0 B',
            'target_size': '0 B',
            'success': False
        }

def process_tables(source_params, target_params, tables):
    source_conn = psycopg2.connect(**source_params)
    target_conn = psycopg2.connect(**target_params)
    
    source_total_size = get_database_size(source_conn)
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console
    ) as progress:
        
        tables_info = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            
            for table in tables:
                task_id = progress.add_task(f"[cyan]Migrating {table}", total=None)
                future = executor.submit(
                    copy_table_data,
                    psycopg2.connect(**source_params),
                    psycopg2.connect(**target_params),
                    table,
                    progress,
                    task_id
                )
                futures.append((future, table))
            
            for future, _ in futures:
                tables_info.append(future.result())
    
    target_total_size = get_database_size(target_conn)
    
    source_conn.close()
    target_conn.close()
    
    return tables_info, source_total_size, target_total_size

def main():
    try:
        with console.status("[bold green]Reading configuration...") as status:
            option_chain_params = read_db_config(os.path.join('api', 'ini', 'test.ini'))
            option_data_params = read_db_config(os.path.join('api', 'ini', 'optiondata.ini'))
            
            with psycopg2.connect(**option_chain_params) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public'
                    """)
                    tables = [table[0] for table in cur.fetchall()]
            
            status.update("[bold green]Configuration loaded successfully!")

        console.print(Panel.fit(
            f"[bold]Found {len(tables)} tables to migrate[/bold]",
            border_style="blue"
        ))
        
        tables_info, source_size, target_size = process_tables(option_chain_params, option_data_params, tables)
        print_migration_summary(tables_info, source_size, target_size)

    except Exception as e:
        console.print(f"[bold red]Migration failed: {str(e)}[/bold red]")
        raise

if __name__ == "__main__":
    console.print("[bold blue]Database Migration Tool[/bold blue]")
    console.print("=" * 50)
    main()
    console.print("\n[bold green]Migration completed![/bold green]")
