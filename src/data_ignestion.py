import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import MetaData, Table
import pandas as pd
import pymysql
from collections import defaultdict


def get_mysql_engine(user: str, password: str, host: str, port: int, db_name: str):
    """
    Returns a SQLAlchemy engine connected to the MySQL database.
    If the database does not exist, it is created.
    """
    try:
        base_connection_str = f"mysql+pymysql://{user}:{password}@{host}:{port}"
        base_engine = create_engine(base_connection_str)
        
        with base_engine.connect() as conn:
            result = conn.execute(text("SHOW DATABASES LIKE :db"), {"db": db_name})
            if not result.fetchone():
                conn.execute(text(f"CREATE DATABASE {db_name}"))
                print(f"[MySQL] Database '{db_name}' created.")

        full_connection_str = f"{base_connection_str}/{db_name}"
        engine = create_engine(full_connection_str)

        with engine.connect() as connection:
            print(f"[MySQL] Successfully connected to {db_name} at {host}:{port}")
        
        return engine

    except Exception as e:
        print(f"[MySQL] Connection failed: {e}")
        return None

def extract_from_csv(filepath: str, table_name: str, mysql_engine):
    """
    Extract data from a CSV file, store it in MySQL, and return the DataFrame.
    """
    try:
        df = pd.read_csv(filepath, encoding='latin')
        print(f"\n[CSV] Preview of {filepath}:")
        print(df.head())

        # Store in DB
        df.to_sql(table_name, con=mysql_engine, if_exists='replace', index=False)
        print(f"[CSV] Data successfully inserted into MySQL table: {table_name}")

        # Return success flag + DataFrame
        return {"success": True, "data": df}
    
    except Exception as e:
        print(f"[CSV] Error: {e}")
        return {"success": False, "error": str(e)}


def extract_all_tables_with_relations(source_engine, mysql_engine):
    """
    Extract all tables from a source DB, including relationships (foreign keys),
    and load them into MySQL in a dependency-respecting order.
    """
    try:
        metadata = MetaData()
        metadata.reflect(bind=source_engine)

        sorted_tables = metadata.sorted_tables

        print(f"[DB] Found {len(sorted_tables)} tables in source database.")
        
        table_data = []
        for table in sorted_tables:
            table_name = table.name
            print(f"\n[DB] Processing table: {table_name}")
            
            df = pd.read_sql_table(table_name, con=source_engine)
            print(df.head())

            df.to_sql(table_name, con=mysql_engine, if_exists='replace', index=False)
            print(f"[DB] -> Inserted into MySQL: {table_name}")
            
            table_data.append({"Table Name": table_name, "Row Count": len(df)})

        # Display tables in a nice format
        table_summary = pd.DataFrame(table_data)
        print("\n[DB] Summary of Tables Processed:")
        print(table_summary.to_string(index=False))
    
    except Exception as e:
        print(f"[DB] Error during full schema migration: {e}")


def parse_schema(raw_schema: list, db_name: str) -> dict:
    """
    Parse MySQL schema rows into a table → [(col, type)] mapping.
    Skips rows where table name == database name.
    """
    from collections import defaultdict

    table_columns = defaultdict(list)

    for row in raw_schema:
        table = row[2]  # table name
        column = row[3]
        datatype = row[7]

        # ✅ Filter out rows where the table name is actually the database name
        if table.lower() != db_name.lower():
            table_columns[table].append((column, datatype))

    return table_columns


def format_schema_for_embedding(table_columns: dict) -> list[str]:
    formatted = []
    for table, columns in table_columns.items():
        cols = ", ".join([f"{col} ({dtype})" for col, dtype in columns])
        formatted.append(f"Table: {table} | Columns: {cols}")
    return formatted    


def format_relationships_for_llm(rows):
    rels = []
    for r in rows:
        rels.append(f"{r['child_table']}.{r['child_column']} → {r['parent_table']}.{r['parent_column']}")
    return "Relationships:\n" + "\n".join(f"- {r}" for r in rels)