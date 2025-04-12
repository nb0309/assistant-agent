import pandas as pd

import mysql.connector
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="nava",
    database="newcompany",
    port=3306
)
cursor=conn.cursor()

def run_sql(sql: str):
    import pandas as pd
    import mysql.connector

    if conn:
        try:
            cursor = conn.cursor()
            is_select = sql.strip().lower().startswith("select")

            cursor.execute(sql)

            if is_select:
                rows = cursor.fetchall()
                df = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
                return df
            else:
                conn.commit()
                print("Query executed and committed successfully.")

        except mysql.connector.Error as e:
            conn.rollback()
            print(f"Error: {e}")
            raise KeyError(e)

        finally:
            cursor.close()

        
def extract_sql( llm_response: str) -> str:
        import re
        """
        Extracts the SQL query from the LLM response, handling various formats including:
        - WITH clause
        - SELECT statement
        - CREATE TABLE AS SELECT
        - Markdown code blocks
        """

        sqls = re.findall(r"\bCREATE\s+TABLE\b.*?\bAS\b.*?;", llm_response, re.DOTALL | re.IGNORECASE)
        if sqls:
            sql = sqls[-1]
            print(title="Extracted SQL", message=f"{sql}")
            return sql

        sqls = re.findall(r"\bWITH\b .*?;", llm_response, re.DOTALL | re.IGNORECASE)
        if sqls:
            sql = sqls[-1]
            print(title="Extracted SQL", message=f"{sql}")
            return sql

        sqls = re.findall(r"\bSELECT\b .*?;", llm_response, re.DOTALL | re.IGNORECASE)
        if sqls:
            sql = sqls[-1]
            print(title="Extracted SQL", message=f"{sql}")
            return sql

        sqls = re.findall(r"```sql\s*\n(.*?)```", llm_response, re.DOTALL | re.IGNORECASE)
        if sqls:
            sql = sqls[-1].strip()
            print(title="Extracted SQL", message=f"{sql}")
            return sql

        sqls = re.findall(r"```(.*?)```", llm_response, re.DOTALL | re.IGNORECASE)
        if sqls:
            sql = sqls[-1].strip()
            print(title="Extracted SQL", message=f"{sql}")
            return sql

        return llm_response