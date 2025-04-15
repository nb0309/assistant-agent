import pandas as pd
import mysql.connector
import re

# Establish connection once
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="nava",
    database="newcompany",
    port=3306
)


def run_sql(sql: str):
    """
    Executes a SQL query on the connected MySQL database.
    Returns a pandas DataFrame if SELECT, else commits the transaction.
    Stores SELECT results in `last_result_df`.
    """
    global last_result_df

    try:
        with conn.cursor() as cursor:
            # Strip comments and whitespace before checking if it's a SELECT
            sql_clean = re.sub(r"(--.*?$|/\*.*?\*/)", "", sql, flags=re.MULTILINE | re.DOTALL).strip().lower()
            is_select = sql_clean.startswith("select")

            cursor.execute(sql)

            if is_select:
                rows = cursor.fetchall()
                df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
                last_result_df = df.copy()
                return df
            else:
                # Clear unread results
                while cursor.nextset():
                    pass
                conn.commit()
                print("Query executed and committed successfully.")
                last_result_df = pd.DataFrame()
                return pd.DataFrame([{"message": "Query executed successfully."}])

    except mysql.connector.Error as e:
        conn.rollback()
        print(f"Error: {e}")
        last_result_df = pd.DataFrame([{"error": str(e)}])
        return last_result_df


import pandas as pd
import mysql.connector
import re

# Establish connection once
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="nava",
    database="newcompany",
    port=3306
)


last_result_df = pd.DataFrame()  # Global to store the last SELECT result

import re
import pandas as pd
import mysql.connector

def run_sql(sql: str):
    """
    Executes a SQL query on the connected MySQL database.
    Returns a pandas DataFrame if the query returns results.
    Stores results in `last_result_df`.
    """
    global last_result_df
    try:
        with conn.cursor() as cursor:
            # Clean up the SQL for checks
            sql_clean = re.sub(r"(--.*?$|/\*.*?\*/)", "", sql, flags=re.MULTILINE | re.DOTALL).strip().lower()
            is_result_returning = sql_clean.startswith(("select", "desc", "describe", "show", "with"))

            cursor.execute(sql)

            if is_result_returning:
                rows = cursor.fetchall()
                df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
                last_result_df = df.copy()
                return df
            else:
                # Clear unread results
                while cursor.nextset():
                    pass
                conn.commit()
                print("Query executed and committed successfully.")
                last_result_df = pd.DataFrame()
                return pd.DataFrame([{"message": "Query executed successfully."}])

    except mysql.connector.Error as e:
        conn.rollback()
        print(f"Error: {e}")
        last_result_df = pd.DataFrame([{"error": str(e)}])
        return last_result_df



import re

def extract_sql(llm_response: str) -> str:
    """
    Extracts only the SQL query from the LLM response.
    Detects SQL blocks using markdown, or valid SQL patterns like CREATE/SELECT/WITH with structure checks.
    """

    # 1. Try extracting from ```sql markdown blocks
    sql_blocks = re.findall(r"```sql\s*\n(.*?)```", llm_response, re.DOTALL | re.IGNORECASE)
    if sql_blocks:
        return sql_blocks[-1].strip()

    # 2. Try generic ``` blocks (if not marked as sql)
    generic_blocks = re.findall(r"```(.*?)```", llm_response, re.DOTALL)
    if generic_blocks:
        return generic_blocks[-1].strip()

    # 3. Look for valid SQL beginning from CREATE TABLE ... AS
    create_match = re.search(
        r"(CREATE\s+TABLE\s+\w+\s+AS\s+SELECT\b.*?;)", 
        llm_response, 
        re.IGNORECASE | re.DOTALL
    )
    if create_match:
        return create_match.group(1).strip()

    # 4. Look for WITH <cte> AS (...) SELECT ...;
    with_match = re.search(
        r"(WITH\s+\w+\s+AS\s*\(.*?\)\s*SELECT.*?;)", 
        llm_response, 
        re.IGNORECASE | re.DOTALL
    )
    if with_match:
        return with_match.group(1).strip()

    # 5. Look for SELECT ... FROM ... ;
    select_match = re.search(
        r"(SELECT\s+.*?\s+FROM\s+.*?;)", 
        llm_response, 
        re.IGNORECASE | re.DOTALL
    )
    if select_match:
        return select_match.group(1).strip()

    # 6. Fallback to everything starting from first actual SQL keyword on new line
    fallback_match = re.search(r"(?i)(^|\n)(CREATE\s+TABLE|WITH|SELECT)\b.*", llm_response, re.DOTALL)
    if fallback_match:
        return fallback_match.group(0).strip()

    return llm_response.strip()


message='''WITH ios_users AS (
    SELECT * FROM customer WHERE platform = 'iOS'
)

SELECT
    'Android' AS platform,
    COUNT(*) AS total_users,
    AVG(session_duration) AS avg_session_duration,
    AVG(payment_amount) AS avg_payment,
    COUNT(CASE WHEN downgrade_reason IS NOT NULL THEN 1 END) AS downgraded_users,
    ROUND(COUNT(CASE WHEN downgrade_reason IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS downgrade_percentage
FROM
    android

UNION ALL

SELECT
    'iOS' AS platform,
    COUNT(*) AS total_users,
    AVG(session_duration) AS avg_session_duration,
    AVG(payment_amount) AS avg_payment,
    COUNT(CASE WHEN downgrade_reason IS NOT NULL THEN 1 END) AS downgraded_users,
    ROUND(COUNT(CASE WHEN downgrade_reason IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS downgrade_percentage
FROM
    ios_users;'''
print(run_sql(message))