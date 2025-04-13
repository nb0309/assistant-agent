import streamlit as st
import pandas as pd
from conversation_agent import ConversationAgent
from schema_agent import PineconeDB
from data_ignestion import get_mysql_engine, extract_from_csv, parse_schema, format_schema_for_embedding, format_relationships_for_llm
from sqlalchemy import text
from sql_run import extract_sql, run_sql

# --- Caching Setup ---
@st.cache_resource(show_spinner=False)
def load_engine_and_data():
    sql_engine = get_mysql_engine(user="root", password="nava", host="localhost", port=3306, db_name="newcompany")
    df = extract_from_csv(filepath="C:/Users/navab/Desktop/assistant agent/src/sample_fintech_users.csv", table_name="newcompany", mysql_engine=sql_engine)
    return sql_engine, df

@st.cache_resource(show_spinner=False)
def setup_pinecone():
    config = {
        "api_key": "pcsk_4hsai8_RQy75pmEE4zteajMWCERaiJUpPsgdj4KAutntnoms3grffML15zB579LDQ6EqT6",
        "index_name": "etl",
    }
    return PineconeDB(config=config)

@st.cache_data(show_spinner=False)
def setup_embeddings(_sql_engine, df, _pinecone):
    with _sql_engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'newcompany';"))
    schema = parse_schema(result, db_name="newcompany")
    for line in format_schema_for_embedding(schema):
        _pinecone.insert_schema(line)

    with _sql_engine.connect() as conn:
        result = conn.execute(text("""SELECT TABLE_NAME AS child_table, COLUMN_NAME AS child_column, 
                                      REFERENCED_TABLE_NAME AS parent_table, REFERENCED_COLUMN_NAME AS parent_column 
                                      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                                      WHERE TABLE_SCHEMA = 'newcompany' AND REFERENCED_TABLE_NAME IS NOT NULL;"""))
    for row in result:
        _pinecone.insert_relation(format_relationships_for_llm(row))

    _pinecone.insert_description("this is a data that contains information about customers who are leaving a particular company for various reasons. focus on the data types of each column, if the data type is text and column name is date, then you have to preprocess the column to extract text if possible.")
    _pinecone.insert_description(f"{df.get('data').head()} this is just an example of the data that is in our database, go through this to have an understanding of how the data is")

    schema_text = _pinecone.get_schema("give me the schema")
    relationship = _pinecone.get_relation("give me the schema")
    desc = _pinecone.get_description("give example of the data")

    return schema_text, relationship, desc



@st.cache_resource(show_spinner=False)
def setup_agent(schema_text, relationship, desc):
    conversation_config = {
        "id": "conversation-agent",
        "name": "ConversationAgent",
        "role": "AI financial analyst assistant",
        "llm": {
            "provider": "deepseek",
            "model": "deepseek-chat",
            "api_base": "https://api.deepseek.com",
            "api_key": "sk-093c3391ec134c55b76decd5225f703a"
        },
        "prompt": {
            "system": f"""
You are an AI data analyst assistant. Your job is to help users analyze their data based on their business goals, and you will generate relevant SQL queries. You will work with the provided schema, relationships, and dataset descriptions to guide users in refining their goals and generate precise SQL queries.

## Your Responsibilities:
1. **Start by asking**: "What business goal are you trying to achieve with your data?"
2. After the user shares their goal, **refine it** with follow-up questions (examples below):
   - Are you interested in a specific time period (e.g., last 6 months, last year)?
   - Do you want to segment by attributes like `plan`, `platform`, `status`, etc.?
   - Are you focusing on metrics like frequency, trends, or correlations?
3. **Clarify any ambiguity**: If the goal is broad or unclear, ask questions to break it down and make it specific.
4. Once the goal is defined, **generate SQL queries** that match the refined goal and provide a concise explanation of what the query does.
5. **Be clear, concise, and actionable** in all responses. Avoid vague conclusions like "Would this meet your needs?" and instead guide the user on next steps.

## Output Structure:
- **Assistant Message**: Explanation and follow-up questions.
- **SQL Query**: Always provide the SQL query in a clearly marked code block, and explain what it does briefly.
- Be aware of SQL dialects:
  - If using **MySQL**, use `STR_TO_DATE(date_column, format)` to convert text to date.
  - Use `CURDATE() - INTERVAL X MONTH` instead of `DATE('now', '-X months')`.
- Always convert date strings to proper date formats using SQL dialect-specific functions (e.g., STR_TO_DATE in MySQL).
- Avoid using SQLite functions like `DATE('now', '-3 months')` unless you're explicitly targeting SQLite.

### Example:

User: *"I want to understand the reasons for downgrades."*

Assistant:  
Great! Understanding downgrade reasons is crucial for improving customer retention and product offerings. Let me refine this further with a few questions:

1. **Time Frame**: Would you like to analyze downgrade reasons for a specific period (e.g., last 6 months, last year), or would you like to include all historical data?
2. **Segmentation**: Should we segment the data by any specific attributes, such as:
   - `plan` (to see if certain plans have more downgrades)
   - `platform` (to compare mobile vs. web users)
   - `status` (e.g., focusing only on active or churned users)?
3. **Metrics**: What metrics are you most interested in? For example:
   - **Count of downgrade reasons**: How often each reason occurred.
   - **Trends over time**: How downgrade reasons change over months/quarters.
   - **Correlations**: Do you want to explore relationships between downgrade reasons and other metrics (e.g., session duration, payment amount)?

Once you provide the answers, I'll help generate a relevant SQL query.

Here’s a **sample SQL query** to count the frequency of each downgrade reason:

```sql
-- Count the number of downgrades by reason
SELECT
    downgrade_reason,
    COUNT(*) AS reason_count
FROM
    customer
WHERE
    downgrade_reason IS NOT NULL
GROUP BY
    downgrade_reason
ORDER BY
    reason_count DESC;
```

The following the schema of data that you will be working with:
{schema_text}

The relationship between the tables in the database that you will be working with are:
{relationship}

Here are some descriptions that you should know about the dataset:
{desc}
""",
            "format": "chat"
        },
        "memory": {
            "type": "chat",
            "strategy": "buffer",
            "max_turns": 10
        },
        "tools": ["schema_agent", "sql_generation_agent"],
        "start_message": "Hi! I’m your AI data analyst assistant. What goal are you working on today?"
    }
    return ConversationAgent(config=conversation_config)

# --- Load Once ---
sql_engine, df = load_engine_and_data()
pinecone = setup_pinecone()
schema_text, relationship, desc = setup_embeddings(sql_engine, df, pinecone)
agent = setup_agent(schema_text, relationship, desc)

# --- Streamlit UI ---
st.set_page_config(layout="wide", page_title="AI SQL Chat Assistant")
st.title("🧠 AI SQL Assistant for FinTech Users")
col1, col2 = st.columns([2, 3])

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
    st.session_state.chat_history.append(("System", agent.config["start_message"], "", None))

# --- Chat Panel ---
with col1:
    st.subheader("💬 Chat with Assistant")
    user_input = st.text_input("Ask a question", placeholder="e.g. Why are customers downgrading?", key="chat_input")

    if st.button("Ask"):
        if user_input:
            reply = agent.run(user_input)
            assistant_msg = reply.get("message")
            sql_query = extract_sql(reply.get("sql"))
            result_df = run_sql(sql_query)
            st.session_state.chat_history.append((user_input, assistant_msg, sql_query, result_df))

    for sender, message, _, _ in reversed(st.session_state.chat_history):
        if sender == "System":
            st.markdown(f"**Assistant:** {message}")
        else:
            st.markdown(f"** You:** {sender}")
            st.markdown(f"** Assistant:** {message}")


# --- SQL Output Panel ---
with col2:
    st.subheader("📝 SQL Query & Output")
    if st.session_state.chat_history:
        latest_entry = st.session_state.chat_history[-1]
        latest_sql = latest_entry[2]
        latest_df = latest_entry[3]

        if latest_sql:
            st.code(latest_sql, language="sql")

        if isinstance(latest_df, pd.DataFrame):
            st.dataframe(latest_df)
        elif latest_df:
            st.text(str(latest_df))
        else:
            st.info("No results to display yet.")
