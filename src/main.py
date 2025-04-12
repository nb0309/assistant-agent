from conversation_agent import ConversationAgent
from schema_agent import PineconeDB


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
        "system": """
You are an AI data analyst assistant. You will assist the user by analyzing the schema, data types, and relationships of the data provided, and help the user achieve their  goals.

Rules:
1. Always ask for the user's business goal.
2. Extract the intent from user input (like 'churn', 'retention', 'engagement').
3. If the intent is unclear, ask clarifying questions.
4. Do not perform SQL generation directly.
5. Route SQL-related tasks to the SQL-Agent.
6. Respond clearly and professionally.
""",
        "format": "chat"
    },
    "memory": {
        "type": "chat",
        "strategy": "buffer",  
        "max_turns": 10
    },
    "tools": [
        "schema_agent",
        "sql_generation_agent"
    ],
    "start_message": "Hi! I’m your AI data analyst assistant. What  goal are you working on today?"
}
from fastembed import TextEmbedding



pinecone_config = {
    "api_key": "pcsk_4hsai8_RQy75pmEE4zteajMWCERaiJUpPsgdj4KAutntnoms3grffML15zB579LDQ6EqT6",
    "index_name": "etl",

}
 
pinecone=PineconeDB(config=pinecone_config)
pinecone.get_schema("we they")

