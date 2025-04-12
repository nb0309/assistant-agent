from openai import OpenAI
import openai

class ConversationAgent:
    def __init__(self, config):
        self.config = config
        self.model = config["llm"]["model"]
        self.api_base = config["llm"]["api_base"]
        self.api_key = config["llm"]["api_key"]
        self.max_turns = config["memory"].get("max_turns", 10)
        self.system_prompt = config["prompt"]["system"]

        if config['llm']['api_base'] is None:
            self.api_base="https://api.deepseek.com"
        
        openai.api_base = self.api_base
        openai.api_key = self.api_key
        self.client=OpenAI(api_key=self.api_key,base_url=self.api_base)
        
        self.messages = [{"role": "system", "content": self.system_prompt}]

        self.start_message = config.get("start_message", "Hi! I’m your AI analyst assistant. What goal are you working on today?")
        self.messages.append({"role": "assistant", "content": self.start_message})
        print(f"\n🤖: {self.start_message}")

    def extract_message_and_sql(self, response_text):
        # Pattern to extract SQL code block
        sql_match = re.search(r"```sql\s*(.*?)```", response_text, re.DOTALL)

        if sql_match:
            sql_code = sql_match.group(1).strip()
            # Remove the code block to isolate the message
            message = re.sub(r"```sql\s*.*?```", "", response_text, flags=re.DOTALL).strip()
        else:
            sql_code = None
            message = response_text.strip()

        return message, sql_code

        
    def run(self, user_input: str) -> dict:
        self.messages.append({"role": "user", "content": user_input})

        if len(self.messages) > 2 + self.max_turns * 2:
            self.messages = [self.messages[0], self.messages[1]] + self.messages[-self.max_turns * 2:]

        response = self.client.chat.completions.create(
            model=self.model,
            messages=self.messages
        )

        reply = response.choices[0].message.content.strip()
        self.messages.append({"role": "assistant", "content": reply})

        message, sql_query = self.extract_message_and_sql(reply)

        return {
            "message": message,
            "sql": sql_query
        }



import re

