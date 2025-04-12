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

        self.messages = [{"role": "system", "content": self.system_prompt}]

        # Start the conversation with the user
        self.start_message = config.get("start_message", "Hi! I’m your AI analyst assistant. What goal are you working on today?")
        self.messages.append({"role": "assistant", "content": self.start_message})
        print(f"\n🤖: {self.start_message}")

    def run(self, user_input: str) -> str:
        self.messages.append({"role": "user", "content": user_input})

        if len(self.messages) > 2 + self.max_turns * 2:
            self.messages = [self.messages[0], self.messages[1]] + self.messages[-self.max_turns * 2:]

        response = openai.chat.completions.create(
            model=self.model,
            messages=self.messages
        )

        reply = response["choices"][0]["message"]["content"]
        self.messages.append({"role": "assistant", "content": reply})
        return reply

