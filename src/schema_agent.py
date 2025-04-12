from pinecone import Pinecone, PodSpec, ServerlessSpec
from fastembed import TextEmbedding
import hashlib

class PineconeDB:
    def __init__(self, config):
        if not config:
            raise ValueError("config is required, pass either a Pinecone client or an API key.")
        
        api_key = config.get("api_key")
        if not api_key:
            raise ValueError("api_key is required in config or pass a configured client.")
        self._client=Pinecone(api_key=api_key)
        self.index_name=config.get("index_name")
        self.embedding_model="BAAI/bge-small-en-v1.5"
        self.dimensions=384
        if not self._client.has_index(self.index_name):
            self._client.create_index(
                name=self.index_name,
                vector_type="dense",
                dimension=self.dimensions,
                metric="cosine",
                spec=ServerlessSpec(
                    cloud="aws",
                    region="us-east-1"
                ),
                deletion_protection="disabled",
                tags={
                    "environment": "development"
                }
            )
        self.host=self._client.Index(host=self._client.describe_index(self.index_name).get('host'))
        
        print(self._client.describe_index(self.index_name))

    def generate_id(self,input: str) -> str:
        return hashlib.sha256(input.encode('utf-8')).hexdigest()

    def insert_schema(self,input:str):
        id=self.generate_id(input=input)
        fetch_response=self.host.fetch(ids=[id],namespace="schema")
        if fetch_response.vectors=={}:
            self.host.upsert(vectors=[(id, self.generate_embedding(input), {"text": input})], namespace="schema"
)
            print(f"Inserted successfully, Id:{id}")
        else:
            print("Given input exists")


    def insert_description(self,input:str):
        id=self.generate_id(input=input)
        fetch_response=self.host.fetch(ids=[id],namespace="description")
        if fetch_response.vectors=={}:
            self.host.upsert(vectors=[(id, self.generate_embedding(input), {"text": input})], namespace="description"
)
            print(f"Inserted successfully, Id:{id}")
        else:
            print("Given input exists")
    
    def insert_relation(self,input:str):
        id=self.generate_id(input=input)
        fetch_response=self.host.fetch(ids=[id],namespace="relation")
        if fetch_response.vectors=={}:
            self.host.upsert(vectors=[(id, self.generate_embedding(input), {"text": input},)], namespace="relation"
)
            print(f"Inserted successfully, Id:{id}")
        else:
            print("Given input exists")


    def get_schema(self,question:str):
        res=self.host.query(namespace="schema",vector=self.generate_embedding(question),top_k=10,include_metadata=True,include_values=True)
        return [match["metadata"]["text"] for match in res["matches"]] if res else []

    def get_relation(self,question:str):
        res=self.host.query(namespace="relation",vector=self.generate_embedding(question),top_k=10,include_metadata=True,include_values=True)
        return [match["metadata"]["text"] for match in res["matches"]] if res else []
    
    def get_description(self,question:str):
            res=self.host.query(namespace="description",vector=self.generate_embedding(question),top_k=10,include_metadata=True,include_values=True)
            return [match["metadata"]["text"] for match in res["matches"]] if res else []   

    def generate_embedding(self,input:str):
        embedding_model=TextEmbedding(model_name=self.embedding_model)
        embedding=next(embedding_model.embed(input))
        return embedding.tolist()
