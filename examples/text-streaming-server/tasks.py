
from modelq import ModelQ
from modelq.app.middleware import Middleware
import os
import torch
import numpy as np

from redis import Redis

from transformers import AutoModelForCausalLM, AutoTokenizer, TextIteratorStreamer
from threading import Thread

from huggingface_hub import login


login("your token here")  # Replace with your Hugging Face token

redis_client = Redis(host="localhost", port=6379, db=0)
modelq_app = ModelQ(redis_client = redis_client)


class Model:
    def __init__(self):
        self.model = None
        self.tok = None
        
    def load_model(self):
        device = "cuda"

        self.tok = AutoTokenizer.from_pretrained("openai-community/gpt2")
        self.model = AutoModelForCausalLM.from_pretrained("openai-community/gpt2")


MODEL = Model()

class ModelInit(Middleware):
    def before_worker_boot(self):
        MODEL.load_model()

modelq_app.middleware = ModelInit()

@modelq_app.task(timeout=15, stream=True)
def stream(params):

    inputs = MODEL.tok([params], return_tensors="pt")
    streamer = TextIteratorStreamer(MODEL.tok)
    generation_kwargs = dict(inputs, streamer=streamer, max_new_tokens=20)
    thread = Thread(target=MODEL.model.generate, kwargs=generation_kwargs)
    thread.start()

    for new_text in streamer:
        yield new_text
