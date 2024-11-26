from transformers import AutoModelForCausalLM, AutoTokenizer, TextIteratorStreamer
from threading import Thread
from modelq import ModelQ
from modelq.app.middleware import Middleware


modelq = ModelQ()

class BeforeWorker(Middleware):
    def before_worker_boot(self):
        tok = AutoTokenizer.from_pretrained("openai-community/gpt2")
        model = AutoModelForCausalLM.from_pretrained("openai-community/gpt2")

modelq.middleware = BeforeWorker()

@modelq.task(timeout=15, stream=True)
def stream(params):
    tok = AutoTokenizer.from_pretrained("openai-community/gpt2")
    model = AutoModelForCausalLM.from_pretrained("openai-community/gpt2")
    inputs = tok([params], return_tensors="pt")
    streamer = TextIteratorStreamer(tok)
    # # Run the generation in a separate thread, so that we can fetch the generated text in a non-blocking way.
    generation_kwargs = dict(inputs, streamer=streamer, max_new_tokens=20)
    thread = Thread(target=model.generate, kwargs=generation_kwargs)
    thread.start()
    generated_text = ""
    for new_text in streamer:
        yield new_text

@modelq.task(timeout=15)
def add_task():
    return 2 + 3

modelq.start_worker()


