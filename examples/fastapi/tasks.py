
from threading import Thread
from modelq import ModelQ
from modelq.app.middleware import Middleware
# from PIL import Image
import time
import os
# import torch
# import numpy as np
from redis import Redis
# import base64

imagine_db = Redis(host="localhost", port=6379, db=0)

modelq_app = ModelQ(redis_client = imagine_db)

class CurrentModel:
    def __init__(self):
        self.model = None
        self.config = None
        
    def load_model(self):
        device = "cuda"

        model_path = "/workspace/XTTS-v2"
        model_name = "tts_models/multilingual/multi-dataset/xtts_v2"
        self.model = model_path
        print(f"Loading model from {model_path}...")

CURRENT_MODEL = CurrentModel()

class BeforeWorker(Middleware):
    def before_worker_boot(self):
        print("Loading model...")
        CURRENT_MODEL.load_model()

modelq_app.middleware = BeforeWorker()


# def wav_postprocess(wav):
#     """Post process the output waveform"""
#     if isinstance(wav, list):
#         wav = torch.cat(wav, dim=0)
#     wav = wav.clone().detach().cpu().numpy()
#     wav = np.clip(wav, -1, 1)
#     wav = (wav * 32767).astype(np.int16)
#     return wav

# @modelq.task(timeout=15, stream=True)
# def stream(params):
#     time.sleep(10)
#     gpt_cond_latent, speaker_embedding = CURRENT_MODEL.model.get_conditioning_latents(audio_path=["/workspace/XTTS-v2/samples/en_sample.wav"])
#     streamer = CURRENT_MODEL.model.inference_stream(
#         params,
#         "en",
#         gpt_cond_latent,
#         speaker_embedding,
#         stream_chunk_size=150,
#         enable_text_splitting=True,
#     )
#     for chunk in streamer:
#         processed_chunk = wav_postprocess(chunk)
#         processed_bytes = processed_chunk.tobytes()
#         base64_chunk = base64.b64encode(processed_bytes).decode("utf-8")
#         yield base64_chunk


@modelq_app.task()
def add_task():
    time.sleep(20)
    return 2 + 3


# @modelq.task(timeout=15)
# def image_task():
#     return Image.open("lmao.png")

# @modelq_app.cron_task(interval_seconds=10)
# def cron_task():
#     print(CURRENT_MODEL.model)
#     print("Cron task executed")


# if __name__ == "__main__":
#     modelq_app.start_workers()

#     # Keep the worker running indefinitely
#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         print("\nGracefully shutting down...")