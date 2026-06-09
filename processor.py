"""
processor.py — Phase 2: AI Engine (Async Multi-Stream)

Architecture:
  - Uses asyncio to orchestrate multiple streams concurrently.
  - Queries Orchestrator API to get active STREAM_IDs.
  - For each active stream, polls its dedicated output/[STREAM_ID]/ directory.
  - Uses a single global Whisper model protected by a threading.Lock() to prevent RAM corruption.
  - ThreadPoolExecutor runs blocking tasks (Gemini API, Whisper transcribe, file I/O).
"""

import asyncio
import concurrent.futures
import json
import logging
import os
import threading
import time
from pathlib import Path
from urllib.request import urlopen

from dotenv import load_dotenv
from faster_whisper import WhisperModel
from google import genai
from google.genai import types as genai_types

from decision_engine import DecisionEngine

# ─── Configuration ────────────────────────────────────────────────────────────
load_dotenv()

BASE_DIR   = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / 'output'

# Orchestrator
ORCHESTRATOR_URL = os.getenv('ORCHESTRATOR_URL', 'http://localhost:3001')

# Gemini
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '')
GEMINI_MODEL   = os.getenv('GEMINI_MODEL', 'gemini-2.0-flash')

# Faster-Whisper
WHISPER_MODEL_SIZE = os.getenv('WHISPER_MODEL_SIZE', 'base')
WHISPER_DEVICE     = os.getenv('WHISPER_DEVICE', 'cpu')
WHISPER_COMPUTE    = os.getenv('WHISPER_COMPUTE', 'int8')

POLL_INTERVAL = float(os.getenv('POLL_INTERVAL', '2.0'))
MAX_WORKERS   = int(os.getenv('MAX_WORKERS', '8'))
WAV_MIN_SIZE  = 100_000

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger("AI_ENGINE")

# ─── Globals & Locks ──────────────────────────────────────────────────────────
# Global executor for blocking calls
executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)

# Whisper Lock: Ensures only ONE thread transcribes audio at a time
whisper_lock = threading.Lock()
whisper_model = None  # Lazy-loaded once

# Gemini client
try:
    genai_client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    log.error(f"Failed to initialize Gemini client: {e}")
    genai_client = None

# Stream Processors Tracking
stream_tasks = {}          # stream_id -> asyncio.Task
stream_engines = {}        # stream_id -> DecisionEngine
stream_transcripts = {}    # stream_id -> last_transcript_text

def init_whisper():
    """Load Whisper model globally if not already loaded (Thread-safe)"""
    global whisper_model
    with whisper_lock:
        if whisper_model is None:
            log.info(f"Loading Whisper model ({WHISPER_MODEL_SIZE}) on {WHISPER_DEVICE}...")
            whisper_model = WhisperModel(
                model_size_or_path=WHISPER_MODEL_SIZE,
                device=WHISPER_DEVICE,
                compute_type=WHISPER_COMPUTE
            )
            log.info("Whisper model loaded successfully.")

def sync_transcribe(audio_path: str) -> str:
    """Synchronous transcribe protected by a global lock to prevent RAM crash."""
    init_whisper()
    with whisper_lock:
        try:
            segments, _ = whisper_model.transcribe(audio_path, language="es") # Force ES or use ''
            return " ".join([s.text for s in segments]).strip()
        except Exception as e:
            log.error(f"Whisper transcription error: {e}")
            return ""

def sync_analyze_frame(stream_id: str, frame_path: str, transcript: str):
    """Synchronous Gemini analysis"""
    if not genai_client:
        return
        
    try:
        # Load image bytes
        with open(frame_path, 'rb') as f:
            image_bytes = f.read()
            
        prompt = f"""
        Eres un analista de trading profesional viendo un stream en vivo.
        Revisa este frame del stream.
        Transcripción reciente del audio del streamer: "{transcript}"
        
        Extrae y devuelve un JSON estricto con la siguiente estructura:
        {{
            "activo": "BTC/USDT", 
            "direccion": "LONG" | "SHORT" | "NEUTRAL",
            "precio_entrada": 65000.50,
            "stop_loss": 64000.00,
            "take_profit": 67000.00,
            "razon_tecnica": "Explicación breve de por qué."
        }}
        Solo responde con el JSON, sin formato markdown.
        """
        
        response = genai_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[
                genai_types.Part.from_bytes(data=image_bytes, mime_type='image/jpeg'),
                prompt,
            ]
        )
        
        text = response.text.replace('```json', '').replace('```', '').strip()
        data = json.loads(text)
        
        # Pass to decision engine
        if stream_id not in stream_engines:
            stream_engines[stream_id] = DecisionEngine(stream_id)
            
        stream_engines[stream_id].process_signal(data)
        
    except Exception as e:
        log.error(f"[{stream_id}] Gemini Analysis Error: {e}")

async def process_stream(stream_id: str):
    """Async loop running for a specific stream."""
    log.info(f"[{stream_id}] Started monitoring.")
    
    frames_dir = OUTPUT_DIR / stream_id / 'frames'
    audio_dir = OUTPUT_DIR / stream_id / 'audio'
    
    last_processed_frame = None
    last_processed_audio = None
    
    stream_transcripts[stream_id] = ""

    while True:
        try:
            # 1. Check Audio
            if audio_dir.exists():
                wavs = sorted(audio_dir.glob("*.wav"))
                if wavs:
                    latest_wav = wavs[-1]
                    if latest_wav != last_processed_audio and latest_wav.stat().st_size > WAV_MIN_SIZE:
                        # Offload Whisper to thread pool
                        transcript = await asyncio.get_running_loop().run_in_executor(
                            executor, sync_transcribe, str(latest_wav)
                        )
                        if transcript:
                            stream_transcripts[stream_id] = transcript
                            log.info(f"[{stream_id}] Transcribed: {transcript[:50]}...")
                        last_processed_audio = latest_wav
            
            # 2. Check Frames
            if frames_dir.exists():
                jpgs = sorted(frames_dir.glob("*.jpg"))
                if jpgs:
                    latest_jpg = jpgs[-1]
                    if latest_jpg != last_processed_frame:
                        transcript = stream_transcripts.get(stream_id, "")
                        # Offload Gemini to thread pool
                        await asyncio.get_running_loop().run_in_executor(
                            executor, sync_analyze_frame, stream_id, str(latest_jpg), transcript
                        )
                        last_processed_frame = latest_jpg
                        
        except asyncio.CancelledError:
            log.info(f"[{stream_id}] Task cancelled. Stopping.")
            break
        except Exception as e:
            log.error(f"[{stream_id}] Error in stream loop: {e}")
            
        await asyncio.sleep(POLL_INTERVAL)

async def orchestrator_sync():
    """Polls Orchestrator to sync active streams."""
    api_url = f"{ORCHESTRATOR_URL}/api/streams/active"
    while True:
        try:
            # Fetch active streams
            def fetch():
                with urlopen(api_url, timeout=5) as res:
                    return json.loads(res.read().decode())
            
            active_streams = await asyncio.get_running_loop().run_in_executor(executor, fetch)
            active_set = set(active_streams)
            
            # Start new tasks
            for s_id in active_set:
                if s_id not in stream_tasks or stream_tasks[s_id].done():
                    task = asyncio.create_task(process_stream(s_id))
                    stream_tasks[s_id] = task
            
            # Cancel stopped tasks
            for s_id in list(stream_tasks.keys()):
                if s_id not in active_set:
                    log.info(f"Stopping task for {s_id}")
                    stream_tasks[s_id].cancel()
                    del stream_tasks[s_id]
                    if s_id in stream_engines:
                        del stream_engines[s_id]
                        
        except Exception as e:
            log.warning(f"Could not sync with Orchestrator at {api_url}: {e}")
            
        await asyncio.sleep(10) # Sync every 10 seconds

async def main():
    log.info("Starting AI Engine Orchestrator Sync...")
    await orchestrator_sync()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("AI Engine Shutting Down.")
