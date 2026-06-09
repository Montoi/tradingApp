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

def sync_analyze_frame(stream_id: str, frame_path: str, transcript: str, streamer_config: dict, streamer_name: str):
    """Synchronous Gemini analysis with full streamer context."""
    if not genai_client:
        return
        
    try:
        # Load image bytes
        with open(frame_path, 'rb') as f:
            image_bytes = f.read()

        # Build context from the streamer's profile
        assets_monitored   = ', '.join(streamer_config.get('assets', ['Desconocido']))
        stream_focus       = streamer_config.get('streamFocus', 'entries')
        commentary_mode    = streamer_config.get('commentaryMode', 'spoken')
        notes              = streamer_config.get('notes', '').strip()
        tracks_entries     = streamer_config.get('tracksEntries', True)

        focus_map = {
            'entries':       'El streamer ejecuta operaciones reales en pantalla.',
            'analysis-only': 'El streamer solo analiza el gráfico, NO ejecuta operaciones.',
            'educational':   'El streamer explica contexto y sesgo del mercado. NO hay entradas en vivo.',
        }
        focus_desc = focus_map.get(stream_focus, focus_map['entries'])

        audio_map = {
            'spoken': 'El streamer habla continuamente. La transcripción es muy relevante.',
            'mixed':  'El streamer habla ocasionalmente. Usa la transcripción cuando esté disponible.',
            'silent': 'El stream no tiene voz. Ignora la transcripción y enfocate solo en el gráfico.',
        }
        audio_desc = audio_map.get(commentary_mode, audio_map['spoken'])

        notes_section = f'Instrucciones adicionales del usuario: "{notes}"' if notes else ''

        prompt = f"""
        Eres un motor de análisis de trading que observa un stream en vivo.

        === PERFIL DEL STREAMER ===
        - Activos que opera: {assets_monitored}
        - Foco: {focus_desc}
        - Audio: {audio_desc}
        - Registra entradas reales: {'Sí' if tracks_entries else 'No'}
        {notes_section}

        === TRANSCRIPCIóN RECIENTE DEL AUDIO ===
        "{transcript if transcript else 'Sin audio disponible.'}"

        === INSTRUCCIONES DE ANÁLISIS ===
        Observa el frame del gráfico y la transcripción. Determina si hay una señal de trading válida.

        REGLAS ESTRICTAS para Stop Loss y Take Profit:
        1. El SL debe colocarse FUERA de la estructura del precio, no dentro de la vela actual.
           - Para LONG: SL debe estar DEBAJO del mínimo del último swing relevante.
           - Para SHORT: SL debe estar ENCIMA del máximo del último swing relevante.
        2. El SL mínimo debe ser al menos el 0.3% del precio de entrada para crypto, 
           o al menos 3 pips para forex (XAU/USD = mínimo $1.50 de distancia).
        3. El RR (Risk/Reward) debe ser de al menos 1.5:1. Si no hay TP visible con ese RR, direccion = NEUTRAL.
        4. Si el gráfico no muestra una señal clara o el streamer dice NEUTRAL/sin señal, devuelve direccion = NEUTRAL.
        5. Si el foco es 'solo analiza' o 'educativo', devuelve SIEMPRE direccion = NEUTRAL.
        6. DETECTA LA TEMPORALIDAD: Observa la interfaz de TradingView (o similar) y extrae la temporalidad actual del gráfico (ej. 1m, 5m, 15m, 1H, 4H, 1D). Si no es visible, asume "desconocida".

        === RESPUESTA ===
        Responde SOLAMENTE con este JSON válido (sin markdown, sin texto extra):
        {{
            "activo": "BTC/USDT",
            "temporalidad": "15m",
            "direccion": "LONG" | "SHORT" | "NEUTRAL",
            "precio_entrada": 65000.50,
            "stop_loss": 64700.00,
            "take_profit": 65750.00,
            "razon_tecnica": "Descripción clara de por qué esta entrada, dónde está el SL y por qué."
        }}
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

        # Basic sanity check on SL distance before passing to decision engine
        direccion = data.get('direccion', 'NEUTRAL').upper()
        entrada   = float(data.get('precio_entrada', 0) or 0)
        sl        = float(data.get('stop_loss', 0) or 0)
        tp        = float(data.get('take_profit', 0) or 0)
        activo    = data.get('activo', '').upper()
        temporalidad = data.get('temporalidad', '').lower()

        # Parse timeframe
        min_sl_pct = 0.3  # Default crypto
        min_sl_usd = 1.5  # Default forex

        if temporalidad in ['1m', '3m', '5m']:
            min_sl_pct, min_sl_usd = 0.1, 0.5
        elif temporalidad in ['15m', '30m']:
            min_sl_pct, min_sl_usd = 0.3, 1.5
        elif temporalidad in ['1h', '2h', '4h']:
            min_sl_pct, min_sl_usd = 0.7, 3.0
        elif temporalidad in ['1d', '1w']:
            min_sl_pct, min_sl_usd = 1.5, 8.0

        if direccion in ('LONG', 'SHORT') and entrada > 0 and sl > 0:
            sl_dist = abs(entrada - sl)
            sl_pct = sl_dist / entrada * 100
            rr = abs(tp - entrada) / sl_dist if sl_dist > 0 else 0

            # Check SL
            is_forex = 'USD' in activo and 'BTC' not in activo and 'ETH' not in activo
            if is_forex:
                if sl_dist < min_sl_usd:
                    log.warning(f"[{stream_id}] SL muy ajustado para forex/commodities en {temporalidad} (${sl_dist:.2f} < ${min_sl_usd}). Señal descartada.")
                    data['direccion'] = 'NEUTRAL'
            else:
                if sl_pct < min_sl_pct:
                    log.warning(f"[{stream_id}] SL muy ajustado para crypto en {temporalidad} ({sl_pct:.3f}% < {min_sl_pct}%). Señal descartada.")
                    data['direccion'] = 'NEUTRAL'

            if rr < 1.2:        # RR below 1.2 — not worth the trade
                log.warning(f"[{stream_id}] RR insuficiente ({rr:.2f}:1). Señal descartada.")
                data['direccion'] = 'NEUTRAL'
        
        # Pass to decision engine
        if stream_id not in stream_engines:
            stream_engines[stream_id] = DecisionEngine(stream_id, streamer_name=streamer_name)
            
        stream_engines[stream_id].process_signal(data)
        
    except Exception as e:
        log.error(f"[{stream_id}] Gemini Analysis Error: {e}")

async def process_stream(stream_id: str):
    """Async loop running for a specific stream."""
    log.info(f"[{stream_id}] Started monitoring.")

    # Fetch the streamer's config from the orchestrator API
    streamer_config = {}
    streamer_name = stream_id
    try:
        def fetch_streamer_info():
            import urllib.request
            url = f"{ORCHESTRATOR_URL}/api/streamers"
            with urllib.request.urlopen(url, timeout=5) as res:
                streamers = json.loads(res.read().decode())
                for s in streamers:
                    if s.get('stream_id') == stream_id:
                        return s
            return {}
        streamer_info = await asyncio.get_running_loop().run_in_executor(executor, fetch_streamer_info)
        streamer_config = streamer_info.get('config_json') or {}
        streamer_name = streamer_info.get('name') or stream_id
        log.info(f"[{stream_id}] Config loaded: name={streamer_name} focus={streamer_config.get('streamFocus','?')} assets={streamer_config.get('assets','?')}")
    except Exception as e:
        log.warning(f"[{stream_id}] Could not fetch streamer config: {e}. Using defaults.")
    
    frames_dir = OUTPUT_DIR / stream_id / 'frames'
    audio_dir  = OUTPUT_DIR / stream_id / 'audio'
    
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
                            executor, sync_analyze_frame, stream_id, str(latest_jpg), transcript, streamer_config, streamer_name
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
