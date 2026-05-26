"""
processor.py — Phase 2: AI Engine  (v2 — polling-based)

Architecture:
  ┌──────────────────────────────┐
  │     polling_loop()  (2s)     │  Scans output/ every POLL_INTERVAL seconds
  │  ┌───────────────────────┐   │    → detects NEW or MODIFIED files reliably
  │  │  FRAMES (*.jpg)       │   │    → handles file overwrites (no watchdog needed)
  │  │  latest unprocessed   │───┼──► ThreadPoolExecutor ──► analyze_frame()
  │  │                       │   │                               │
  │  │  AUDIO  (*.wav)       │   │                               ▼
  │  │  size-stable chunks   │───┼──► ThreadPoolExecutor ──► transcribe_audio()
  │  └───────────────────────┘   │                               │
  └──────────────────────────────┘              _last_transcript (shared, lock-protected)
                                                        │
                                        fused into every Gemini Vision prompt

Why polling instead of watchdog?
  FFmpeg overwrites existing frame files (frame_000001.jpg → on_modified, not on_created).
  Watchdog's on_created never fires for overwrites. A simple polling loop handles both
  new files and overwrites with zero race conditions.

Why track the latest frame only?
  Gemini takes ~1-2s per call. If we queued every frame (1/s), the backlog would grow
  unbounded. We always analyze the freshest frame available.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

from dotenv import load_dotenv
from faster_whisper import WhisperModel
from google import genai
from google.genai import types as genai_types

from decision_engine import DecisionEngine

# ─── Configuration ────────────────────────────────────────────────────────────

load_dotenv()

BASE_DIR   = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / 'output'
FRAMES_DIR = OUTPUT_DIR / 'frames'
AUDIO_DIR  = OUTPUT_DIR / 'audio'

# Gemini
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '')
GEMINI_MODEL   = os.getenv('GEMINI_MODEL', 'gemini-2.0-flash')

# Faster-Whisper
WHISPER_MODEL_SIZE = os.getenv('WHISPER_MODEL_SIZE', 'base')
WHISPER_LANGUAGE   = os.getenv('WHISPER_LANGUAGE', '')   # '' = auto-detect
WHISPER_DEVICE     = os.getenv('WHISPER_DEVICE', 'cpu')
WHISPER_COMPUTE    = os.getenv('WHISPER_COMPUTE', 'int8')

# Polling
POLL_INTERVAL = float(os.getenv('POLL_INTERVAL', '2.0'))  # seconds
MAX_WORKERS   = int(os.getenv('MAX_WORKERS', '4'))

# WAV stability: a chunk must have the same file size for 2 consecutive polls
# (2 × POLL_INTERVAL seconds) before we consider it fully written by FFmpeg.
WAV_MIN_SIZE = 100_000  # bytes — ignore tiny/incomplete files

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

# ─── Shared State (thread-safe) ───────────────────────────────────────────────

_transcript_lock      = threading.Lock()
_last_transcript: str = ''   # written by audio thread, read by vision thread
_stop_event           = threading.Event()

engine = DecisionEngine(required_confirmations=2, cooldown_minutes=15)


def get_last_transcript() -> str:
    with _transcript_lock:
        return _last_transcript


def set_last_transcript(text: str) -> None:
    global _last_transcript
    with _transcript_lock:
        _last_transcript = text


# ─── Gemini Analysis ─────────────────────────────────────────────────────────

_VISION_PROMPT = """\
Eres un analista de trading cuantitativo estricto. Tu tarea es extraer señales estructuradas.
Se te proporciona:
  1. Un fotograma del gráfico de un stream de YouTube de trading.
  2. Contexto de audio (transcripción): {transcript_context}

Instrucciones Críticas:
- Realiza TU PROPIO análisis técnico puramente visual basándote en la acción del precio, velas, soportes, resistencias e indicadores visibles en la imagen.
- ¡MUY IMPORTANTE!: Presta especial atención a si el streamer acaba de abrir una operación visualmente (ej. dibujó una herramienta de Posición Larga/Corta de TradingView, o se ve una orden abierta en un exchange como Binance/Bybit). Si ves una posición claramente abierta en pantalla, extrae esos datos (Entrada, SL, TP) aunque el audio no diga nada al respecto.
- Si hay contexto de audio (transcripción), compáralo con tu propio análisis. NO entres en LONG o SHORT solo porque el audio lo diga. Si tu análisis visual contradice el audio, declara NEUTRAL o descártalo.
- ¡ENFOQUE SCALPING!: El usuario es un Scalper. Si el streamer o el gráfico sugieren un trade de largo plazo o "Swing", DEBES adaptar la señal para Scalping. Calcula un Take Profit (TP) mucho más cercano (el próximo soporte/resistencia inmediato) y un Stop Loss (SL) muy ajustado. No permitas trades con rangos amplios.
- Si NO hay transcripción de audio o es irrelevante, actúa de manera 100% autónoma. Analiza las líneas trazadas, los textos en pantalla y la estructura del gráfico para tomar una decisión.
- Retorna ÚNICAMENTE un objeto JSON válido con esta estructura estricta y transaccional:

{{
  "activo": "Par o activo analizado, e.g., BTC/USDT",
  "direccion": "LONG | SHORT | NEUTRAL",
  "precio_entrada": 0.0,
  "stop_loss": 0.0,
  "take_profit": 0.0,
  "razon_tecnica": "Explica brevemente tu propio análisis técnico (velas, RSI, EMAs) y, si aplica, cómo coincide o contradice el audio"
}}"""


def analyze_frame(filepath: str, client: genai.Client) -> None:
    """Sends a JPEG frame + latest transcript to Gemini in a single multimodal call."""
    name = Path(filepath).name
    log.info(f'👀 [VISION]  Analyzing → {name}')

    try:
        image_bytes = Path(filepath).read_bytes()

        transcript = get_last_transcript()
        transcript_context = (
            f'"{transcript}"' if transcript
            else '(sin transcripción aún — primer análisis del stream)'
        )

        prompt = _VISION_PROMPT.format(transcript_context=transcript_context)

        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[
                genai_types.Part.from_bytes(data=image_bytes, mime_type='image/jpeg'),
                prompt,
            ],
        )

        raw = response.text.strip()

        # Strip markdown fences if Gemini wraps JSON in ```json ... ```
        if raw.startswith('```'):
            parts = raw.split('```')
            raw = parts[1].lstrip('json').strip() if len(parts) > 1 else raw

        try:
            a = json.loads(raw)
            log.info(
                f'📊 [GEMINI]  {name} → '
                f'activo={a.get("activo","?")}  |  '
                f'dir={a.get("direccion","?")}  |  '
                f'SL={a.get("stop_loss","?")}'
            )
            # Full JSON at DEBUG level (visible with LOG_LEVEL=DEBUG)
            log.debug(f'Full analysis:\n{json.dumps(a, ensure_ascii=False, indent=2)}')

            # Procesar señal en el motor de decisiones
            engine.process_signal(a)

        except json.JSONDecodeError:
            log.info(f'📊 [GEMINI]  {name} → {raw[:200]}')

    except Exception as exc:
        log.error(f'[VISION] Error processing {name}: {exc}')
    finally:
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
                log.debug(f'🗑️  [JANITOR] Eliminado {name}')
        except Exception as e:
            log.debug(f'[JANITOR] No se pudo eliminar {name}: {e}')


# ─── Whisper Transcription ────────────────────────────────────────────────────

def transcribe_audio(filepath: str, model: WhisperModel) -> None:
    """Transcribes a completed WAV chunk and updates the shared transcript."""
    name = Path(filepath).name
    log.info(f'🎙️  [AUDIO]   Transcribing → {name}')

    try:
        language_arg: str | None = WHISPER_LANGUAGE if WHISPER_LANGUAGE else None

        segments, _info = model.transcribe(
            filepath,
            language=language_arg,
            beam_size=5,
            vad_filter=True,
            vad_parameters={'min_silence_duration_ms': 300},
        )

        text = ' '.join(seg.text.strip() for seg in segments).strip()

        if text:
            set_last_transcript(text)
            preview = text[:120] + ('…' if len(text) > 120 else '')
            log.info(f'🗣️  [WHISPER] {name} → "{preview}"')
        else:
            log.info(f'🔇 [WHISPER] {name} → (silence / no speech detected)')

    except Exception as exc:
        log.error(f'[AUDIO] Error transcribing {name}: {exc}')
    finally:
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
                log.debug(f'🗑️  [JANITOR] Eliminado {name}')
        except Exception as e:
            log.debug(f'[JANITOR] No se pudo eliminar {name}: {e}')


# ─── Polling Engine ───────────────────────────────────────────────────────────

def polling_loop(
    executor: ThreadPoolExecutor,
    client: genai.Client,
    whisper: WhisperModel,
) -> None:
    """
    Core loop — scans output/ every POLL_INTERVAL seconds.

    FRAMES strategy:
      Track each file's mtime. When a frame's mtime is newer than our last
      recorded value, it's a new/overwritten file ready for analysis.
      We only submit the LATEST such frame (skip stale ones) so that
      Gemini always sees the freshest chart and never builds a backlog.

    AUDIO strategy:
      Track each WAV file's size across two consecutive polls. If the size
      is identical in both polls AND above WAV_MIN_SIZE, FFmpeg has finished
      writing the chunk → safe to transcribe.
    """
    # frame tracking: filepath → last mtime seen
    frame_mtimes: dict[str, float] = {}
    # audio tracking: filepath → size at last poll
    audio_sizes: dict[str, int] = {}
    # set of audio files already submitted for transcription
    audio_done: set[str] = set()
    # handle to the currently running Gemini future (avoid backlog)
    vision_future: Future | None = None  # type: ignore[type-arg]

    log.info(f'🔄 Polling every {POLL_INTERVAL}s — watching {OUTPUT_DIR}')

    while not _stop_event.is_set():

        # ── FRAMES: find the newest modified/created JPEG ─────────────────────
        if FRAMES_DIR.exists():
            new_frames: list[Path] = []
            for jpg in FRAMES_DIR.glob('*.jpg'):
                fp = str(jpg)
                try:
                    mtime = jpg.stat().st_mtime
                except OSError:
                    continue
                if mtime != frame_mtimes.get(fp):
                    frame_mtimes[fp] = mtime
                    new_frames.append(jpg)

            if new_frames:
                # Sort by mtime → analyze only the very latest frame
                latest = max(new_frames, key=lambda p: frame_mtimes[str(p)])
                if vision_future is None or vision_future.done():
                    vision_future = executor.submit(analyze_frame, str(latest), client)
                else:
                    # Gemini still busy — log skipped frames quietly
                    skipped = len(new_frames) - 1
                    if skipped:
                        log.debug(f'[VISION] Gemini busy, skipped {skipped} intermediate frame(s)')

        # ── AUDIO: detect size-stable WAV chunks ─────────────────────────────
        if AUDIO_DIR.exists():
            for wav in sorted(AUDIO_DIR.glob('*.wav')):
                fp = str(wav)
                if fp in audio_done:
                    continue
                try:
                    size = wav.stat().st_size
                except OSError:
                    continue

                if size < WAV_MIN_SIZE:
                    audio_sizes[fp] = size   # too small, still being written
                    continue

                prev_size = audio_sizes.get(fp, -1)
                if size == prev_size:
                    # Size unchanged since last poll → FFmpeg closed the file
                    audio_done.add(fp)
                    audio_sizes.pop(fp, None)
                    executor.submit(transcribe_audio, fp, whisper)
                else:
                    audio_sizes[fp] = size   # update for next poll

        time.sleep(POLL_INTERVAL)


# ─── Model Loader ─────────────────────────────────────────────────────────────

def load_models() -> tuple[genai.Client, WhisperModel]:
    if not GEMINI_API_KEY:
        raise RuntimeError(
            'GEMINI_API_KEY not set.\n'
            'Add it to your .env file:\n\n'
            '  GEMINI_API_KEY=your_key_here\n\n'
            'Get a free key: https://aistudio.google.com/apikey'
        )

    client = genai.Client(api_key=GEMINI_API_KEY)
    log.info(f'✅ Gemini ready  (model: {GEMINI_MODEL})')

    log.info(
        f'⏳ Loading Faster-Whisper [{WHISPER_MODEL_SIZE}] on {WHISPER_DEVICE} '
        f'(first run downloads model)…'
    )
    whisper = WhisperModel(
        WHISPER_MODEL_SIZE,
        device=WHISPER_DEVICE,
        compute_type=WHISPER_COMPUTE,
    )
    log.info(f'✅ Faster-Whisper [{WHISPER_MODEL_SIZE}] ready')

    return client, whisper


# ─── Janitor ──────────────────────────────────────────────────────────────────

def janitor_loop() -> None:
    """Background daemon to clean up files older than 5 minutes."""
    log.info("🧹 [JANITOR] Hilo de limpieza de basura iniciado (escaneo cada 5 mins).")
    while not _stop_event.is_set():
        try:
            now = time.time()
            for directory in [FRAMES_DIR, AUDIO_DIR]:
                if not directory.exists():
                    continue
                for file in directory.iterdir():
                    if file.is_file():
                        try:
                            # If older than 5 minutes (300 seconds)
                            if now - file.stat().st_ctime > 300:
                                os.remove(str(file))
                                log.info(f"🧹 [JANITOR] Limpieza forzada (huérfano): {file.name}")
                        except Exception:
                            pass
        except Exception as e:
            log.debug(f"[JANITOR] Error en bucle: {e}")
        
        # Sleep for 5 minutes (in short bursts to respect stop_event)
        for _ in range(300):
            if _stop_event.is_set():
                break
            time.sleep(1)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info('─' * 60)
    log.info('  🧠  Trading Analyzer — AI Engine  (Phase 2)')
    log.info('─' * 60)
    log.info(f'Output dir : {OUTPUT_DIR}')
    log.info(f'Gemini     : {GEMINI_MODEL}')
    log.info(f'Whisper    : {WHISPER_MODEL_SIZE} | lang={WHISPER_LANGUAGE or "auto"}')
    log.info(f'Workers    : {MAX_WORKERS} | Poll: {POLL_INTERVAL}s')
    log.info('─' * 60)

    if not OUTPUT_DIR.exists():
        log.error(f'Output directory not found: {OUTPUT_DIR}')
        log.error('Start the Node.js pipeline first:')
        log.error('  npm run dev -- <youtube-live-url>')
        return

    try:
        client, whisper = load_models()
    except RuntimeError as exc:
        log.error(str(exc))
        return

    log.info('')

    # Start Janitor Thread
    janitor_thread = threading.Thread(target=janitor_loop, daemon=True, name="JanitorThread")
    janitor_thread.start()

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS,
        thread_name_prefix='ai_worker',
    ) as executor:
        try:
            polling_loop(executor, client, whisper)
        except KeyboardInterrupt:
            log.info('\nShutting down AI engine…')
            _stop_event.set()

    log.info('AI engine stopped.')


if __name__ == '__main__':
    main()
