/**
 * Core type definitions for the Trading Analyzer — Ingestion Layer (Phase 1)
 */

// ─── Extractor Configuration ─────────────────────────────────────────────────

export interface ExtractorConfig {
  /** YouTube Live URL to capture */
  youtubeUrl: string;
  /** Root directory for all output files */
  outputDir: string;
  /** Frames per second to extract from the video stream */
  videoFps: number;
  /** Audio sample rate in Hz (16000 recommended for speech/AI models) */
  audioSampleRate: number;
  /** Number of audio channels (1 = mono, 2 = stereo) */
  audioChannels: number;
  /** Duration in seconds for each audio chunk segment */
  audioChunkDurationSecs: number;
  /** Streamlink quality level: 'best' | '720p' | '480p' | etc. */
  streamlinkQuality: string;
  /** JPEG quality for captured frames (1–31, lower = better) */
  frameJpegQuality: number;
  /** Milliseconds to wait before retrying after a stream drops (default: 5 000) */
  retryDelayMs?: number;
  /** Maximum number of retry attempts; 0 = unlimited (default: 0) */
  maxRetries?: number;
}

// ─── Pipeline Stats ───────────────────────────────────────────────────────────

export interface PipelineStats {
  startTime: Date;
  framesCaptured: number;
  audioChunksSaved: number;
  lastFramePath: string | null;
  lastAudioChunkPath: string | null;
}

// ─── Streamlink Result ────────────────────────────────────────────────────────

export interface StreamResolution {
  /** The resolved HLS/DASH stream URL */
  url: string;
  /** The quality level that was resolved */
  quality: string;
}
