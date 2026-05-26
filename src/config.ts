import path from 'path';
import type { ExtractorConfig } from './types/index.js';

/**
 * Default configuration for the ingestion pipeline.
 * The YouTube URL is read from the first CLI argument at runtime.
 */
export const defaultConfig: ExtractorConfig = {
  // Passed as CLI arg: npm run dev -- <youtube-url>
  youtubeUrl: process.argv[2] ?? '',

  // All output files land under ./output/
  outputDir: path.resolve('output'),

  // 1 frame/sec is enough for chart analysis without overwhelming storage
  videoFps: 1,

  // 16 kHz mono — optimal for Whisper / speech AI models
  audioSampleRate: 16_000,
  audioChannels: 1,

  // 10-second WAV chunks for audio AI processing
  audioChunkDurationSecs: 10,

  // Streamlink quality: 'best' | '720p60' | '720p' | '480p' | '360p'
  streamlinkQuality: 'best',

  // JPEG quality scale: 1 (best) – 31 (worst). 2 gives HQ frames.
  frameJpegQuality: 2,

  // Auto-retry when stream drops (or is offline): wait 5 minutes, retry indefinitely (0 = ∞)
  retryDelayMs: 300_000,
  maxRetries:   0,
};
