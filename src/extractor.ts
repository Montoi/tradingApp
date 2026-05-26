/**
 * extractor.ts — Phase 1: Ingestion Layer
 *
 * Captures a YouTube Live trading stream and bifurcates it into two pipelines:
 *
 *   ┌─────────────┐
 *   │  streamlink  │  Resolves YT Live → HLS/DASH URL
 *   └──────┬──────┘
 *          │ stream URL
 *   ┌──────▼──────────────────────────────────┐
 *   │              FFmpeg (fluent-ffmpeg)      │
 *   │  ┌────────────────┐  ┌────────────────┐ │
 *   │  │  VIDEO branch  │  │  AUDIO branch  │ │
 *   │  │  JPEG frames   │  │  WAV segments  │ │
 *   │  │  output/frames │  │  output/audio  │ │
 *   │  └────────────────┘  └────────────────┘ │
 *   └─────────────────────────────────────────┘
 *
 *   On stream drop → auto-retry after retryDelayMs (configurable).
 *   Stats (frames, audio chunks) are cumulative across all retries.
 *
 * Usage:
 *   npm run dev -- <youtube-live-url>
 *   npm run dev -- https://www.youtube.com/watch?v=<id>
 */

import ffmpeg from 'fluent-ffmpeg';
import fs from 'fs';
import path from 'path';

import { defaultConfig } from './config.js';
import { logger } from './utils/logger.js';
import { resolveStreamUrl } from './utils/streamlink.js';
import type { ExtractorConfig, PipelineStats } from './types/index.js';

// ─── Directory Setup ──────────────────────────────────────────────────────────

/**
 * Ensures the required output directories exist and are completely clean.
 * This prevents the AI from analyzing leftover frames/audio from previous sessions.
 */
function ensureDirectories(outputDir: string) {
  const framesDir = path.join(outputDir, 'frames');
  const audioDir  = path.join(outputDir, 'audio');

  // Clear existing directories to avoid ghost trades
  if (fs.existsSync(framesDir)) { fs.rmSync(framesDir, { recursive: true, force: true }); }
  if (fs.existsSync(audioDir)) { fs.rmSync(audioDir, { recursive: true, force: true }); }

  fs.mkdirSync(framesDir, { recursive: true });
  fs.mkdirSync(audioDir,  { recursive: true });

  return { framesDir, audioDir };
}

// ─── Audio Chunk Watcher ──────────────────────────────────────────────────────

/**
 * Watches the audio output directory for completed WAV chunks.
 * Uses a Set to deduplicate fs.watch events (which can fire multiple times
 * for the same file creation).
 */
function watchAudioDir(audioDir: string, stats: PipelineStats): fs.FSWatcher {
  const seen = new Set<string>();

  return fs.watch(audioDir, (_event, filename) => {
    if (typeof filename === 'string' && filename.endsWith('.wav') && !seen.has(filename)) {
      seen.add(filename);
      stats.audioChunksSaved += 1;
      stats.lastAudioChunkPath = path.join(audioDir, filename);
      logger.audio(stats.audioChunksSaved, stats.lastAudioChunkPath);
    }
  });
}

// ─── FFmpeg Pipeline ──────────────────────────────────────────────────────────

/**
 * Builds the bifurcated FFmpeg command.
 *
 * Output 1 — VIDEO branch
 *   - Extracts one JPEG frame every (1 / videoFps) seconds
 *   - Saves to: output/frames/frame_NNNNNN.jpg
 *   - No audio
 *
 * Output 2 — AUDIO branch
 *   - PCM 16-bit signed, little-endian
 *   - 16 kHz mono (optimised for Whisper / audio AI)
 *   - Segmented WAV files every N seconds
 *   - Saves to: output/audio/chunk_NNN.wav
 *   - No video
 *
 * NOTE: 'end' and retry 'error' handlers are NOT attached here.
 *       They are added in attempt() so retry logic stays in one place.
 */
function buildPipeline(
  streamUrl: string,
  framesDir: string,
  audioDir: string,
  cfg: ExtractorConfig,
  stats: PipelineStats,
): ffmpeg.FfmpegCommand {
  const framesPattern = path.join(framesDir, 'frame_%06d.jpg');
  const audioPattern  = path.join(audioDir,  'chunk_%03d.wav');

  return ffmpeg(streamUrl)

    // ── Output 1: Video frames (JPEG) ────────────────────────────────────────
    .output(framesPattern)
    .outputOptions([
      `-vf fps=${cfg.videoFps}`,           // e.g. fps=1 → 1 frame/sec
      `-q:v ${cfg.frameJpegQuality}`,      // JPEG quality (1=best, 31=worst)
    ])
    .noAudio()                             // drop audio on this output

    // ── Output 2: Segmented audio (WAV) ──────────────────────────────────────
    .output(audioPattern)
    .outputOptions([
      `-ar ${cfg.audioSampleRate}`,        // sample rate, e.g. 16000
      `-ac ${cfg.audioChannels}`,          // channels: 1 = mono
      '-acodec pcm_s16le',                 // raw PCM — maximum compatibility
      '-f segment',                        // segment muxer
      `-segment_time ${cfg.audioChunkDurationSecs}`,
      '-segment_format wav',
      '-reset_timestamps 1',               // each chunk starts at t=0
    ])
    .noVideo()                             // drop video on this output

    // ── Logging-only handlers (retry logic lives in attempt()) ───────────────
    .on('start', (cmdStr: string) => {
      logger.success('FFmpeg pipeline is running');
      logger.debug(`Full command: ${cmdStr}`);
    })

    .on('stderr', (line: string) => {
      // Parse FFmpeg progress lines to count frames
      // Example: "frame=   42 fps= 1.0 q=2.0 …"
      const m = /frame=\s*(\d+)/.exec(line);
      if (m?.[1] !== undefined) {
        const n = parseInt(m[1], 10);
        if (n > stats.framesCaptured) {
          stats.framesCaptured = n;
          const framePath = path.join(
            framesDir,
            `frame_${String(n).padStart(6, '0')}.jpg`,
          );
          stats.lastFramePath = framePath;
          logger.frame(n, framePath);
        }
      }
    })

    .on('error', (err: Error, _stdout: string | null, stderr: string | null) => {
      // Silently ignore expected signals from our own kill() calls
      if (err.message.includes('SIGKILL') || err.message.includes('pipe')) return;

      logger.error(`FFmpeg error: ${err.message}`);
      if (stderr) {
        // Show only the last few lines to avoid noise
        const tail = stderr.trim().split('\n').slice(-5).join('\n');
        logger.error(`FFmpeg stderr (tail):\n${tail}`);
      }
    });
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function elapsedSeconds(since: Date): string {
  return ((Date.now() - since.getTime()) / 1_000).toFixed(1);
}

function printSessionSummary(stats: PipelineStats): void {
  logger.info('');
  logger.info('─── Session summary ───────────────────────────────────');
  logger.info(`  Frames captured  : ${stats.framesCaptured}`);
  logger.info(`  Audio chunks     : ${stats.audioChunksSaved}`);
  logger.info(`  Total duration   : ${elapsedSeconds(stats.startTime)}s`);
  if (stats.lastFramePath)      logger.info(`  Last frame       : ${stats.lastFramePath}`);
  if (stats.lastAudioChunkPath) logger.info(`  Last audio chunk : ${stats.lastAudioChunkPath}`);
  logger.info('───────────────────────────────────────────────────────');
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function run(cfg: ExtractorConfig): Promise<void> {
  logger.banner();

  // ── Validate input ─────────────────────────────────────────────────────────
  if (!cfg.youtubeUrl) {
    logger.error('No YouTube URL provided.');
    logger.error('Usage:  npm run dev -- <youtube-live-url>');
    logger.error('Example: npm run dev -- https://www.youtube.com/watch?v=abc123');
    process.exit(1);
  }

  const retryDelayMs = cfg.retryDelayMs ?? 5_000;
  const maxRetries   = cfg.maxRetries   ?? 0;

  logger.info(`URL      : ${cfg.youtubeUrl}`);
  logger.info(`Quality  : ${cfg.streamlinkQuality}`);
  logger.info(`Video    : ${cfg.videoFps} fps  |  JPEG quality ${cfg.frameJpegQuality}`);
  logger.info(`Audio    : ${cfg.audioSampleRate} Hz · ${cfg.audioChannels}ch · ${cfg.audioChunkDurationSecs}s chunks`);
  logger.info(`Output   : ${cfg.outputDir}`);
  logger.info(`Retry    : ${retryDelayMs / 1_000}s delay | max ${maxRetries === 0 ? '∞' : maxRetries} retries`);
  logger.info('');

  // ── Prepare output directories (once, reused across retries) ───────────────
  const { framesDir, audioDir } = ensureDirectories(cfg.outputDir);
  logger.success(`Frames → ${framesDir}`);
  logger.success(`Audio  → ${audioDir}`);
  logger.info('');

  // ── Cumulative stats (survive across retries) ───────────────────────────────
  const stats: PipelineStats = {
    startTime:          new Date(),
    framesCaptured:     0,
    audioChunksSaved:   0,
    lastFramePath:      null,
    lastAudioChunkPath: null,
  };

  // ── Shared mutable state ────────────────────────────────────────────────────
  let isShuttingDown = false;
  let retryCount     = 0;
  let currentWatcher:  fs.FSWatcher         | null = null;
  let currentPipeline: ffmpeg.FfmpegCommand  | null = null;

  // ── Graceful shutdown (registered once for the whole session) ───────────────
  const shutdown = (signal: string): void => {
    if (isShuttingDown) return;          // guard against double-fire
    isShuttingDown = true;
    logger.warn(`\n${signal} received — shutting down gracefully…`);
    currentWatcher?.close();
    currentPipeline?.kill('SIGTERM');
    printSessionSummary(stats);
    process.exit(0);
  };

  process.on('SIGINT',  () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  // ── Retry scheduler ─────────────────────────────────────────────────────────
  function scheduleRetry(): void {
    if (isShuttingDown) return;

    if (maxRetries > 0 && retryCount >= maxRetries) {
      logger.error(`Max retries (${maxRetries}) reached. Stopping.`);
      printSessionSummary(stats);
      process.exit(1);
    }

    retryCount += 1;
    logger.warn(`Stream dropped. Retry #${retryCount} in ${retryDelayMs / 1_000}s…`);
    setTimeout(() => { void attempt(); }, retryDelayMs);
  }

  // ── Single capture attempt ──────────────────────────────────────────────────
  async function attempt(): Promise<void> {
    if (isShuttingDown) return;

    const label = retryCount === 0 ? 'Starting' : `Retry #${retryCount} —`;
    logger.info(`${label} Resolving stream URL via Streamlink…`);

    // Step 1: Resolve the real HLS/DASH URL (re-resolved each attempt
    //         because HLS manifests are time-limited and change on reconnect)
    let streamUrl: string;
    try {
      const resolution = await resolveStreamUrl(cfg.youtubeUrl, cfg.streamlinkQuality);
      streamUrl = resolution.url;
      logger.success(`Resolved [${resolution.quality}]: ${streamUrl.slice(0, 90)}…`);
    } catch (err) {
      logger.error(err instanceof Error ? err.message : String(err));
      scheduleRetry();
      return;
    }

    if (isShuttingDown) return;

    // Step 2: Reset attempt-level timer; close any previous watcher
    stats.startTime = new Date();
    currentWatcher?.close();
    currentWatcher = watchAudioDir(audioDir, stats);

    // Step 3: Build pipeline and attach retry-aware end/error handlers
    logger.info('Starting bifurcated FFmpeg pipeline…');
    logger.info('  Branch 1 (VIDEO) → JPEG frames to disk');
    logger.info('  Branch 2 (AUDIO) → WAV segments to disk');
    logger.info('  Press Ctrl+C to stop.\n');

    currentPipeline = buildPipeline(streamUrl, framesDir, audioDir, cfg, stats)

      .on('end', () => {
        const elapsed = elapsedSeconds(stats.startTime);
        logger.success(
          `Stream ended — ${stats.framesCaptured} frames · ` +
          `${stats.audioChunksSaved} audio chunks · ${elapsed}s elapsed`,
        );
        currentWatcher?.close();
        scheduleRetry();            // stream ended naturally → reconnect
      })

      .on('error', () => {
        // Error message is already logged inside buildPipeline.
        // We only handle the retry decision here.
        currentWatcher?.close();
        scheduleRetry();
      });

    currentPipeline.run();
  }

  // ── Kick off first attempt ──────────────────────────────────────────────────
  await attempt();
}

// ─── Entry Point ──────────────────────────────────────────────────────────────

run(defaultConfig).catch((err: unknown) => {
  logger.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
