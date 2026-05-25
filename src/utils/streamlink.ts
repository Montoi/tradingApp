import { spawn } from 'child_process';
import type { StreamResolution } from '../types/index.js';

/**
 * Resolves a YouTube Live URL to its actual HLS/DASH stream URL
 * by invoking the system `streamlink` CLI with `--stream-url`.
 *
 * Requires: streamlink installed and available on PATH.
 *
 * @param youtubeUrl  The full YouTube Live page URL.
 * @param quality     Quality selector: 'best' | '720p60' | '720p' | '480p' | …
 * @returns           Resolved StreamResolution with the direct stream URL.
 */
export function resolveStreamUrl(
  youtubeUrl: string,
  quality: string = 'best',
): Promise<StreamResolution> {
  return new Promise((resolve, reject) => {
    // streamlink --stream-url <url> <quality>
    const proc = spawn('streamlink', ['--stream-url', youtubeUrl, quality], {
      windowsHide: true,
    });

    let stdout = '';
    let stderr = '';

    proc.stdout.on('data', (chunk: Buffer) => {
      stdout += chunk.toString();
    });

    proc.stderr.on('data', (chunk: Buffer) => {
      stderr += chunk.toString();
    });

    proc.on('close', (code) => {
      if (code !== 0) {
        reject(
          new Error(
            `Streamlink exited with code ${code}.\n` +
            `stderr: ${stderr.trim().slice(0, 500)}`,
          ),
        );
        return;
      }

      const url = stdout.trim();
      if (!url) {
        reject(new Error('Streamlink returned an empty URL. Is the stream live?'));
        return;
      }

      resolve({ url, quality });
    });

    proc.on('error', (err) => {
      reject(
        new Error(
          `Failed to spawn 'streamlink'. Is it installed and on PATH?\n` +
          `Original error: ${err.message}`,
        ),
      );
    });
  });
}
