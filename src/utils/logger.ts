/**
 * Minimal colourised logger for the ingestion pipeline.
 * No external dependencies — pure Node.js.
 */

const C = {
  reset:  '\x1b[0m',
  bold:   '\x1b[1m',
  dim:    '\x1b[2m',
  cyan:   '\x1b[36m',
  green:  '\x1b[32m',
  yellow: '\x1b[33m',
  red:    '\x1b[31m',
  blue:   '\x1b[34m',
  magenta:'\x1b[35m',
} as const;

function ts(): string {
  return new Date().toISOString().replace('T', ' ').slice(0, 23);
}

function prefix(level: string, color: string): string {
  return `${C.dim}[${ts()}]${C.reset} ${color}${C.bold}${level}${C.reset}`;
}

export const logger = {
  info:    (msg: string) => console.log(`${prefix('INFO ', C.cyan)}  ${msg}`),
  success: (msg: string) => console.log(`${prefix('OK   ', C.green)}  ${msg}`),
  warn:    (msg: string) => console.warn(`${prefix('WARN ', C.yellow)}  ${msg}`),
  error:   (msg: string) => console.error(`${prefix('ERROR', C.red)}  ${msg}`),
  debug:   (msg: string) => console.log(`${prefix('DEBUG', C.dim)}  ${C.dim}${msg}${C.reset}`),

  /** Log a newly captured video frame */
  frame: (n: number, filePath: string) =>
    console.log(`${prefix('FRAME', C.blue)}  #${String(n).padStart(5, '0')} → ${filePath}`),

  /** Log a newly completed audio chunk */
  audio: (n: number, filePath: string) =>
    console.log(`${prefix('AUDIO', C.magenta)}  chunk #${String(n).padStart(3, '0')} → ${filePath}`),

  /** Print startup banner */
  banner: () => {
    const line = '─'.repeat(60);
    console.log(`\n${C.cyan}${C.bold}${line}`);
    console.log('  📈  Trading Analyzer — Ingestion Layer  (Phase 1)');
    console.log(`${line}${C.reset}\n`);
  },
};
