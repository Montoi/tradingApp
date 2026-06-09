import express from 'express';
import cors from 'cors';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { spawn } from 'child_process';
import { PrismaClient } from '@prisma/client';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = 3001;
const prisma = new PrismaClient();

app.use(cors());
app.use(express.json());

// Dictionary to keep track of active child processes (extractor)
// Key: stream_id, Value: ChildProcess
const activeProcesses = new Map();

// ─── API Endpoints for Frontend and Python ────────────────────────────────────

// Python queries this to know which streams to process
app.get('/api/streams/active', async (req, res) => {
    try {
        const activeStreams = await prisma.streamer.findMany({
            where: { is_active: true }
        });
        res.json(activeStreams.map(s => s.stream_id));
    } catch (error) {
        console.error('Error fetching active streams:', error);
        res.status(500).json({ error: 'Failed to fetch streams' });
    }
});

// Consolidates all state_[STREAM_ID].json files for the Frontend
app.get('/api/state', (req, res) => {
    try {
        const outputDir = path.join(__dirname, 'output');
        if (!fs.existsSync(outputDir)) {
            return res.json({});
        }

        const files = fs.readdirSync(outputDir);
        const consolidatedState = {};

        for (const file of files) {
            if (file.startsWith('state_') && file.endsWith('.json')) {
                const streamId = file.replace('state_', '').replace('.json', '');
                try {
                    const data = fs.readFileSync(path.join(outputDir, file), 'utf8');
                    consolidatedState[streamId] = JSON.parse(data);
                } catch (e) {
                    console.error(`Error reading ${file}:`, e);
                }
            }
        }
        res.json(consolidatedState);
    } catch (error) {
        console.error('Error consolidating state:', error);
        res.status(500).json({ error: 'Failed to consolidate state' });
    }
});

// ─── Orchestrator Loop ────────────────────────────────────────────────────────

async function syncProcesses() {
    try {
        const dbStreams = await prisma.streamer.findMany();

        // 1. Start processes for active streams that are not running
        for (const stream of dbStreams) {
            if (stream.is_active && !activeProcesses.has(stream.stream_id)) {
                console.log(`[ORCHESTRATOR] Starting stream ${stream.stream_id}...`);
                
                // Pass URL and STREAM_ID via environment variables
                const child = spawn('npx', ['tsx', 'src/extractor.ts'], {
                    env: {
                        ...process.env,
                        YOUTUBE_URL: stream.url,
                        STREAM_ID: stream.stream_id
                    },
                    stdio: 'inherit' // Pipe logs to main process
                });

                child.on('error', (err) => {
                    console.error(`[ORCHESTRATOR] Failed to start extractor for ${stream.stream_id}:`, err);
                });

                child.on('exit', (code, signal) => {
                    console.log(`[ORCHESTRATOR] Stream ${stream.stream_id} exited with code ${code} and signal ${signal}`);
                    activeProcesses.delete(stream.stream_id);
                });

                activeProcesses.set(stream.stream_id, child);
            }
            
            // 2. Stop processes for streams that are no longer active
            if (!stream.is_active && activeProcesses.has(stream.stream_id)) {
                console.log(`[ORCHESTRATOR] Stopping stream ${stream.stream_id}...`);
                const child = activeProcesses.get(stream.stream_id);
                child.kill('SIGTERM');
                activeProcesses.delete(stream.stream_id);
            }
        }

        // 3. Stop processes that were deleted from the database
        const dbStreamIds = new Set(dbStreams.map(s => s.stream_id));
        for (const [streamId, child] of activeProcesses.entries()) {
            if (!dbStreamIds.has(streamId)) {
                console.log(`[ORCHESTRATOR] Stream ${streamId} deleted from DB. Stopping...`);
                child.kill('SIGTERM');
                activeProcesses.delete(streamId);
            }
        }
    } catch (error) {
        console.error('[ORCHESTRATOR] Error syncing processes:', error);
    }
}

// Run the sync every 10 seconds
setInterval(syncProcesses, 10000);

// ─── Graceful Shutdown (Zombie Prevention) ────────────────────────────────────

function shutdown() {
    console.log('\n[ORCHESTRATOR] SIGINT/SIGTERM received. Terminating all child processes...');
    for (const [streamId, child] of activeProcesses.entries()) {
        console.log(`[ORCHESTRATOR] Killing ${streamId} (PID: ${child.pid})`);
        child.kill('SIGTERM');
    }
    
    // Give children time to exit cleanly
    setTimeout(() => {
        prisma.$disconnect();
        process.exit(0);
    }, 2000);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// ─── Start Server ─────────────────────────────────────────────────────────────

app.listen(PORT, () => {
    console.log(`📡 Orchestrator Server running at http://localhost:${PORT}`);
    console.log(`- State endpoint:  http://localhost:${PORT}/api/state`);
    console.log(`- Active streams:  http://localhost:${PORT}/api/streams/active`);
    // Run an initial sync immediately
    syncProcesses();
});
