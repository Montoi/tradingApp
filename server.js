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

// Consolidates state files for the Frontend
app.get('/api/state', (req, res) => {
    try {
        const outputDir = path.join(__dirname, 'output');
        if (!fs.existsSync(outputDir)) {
            return res.json({});
        }

        let consolidatedState = {};

        // Read the main state.json (written by decision_engine.py)
        const mainStateFile = path.join(outputDir, 'state.json');
        if (fs.existsSync(mainStateFile)) {
            try {
                const data = fs.readFileSync(mainStateFile, 'utf8');
                consolidatedState = { ...consolidatedState, ...JSON.parse(data) };
            } catch (e) {
                console.error('Error reading state.json:', e);
            }
        }

        // Also read any per-stream state_[STREAM_ID].json files
        const files = fs.readdirSync(outputDir);
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

// Reads signals_history.jsonl for the Dashboard feed
app.get('/api/history', (req, res) => {
    try {
        const historyFile = path.join(__dirname, 'output', 'signals_history.jsonl');
        if (!fs.existsSync(historyFile)) {
            return res.json([]);
        }
        const raw = fs.readFileSync(historyFile, 'utf8');
        const lines = raw.trim().split('\n').filter(Boolean);
        const signals = lines
            .map(line => { try { return JSON.parse(line); } catch { return null; } })
            .filter(Boolean)
            .reverse(); // newest first
        res.json(signals);
    } catch (error) {
        console.error('Error reading history:', error);
        res.status(500).json({ error: 'Failed to read history' });
    }
});

// ─── Streamers CRUD ───────────────────────────────────────────────────────────

app.get('/api/streamers', async (req, res) => {
    try {
        const streamers = await prisma.streamer.findMany({
            orderBy: { updated_at: 'desc' }
        });
        res.json(streamers);
    } catch (error) {
        console.error('Error fetching streamers:', error);
        res.status(500).json({ error: 'Failed to fetch streamers' });
    }
});

app.post('/api/streamers', async (req, res) => {
    try {
        const { id, name, liveUrl, ...config_json } = req.body;
        const streamer = await prisma.streamer.create({
            data: {
                stream_id: id,
                name: name,
                url: liveUrl,
                is_active: false,
                config_json: config_json
            }
        });
        res.status(201).json(streamer);
    } catch (error) {
        console.error('Error creating streamer:', error);
        res.status(500).json({ error: 'Failed to create streamer' });
    }
});

app.put('/api/streamers/:id', async (req, res) => {
    try {
        const { name, liveUrl, isActive, ...config_json } = req.body;
        const data = {};
        if (name !== undefined) data.name = name;
        if (liveUrl !== undefined) data.url = liveUrl;
        if (isActive !== undefined) data.is_active = isActive;
        // Merge the existing config_json with the new config_json if provided
        if (Object.keys(config_json).length > 0) {
            data.config_json = config_json;
        }

        const streamer = await prisma.streamer.update({
            where: { stream_id: req.params.id },
            data
        });
        res.json(streamer);
    } catch (error) {
        console.error('Error updating streamer:', error);
        res.status(500).json({ error: 'Failed to update streamer' });
    }
});

app.delete('/api/streamers/:id', async (req, res) => {
    try {
        await prisma.streamer.delete({
            where: { stream_id: req.params.id }
        });
        res.status(204).send();
    } catch (error) {
        console.error('Error deleting streamer:', error);
        res.status(500).json({ error: 'Failed to delete streamer' });
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
                    stdio: 'inherit', // Pipe logs to main process
                    shell: true       // Required on Windows to resolve npx.cmd
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

// ─── Janitor (Disk Cleanup) ───────────────────────────────────────────────────

const JANITOR_INTERVAL_MS  = 5 * 60 * 1000;  // Run every 5 minutes
const FILE_MAX_AGE_MS      = 2 * 60 * 1000;  // Delete files older than 2 minutes

function runJanitor() {
    const outputDir = path.join(__dirname, 'output');
    if (!fs.existsSync(outputDir)) return;

    const now = Date.now();
    let deletedCount = 0;
    let freedBytes = 0;

    try {
        // Walk every stream folder inside output/
        const entries = fs.readdirSync(outputDir, { withFileTypes: true });
        for (const entry of entries) {
            if (!entry.isDirectory()) continue; // skip state.json, history.jsonl, etc.

            const streamDir = path.join(outputDir, entry.name);

            for (const subdir of ['frames', 'audio']) {
                const targetDir = path.join(streamDir, subdir);
                if (!fs.existsSync(targetDir)) continue;

                const files = fs.readdirSync(targetDir);
                for (const file of files) {
                    const filePath = path.join(targetDir, file);
                    try {
                        const stat = fs.statSync(filePath);
                        const ageMs = now - stat.mtimeMs;
                        if (ageMs > FILE_MAX_AGE_MS) {
                            freedBytes += stat.size;
                            fs.unlinkSync(filePath);
                            deletedCount++;
                        }
                    } catch (e) {
                        // File may have been deleted by another process — skip silently
                    }
                }
            }
        }

        if (deletedCount > 0) {
            const freedMB = (freedBytes / 1024 / 1024).toFixed(1);
            console.log(`[JANITOR] Cleaned ${deletedCount} files, freed ${freedMB} MB`);
        }
    } catch (error) {
        console.error('[JANITOR] Error during cleanup:', error);
    }
}

setInterval(runJanitor, JANITOR_INTERVAL_MS);
// Also run once on startup to clear any leftover files
runJanitor();

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
