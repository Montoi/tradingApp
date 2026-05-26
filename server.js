import express from 'express';
import cors from 'cors';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = 3001;

app.use(cors());

// Paths to our backend files
const stateFile = path.join(__dirname, 'output', 'state.json');
const historyFile = path.join(__dirname, 'output', 'signals_history.jsonl');

app.get('/api/state', (req, res) => {
    try {
        if (!fs.existsSync(stateFile)) {
            return res.json({});
        }
        const data = fs.readFileSync(stateFile, 'utf8');
        res.json(JSON.parse(data));
    } catch (error) {
        console.error('Error reading state.json:', error);
        res.status(500).json({ error: 'Failed to read state' });
    }
});

app.get('/api/history', (req, res) => {
    try {
        if (!fs.existsSync(historyFile)) {
            return res.json([]);
        }
        const data = fs.readFileSync(historyFile, 'utf8');
        // Parse JSONL to array of objects
        const lines = data.split('\n').filter(line => line.trim() !== '');
        const history = lines.map(line => {
            try {
                return JSON.parse(line);
            } catch (e) {
                return null;
            }
        }).filter(item => item !== null);
        
        // Return only the last 50 signals
        res.json(history.slice(-50).reverse()); // Newest first
    } catch (error) {
        console.error('Error reading signals_history.jsonl:', error);
        res.status(500).json({ error: 'Failed to read history' });
    }
});

app.listen(PORT, () => {
    console.log(`📡 Backend API Server running at http://localhost:${PORT}`);
    console.log(`- State endpoint:   http://localhost:${PORT}/api/state`);
    console.log(`- History endpoint: http://localhost:${PORT}/api/history`);
});
