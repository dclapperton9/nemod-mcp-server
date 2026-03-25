/**
 * Nemod MCP Server
 * Model Context Protocol server for nemodfacts.racestatcentral.com
 */

import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { createPool } from './db.js';

// ── Import tool registrars ─────────────────────────────────────────────────
import { registerQueryRacingData } from './tools/queryRacingData.js';
import { registerGetDriverInfo }   from './tools/getDriverInfo.js';

// ── Database Pool ──────────────────────────────────────────────────────────
let pool;
function getPool() {
  if (!pool) pool = createPool();
  return pool;
}

// ── MCP Server Factory ──────────────────────────────────────────────────────
function createMcpServer() {
  const server = new McpServer({
    name: 'nemod-racing-mcp',
    version: '1.0.0',
  });
  const p = getPool();
  registerQueryRacingData(server, p);
  registerGetDriverInfo(server, p);
  return server;
}

// ── Bootstrap ──────────────────────────────────────────────────────────────
async function main() {
  const p = getPool();
  try {
    const conn = await p.getConnection();
    conn.release();
    console.error('[nemod-mcp] ✓ Database connection pool established.');
  } catch (err) {
    console.error('[nemod-mcp] ✗ Failed to connect to the database:', err.message);
    process.exit(1);
  }

  if (process.argv.includes('--sse')) {
    const app = express();
    app.use(cors());
    // NOTE: Do NOT add express.json() here. The MCP SDK reads the raw request
    // body stream internally. Pre-parsing it with express.json() consumes the
    // stream and causes "stream is not readable" errors in handlePostMessage.
    
    const port = process.env.PORT || 3000;
    const sessions = new Map();

    app.get('/sse', async (req, res) => {
      console.error(`[nemod-mcp] [SSE] [NEW] ${req.ip}`);
      
      // SSE Stability: No timeout, no buffering
      req.setTimeout(0);
      res.setTimeout(0);
      
      const protocol = req.protocol === 'https' ? 'https' : 'http';
      const host = req.get('host');
      const absoluteUrl = `${protocol}://${host}/sse`;
      
      const serverInstance = createMcpServer();
      const transport = new SSEServerTransport(absoluteUrl, res);
      
      // Periodically write a comment to keep the connection alive
      const heartbeat = setInterval(() => {
        if (!res.writableEnded) {
          res.write(':\n\n'); 
        }
      }, 30000);

      try {
        await serverInstance.connect(transport);
        const sessionId = transport.sessionId;
        sessions.set(sessionId, transport);
        console.error(`[nemod-mcp] [SSE] [ID] ${sessionId}`);

        // PERSIST the connection
        await new Promise((resolve) => {
          req.on('close', () => {
            console.error(`[nemod-mcp] [SSE] [CLOSE] ${sessionId}`);
            clearInterval(heartbeat);
            sessions.delete(sessionId);
            resolve();
          });
          req.on('error', (err) => {
            console.error(`[nemod-mcp] [SSE] [ERROR] ${sessionId}: ${err.message}`);
            clearInterval(heartbeat);
            sessions.delete(sessionId);
            resolve();
          });
        });
      } catch (err) {
        console.error(`[nemod-mcp] [SSE] [FATAL] ${err.message}`);
        clearInterval(heartbeat);
      }
    });

    app.post('/sse', async (req, res) => {
      let sessionId = req.query.sessionId;
      if (!sessionId && sessions.size === 1) {
        sessionId = sessions.keys().next().value;
      }

      const transport = sessions.get(sessionId);
      if (transport) {
        try {
          await transport.handlePostMessage(req, res);
        } catch (err) {
          console.error(`[nemod-mcp] [POST] [ERROR] ${sessionId}: ${err.message}`);
          res.status(500).send(err.message);
        }
      } else {
        res.status(400).send('No active SSE connection');
      }
    });

    app.listen(port, () => {
      console.error(`[nemod-mcp] Running on http://localhost:${port}/sse`);
    });

  } else {
    const serverInstance = createMcpServer();
    const transport = new StdioServerTransport();
    await serverInstance.connect(transport);
    console.error('[nemod-mcp] Listening on stdio.');
  }
}

main().catch((err) => {
  console.error('[nemod-mcp] Fatal error:', err);
  process.exit(1);
});
