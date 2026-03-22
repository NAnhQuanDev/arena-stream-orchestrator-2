const express = require('express');
const http = require('http');
const { config } = require('./config');
const logger = require('./utils/logger');
const { BizflyClient } = require('./services/bizflyClient');
const { OrchestratorService } = require('./services/orchestratorService');
const { buildRoutes } = require('./routes/orchestratorRoutes');
const { buildLiveRoutes } = require('./routes/liveRoutes');
const { maskStreamUrl } = require('./services/httpClient');
const { toClientError } = require('./utils/clientResponse');
const { initServerSocket } = require('./websocket/websocketServer');
const { LiveWebSocket } = require('./websocket/liveWebSocket');
const { DeviceRegistry } = require('./services/deviceStateMachine');

function sanitizeBody(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) return body;
  const out = { ...body };
  if (typeof out.outputStream === 'string') {
    out.outputStream = maskStreamUrl(out.outputStream);
  }
  return out;
}

/**
 * Build RTMP target URL cho device.
 * Android sẽ push RTMP stream tới URL này.
 * Worker sẽ pull từ URL này (= URL_INPUT_STREAM trên worker).
 */
function buildRtmpTarget(deviceId) {
  const base = process.env.RTMP_RELAY_URL || process.env.STREAM_SERVER_URL || '';
  if (!base) return deviceId; // fallback — worker sẽ tự build từ URL_INPUT_STREAM
  return `${base.replace(/\/+$/, '')}/${deviceId}`;
}

async function main() {
  if (!Array.isArray(config.streamNodes) || config.streamNodes.length === 0) {
    throw new Error('Missing stream nodes. Configure STREAM_NODES (JSON) or STREAM_SERVER_IDS');
  }

  const bizflyClient = new BizflyClient(config.bizfly);
  const orchestrator = new OrchestratorService({ config, bizflyClient, logger });

  // -----------------------------------------------------------------------
  // Device Registry + LiveWebSocket (HỆ THỐNG MỚI)
  // -----------------------------------------------------------------------
  const deviceRegistry = new DeviceRegistry({
    logger,
    onTimeout: (deviceId, state, requestId) => {
      liveWs.handleTimeout(deviceId, state, requestId);
    },
  });

  const liveWs = new LiveWebSocket({
    logger,
    deviceRegistry,
    orchestrator,
    buildRtmpTarget,
  });

  // GC idle state machines mỗi 10 phút
  const gcTimer = setInterval(() => deviceRegistry.gc(), 10 * 60 * 1000);

  // Ping/pong mỗi 30s
  const pingTimer = setInterval(() => liveWs.pingAll(), 30_000);

  // -----------------------------------------------------------------------
  // Express app
  // -----------------------------------------------------------------------
  const app = express();
  app.use(express.urlencoded({ extended: true }));
  app.use(express.json());

  // Request logging
  app.use((req, res, next) => {
    if (!req.path.startsWith('/api')) return next();

    const startedAt = Date.now();
    const requestBody = sanitizeBody(req.body);
    logger.info('api', `IN ${req.method} ${req.originalUrl} body=${JSON.stringify(requestBody || {})}`);

    res.on('finish', () => {
      const ms = Date.now() - startedAt;
      logger.info('api', `OUT ${req.method} ${req.originalUrl} status=${res.statusCode} timeMs=${ms}`);
    });

    next();
  });

  // Health check
  app.get('/healthz', (_req, res) => {
    res.json({ ok: true, role: 'stream-orchestrator', ts: new Date().toISOString() });
  });

  // API cũ — giữ nguyên, worker + Android cũ vẫn dùng được
  app.use('/api', buildRoutes(orchestrator));

  // API mới v1 — cho website/admin + Android mới
  app.use('/api/v1', buildLiveRoutes(liveWs, deviceRegistry));

  // Error handler
  app.use((error, _req, res, _next) => {
    const { status, payload } = toClientError(_req, error);
    res.status(status).json(payload);
  });

  // -----------------------------------------------------------------------
  // HTTP Server + WebSocket
  // -----------------------------------------------------------------------
  const server = http.createServer(app);

  // Legacy WebSocket (/ws/:deviceId) — giữ nguyên
  initServerSocket(server);

  // New LiveWebSocket (/ws/live/:deviceId) — hệ thống mới
  liveWs.attach(server);

  server.listen(config.port, () => {
    logger.simple(`Orchestrator API running at ${config.port}`);
    logger.simple(`Configured stream nodes: ${config.streamNodes.length}`);
    logger.simple(`Legacy WS:  ws://<host>:${config.port}/ws/:deviceId`);
    logger.simple(`New WS:     ws://<host>:${config.port}/ws/live/:deviceId`);
    logger.simple(`New REST:   /api/v1/live/start | /api/v1/live/stop | /api/v1/live/status/:deviceId`);
  });

  try {
    await orchestrator.bootstrap();
    logger.simple('Orchestrator bootstrap completed');
  } catch (error) {
    server.close();
    throw error;
  }

  // -----------------------------------------------------------------------
  // Graceful shutdown
  // -----------------------------------------------------------------------
  const shutdown = () => {
    logger.simple('Shutting down orchestrator...');
    clearInterval(gcTimer);
    clearInterval(pingTimer);
    orchestrator.stop();
    server.close(() => process.exit(0));
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((error) => {
  console.error('[ORCHESTRATOR] Fatal error:', error.message);
  process.exit(1);
});
