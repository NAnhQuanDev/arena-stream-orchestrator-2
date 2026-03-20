const express = require('express');
const { config } = require('./config');
const logger = require('./utils/logger');
const { BizflyClient } = require('./services/bizflyClient');
const { OrchestratorService } = require('./services/orchestratorService');
const { buildRoutes } = require('./routes/orchestratorRoutes');
const { maskStreamUrl } = require('./services/httpClient');

function sanitizeBody(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) return body;
  const out = { ...body };
  if (typeof out.outputStream === 'string') {
    out.outputStream = maskStreamUrl(out.outputStream);
  }
  return out;
}

async function main() {
  if (!Array.isArray(config.streamNodes) || config.streamNodes.length === 0) {
    throw new Error('Missing stream nodes. Configure STREAM_NODES (JSON) or STREAM_SERVER_IDS');
  }

  const bizflyClient = new BizflyClient(config.bizfly);
  const orchestrator = new OrchestratorService({ config, bizflyClient, logger });

  const app = express();
  app.use(express.urlencoded({ extended: true }));
  app.use(express.json());

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

  app.get('/healthz', (_req, res) => {
    res.json({ ok: true, role: 'stream-orchestrator', ts: new Date().toISOString() });
  });

  app.use('/api', buildRoutes(orchestrator));

  app.use((error, _req, res, _next) => {
    const status = Number(error.status) || 500;
    res.status(status).json({
      error: error.message || 'Internal Server Error',
      code: error.code || 'INTERNAL_ERROR',
    });
  });

  const server = app.listen(config.port, () => {
    logger.simple(`Orchestrator API running at ${config.port}`);
    logger.simple(`Configured stream nodes: ${config.streamNodes.length}`);
  });

  try {
    await orchestrator.bootstrap();
    logger.simple('Orchestrator bootstrap completed');
  } catch (error) {
    server.close();
    throw error;
  }

  const shutdown = () => {
    logger.simple('Shutting down orchestrator...');
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
