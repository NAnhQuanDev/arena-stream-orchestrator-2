const { WebSocketServer } = require('ws');
const url = require('url');
const logger = require('../utils/logger');

const rooms = new Map(); // deviceId -> Set<ws>

function sendJSON(ws, obj) {
  try { ws.send(JSON.stringify(obj)); } catch {}
}

function sendToDevice(deviceId, payload) {
  const key = String(deviceId || '').trim();
  if (!key) return false;

  const set = rooms.get(key);
  if (!set || set.size === 0) {
    logger.warn(key, 'WS offline');
    return false;
  }

  logger.info(key, `WS send: ${payload?.action || 'message'}`);
  set.forEach((ws) => sendJSON(ws, payload));
  return true;
}

function initServerSocket(httpServer) {
  const wss = new WebSocketServer({ noServer: true });

  wss.on('connection', (ws, req) => {
    const { pathname, query } = url.parse(req.url, true);
    const deviceId = (pathname || '').split('/').filter(Boolean)[1] || query.deviceId;
    if (!deviceId) {
      sendJSON(ws, { status: 'error', message: 'Missing deviceId' });
      return ws.close();
    }

    ws.ip = req.socket?.remoteAddress || 'unknown-ip';

    if (!rooms.has(deviceId)) rooms.set(deviceId, new Set());
    rooms.get(deviceId).add(ws);
    ws.deviceId = deviceId;

    logger.success(deviceId, 'WS connected');
    sendJSON(ws, { status: 'message', action: 'connected', deviceId });

    ws.on('message', (raw) => {
      try {
        const obj = JSON.parse(raw.toString());
        const action = obj.action || obj.cmd || obj.status || '-';
        logger.info(ws.deviceId, `WS recv: ${action}`);
      } catch {
        // Not JSON
      }
    });

    ws.on('close', () => {
      const set = rooms.get(deviceId);
      if (set) {
        set.delete(ws);
        if (set.size === 0) rooms.delete(deviceId);
      }
      logger.warn(deviceId, 'WS disconnected');
    });
  });

  httpServer.on('upgrade', (req, socket, head) => {
    const { pathname } = url.parse(req.url);
    if (!pathname?.startsWith('/ws')) return socket.destroy();
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
  });

  logger.simple('WebSocket server ready');
}

module.exports = {
  initServerSocket,
  sendToDevice,
};
