const { WebSocketServer } = require('ws');
const url = require('url');
const { randomUUID } = require('crypto');

/**
 * LiveWebSocket — WebSocket server mới cho livestream protocol.
 *
 * Protocol:
 *   Android → ws://{host}:{port}/ws/live/{deviceId}
 *
 * Message types:
 *   register, request, ack, event, heartbeat
 */
class LiveWebSocket {
  constructor({ logger, deviceRegistry, orchestrator, buildRtmpTarget }) {
    this.logger = logger;
    this.registry = deviceRegistry;
    this.orchestrator = orchestrator;
    this.buildRtmpTarget = buildRtmpTarget;

    // deviceId → Set<ws>
    this.connections = new Map();
    this.wss = null;
  }

  // ---------------------------------------------------------------------------
  // Setup
  // ---------------------------------------------------------------------------

  attach(httpServer) {
    this.wss = new WebSocketServer({ noServer: true });

    this.wss.on('connection', (ws, req) => {
      this._onConnection(ws, req);
    });

    httpServer.on('upgrade', (req, socket, head) => {
      const { pathname } = url.parse(req.url);

      // Chỉ handle /ws/live/...  — để path cũ /ws/... cho legacy WS
      if (!pathname || !pathname.startsWith('/ws/live')) {
        return; // không destroy, để legacy handler xử lý
      }

      this.wss.handleUpgrade(req, socket, head, (ws) => {
        this.wss.emit('connection', ws, req);
      });
    });

    this.logger.simple('LiveWebSocket server ready (path: /ws/live/:deviceId)');
  }

  // ---------------------------------------------------------------------------
  // Connection lifecycle
  // ---------------------------------------------------------------------------

  _onConnection(ws, req) {
    const { pathname, query } = url.parse(req.url, true);

    // Extract deviceId from /ws/live/{deviceId} or ?deviceId=...
    const segments = (pathname || '').split('/').filter(Boolean);
    // segments: ['ws', 'live', '{deviceId}']
    const deviceId = segments[2] || query.deviceId;

    if (!deviceId || typeof deviceId !== 'string' || deviceId.length > 128) {
      this._sendJSON(ws, { type: 'error', message: 'Missing or invalid deviceId' });
      return ws.close(4000, 'Missing deviceId');
    }

    const cleanId = deviceId.trim();
    ws.deviceId = cleanId;
    ws.isAlive = true;

    // Add to connections
    if (!this.connections.has(cleanId)) {
      this.connections.set(cleanId, new Set());
    }
    this.connections.get(cleanId).add(ws);

    this.logger.success(cleanId, 'LiveWS connected');

    // Send welcome + current state
    const sm = this.registry.get(cleanId);
    this._sendJSON(ws, {
      type: 'event',
      action: 'connected',
      deviceId: cleanId,
      currentState: sm.state,
    });

    // If device is in non-IDLE state (reconnect scenario), send state-sync
    if (sm.state !== 'IDLE') {
      this._sendJSON(ws, {
        type: 'event',
        action: 'state-sync',
        state: sm.state,
        requestId: sm.requestId,
        since: sm.startedAt,
      });
    }

    // Message handler
    ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        this._onMessage(cleanId, msg, ws);
      } catch {
        this.logger.warn(cleanId, 'LiveWS: invalid JSON received');
      }
    });

    // Pong for heartbeat
    ws.on('pong', () => {
      ws.isAlive = true;
    });

    // Cleanup on close
    ws.on('close', () => {
      const set = this.connections.get(cleanId);
      if (set) {
        set.delete(ws);
        if (set.size === 0) this.connections.delete(cleanId);
      }
      this.logger.warn(cleanId, 'LiveWS disconnected');
    });
  }

  // ---------------------------------------------------------------------------
  // Message routing
  // ---------------------------------------------------------------------------

  _onMessage(deviceId, msg, ws) {
    const { type, action } = msg;

    this.logger.debug(deviceId, `LiveWS recv: ${type}/${action || '-'}`);

    switch (type) {
      case 'register':
        this._handleRegister(deviceId, msg);
        break;

      case 'request':
        if (action === 'start-live') {
          this._handleManualStart(deviceId, msg);
        } else if (action === 'stop-live') {
          this._handleStopRequest(deviceId, msg);
        }
        break;

      case 'ack':
        this._handleAck(deviceId, msg);
        break;

      case 'event':
        if (action === 'rtmp-pushing') {
          this._handleRtmpReady(deviceId, msg);
        } else if (action === 'prepare-failed') {
          this._handlePrepareFailed(deviceId, msg);
        }
        break;

      case 'heartbeat':
        this._sendToDevice(deviceId, { type: 'ack', action: 'heartbeat' });
        break;

      default:
        this.logger.warn(deviceId, `LiveWS: unknown message type: ${type}`);
    }
  }

  // ---------------------------------------------------------------------------
  // Handlers
  // ---------------------------------------------------------------------------

  _handleRegister(deviceId, msg) {
    this.logger.info(deviceId, `LiveWS register: version=${msg.appVersion || '?'}`);
    // Could store capabilities if needed in future
  }

  /**
   * Manual start: Android user clicked "Live" button.
   */
  async _handleManualStart(deviceId, msg) {
    const sm = this.registry.get(deviceId);
    const requestId = msg.requestId || randomUUID();

    if (sm.state !== 'IDLE') {
      return this._sendToDevice(deviceId, {
        type: 'event',
        action: 'live-failed',
        requestId,
        error: `Device is not idle (current: ${sm.state})`,
        errorCode: 'ALREADY_IN_PROGRESS',
      });
    }

    const streamUrl = msg.payload?.streamUrl;
    const gameType = msg.payload?.gameType || 'carom';

    if (!streamUrl) {
      return this._sendToDevice(deviceId, {
        type: 'event',
        action: 'live-failed',
        requestId,
        error: 'Missing streamUrl',
        errorCode: 'MISSING_STREAM_URL',
      });
    }

    const rtmpTarget = this.buildRtmpTarget(deviceId);

    try {
      sm.beginLive('manual', { streamUrl, gameType, rtmpTarget }, requestId);
    } catch (err) {
      return this._sendToDevice(deviceId, {
        type: 'event',
        action: 'live-failed',
        requestId,
        error: err.message,
        errorCode: err.code || 'START_FAILED',
      });
    }

    // Tell Android to prepare camera
    this._sendToDevice(deviceId, {
      type: 'command',
      action: 'prepare-camera',
      requestId: sm.requestId,
      triggerSource: 'manual',
      payload: {
        streamUrl,
        gameType,
        rtmpTarget,
      },
    });
  }

  /**
   * Remote start: triggered by REST API from website.
   */
  startFromRemote(deviceId, { streamUrl, gameType, requestId }) {
    const sm = this.registry.get(deviceId);

    if (sm.state !== 'IDLE') {
      const err = new Error(`Device ${deviceId} is not idle (current: ${sm.state})`);
      err.code = 'ALREADY_IN_PROGRESS';
      err.status = 409;
      throw err;
    }

    if (!this.isDeviceOnline(deviceId)) {
      const err = new Error(`Device ${deviceId} is offline`);
      err.code = 'DEVICE_OFFLINE';
      err.status = 404;
      throw err;
    }

    const rtmpTarget = this.buildRtmpTarget(deviceId);
    const finalRequestId = sm.beginLive('remote', { streamUrl, gameType, rtmpTarget }, requestId);

    this._sendToDevice(deviceId, {
      type: 'command',
      action: 'prepare-camera',
      requestId: finalRequestId,
      triggerSource: 'remote',
      payload: {
        streamUrl,
        gameType,
        rtmpTarget,
      },
    });

    return { requestId: finalRequestId, state: sm.state };
  }

  /**
   * Remote stop: triggered by REST API from website.
   */
  async stopFromRemote(deviceId) {
    const sm = this.registry.get(deviceId);

    if (sm.state === 'IDLE') {
      return { message: 'Device is already idle' };
    }

    const requestId = randomUUID();

    // If currently streaming, also stop on worker
    if (sm.state === 'STREAMING' && sm.assignedNode) {
      try {
        await this.orchestrator.stopLive({ deviceid: deviceId });
      } catch (err) {
        this.logger.warn(deviceId, `Worker stop failed: ${err.message}`);
      }
    }

    // Tell Android to stop
    this._sendToDevice(deviceId, {
      type: 'command',
      action: 'stop-live',
      requestId,
    });

    sm.transition('IDLE', { reason: 'remote-stop' });

    return { message: 'Stop command sent', requestId };
  }

  /**
   * ACK from Android — currently only used for confirming stop.
   */
  _handleAck(deviceId, msg) {
    this.logger.debug(deviceId, `LiveWS ack: ${msg.action}`);
    // ACK is informational; state transitions happen via events
  }

  /**
   * Android reports: RTMP stream is now being pushed to relay.
   */
  async _handleRtmpReady(deviceId, msg) {
    const sm = this.registry.get(deviceId);

    if (sm.state !== 'PREPARING') {
      this.logger.warn(deviceId, `rtmp-pushing received but state is ${sm.state}, ignoring`);
      return;
    }

    if (msg.requestId && msg.requestId !== sm.requestId) {
      this.logger.warn(deviceId, `rtmp-pushing requestId mismatch: ${msg.requestId} vs ${sm.requestId}`);
      return;
    }

    const rtmpInput = msg.payload?.rtmpInput || sm.rtmpTarget;
    const resolution = msg.payload?.resolution || '1920x1080';

    this.logger.info(deviceId, `RTMP pushing confirmed: ${resolution}`);

    // Transition to DISPATCHING
    sm.transition('DISPATCHING', { reason: 'rtmp-ready' });

    // Dispatch to worker via orchestrator
    try {
      const result = await this.orchestrator.startLive({
        deviceid: deviceId,
        outputStream: sm.streamUrl,
        type: sm.gameType,
      });

      sm.assignedNode = result.routedTo?.name || null;
      sm.transition('STREAMING', { reason: 'worker-started' });

      this._sendToDevice(deviceId, {
        type: 'event',
        action: 'live-started',
        requestId: sm.requestId,
        payload: {
          workerNode: sm.assignedNode,
          startedAt: sm.startedAt,
        },
      });

      this.logger.success(deviceId, `Live started on ${sm.assignedNode}`);
    } catch (err) {
      this.logger.error(deviceId, `Dispatch failed: ${err.message}`);

      sm.transition('IDLE', { reason: `dispatch-failed: ${err.message}` });

      this._sendToDevice(deviceId, {
        type: 'event',
        action: 'live-failed',
        requestId: sm.requestId,
        error: err.message,
        errorCode: err.code || 'DISPATCH_FAILED',
      });
    }
  }

  /**
   * Android reports: camera/FFmpeg preparation failed.
   */
  _handlePrepareFailed(deviceId, msg) {
    const sm = this.registry.get(deviceId);

    if (sm.state !== 'PREPARING') {
      this.logger.warn(deviceId, `prepare-failed but state is ${sm.state}, ignoring`);
      return;
    }

    const error = msg.error || 'Camera preparation failed';
    this.logger.warn(deviceId, `Prepare failed: ${error}`);

    sm.transition('IDLE', { reason: `prepare-failed: ${error}` });

    this._sendToDevice(deviceId, {
      type: 'event',
      action: 'live-failed',
      requestId: sm.requestId,
      error,
      errorCode: 'PREPARE_FAILED',
    });
  }

  /**
   * Handle state machine timeout — called by DeviceRegistry.
   */
  async handleTimeout(deviceId, timedOutState, requestId) {
    const sm = this.registry.get(deviceId);

    // If state already changed, skip
    if (sm.state !== timedOutState) return;

    // If was dispatching, try to stop worker
    if (timedOutState === 'DISPATCHING' || timedOutState === 'STREAMING') {
      try {
        await this.orchestrator.stopLive({ deviceid: deviceId });
      } catch {}
    }

    sm.transition('IDLE', { reason: `${timedOutState}_TIMEOUT` });

    this._sendToDevice(deviceId, {
      type: 'event',
      action: 'live-failed',
      requestId: requestId || sm.requestId,
      error: `Timeout in state ${timedOutState}`,
      errorCode: `${timedOutState}_TIMEOUT`,
    });

    // Also tell Android to stop/cleanup
    this._sendToDevice(deviceId, {
      type: 'command',
      action: 'stop-live',
      requestId: randomUUID(),
    });
  }

  // ---------------------------------------------------------------------------
  // Stop request from Android
  // ---------------------------------------------------------------------------

  async _handleStopRequest(deviceId, msg) {
    const sm = this.registry.get(deviceId);
    const requestId = msg.requestId || randomUUID();

    if (sm.state === 'IDLE') {
      return this._sendToDevice(deviceId, {
        type: 'event',
        action: 'live-stopped',
        requestId,
      });
    }

    // If streaming, stop worker
    if (sm.state === 'STREAMING') {
      try {
        await this.orchestrator.stopLive({ deviceid: deviceId });
      } catch (err) {
        this.logger.warn(deviceId, `Worker stop failed: ${err.message}`);
      }
    }

    sm.transition('IDLE', { reason: 'manual-stop' });

    this._sendToDevice(deviceId, {
      type: 'event',
      action: 'live-stopped',
      requestId,
    });
  }

  // ---------------------------------------------------------------------------
  // Send helpers
  // ---------------------------------------------------------------------------

  _sendJSON(ws, obj) {
    try {
      ws.send(JSON.stringify(obj));
    } catch {}
  }

  _sendToDevice(deviceId, payload) {
    const set = this.connections.get(deviceId);
    if (!set || set.size === 0) {
      this.logger.warn(deviceId, `LiveWS: device offline, cannot send ${payload.action || payload.type}`);
      return false;
    }

    this.logger.debug(deviceId, `LiveWS send: ${payload.type}/${payload.action || '-'}`);
    for (const ws of set) {
      this._sendJSON(ws, payload);
    }
    return true;
  }

  isDeviceOnline(deviceId) {
    const set = this.connections.get(deviceId);
    return !!(set && set.size > 0);
  }

  getOnlineDevices() {
    return [...this.connections.keys()];
  }

  // ---------------------------------------------------------------------------
  // Ping/pong heartbeat (call from setInterval)
  // ---------------------------------------------------------------------------

  pingAll() {
    for (const [deviceId, set] of this.connections) {
      for (const ws of set) {
        if (!ws.isAlive) {
          this.logger.warn(deviceId, 'LiveWS: ping timeout, closing');
          ws.terminate();
          set.delete(ws);
          if (set.size === 0) this.connections.delete(deviceId);
          continue;
        }
        ws.isAlive = false;
        try { ws.ping(); } catch {}
      }
    }
  }
}

module.exports = { LiveWebSocket };
