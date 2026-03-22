const express = require('express');

function asyncHandler(fn) {
  return (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
}

/**
 * REST API v1 cho website/admin điều khiển livestream.
 *
 * POST /api/v1/live/start    — yêu cầu device bắt đầu live
 * POST /api/v1/live/stop     — yêu cầu device dừng live
 * GET  /api/v1/live/status/:deviceId — xem trạng thái live
 * GET  /api/v1/devices       — danh sách devices online
 */
function buildLiveRoutes(liveWs, deviceRegistry) {
  const router = express.Router();

  // -----------------------------------------------------------------------
  // Start live (remote trigger từ website)
  // -----------------------------------------------------------------------
  router.post('/live/start', asyncHandler(async (req, res) => {
    const deviceId = String(req.body?.deviceId || req.body?.deviceid || '').trim();
    const streamUrl = req.body?.streamUrl || req.body?.outputStream || '';
    const gameType = req.body?.gameType || req.body?.type || 'carom';

    if (!deviceId) {
      return res.status(400).json({ ok: false, error: 'Missing deviceId' });
    }
    if (!streamUrl) {
      return res.status(400).json({ ok: false, error: 'Missing streamUrl' });
    }

    try {
      const result = liveWs.startFromRemote(deviceId, { streamUrl, gameType });

      // 202 Accepted — async operation, client polls status
      return res.status(202).json({
        ok: true,
        message: 'Live start command sent to device',
        requestId: result.requestId,
        state: result.state,
        deviceId,
      });
    } catch (err) {
      const status = err.status || 500;
      return res.status(status).json({
        ok: false,
        error: err.message,
        code: err.code || 'START_FAILED',
      });
    }
  }));

  // -----------------------------------------------------------------------
  // Stop live (remote trigger từ website)
  // -----------------------------------------------------------------------
  router.post('/live/stop', asyncHandler(async (req, res) => {
    const deviceId = String(req.body?.deviceId || req.body?.deviceid || '').trim();

    if (!deviceId) {
      return res.status(400).json({ ok: false, error: 'Missing deviceId' });
    }

    try {
      const result = await liveWs.stopFromRemote(deviceId);
      return res.json({ ok: true, ...result });
    } catch (err) {
      const status = err.status || 500;
      return res.status(status).json({
        ok: false,
        error: err.message,
        code: err.code || 'STOP_FAILED',
      });
    }
  }));

  // -----------------------------------------------------------------------
  // Status (poll từ website)
  // -----------------------------------------------------------------------
  router.get('/live/status/:deviceId', (req, res) => {
    const deviceId = String(req.params.deviceId || '').trim();
    if (!deviceId) {
      return res.status(400).json({ ok: false, error: 'Missing deviceId' });
    }

    const sm = deviceRegistry.get(deviceId);
    const online = liveWs.isDeviceOnline(deviceId);

    return res.json({
      ok: true,
      online,
      ...sm.toJSON(),
    });
  });

  // -----------------------------------------------------------------------
  // Devices online
  // -----------------------------------------------------------------------
  router.get('/devices', (_req, res) => {
    const onlineIds = liveWs.getOnlineDevices();
    const devices = onlineIds.map((deviceId) => {
      const sm = deviceRegistry.get(deviceId);
      return {
        deviceId,
        state: sm.state,
        requestId: sm.requestId,
        gameType: sm.gameType,
        startedAt: sm.startedAt,
      };
    });

    return res.json({
      ok: true,
      count: devices.length,
      devices,
    });
  });

  return router;
}

module.exports = { buildLiveRoutes };
