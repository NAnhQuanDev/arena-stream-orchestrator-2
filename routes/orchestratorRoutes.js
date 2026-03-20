const express = require('express');
const { success } = require('../utils/clientResponse');

function asyncHandler(fn) {
  return (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
}

function buildRoutes(orchestrator) {
  const router = express.Router();

  const handleStartLive = asyncHandler(async (req, res) => {
    const result = await orchestrator.startLive(req.body || {});
    res.json(success('STARTLIVE_SUCCESS', 'Start live thanh cong.', result));
  });

  const handleStopLive = asyncHandler(async (req, res) => {
    const result = await orchestrator.stopLive(req.body || {});
    res.json(success('STOPLIVE_SUCCESS', 'Stop live thanh cong.', result));
  });

  router.post('/startlive', handleStartLive);
  router.post('/stoplive', handleStopLive);

  router.post('/orchestrator/startlive', handleStartLive);
  router.post('/orchestrator/stoplive', handleStopLive);

  router.get('/orchestrator/cluster', asyncHandler(async (_req, res) => {
    res.json(orchestrator.getClusterState());
  }));

  router.post('/orchestrator/reconcile', asyncHandler(async (_req, res) => {
    const state = await orchestrator.reconcileNow();
    res.json({ message: 'Reconcile done', ...state });
  }));

  router.post('/orchestrator/servers/:serverId/wake', asyncHandler(async (req, res) => {
    const node = await orchestrator.wakeNode(req.params.serverId);
    res.json({ message: 'Wake requested', node });
  }));

  router.post('/orchestrator/servers/:serverId/sleep', asyncHandler(async (req, res) => {
    const force = req.body?.force === true || req.query.force === 'true';
    const node = await orchestrator.sleepNode(req.params.serverId, {
      force,
      reason: 'manual-api',
    });

    res.json({ message: 'Sleep requested', node });
  }));

  return router;
}

module.exports = {
  buildRoutes,
};
