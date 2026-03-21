const { requestJson, sleep, maskStreamUrl } = require('./httpClient');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function nowIso() {
  return new Date().toISOString();
}

function toNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function normalizeApiPrefix(prefix) {
  const v = String(prefix || '/api').trim() || '/api';
  const s = v.startsWith('/') ? v : `/${v}`;
  return s.endsWith('/') ? s.slice(0, -1) : s;
}

function safeJson(value, maxLen = 800) {
  try {
    const t = JSON.stringify(value);
    return t.length <= maxLen ? t : `${t.slice(0, maxLen)}…`;
  } catch {
    return String(value);
  }
}

// ---------------------------------------------------------------------------
// OrchestratorService
// ---------------------------------------------------------------------------

class OrchestratorService {
  constructor({ config, bizflyClient, logger }) {
    this.config        = config;
    this.bizflyClient  = bizflyClient;
    this.logger        = logger;

    this.nodes                  = new Map(); // serverId → node
    this.deviceAssignments      = new Map(); // deviceId → serverId

    // Track device đang trong quá trình startLive (kể cả giai đoạn wakeup).
    // value: { cancelled: bool, epoch: number } — stopLive set cancelled=true để abort ngay.
    this.inflightStartState = new Map(); // deviceId → { cancelled, epoch }
    this.deviceStartEpoch = new Map();   // deviceId → epoch (monotonic)

    // Serial queue — serialize chỉ bước pick+reserve slot (sync, micro-giây).
    this.lastOperation = Promise.resolve();

    // Dedup concurrent refreshClusterStatus.
    this.clusterRefreshPromise = null;
    this.lastClusterRefreshAt  = 0;

    // Guard chống 2 reconcileNow chạy đồng thời (BUG-12).
    this.reconcileRunning = false;
    this.reconcileTimer   = null;

    for (const [priority, cfg] of config.streamNodes.entries()) {
      this.nodes.set(cfg.serverId, {
        ...cfg,
        priority,
        apiPrefix:                normalizeApiPrefix(cfg.apiPrefix),
        powerStatus:              'UNKNOWN',
        ipAddress:                null,
        health:                   { reachable: false, checkedAt: null, error: 'Not checked yet', statusCode: 0, stats: null },
        waking:                   false,
        wakePromise:              null,
        pendingStartReservations: 0,
        lastUsedAt:               null,
        idleSince:                Date.now(),
        updatedAt:                nowIso(),
      });
    }
  }

  // -------------------------------------------------------------------------
  // Serial queue — serialize pick+reserve
  // -------------------------------------------------------------------------

  enqueue(task) {
    const run = this.lastOperation.then(task, task);
    this.lastOperation = run.catch(() => {});
    return run;
  }

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  async bootstrap() {
    await this.refreshClusterStatus({ force: true });
    await this.ensureAlwaysOnNodes();
    await this.refreshClusterStatus({ force: true });

    this.reconcileTimer = setInterval(() => {
      this.reconcileNow().catch((err) =>
        this.logger.warn('orchestrator', `Reconcile failed: ${err.message}`)
      );
    }, this.config.reconcileIntervalMs);
  }

  stop() {
    if (this.reconcileTimer) {
      clearInterval(this.reconcileTimer);
      this.reconcileTimer = null;
    }
  }

  // -------------------------------------------------------------------------
  // Node helpers
  // -------------------------------------------------------------------------

  getNodeBaseUrl(node) {
    const host = node.streamHost || node.ipAddress;
    return host ? `${node.protocol}://${host}:${node.streamPort}` : null;
  }

  markNodeUnreachable(node, reason, statusCode = 0) {
    node.health    = { reachable: false, checkedAt: nowIso(), error: reason || 'Unreachable', statusCode, stats: null };
    node.updatedAt = nowIso();
  }

  normalizeStats(raw = {}) {
    const currentWorkers = Math.max(0, toNumber(raw.currentWorkers));
    const maxSlots       = Math.max(0, toNumber(raw.maxSlots));
    return {
      currentWorkers,
      maxSlots,
      availableSlots:   Math.max(0, toNumber(raw.availableSlots, maxSlots - currentWorkers)),
      runningDeviceIds: Array.isArray(raw.runningDeviceIds) ? raw.runningDeviceIds.map(String) : [],
      pendingDeviceIds: Array.isArray(raw.pendingDeviceIds) ? raw.pendingDeviceIds.map(String) : [],
      serverName:       raw.serverName || null,
      timestamp:        raw.timestamp  || null,
    };
  }

  // -------------------------------------------------------------------------
  // Health check
  // -------------------------------------------------------------------------

  async refreshNodeHealth(node) {
    const baseUrl = this.getNodeBaseUrl(node);
    if (!baseUrl) {
      this.markNodeUnreachable(node, 'Missing host/IP');
      return;
    }

    const timeoutMs = (node.waking || node.alwaysOn || node.powerStatus === 'ACTIVE')
      ? this.config.nodeHealthTimeoutMs
      : Math.min(this.config.nodeHealthTimeoutMs, 1500);

    const primary = await requestJson(`${baseUrl}${node.apiPrefix}/worker-status`, { method: 'GET', timeoutMs });

    if (primary.ok && primary.data) {
      const stats      = this.normalizeStats(primary.data);
      node.health      = { reachable: true, checkedAt: nowIso(), error: null, statusCode: primary.status, stats };
      node.powerStatus = 'ACTIVE';
      node.idleSince   = stats.currentWorkers === 0 ? (node.idleSince ?? Date.now()) : null;
      if (stats.currentWorkers > 0) node.lastUsedAt = Date.now();
      node.updatedAt   = nowIso();
      return;
    }

    const fallback = await requestJson(`${baseUrl}/healthz`, { method: 'GET', timeoutMs });
    if (fallback.ok) {
      const stats      = node.health?.stats ?? this.normalizeStats({
        currentWorkers: 0, maxSlots: this.config.fallbackNodeSlots, availableSlots: this.config.fallbackNodeSlots,
      });
      node.health      = {
        reachable: true, checkedAt: nowIso(),
        error: 'worker-status unavailable, used /healthz fallback',
        statusCode: primary.status || fallback.status || 200,
        stats,
      };
      node.powerStatus = 'ACTIVE';
      node.idleSince   = node.idleSince ?? Date.now();
      node.updatedAt   = nowIso();
      return;
    }

    const reason = `${primary.error || `worker-status ${primary.status}`}; ${fallback.error || `healthz ${fallback.status}`}`;
    this.markNodeUnreachable(node, reason, primary.status || fallback.status);
  }

  async safeRefreshNodeHealth(node, ctx = 'post-action') {
    try {
      await this.refreshNodeHealth(node);
    } catch (err) {
      this.logger.warn('orchestrator', `Health refresh failed (${ctx}) node=${node?.name}: ${err.message}`);
    }
  }

  // -------------------------------------------------------------------------
  // Cluster refresh — deduped, cooldown-aware
  // -------------------------------------------------------------------------

  async refreshClusterStatus({ force = false } = {}) {
    const cooldown = Math.max(0, toNumber(this.config.clusterRefreshCooldownMs));
    if (!force && cooldown > 0 && Date.now() - this.lastClusterRefreshAt < cooldown) return;
    if (this.clusterRefreshPromise) return this.clusterRefreshPromise;

    const promise = (async () => {
      const servers   = await this.bizflyClient.listServers();
      const serverMap = new Map(servers.map((s) => [s.id, s]));

      for (const node of this.nodes.values()) {
        const server = serverMap.get(node.serverId);
        if (!server) {
          node.powerStatus = 'NOT_FOUND';
          node.ipAddress   = null;
          this.markNodeUnreachable(node, 'Server not found in Bizfly list');
          for (const [did, sid] of this.deviceAssignments) {
            if (sid === node.serverId) this.deviceAssignments.delete(did);
          }
          continue;
        }
        node.powerStatus = server.status || 'UNKNOWN';
        node.ipAddress   = server.ip_addresses?.WAN_V4?.[0]?.addr || node.ipAddress || null;
        node.updatedAt   = nowIso();
      }

      await Promise.all([...this.nodes.values()].map((n) => this.refreshNodeHealth(n)));
      this.syncAssignmentsFromHealth();
      this.lastClusterRefreshAt = Date.now();
    })();

    this.clusterRefreshPromise = promise;
    try {
      await promise;
    } finally {
      if (this.clusterRefreshPromise === promise) this.clusterRefreshPromise = null;
    }
  }

  syncAssignmentsFromHealth() {
    // Xây dựng ground-truth từ health report của tất cả reachable node.
    const fromHealth = new Map();
    for (const node of this.nodes.values()) {
      if (!node.health?.reachable) continue;
      for (const did of node.health?.stats?.runningDeviceIds ?? []) {
        fromHealth.set(did, node.serverId);
      }
    }

    for (const [did, sid] of this.deviceAssignments) {
      const node = this.nodes.get(sid);

      if (!node || !node.health?.reachable) {
        // Node không reachable → xóa, không biết thật sự đang chạy gì.
        this.deviceAssignments.delete(did);
        continue;
      }

      // Node reachable nhưng device không có trong runningDeviceIds của nó
      // VÀ không có trong inflightStartState (không đang trong giai đoạn wakeup/dispatch).
      // → assignment stale, xóa để không chặn start lại.
      if (!fromHealth.has(did) && !this.inflightStartState.has(did)) {
        this.deviceAssignments.delete(did);
      }
    }

    // Thêm/cập nhật từ health report.
    for (const [did, sid] of fromHealth) {
      this.deviceAssignments.set(did, sid);
    }
  }

  // -------------------------------------------------------------------------
  // Slot management
  // -------------------------------------------------------------------------

  getEffectiveAvailableSlots(node) {
    if (!node || node.powerStatus === 'NOT_FOUND') return 0;
    const reserved = Math.max(0, toNumber(node.pendingStartReservations));
    if (node.health?.reachable) {
      return Math.max(0, toNumber(node.health.stats?.availableSlots) - reserved);
    }
    const hint = toNumber(node.health?.stats?.maxSlots) || Math.max(1, toNumber(this.config.fallbackNodeSlots, 1));
    return Math.max(0, hint - reserved);
  }

  // PHẢI gọi trong enqueue() — sync, không có await.
  pickAndReserveNode({ excludeServerIds = new Set(), onlyServerId = null } = {}) {
    const candidates = onlyServerId
      ? [this.nodes.get(onlyServerId)].filter(Boolean)
      : [...this.nodes.values()]
          .filter((n) => !excludeServerIds.has(n.serverId))
          .filter((n) => this.getEffectiveAvailableSlots(n) > 0)
          .sort((a, b) => {
            const r = (b.health?.reachable ? 1 : 0) - (a.health?.reachable ? 1 : 0);
            if (r !== 0) return r;
            const ac = (b.powerStatus === 'ACTIVE' ? 1 : 0) - (a.powerStatus === 'ACTIVE' ? 1 : 0);
            if (ac !== 0) return ac;
            // PACK strategy: dồn stream vào node đang có worker trước, chỉ spill sang node khác khi gần/full.
            const w = toNumber(b.health?.stats?.currentWorkers) - toNumber(a.health?.stats?.currentWorkers);
            if (w !== 0) return w;
            const e = this.getEffectiveAvailableSlots(a) - this.getEffectiveAvailableSlots(b);
            if (e !== 0) return e;
            if (a.alwaysOn !== b.alwaysOn) return a.alwaysOn ? -1 : 1;
            return (a.priority ?? Infinity) - (b.priority ?? Infinity);
          });

    for (const node of candidates) {
      if (this.getEffectiveAvailableSlots(node) <= 0) continue;
      node.pendingStartReservations = Math.max(0, toNumber(node.pendingStartReservations)) + 1;
      this.logger.info(
        'orchestrator',
        `Reserved slot node=${node.name} reserved=${node.pendingStartReservations} effective=${this.getEffectiveAvailableSlots(node)}`,
      );
      return node;
    }
    return null;
  }

  releaseSlot(node) {
    if (!node) return;
    node.pendingStartReservations = Math.max(0, toNumber(node.pendingStartReservations) - 1);
  }

  createNoSlotError(node, msg) {
    const err  = new Error(msg || `No available slot on node ${node?.name}`);
    err.status = 429;
    err.code   = 'NO_SLOT_AVAILABLE';
    err.noSlot = true;
    return err;
  }

  isNoSlotResponse(res) {
    return res.status === 429
      || res.data?.code === 'NO_SLOT_AVAILABLE'
      || String(res.data?.error || '').toLowerCase().includes('hết slot');
  }

  // -------------------------------------------------------------------------
  // Wake / sleep
  // -------------------------------------------------------------------------

  async wakeNode(serverId) {
    return this._wakeNode(serverId);
  }

  async _wakeNode(serverId) {
    const node = this.nodes.get(serverId);
    if (!node) throw new Error(`Unknown serverId: ${serverId}`);
    if (node.wakePromise) return node.wakePromise;

    const promise = (async () => {
      this.logger.info('orchestrator', `Wake requested node=${node.name}`);
      await this.refreshNodeHealth(node);
      if (node.health?.reachable) return this.serializeNode(node);

      node.waking = true;
      try {
        if (node.powerStatus !== 'ACTIVE') {
          try {
            await this.bizflyClient.startServer(serverId);
          } catch (err) {
            if (!this.isStartInProgressError(err)) throw err;
            this.logger.warn('orchestrator', `Bizfly start busy for ${node.name}, keep polling`);
          }
        }

        const ready = await this.waitUntilNodeReady(node);
        if (!ready) throw new Error(`Wake timeout for ${node.name}`);

        this.logger.info('orchestrator', `Wake success node=${node.name}`);
        return this.serializeNode(node);
      } finally {
        node.waking    = false;
        node.updatedAt = nowIso();
      }
    })();

    node.wakePromise = promise;
    try {
      return await promise;
    } finally {
      if (node.wakePromise === promise) node.wakePromise = null;
    }
  }

  async waitUntilNodeReady(node) {
    const deadline = Date.now() + this.config.wakeTimeoutMs;
    let attempt    = 0;

    while (Date.now() < deadline) {
      attempt += 1;
      try {
        this.logger.info('orchestrator', `Wake poll #${attempt} node=${node.name} power=${node.powerStatus} ip=${node.ipAddress || 'n/a'}`);

        if (!node.ipAddress || node.powerStatus !== 'ACTIVE') {
          const server = await this.bizflyClient.getServer(node.serverId).catch((err) => {
            this.logger.warn('orchestrator', `Bizfly poll ${node.name} failed: ${err.message}`);
            return null;
          });
          if (server) {
            node.powerStatus = server.status || node.powerStatus;
            node.ipAddress   = server.ip_addresses?.WAN_V4?.[0]?.addr || node.ipAddress || null;
          }
        }

        await this.refreshNodeHealth(node);
        this.logger.info('orchestrator', `Wake health #${attempt} node=${node.name} reachable=${!!node.health?.reachable}`);
        if (node.health?.reachable) return true;
      } catch (err) {
        this.logger.warn('orchestrator', `Wake poll error ${node.name}: ${err.message}`);
      }

      await sleep(this.config.wakePollIntervalMs);
    }

    return false;
  }

  isStartInProgressError(err) {
    const m = String(err?.message || '').toLowerCase();
    return m.includes('server is busy') || m.includes('already in progress')
      || m.includes('conflict') || m.includes('already active');
  }

  async sleepNode(serverId, { force = false, reason = 'manual' } = {}) {
    return this.enqueue(() => this._sleepNode(serverId, { force, reason }));
  }

  // FIX BUG-14: _sleepNode luôn chạy trong enqueue (cả autoSleep lẫn manual).
  // activeCount đọc bên trong lock → không thể race với _sleepNode khác.
  async _sleepNode(serverId, { force = false, reason = 'manual' } = {}) {
    const node = this.nodes.get(serverId);
    if (!node) throw new Error(`Unknown serverId: ${serverId}`);
    if (node.powerStatus !== 'ACTIVE') return this.serializeNode(node);

    if (node.alwaysOn && !force) throw new Error(`Cannot sleep always-on node (${node.name}) without force=true`);

    const workers = node.health?.stats?.currentWorkers || 0;
    if (workers > 0 && !force) throw new Error(`Node ${node.name} still has ${workers} running worker(s)`);

    const activeCount = [...this.nodes.values()].filter((n) => n.powerStatus === 'ACTIVE').length;
    if (!force && activeCount <= this.config.minActiveNodes) {
      throw new Error(`Cannot sleep node — min active nodes is ${this.config.minActiveNodes}`);
    }

    await this.bizflyClient.stopServer(serverId);
    node.powerStatus = 'SHUTOFF';
    node.idleSince   = Date.now();
    this.markNodeUnreachable(node, `Stopped by orchestrator (${reason})`);

    for (const [did, sid] of this.deviceAssignments) {
      if (sid === serverId) this.deviceAssignments.delete(did);
    }

    return this.serializeNode(node);
  }

  // -------------------------------------------------------------------------
  // Start live
  // -------------------------------------------------------------------------

  async startLive(payload) {
    const { deviceid, outputStream, serverId, targetServerId } = payload || {};

    if (!deviceid || !outputStream) {
      const err  = new Error('Missing required fields: deviceid, outputStream');
      err.status = 400;
      throw err;
    }

    const deviceId = String(deviceid);
    this.logger.info(
      'orchestrator',
      `startLive device=${deviceId} output=${maskStreamUrl(outputStream)} target=${targetServerId || serverId || 'auto'}`,
    );

    // Chặn request trùng: đang trong quá trình start (wakeup/dispatch) hoặc đã live.
    // inflightStartState tồn tại từ khi bắt đầu startLive đến khi finally xóa —
    // bao phủ toàn bộ giai đoạn wakeup, tức là spam lần 2 trong lúc đang wake cũng bị chặn.
    const existingInflight = this.inflightStartState.get(deviceId);
    if (existingInflight) {
      // Nếu request cũ đã bị stopLive cancel, cho phép request mới vào ngay.
      // Tránh kẹt DEVICE_START_BUSY khi user stop rồi start lại nhanh.
      if (existingInflight.cancelled) {
        this.inflightStartState.delete(deviceId);
      } else {
        const err  = new Error(`Device ${deviceId} already has an in-flight start request`);
        err.status = 409;
        err.code   = 'DEVICE_START_BUSY';
        throw err;
      }
    }

    if (this.deviceAssignments.has(deviceId)) {
      const assignedServerId = this.deviceAssignments.get(deviceId);
      const assignedNode     = this.nodes.get(assignedServerId);
      const err  = new Error(`Device ${deviceId} is already live on node ${assignedNode?.name ?? assignedServerId}`);
      err.status = 409;
      err.code   = 'DEVICE_ALREADY_LIVE';
      throw err;
    }

    // state.cancelled = true khi stopLive được gọi trong lúc đang start.
    // epoch giúp phân biệt request cũ/mới để tránh stop nhầm request mới.
    const nextEpoch = (this.deviceStartEpoch.get(deviceId) || 0) + 1;
    this.deviceStartEpoch.set(deviceId, nextEpoch);
    const state = { cancelled: false, epoch: nextEpoch };
    this.inflightStartState.set(deviceId, state);
    let reservedNode = null;

    const release = () => {
      this.releaseSlot(reservedNode);
      reservedNode = null;
    };

    // Throw ngay nếu stopLive đã cancel trong khi đang await (trước dispatch).
    const checkCancelled = () => {
      if (!state.cancelled) return;
      const err  = new Error(`Device ${deviceId} start was cancelled by stopLive`);
      err.status = 409;
      err.code   = 'DEVICE_START_CANCELLED';
      throw err;
    };

    // Sau khi dispatch thành công — stream đã chạy thật trên worker.
    // Nếu stopLive cancel trong cửa sổ này, gửi stoplive để dọn dẹp rồi throw.
    const checkCancelledAfterDispatch = (node) => {
      if (!state.cancelled) return;
      // Nếu đã có start request mới hơn, không gửi stoplive nữa để tránh kill nhầm stream mới.
      const latestEpoch = this.deviceStartEpoch.get(deviceId);
      if (latestEpoch !== state.epoch) {
        const err  = new Error(`Device ${deviceId} start was cancelled by stopLive`);
        err.status = 409;
        err.code   = 'DEVICE_START_CANCELLED';
        throw err;
      }
      this.requestWorker(node, '/stoplive', 'POST', { deviceid: deviceId }, this.config.nodeCommandTimeoutMs)
        .catch((e) => this.logger.warn('orchestrator', `Cancel stoplive failed device=${deviceId}: ${e.message}`));
      const err  = new Error(`Device ${deviceId} start was cancelled by stopLive`);
      err.status = 409;
      err.code   = 'DEVICE_START_CANCELLED';
      throw err;
    };

    try {
      await this.refreshClusterStatus();
      checkCancelled();

      // ---- Pinned target ----
      const pinnedId = String(targetServerId || serverId || '').trim();
      if (pinnedId) {
        const target = this.nodes.get(pinnedId);
        if (!target) {
          const err  = new Error(`Unknown target serverId: ${pinnedId}`);
          err.status = 400;
          err.code   = 'UNKNOWN_TARGET_NODE';
          throw err;
        }

        reservedNode = await this.enqueue(() => this.pickAndReserveNode({ onlyServerId: pinnedId }));
        if (!reservedNode) throw this.createNoSlotError(target, `No available slot on target node: ${target.name}`);
        checkCancelled();

        await this._wakeNode(pinnedId);
        checkCancelled();

        if (!target.health?.reachable) {
          const err  = new Error(`Target node not reachable: ${target.name}`);
          err.status = 503;
          err.code   = 'TARGET_NODE_UNREACHABLE';
          throw err;
        }

        const workerResponse = await this.dispatchStart(target, payload);
        checkCancelledAfterDispatch(target);
        this.deviceAssignments.set(deviceId, target.serverId);
        await this.safeRefreshNodeHealth(target, 'startlive-target');
        return { message: 'Livestream routed successfully (target node)', routedTo: this.serializeNode(target), workerResponse };
      }

      // ---- Auto-select ----
      reservedNode = await this.enqueue(() => this.pickAndReserveNode());

      if (!reservedNode) {
        const err  = new Error('No capacity available. All stream nodes are full or offline.');
        err.status = 503;
        err.code   = 'NO_CAPACITY';
        throw err;
      }

      await this._wakeNode(reservedNode.serverId);
      checkCancelled();

      if (!reservedNode.health?.reachable) {
        const failedId = reservedNode.serverId;
        release();
        await this.refreshClusterStatus({ force: true });
        checkCancelled();
        reservedNode = await this.enqueue(() => this.pickAndReserveNode({ excludeServerIds: new Set([failedId]) }));
        if (!reservedNode) {
          const err  = new Error('No capacity available after wake failure.');
          err.status = 503;
          err.code   = 'NO_CAPACITY';
          throw err;
        }
        await this._wakeNode(reservedNode.serverId);
        checkCancelled();
      }

      try {
        const workerResponse = await this.dispatchStart(reservedNode, payload);
        checkCancelledAfterDispatch(reservedNode);
        this.deviceAssignments.set(deviceId, reservedNode.serverId);
        await this.safeRefreshNodeHealth(reservedNode, 'startlive');
        this.logger.info('orchestrator', `startLive success routedTo=${reservedNode.name} device=${deviceId}`);
        return { message: 'Livestream routed successfully', routedTo: this.serializeNode(reservedNode), workerResponse };
      } catch (err) {
        if (!err.noSlot) throw err;

        const failedId = reservedNode.serverId;
        release();
        await this.refreshClusterStatus({ force: true });
        checkCancelled();
        reservedNode = await this.enqueue(() => this.pickAndReserveNode({ excludeServerIds: new Set([failedId]) }));
        if (!reservedNode) throw err;

        await this._wakeNode(reservedNode.serverId);
        checkCancelled();
        const workerResponse = await this.dispatchStart(reservedNode, payload);
        checkCancelledAfterDispatch(reservedNode);
        this.deviceAssignments.set(deviceId, reservedNode.serverId);
        await this.safeRefreshNodeHealth(reservedNode, 'startlive-fallback');
        this.logger.info('orchestrator', `startLive fallback success routedTo=${reservedNode.name} device=${deviceId}`);
        return { message: 'Livestream routed successfully (fallback node)', routedTo: this.serializeNode(reservedNode), workerResponse };
      }
    } finally {
      release();
      // Chỉ xóa nếu state hiện tại vẫn là của request này.
      // Tránh request cũ xóa nhầm state của request mới (race stop->start nhanh).
      if (this.inflightStartState.get(deviceId) === state) {
        this.inflightStartState.delete(deviceId);
      }
    }
  }

  async dispatchStart(node, payload) {
    this.logger.info('orchestrator', `Dispatch startlive → node=${node.name} device=${payload?.deviceid} output=${maskStreamUrl(payload?.outputStream)}`);

    const res = await this.requestWorker(node, '/startlive', 'POST', payload, this.config.nodeCommandTimeoutMs);

    if (!res.ok || res.data?.error) {
      const detail = res.data?.error || res.error || `status ${res.status}`;
      this.logger.warn('orchestrator', `Node ${node.name} startlive failed: ${detail}`);
      const err  = new Error(`Node ${node.name} startlive failed: ${detail}`);
      err.status = res.status || 502;
      err.code   = res.data?.code || 'NODE_START_FAILED';
      err.noSlot = this.isNoSlotResponse(res);
      throw err;
    }

    node.lastUsedAt = Date.now();
    node.idleSince  = null;

    this.logger.info('orchestrator', `Node ${node.name} startlive ok: ${safeJson({
      message: res.data?.message, resolution: res.data?.resolution,
      delayMethod: res.data?.delayMethod, delaySeconds: res.data?.delaySeconds,
    })}`);

    return res.data;
  }

  // -------------------------------------------------------------------------
  // Stop live
  // -------------------------------------------------------------------------

  async stopLive(payload) {
    const { deviceid } = payload || {};
    if (!deviceid) {
      const err  = new Error('Missing required field: deviceid');
      err.status = 400;
      throw err;
    }

    const deviceId = String(deviceid);

    // Nếu device đang trong quá trình startLive (wakeup / dispatch):
    // set cancelled=true để _startLive dừng ngay sau await tiếp theo,
    // đồng thời xóa assignment phòng trường hợp dispatch vừa xong.
    const inflightState = this.inflightStartState.get(deviceId);
    if (inflightState) {
      inflightState.cancelled = true;
      this.deviceAssignments.delete(deviceId);
      if (this.inflightStartState.get(deviceId) === inflightState) {
        this.inflightStartState.delete(deviceId);
      }
      this.logger.info('orchestrator', `stopLive cancelled in-flight start for device=${deviceId}`);
      return {
        message:        'In-flight start cancelled',
        routedTo:       null,
        workerResponse: { message: 'Start request cancelled by stopLive' },
      };
    }

    await this.refreshClusterStatus({ force: true });

    const target = await this.findNodeByDevice(deviceId);
    if (!target) {
      return {
        message:        'Device is not live on any reachable node',
        routedTo:       null,
        workerResponse: { message: 'Không có worker nào đang chạy' },
      };
    }

    const res = await this.requestWorker(target, '/stoplive', 'POST', { deviceid }, this.config.nodeCommandTimeoutMs);
    if (!res.ok) {
      const detail = res.data?.error || res.error || `status ${res.status}`;
      const err    = new Error(`Node ${target.name} stoplive failed: ${detail}`);
      err.status   = res.status || 502;
      throw err;
    }

    this.deviceAssignments.delete(deviceId);

    // Fire-and-forget sau khi trả response — lỗi chỉ warn, không ảnh hưởng caller.
    this.safeRefreshNodeHealth(target, 'stoplive').catch(() => {});
    this.autoSleepIdleNodes().catch((err) =>
      this.logger.warn('orchestrator', `autoSleep after stop failed: ${err.message}`)
    );

    return { message: 'Stop request routed successfully', routedTo: this.serializeNode(target), workerResponse: res.data };
  }

  // FIX BUG-05: probe song song thay vì tuần tự.
  // Thời gian = max(timeout của 1 node) thay vì N × timeout.
  async findNodeByDevice(deviceId) {
    const cached = this.deviceAssignments.get(deviceId);
    if (cached) {
      const node = this.nodes.get(cached);
      if (node?.health?.reachable) return node;
    }

    const reachable = [...this.nodes.values()].filter((n) => n.health?.reachable);
    if (reachable.length === 0) return null;

    return Promise.any(
      reachable.map(async (node) => {
        const res = await this.requestWorker(
          node, `/check-live-status/${encodeURIComponent(deviceId)}`,
          'GET', undefined, this.config.nodeHealthTimeoutMs,
        );
        if (res.ok && res.data?.isLive) return node;
        throw new Error('not live');
      })
    ).catch(() => null);
  }

  // -------------------------------------------------------------------------
  // Reconcile
  // -------------------------------------------------------------------------

  // FIX BUG-12: guard reconcileRunning chống 2 lần chạy đồng thời.
  async reconcileNow() {
    if (this.reconcileRunning) {
      this.logger.warn('orchestrator', 'Reconcile skipped — previous run still in progress');
      return null;
    }

    this.reconcileRunning = true;
    try {
      await this.refreshClusterStatus({ force: true });
      await this.ensureAlwaysOnNodes();
      await this.autoSleepIdleNodes();
      return this.enqueue(() => this.getClusterState());
    } finally {
      this.reconcileRunning = false;
    }
  }

  async ensureAlwaysOnNodes() {
    for (const node of this.nodes.values()) {
      if (!node.alwaysOn || (node.powerStatus === 'ACTIVE' && node.health?.reachable)) continue;
      await this._wakeNode(node.serverId).catch((err) =>
        this.logger.warn('orchestrator', `Always-on wake failed ${node.name}: ${err.message}`)
      );
    }
  }

  // FIX BUG-14 (phần 2): gọi _sleepNode qua enqueue để activeCount
  // được đọc trong lock, tránh race với manual sleepNode đồng thời.
  async autoSleepIdleNodes() {
    if (this.config.autoSleepIdleMs <= 0) return;

    const now         = Date.now();
    const activeCount = [...this.nodes.values()].filter((n) => n.powerStatus === 'ACTIVE').length;
    let remaining     = activeCount;

    const candidates = [...this.nodes.values()]
      .filter((n) => !n.alwaysOn
        && n.powerStatus    === 'ACTIVE'
        && n.health?.reachable
        && (n.health.stats?.currentWorkers || 0) === 0
        && n.pendingStartReservations === 0
        && n.idleSince && now - n.idleSince >= this.config.autoSleepIdleMs
      )
      .sort((a, b) => (a.idleSince || 0) - (b.idleSince || 0));

    for (const node of candidates) {
      if (remaining <= this.config.minActiveNodes) break;

      const slept = await this.enqueue(() =>
        this._sleepNode(node.serverId, { force: false, reason: 'auto-idle' })
      ).then(() => true).catch((err) => {
        this.logger.warn('orchestrator', `Auto-sleep skipped ${node.name}: ${err.message}`);
        return false;
      });

      if (slept) remaining -= 1;
    }
  }

  // -------------------------------------------------------------------------
  // HTTP helper
  // -------------------------------------------------------------------------

  async requestWorker(node, path, method, body, timeoutMs) {
    const baseUrl = this.getNodeBaseUrl(node);
    if (!baseUrl) return { ok: false, status: 0, data: null, error: 'Node has no reachable host/IP' };
    return requestJson(`${baseUrl}${node.apiPrefix}${path}`, { method, body, timeoutMs });
  }

  // -------------------------------------------------------------------------
  // Serialization / state
  // -------------------------------------------------------------------------

  serializeNode(node) {
    const stats = node.health?.stats ?? null;
    return {
      serverId:                node.serverId,
      name:                    node.name,
      alwaysOn:                node.alwaysOn,
      powerStatus:             node.powerStatus,
      ipAddress:               node.ipAddress,
      baseUrl:                 this.getNodeBaseUrl(node),
      reachable:               !!node.health?.reachable,
      checkedAt:               node.health?.checkedAt ?? null,
      error:                   node.health?.error ?? null,
      currentWorkers:          stats?.currentWorkers ?? null,
      maxSlots:                stats?.maxSlots ?? null,
      availableSlots:          stats?.availableSlots ?? null,
      reservedStartSlots:      Math.max(0, toNumber(node.pendingStartReservations)),
      effectiveAvailableSlots: this.getEffectiveAvailableSlots(node),
      runningDeviceIds:        stats?.runningDeviceIds ?? [],
      pendingDeviceIds:        stats?.pendingDeviceIds ?? [],
      waking:                  !!node.waking,
      idleSince:               node.idleSince  ? new Date(node.idleSince).toISOString()  : null,
      lastUsedAt:              node.lastUsedAt ? new Date(node.lastUsedAt).toISOString() : null,
      updatedAt:               node.updatedAt,
    };
  }

  getClusterState() {
    const nodes = [...this.nodes.values()].map((n) => this.serializeNode(n));
    return {
      summary: {
        configuredNodes: nodes.length,
        activeNodes:     nodes.filter((n) => n.powerStatus === 'ACTIVE').length,
        reachableNodes:  nodes.filter((n) => n.reachable).length,
        totalSlots:      nodes.reduce((s, n) => s + (n.maxSlots || 0), 0),
        usedSlots:       nodes.reduce((s, n) => s + (n.currentWorkers || 0), 0),
        availableSlots:  nodes.reduce((s, n) => s + (n.availableSlots || 0), 0),
        assignedDevices: this.deviceAssignments.size,
        minActiveNodes:  this.config.minActiveNodes,
        autoSleepIdleMs: this.config.autoSleepIdleMs,
        ts:              nowIso(),
      },
      nodes,
      assignments: [...this.deviceAssignments.entries()].map(([deviceId, sid]) => ({
        deviceId,
        serverId: sid,
        nodeName: this.nodes.get(sid)?.name ?? null,
      })),
    };
  }
}

module.exports = { OrchestratorService };
