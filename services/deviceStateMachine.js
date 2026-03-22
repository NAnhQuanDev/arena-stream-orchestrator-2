const { randomUUID } = require('crypto');

// Valid state transitions
const TRANSITIONS = {
  IDLE:        ['PREPARING', 'IDLE'],
  PREPARING:   ['DISPATCHING', 'IDLE'],
  DISPATCHING: ['STREAMING', 'IDLE'],
  STREAMING:   ['STOPPING', 'IDLE'],
  STOPPING:    ['IDLE'],
};

// Timeout per state (ms)
const STATE_TIMEOUTS = {
  PREPARING:   30_000,   // 30s — chờ Android chuẩn bị camera + FFmpeg
  DISPATCHING: 120_000,  // 120s — chờ worker spawn FFmpeg
  STOPPING:    15_000,   // 15s — chờ cleanup
};

class DeviceStateMachine {
  constructor(deviceId, { logger, onTimeout }) {
    this.deviceId = deviceId;
    this.logger = logger;
    this.onTimeout = onTimeout;

    this.state = 'IDLE';
    this.requestId = null;
    this.triggerSource = null;   // 'manual' | 'remote'
    this.streamUrl = null;
    this.gameType = null;
    this.rtmpTarget = null;
    this.assignedNode = null;
    this.timeoutTimer = null;
    this.startedAt = null;
    this.stateHistory = [];
  }

  canTransition(newState) {
    const allowed = TRANSITIONS[this.state];
    return allowed ? allowed.includes(newState) : false;
  }

  transition(newState, context = {}) {
    if (!this.canTransition(newState)) {
      const msg = `Invalid transition: ${this.state} → ${newState}`;
      this.logger.warn(this.deviceId, `StateMachine: ${msg}`);
      throw new Error(msg);
    }

    const prev = this.state;
    this.state = newState;

    this.stateHistory.push({
      from: prev,
      to: newState,
      at: Date.now(),
      reason: context.reason || null,
    });

    // Keep history bounded
    if (this.stateHistory.length > 20) {
      this.stateHistory = this.stateHistory.slice(-20);
    }

    this.logger.info(this.deviceId, `StateMachine: ${prev} → ${newState}${context.reason ? ` (${context.reason})` : ''}`);

    // Clear previous timeout
    this._clearTimeout();

    // Set new timeout
    const timeoutMs = STATE_TIMEOUTS[newState];
    if (timeoutMs) {
      this.timeoutTimer = setTimeout(() => {
        this.logger.warn(this.deviceId, `StateMachine: ${newState} timed out after ${timeoutMs}ms`);
        if (this.onTimeout) {
          this.onTimeout(this.deviceId, newState, this.requestId);
        }
      }, timeoutMs);
    }

    // Reset fields on IDLE
    if (newState === 'IDLE') {
      this.requestId = null;
      this.triggerSource = null;
      this.streamUrl = null;
      this.gameType = null;
      this.rtmpTarget = null;
      this.assignedNode = null;
      this.startedAt = null;
    }

    if (newState === 'STREAMING') {
      this.startedAt = new Date().toISOString();
    }
  }

  /**
   * Begin a new live session.
   * @param {'manual'|'remote'} triggerSource
   * @param {object} params - { streamUrl, gameType, rtmpTarget }
   * @param {string} [requestId] - optional, auto-generated if omitted
   */
  beginLive(triggerSource, params, requestId) {
    if (this.state !== 'IDLE') {
      const err = new Error(`Device ${this.deviceId} is not idle (current: ${this.state})`);
      err.code = 'ALREADY_IN_PROGRESS';
      err.status = 409;
      throw err;
    }

    this.requestId = requestId || randomUUID();
    this.triggerSource = triggerSource;
    this.streamUrl = params.streamUrl;
    this.gameType = params.gameType;
    this.rtmpTarget = params.rtmpTarget;

    this.transition('PREPARING', { reason: `${triggerSource}-start` });
    return this.requestId;
  }

  toJSON() {
    return {
      deviceId: this.deviceId,
      state: this.state,
      requestId: this.requestId,
      triggerSource: this.triggerSource,
      streamUrl: this.streamUrl ? '***' : null,
      gameType: this.gameType,
      assignedNode: this.assignedNode,
      startedAt: this.startedAt,
      history: this.stateHistory.slice(-10),
    };
  }

  _clearTimeout() {
    if (this.timeoutTimer) {
      clearTimeout(this.timeoutTimer);
      this.timeoutTimer = null;
    }
  }

  destroy() {
    this._clearTimeout();
  }
}

/**
 * Registry — manages all device state machines.
 */
class DeviceRegistry {
  constructor({ logger, onTimeout }) {
    this.machines = new Map();
    this.logger = logger;
    this.onTimeout = onTimeout;
  }

  get(deviceId) {
    if (!this.machines.has(deviceId)) {
      this.machines.set(
        deviceId,
        new DeviceStateMachine(deviceId, {
          logger: this.logger,
          onTimeout: this.onTimeout,
        }),
      );
    }
    return this.machines.get(deviceId);
  }

  has(deviceId) {
    return this.machines.has(deviceId);
  }

  all() {
    return [...this.machines.values()];
  }

  /**
   * Cleanup idle machines that haven't been used for a while.
   */
  gc(maxIdleMs = 30 * 60 * 1000) {
    const now = Date.now();
    for (const [deviceId, sm] of this.machines) {
      if (sm.state !== 'IDLE') continue;
      const lastEntry = sm.stateHistory[sm.stateHistory.length - 1];
      if (lastEntry && now - lastEntry.at > maxIdleMs) {
        sm.destroy();
        this.machines.delete(deviceId);
      }
    }
  }
}

module.exports = { DeviceStateMachine, DeviceRegistry, STATE_TIMEOUTS };
