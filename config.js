require('dotenv').config();

function parseInteger(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : fallback;
}

function parseBoolean(value, fallback = false) {
  if (typeof value === 'boolean') return value;
  if (typeof value !== 'string') return fallback;
  const v = value.trim().toLowerCase();
  if (v === 'true' || v === '1' || v === 'yes') return true;
  if (v === 'false' || v === '0' || v === 'no') return false;
  return fallback;
}

function parseJsonEnv(name, fallback) {
  const value = process.env[name];
  if (!value) return fallback;

  try {
    return JSON.parse(value);
  } catch (error) {
    throw new Error(`Invalid JSON in ${name}: ${error.message}`);
  }
}

function uniq(values) {
  return Array.from(new Set(values));
}

function parseCsvEnv(name, fallback = []) {
  const value = process.env[name];
  if (!value) return uniq(fallback.map((item) => String(item).trim()).filter(Boolean));
  return uniq(
    String(value)
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean),
  );
}

function normalizeNode(rawNode, index) {
  if (!rawNode || typeof rawNode !== 'object') {
    throw new Error(`STREAM_NODES[${index}] must be an object`);
  }

  const serverId = String(rawNode.serverId || rawNode.id || '').trim();
  if (!serverId) {
    throw new Error(`STREAM_NODES[${index}].serverId is required`);
  }

  return {
    serverId,
    name: String(rawNode.name || `stream-node-${index + 1}`),
    alwaysOn: parseBoolean(rawNode.alwaysOn, false),
    protocol: String(rawNode.protocol || 'http').toLowerCase(),
    streamPort: parseInteger(rawNode.streamPort, 3000),
    streamHost: rawNode.streamHost ? String(rawNode.streamHost) : null,
    apiPrefix: String(rawNode.apiPrefix || '/api'),
  };
}

function loadStreamNodes() {
  const jsonNodes = parseJsonEnv('STREAM_NODES', null);
  if (Array.isArray(jsonNodes)) {
    return jsonNodes.map(normalizeNode);
  }

  const serverIds = String(process.env.STREAM_SERVER_IDS || '')
    .split(',')
    .map((id) => id.trim())
    .filter(Boolean);

  if (serverIds.length === 0) return [];

  const defaultPort = parseInteger(process.env.STREAM_NODE_PORT, 3000);
  return serverIds.map((serverId, index) => ({
    serverId,
    name: `stream-node-${index + 1}`,
    alwaysOn: index === 0,
    protocol: 'http',
    streamPort: defaultPort,
    streamHost: null,
    apiPrefix: '/api',
  }));
}

const streamNodes = loadStreamNodes();
const minActiveNodes = Math.max(parseInteger(process.env.MIN_ACTIVE_NODES, 1), 0);

if (minActiveNodes > 0 && streamNodes.length > 0 && !streamNodes.some((node) => node.alwaysOn)) {
  streamNodes[0].alwaysOn = true;
}

const streamNodeIds = streamNodes.map((node) => node.serverId);
const whitelistServerIds = parseCsvEnv('WHITELIST_SERVER_IDS', streamNodeIds);
const whitelistServerIdSet = new Set(whitelistServerIds);

const invalidNodeIds = streamNodeIds.filter((serverId) => !whitelistServerIdSet.has(serverId));
if (invalidNodeIds.length > 0) {
  throw new Error(
    `STREAM_NODES contains serverId not in WHITELIST_SERVER_IDS: ${invalidNodeIds.join(', ')}`,
  );
}

const config = {
  port: parseInteger(process.env.ORCHESTRATOR_PORT || process.env.PORT, 4100),
  streamNodes,
  whitelistServerIds,
  minActiveNodes,
  reconcileIntervalMs: Math.max(parseInteger(process.env.RECONCILE_INTERVAL_MS, 30000), 5000),
  clusterRefreshCooldownMs: Math.max(parseInteger(process.env.CLUSTER_REFRESH_COOLDOWN_MS, 1000), 0),
  autoSleepIdleMs: Math.max(parseInteger(process.env.AUTO_SLEEP_IDLE_MS, 15 * 60 * 1000), 30 * 1000),
  nodeHealthTimeoutMs: Math.max(parseInteger(process.env.NODE_HEALTH_TIMEOUT_MS, 500), 100),
  nodeCommandTimeoutMs: Math.max(parseInteger(process.env.NODE_COMMAND_TIMEOUT_MS, 120000), 5000),
  fallbackNodeSlots: Math.max(parseInteger(process.env.FALLBACK_NODE_SLOTS, 1), 1),
  wakeTimeoutMs: Math.max(parseInteger(process.env.WAKE_TIMEOUT_MS, 240000), 30000),
  wakePollIntervalMs: Math.max(parseInteger(process.env.WAKE_POLL_INTERVAL_MS, 5000), 1000),
  bizfly: {
    project: process.env.BIZFLY_PROJECT || process.env.PROJECT || '',
    region: process.env.BIZFLY_REGION || 'hn',
    credentialId: process.env.CREDENTIAL_ID || '',
    credentialSecret: process.env.CREDENTIAL_SECRET || '',
    allowedServerIds: whitelistServerIds,
  },
};

module.exports = { config };
