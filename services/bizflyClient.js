const { requestJson } = require('./httpClient');

class BizflyClient {
  constructor({ project, region, credentialId, credentialSecret, allowedServerIds = [] }) {
    this.project = project;
    this.region = region || 'hn';
    this.credentialId = credentialId;
    this.credentialSecret = credentialSecret;
    this.allowedServerIds = new Set(
      (Array.isArray(allowedServerIds) ? allowedServerIds : [])
        .map((serverId) => String(serverId).trim())
        .filter(Boolean),
    );

    this.cachedToken = null;
    this.tokenExpireAt = 0;
    this.refreshPromise = null;

    this.iaasBase = `https://${this.region}.manage.bizflycloud.vn/iaas-cloud/api`;
  }

  validateConfig() {
    if (!this.project) throw new Error('Missing BIZFLY_PROJECT/PROJECT');
    if (!this.credentialId) throw new Error('Missing CREDENTIAL_ID');
    if (!this.credentialSecret) throw new Error('Missing CREDENTIAL_SECRET');
  }

  isAllowedServer(serverId) {
    if (this.allowedServerIds.size === 0) return true;
    return this.allowedServerIds.has(String(serverId));
  }

  assertAllowedServer(serverId) {
    if (this.isAllowedServer(serverId)) return;
    throw new Error(`Blocked by whitelist: serverId ${serverId} is not allowed`);
  }

  isTokenValid() {
    if (!this.cachedToken || !this.tokenExpireAt) return false;
    return Date.now() < this.tokenExpireAt - 60_000;
  }

  async getToken() {
    if (this.isTokenValid()) return this.cachedToken;

    if (this.refreshPromise) return this.refreshPromise;

    this.refreshPromise = (async () => {
      const payload = {
        auth_method: 'application_credential',
        credential_id: this.credentialId,
        credential_secret: this.credentialSecret,
      };

      const response = await requestJson('https://manage.bizflycloud.vn/api/token', {
        method: 'POST',
        body: payload,
        timeoutMs: 15000,
      });

      if (!response.ok || !response.data?.token) {
        const detail = response.data?.message || response.error || `status ${response.status}`;
        throw new Error(`Bizfly auth failed: ${detail}`);
      }

      this.cachedToken = response.data.token;
      this.tokenExpireAt = new Date(response.data.expire_at || Date.now() + 15 * 60 * 1000).getTime();
      return this.cachedToken;
    })();

    try {
      return await this.refreshPromise;
    } finally {
      this.refreshPromise = null;
    }
  }

  async iaasRequest(method, path, body, timeoutMs = 15000) {
    this.validateConfig();
    const token = await this.getToken();

    const response = await requestJson(`${this.iaasBase}${path}`, {
      method,
      body,
      timeoutMs,
      headers: {
        'X-Auth-Token': token,
        'X-Tenant-Name': this.project,
      },
    });

    if (!response.ok) {
      const detail = response.data?.message || response.error || `status ${response.status}`;
      throw new Error(`Bizfly ${method} ${path} failed: ${detail}`);
    }

    return response.data;
  }

  async listServers() {
    const data = await this.iaasRequest('GET', '/servers', undefined, 20000);
    const servers = data?.servers || data;
    if (!Array.isArray(servers)) return [];
    if (this.allowedServerIds.size === 0) return servers;
    return servers.filter((server) => this.isAllowedServer(server.id));
  }

  async getServer(serverId) {
    this.assertAllowedServer(serverId);
    const data = await this.iaasRequest('GET', `/servers/${serverId}`, undefined, 15000);
    return data?.server || data;
  }

  async startServer(serverId) {
    this.assertAllowedServer(serverId);
    return this.iaasRequest('POST', `/servers/${serverId}/action`, { action: 'start' }, 15000);
  }

  async stopServer(serverId) {
    this.assertAllowedServer(serverId);
    return this.iaasRequest('POST', `/servers/${serverId}/action`, { action: 'stop' }, 15000);
  }
}

module.exports = {
  BizflyClient,
};
