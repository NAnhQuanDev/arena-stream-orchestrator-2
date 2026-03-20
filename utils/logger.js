// Simple logger utility
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  red: '\x1b[31m',
  cyan: '\x1b[36m',
  magenta: '\x1b[35m',
  gray: '\x1b[90m',
};

function timestamp() {
  return new Date().toISOString().substring(11, 19);
}

function log(type, deviceId, message) {
  const time = colors.gray + timestamp() + colors.reset;
  const device = deviceId ? `[${deviceId}]` : '';
  
  switch(type) {
    case 'success':
      console.log(`${time} ${colors.green}✓${colors.reset} ${device} ${message}`);
      break;
    case 'error':
      console.log(`${time} ${colors.red}✗${colors.reset} ${device} ${message}`);
      break;
    case 'warn':
      console.log(`${time} ${colors.yellow}⚠${colors.reset} ${device} ${message}`);
      break;
    case 'info':
      console.log(`${time} ${colors.cyan}→${colors.reset} ${device} ${message}`);
      break;
    case 'debug':
      console.log(`${time} ${colors.dim}${device} ${message}${colors.reset}`);
      break;
    default:
      console.log(`${time} ${device} ${message}`);
  }
}

module.exports = {
  success: (deviceId, msg) => log('success', deviceId, msg),
  error: (deviceId, msg) => log('error', deviceId, msg),
  warn: (deviceId, msg) => log('warn', deviceId, msg),
  info: (deviceId, msg) => log('info', deviceId, msg),
  debug: (deviceId, msg) => log('debug', deviceId, msg),
  api: (endpoint, deviceId = '') => log('info', deviceId, `API ${endpoint}`),
  simple: (msg) => console.log(`${colors.gray}${timestamp()}${colors.reset} ${msg}`),
  
  // Detailed logs with structured info
  detail: (deviceId, title, data) => {
    const time = colors.gray + timestamp() + colors.reset;
    const device = deviceId ? `[${deviceId}]` : '';
    console.log(`${time} ${colors.magenta}●${colors.reset} ${device} ${title}`);
    Object.entries(data).forEach(([key, val]) => {
      console.log(`${time}   ${colors.dim}${key}:${colors.reset} ${val}`);
    });
  },
};
