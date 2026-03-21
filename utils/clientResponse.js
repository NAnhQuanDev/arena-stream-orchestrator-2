function success(type, message, data = null) {
  return {
    ok: true,
    type,
    message,
    data,
  };
}

function isStartLivePath(req) {
  return req.method === 'POST' && req.path.endsWith('/startlive');
}

function isStopLivePath(req) {
  return req.method === 'POST' && req.path.endsWith('/stoplive');
}

function mapStartLiveError(error) {
  const code = String(error?.code || '');

  if (code === 'NO_CAPACITY' || code === 'NO_SLOT_AVAILABLE') {
    return {
      type: 'NO_CAPACITY',
      message: 'Khong du node/slot de livestream, vui long thu lai sau.',
    };
  }

  if (code === 'DEVICE_START_BUSY' || code === 'DEVICE_ALREADY_LIVE') {
    return {
      type: 'DEVICE_BUSY',
      message: 'Thiet bi dang ban, vui long dung stream hien tai hoac thu lai sau.',
    };
  }

  if (code === 'DEVICE_START_CANCELLED') {
    return {
      type: 'STARTLIVE_CANCELLED',
      message: 'Start live da bi huy boi lenh stoplive.',
    };
  }

  return {
    type: 'STARTLIVE_FAILED',
    message: 'Start live that bai.',
  };
}

function mapStopLiveError() {
  return {
    type: 'STOPLIVE_FAILED',
    message: 'Stop live that bai.',
  };
}

function toClientError(req, error) {
  const status = Number(error?.status) || 500;
  const code = error?.code || 'INTERNAL_ERROR';

  let mapped;
  if (isStartLivePath(req)) mapped = mapStartLiveError(error);
  else if (isStopLivePath(req)) mapped = mapStopLiveError(error);
  else {
    mapped = {
      type: 'REQUEST_FAILED',
      message: error?.message || 'Request failed.',
    };
  }

  return {
    status,
    payload: {
      ok: false,
      type: mapped.type,
      message: mapped.message,
      error: error?.message || 'Internal Server Error',
      code,
    },
  };
}

module.exports = {
  success,
  toClientError,
};
