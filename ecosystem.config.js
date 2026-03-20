module.exports = {
  apps: [
    {
      name: 'arena-stream-orchestrator',
      cwd: __dirname,
      script: './index.js',
      instances: 1,
      exec_mode: 'fork',
      watch: false,
      time: true,
      out_file: '~/.pm2/logs/arena-stream-orchestrator-out.log',
      error_file: '~/.pm2/logs/arena-stream-orchestrator-err.log',
      merge_logs: true,
      autorestart: true,
      restart_delay: 2000,
      max_restarts: 20,
      max_memory_restart: '500M',
    },
  ],
};
