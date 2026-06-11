const { app, BrowserWindow, Menu, dialog, shell } = require('electron');
const { spawn } = require('child_process');
const fs = require('fs');
const http = require('http');
const net = require('net');
const path = require('path');

let mainWindow = null;
let backend = null;
let backendPort = null;
let backendUrl = null;
let backendStartupLog = '';
let progressTimer = null;

function projectRoot() {
  if (app.isPackaged) {
    return path.join(process.resourcesPath, 'backend');
  }
  return path.resolve(__dirname, '..');
}

function defaultDbPath() {
  if (process.env.FILE_INDEXER_DB) {
    return process.env.FILE_INDEXER_DB;
  }

  return path.join(app.getPath('userData'), 'files.db');
}

function findUv() {
  const candidates = [
    process.env.UV,
    '/opt/homebrew/bin/uv',
    '/usr/local/bin/uv',
    path.join(app.getPath('home'), '.local', 'bin', 'uv'),
  ].filter(Boolean);

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate;
  }
  return 'uv';
}

function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      server.close(() => resolve(address.port));
    });
  });
}

function waitForBackend(url, timeoutMs = 30000) {
  const started = Date.now();

  return new Promise((resolve, reject) => {
    const probe = () => {
      const req = http.get(`${url}/api/run/status`, (res) => {
        res.resume();
        if (res.statusCode === 200) {
          resolve();
          return;
        }
        retry();
      });

      req.on('error', retry);
      req.setTimeout(1000, () => {
        req.destroy();
        retry();
      });
    };

    const retry = () => {
      if (Date.now() - started > timeoutMs) {
        reject(new Error('Timed out waiting for the Python backend to start.'));
        return;
      }
      setTimeout(probe, 250);
    };

    probe();
  });
}

function getJson(url) {
  return new Promise((resolve, reject) => {
    const req = http.get(url, (res) => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        body += chunk;
      });
      res.on('end', () => {
        if (res.statusCode !== 200) {
          reject(new Error(`HTTP ${res.statusCode}: ${body}`));
          return;
        }
        try {
          resolve(JSON.parse(body));
        } catch (error) {
          reject(error);
        }
      });
    });

    req.on('error', reject);
    req.setTimeout(1000, () => {
      req.destroy(new Error('Request timed out.'));
    });
  });
}

function progressFromStatus(status) {
  if (!status || !status.active) return -1;
  const match = String(status.progress || '').match(/(\d{1,3})%\|/);
  if (!match) return 2;
  return Math.max(0, Math.min(1, Number(match[1]) / 100));
}

function applyNativeProgress(status) {
  if (!mainWindow || mainWindow.isDestroyed()) return;

  const progress = progressFromStatus(status);
  if (progress === -1) {
    mainWindow.setProgressBar(-1);
    mainWindow.setTitle('Kendex');
    return;
  }

  if (progress > 1) {
    mainWindow.setProgressBar(2, { mode: 'indeterminate' });
    mainWindow.setTitle(`Kendex - ${status.mode || 'maintenance'} running`);
    return;
  }

  mainWindow.setProgressBar(progress);
  mainWindow.setTitle(`Kendex - ${Math.round(progress * 100)}%`);
}

function startProgressPolling() {
  if (progressTimer) clearInterval(progressTimer);

  const tick = async () => {
    try {
      applyNativeProgress(await getJson(`${backendUrl}/api/run/status`));
    } catch {
      if (mainWindow && !mainWindow.isDestroyed()) {
        mainWindow.setProgressBar(-1);
      }
    }
  };

  tick();
  progressTimer = setInterval(tick, 1500);
}

function stopProgressPolling() {
  if (!progressTimer) return;
  clearInterval(progressTimer);
  progressTimer = null;
}

async function startBackend() {
  backendPort = await getFreePort();
  backendUrl = `http://127.0.0.1:${backendPort}`;

  const root = projectRoot();
  const dbPath = defaultDbPath();
  const uvStateDir = path.join(app.getPath('userData'), 'uv');
  fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  fs.mkdirSync(uvStateDir, { recursive: true });

  const env = {
    ...process.env,
    FILE_INDEXER_DB: dbPath,
    UV_CACHE_DIR: path.join(uvStateDir, 'cache'),
    UV_PROJECT_ENVIRONMENT: path.join(uvStateDir, 'venv'),
    PATH: [
      '/opt/homebrew/bin',
      '/usr/local/bin',
      path.join(app.getPath('home'), '.local', 'bin'),
      process.env.PATH || '',
    ].join(path.delimiter),
  };

  backendStartupLog = '';
  backend = spawn(
    findUv(),
    ['run', 'query_app.py', '--host', '127.0.0.1', '--port', String(backendPort)],
    {
      cwd: root,
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );

  backend.stdout.on('data', (chunk) => {
    const text = chunk.toString();
    backendStartupLog = (backendStartupLog + text).slice(-8000);
    process.stdout.write(`[backend] ${text}`);
  });
  backend.stderr.on('data', (chunk) => {
    const text = chunk.toString();
    backendStartupLog = (backendStartupLog + text).slice(-8000);
    process.stderr.write(`[backend] ${text}`);
  });
  backend.on('error', (error) => {
    backendStartupLog = (backendStartupLog + error.stack).slice(-8000);
  });
  backend.on('exit', (code, signal) => {
    backend = null;
    if (mainWindow && !mainWindow.isDestroyed()) {
      mainWindow.webContents.send('backend-exit', { code, signal });
    }
  });

  const backendExited = new Promise((_, reject) => {
    backend.once('exit', (code, signal) => {
      reject(new Error(
        `Python backend exited before startup completed (code ${code}, signal ${signal}).\n\n${backendStartupLog}`,
      ));
    });
    backend.once('error', (error) => reject(error));
  });

  await Promise.race([waitForBackend(backendUrl), backendExited]);
}

async function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1280,
    height: 840,
    minWidth: 980,
    minHeight: 640,
    title: 'Kendex',
    show: false,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      sandbox: true,
    },
  });

  mainWindow.once('ready-to-show', () => {
    mainWindow.show();
  });

  await mainWindow.loadURL(backendUrl);
  startProgressPolling();
}

function stopBackend() {
  if (!backend) return;
  const proc = backend;
  backend = null;
  proc.kill('SIGTERM');
}

function installMenu() {
  const revealDb = () => {
    const dbPath = defaultDbPath();
    if (fs.existsSync(dbPath)) {
      shell.showItemInFolder(dbPath);
    } else {
      shell.openPath(path.dirname(dbPath));
    }
  };

  const template = [
    {
      label: app.name,
      submenu: [
        { role: 'about' },
        { type: 'separator' },
        { role: 'quit' },
      ],
    },
    {
      label: 'File',
      submenu: [
        {
          label: 'Open Database Location',
          click: revealDb,
        },
        {
          label: 'Open App Data Folder',
          click: () => shell.openPath(app.getPath('userData')),
        },
        { type: 'separator' },
        { role: 'close' },
      ],
    },
    {
      label: 'View',
      submenu: [
        { role: 'reload' },
        { role: 'forceReload' },
        { role: 'toggleDevTools' },
        { type: 'separator' },
        { role: 'resetZoom' },
        { role: 'zoomIn' },
        { role: 'zoomOut' },
        { type: 'separator' },
        { role: 'togglefullscreen' },
      ],
    },
    {
      label: 'Window',
      submenu: [
        { role: 'minimize' },
        { role: 'zoom' },
      ],
    },
  ];

  Menu.setApplicationMenu(Menu.buildFromTemplate(template));
}

app.whenReady().then(async () => {
  installMenu();
  try {
    await startBackend();
    if (process.env.FILE_INDEXER_ELECTRON_SMOKE === '1') {
      console.log(`Smoke test backend ready at ${backendUrl}`);
      app.quit();
      return;
    }
    await createWindow();
  } catch (error) {
    dialog.showErrorBox('Kendex failed to start', error.stack || String(error));
    app.quit();
  }
});

app.on('window-all-closed', () => {
  app.quit();
});

app.on('before-quit', () => {
  stopProgressPolling();
  stopBackend();
});
