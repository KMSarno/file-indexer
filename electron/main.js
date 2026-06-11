const { app, BrowserWindow, Menu, dialog, powerSaveBlocker, shell } = require('electron');
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

function configPath() {
  return path.join(app.getPath('userData'), 'config.json');
}

function readConfig() {
  try {
    return JSON.parse(fs.readFileSync(configPath(), 'utf8'));
  } catch {
    return {};
  }
}

function writeConfig(patch) {
  const next = { ...readConfig(), ...patch };
  fs.mkdirSync(app.getPath('userData'), { recursive: true });
  fs.writeFileSync(configPath(), JSON.stringify(next, null, 2));
  return next;
}

// Resolve the database path. Precedence: FILE_INDEXER_DB env var (dev / CLI
// override) > the location chosen in-app (config.json) > the default under
// userData. A Finder-launched .app doesn't inherit the shell environment, so
// the in-app choice is the practical way for most users to relocate the index
// onto another volume.
function defaultDbPath() {
  if (process.env.FILE_INDEXER_DB) {
    return process.env.FILE_INDEXER_DB;
  }

  const configured = readConfig().dbPath;
  if (configured) {
    return configured;
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

// First launch can be slow: `uv run` resolves/builds the project virtualenv
// before the server binds. That cold start (especially on a fresh install, or
// the App Translocation copy) routinely exceeds 30s, so give it generous room;
// warm launches still resolve in a second or two.
function waitForBackend(url, timeoutMs = 120000) {
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

let sleepBlockerId = null;

// Hold off idle sleep while a maintenance run is active (the crawler can run
// for hours); released automatically when the run ends or the app quits.
// 'prevent-app-suspension' is deliberate: per the Electron docs it "keeps
// system active, but allows screen to be turned off" (macOS:
// PreventUserIdleSystemSleep) — i.e. caffeinate -i, without forcing the
// display to stay lit all night like 'prevent-display-sleep' would.
function updateSleepBlocker(active) {
  if (active && sleepBlockerId === null) {
    sleepBlockerId = powerSaveBlocker.start('prevent-app-suspension');
  } else if (!active && sleepBlockerId !== null) {
    powerSaveBlocker.stop(sleepBlockerId);
    sleepBlockerId = null;
  }
}

function applyNativeProgress(status) {
  updateSleepBlocker(!!(status && status.active));
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
    closeSplash();
    mainWindow.show();
  });

  await mainWindow.loadURL(backendUrl);
  startProgressPolling();
}

let splashWindow = null;

// A tiny always-on-top window shown immediately at launch, so the first cold
// start (which can spend a minute building the uv venv before the UI loads)
// doesn't look like a frozen Dock bounce with no window.
function showSplash() {
  splashWindow = new BrowserWindow({
    width: 380, height: 220, resizable: false, frame: false,
    show: false, backgroundColor: '#17161b', center: true,
  });
  const html = `<!doctype html><meta charset="utf-8"><style>
    html,body{margin:0;height:100%;font:13px -apple-system,sans-serif;color:#d8d5cf;
      background:linear-gradient(165deg,#1c1a20,#141318);display:flex;
      flex-direction:column;align-items:center;justify-content:center;gap:14px;
      -webkit-user-select:none;cursor:default}
    .mark{width:46px;height:46px;border-radius:11px;display:grid;place-items:center;
      background:linear-gradient(145deg,#f4c178,#b87425);color:#221302;
      font:700 20px ui-monospace,Menlo,monospace}
    .t{font-weight:600;font-size:15px;color:#eae7e1}
    .s{color:#97928c;font:11px ui-monospace,Menlo,monospace}
    .bar{width:200px;height:4px;border-radius:2px;background:#0e0d11;overflow:hidden}
    .bar i{display:block;width:40%;height:100%;border-radius:2px;
      background:linear-gradient(90deg,#b87f33,#f3c98c);animation:s 1.3s ease-in-out infinite}
    @keyframes s{0%{transform:translateX(-120%)}100%{transform:translateX(320%)}}
  </style><div class="mark">K</div><div class="t">Starting Kendex</div>
  <div class="bar"><i></i></div>
  <div class="s">preparing the index engine&hellip;</div>`;
  splashWindow.loadURL('data:text/html;charset=utf-8,' + encodeURIComponent(html));
  splashWindow.once('ready-to-show', () => splashWindow && splashWindow.show());
}

function closeSplash() {
  if (splashWindow && !splashWindow.isDestroyed()) splashWindow.close();
  splashWindow = null;
}

function stopBackend() {
  if (!backend) return;
  const proc = backend;
  backend = null;
  proc.kill('SIGTERM');
}

// Let the user pick a folder (on any volume) to house the index. The backend
// reads its path from FILE_INDEXER_DB at spawn time, so the choice is persisted
// in config.json and applied on the next launch rather than mid-session.
async function chooseDbLocation() {
  const current = defaultDbPath();
  const result = await dialog.showOpenDialog(mainWindow, {
    title: 'Choose a folder to store the Kendex database',
    message: 'A files.db index will be kept in the folder you select.',
    buttonLabel: 'Use This Folder',
    defaultPath: path.dirname(current),
    properties: ['openDirectory', 'createDirectory'],
  });
  if (result.canceled || !result.filePaths.length) return;

  const newPath = path.join(result.filePaths[0], 'files.db');
  if (newPath === current) return;

  writeConfig({ dbPath: newPath });

  if (process.env.FILE_INDEXER_DB) {
    dialog.showMessageBoxSync(mainWindow, {
      type: 'warning',
      message: 'Saved, but FILE_INDEXER_DB overrides it',
      detail: 'This app was launched with the FILE_INDEXER_DB environment '
        + 'variable set, which takes precedence over the in-app setting. Your '
        + 'choice was saved and will apply when the app runs without that '
        + 'variable set.',
    });
    return;
  }

  const choice = dialog.showMessageBoxSync(mainWindow, {
    type: 'question',
    buttons: ['Relaunch Now', 'Later'],
    defaultId: 0,
    cancelId: 1,
    message: 'Database location saved',
    detail: `Kendex will use:\n${newPath}\n\nRelaunch to apply. Your existing `
      + 'index is not moved automatically — relaunching starts a fresh index at '
      + 'the new location. To reuse an existing index, quit and copy your '
      + 'files.db into that folder first.',
  });
  if (choice === 0) {
    app.relaunch();
    app.quit();
  }
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
          label: 'Choose Database Location…',
          click: chooseDbLocation,
        },
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
  const smoke = process.env.FILE_INDEXER_ELECTRON_SMOKE === '1';
  if (!smoke) showSplash();
  try {
    await startBackend();
    if (smoke) {
      console.log(`Smoke test backend ready at ${backendUrl}`);
      app.quit();
      return;
    }
    await createWindow();
  } catch (error) {
    closeSplash();
    dialog.showErrorBox('Kendex failed to start', error.stack || String(error));
    app.quit();
  }
});

app.on('window-all-closed', () => {
  app.quit();
});

app.on('before-quit', () => {
  stopProgressPolling();
  updateSleepBlocker(false);
  stopBackend();
});
