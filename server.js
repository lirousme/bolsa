const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');
const { DatabaseSync } = require('node:sqlite');

loadEnv();

const PORT = Number(process.env.PORT || 3000);
const BRAPI_BASE_URL = (process.env.BRAPI_BASE_URL || 'https://brapi.dev/api').replace(/\/$/, '');
const BRAPI_TOKEN = process.env.BRAPI_TOKEN || process.env.TOKEN_BRAPI || '';
const DB_PATH = path.join(__dirname, 'data.sqlite');

const ENDPOINTS = [
  {
    key: 'quoteTicker',
    path: '/quote/:ticker',
    description: 'Cotações e dados fundamentalistas de ações, FIIs, ETFs, BDRs',
    example: '/quote/PETR4?modules=balanceSheetHistory&dividends=true',
    required: ['ticker'],
    optional: ['modules', 'dividends']
  },
  {
    key: 'quoteList',
    path: '/quote/list',
    description: 'Lista múltiplos tickers',
    example: '/quote/list?tickers=PETR4,VALE3',
    required: ['tickers'],
    optional: []
  },
  {
    key: 'crypto',
    path: '/v2/crypto',
    description: 'Cotações de criptomoedas',
    example: '/v2/crypto?symbols=BTC.BRL',
    required: ['symbols'],
    optional: []
  },
  {
    key: 'currency',
    path: '/v2/currency',
    description: 'Taxas de câmbio',
    example: '/v2/currency?symbols=USD.BRL',
    required: ['symbols'],
    optional: []
  },
  {
    key: 'inflation',
    path: '/v2/inflation',
    description: 'Indicadores de inflação',
    example: '/v2/inflation?country=BR',
    required: ['country'],
    optional: []
  },
  {
    key: 'primeRate',
    path: '/v2/prime-rate',
    description: 'Taxa SELIC e juros básicos',
    example: '/v2/prime-rate',
    required: [],
    optional: []
  }
];

const db = new DatabaseSync(DB_PATH);
db.exec(`
  CREATE TABLE IF NOT EXISTS query_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint_key TEXT NOT NULL,
    request_url TEXT NOT NULL,
    params_json TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
`);

function loadEnv() {
  const envPath = path.join(__dirname, '.env');
  if (!fs.existsSync(envPath)) return;

  const content = fs.readFileSync(envPath, 'utf8');
  for (const line of content.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;

    const index = trimmed.indexOf('=');
    if (index === -1) continue;

    const key = trimmed.slice(0, index).trim();
    const value = trimmed.slice(index + 1).trim();

    if (!(key in process.env)) {
      process.env[key] = value;
    }
  }
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}

function sendFile(res, filePath) {
  fs.readFile(filePath, (error, data) => {
    if (error) {
      sendJson(res, 404, { error: 'Arquivo não encontrado.' });
      return;
    }

    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', (chunk) => {
      raw += chunk;
      if (raw.length > 1_000_000) {
        reject(new Error('Payload excedeu o limite permitido.'));
      }
    });

    req.on('end', () => {
      if (!raw) {
        resolve({});
        return;
      }

      try {
        resolve(JSON.parse(raw));
      } catch {
        reject(new Error('JSON inválido no corpo da requisição.'));
      }
    });

    req.on('error', () => reject(new Error('Falha ao ler a requisição.')));
  });
}

function buildBrapiUrl(endpointKey, params) {
  const endpoint = ENDPOINTS.find((item) => item.key === endpointKey);
  if (!endpoint) {
    throw new Error('Endpoint selecionado é inválido.');
  }

  for (const requiredField of endpoint.required) {
    if (!params[requiredField]) {
      throw new Error(`Campo obrigatório ausente: ${requiredField}.`);
    }
  }

  let endpointPath = endpoint.path;
  if (endpointPath.includes(':ticker')) {
    endpointPath = endpointPath.replace(':ticker', encodeURIComponent(String(params.ticker).trim()));
  }

  const url = new URL(`${BRAPI_BASE_URL}${endpointPath}`);

  const allowed = new Set([...endpoint.required, ...endpoint.optional]);
  for (const [key, value] of Object.entries(params || {})) {
    if (!allowed.has(key)) continue;
    if (value === undefined || value === null || value === '') continue;
    if (key === 'ticker') continue;
    url.searchParams.set(key, String(value));
  }

  if (BRAPI_TOKEN) {
    url.searchParams.set('token', BRAPI_TOKEN);
  }

  return { endpoint, requestUrl: url.toString() };
}

function saveQueryHistory(record) {
  db.prepare(`
    INSERT INTO query_history (endpoint_key, request_url, params_json, status_code, response_json)
    VALUES (?, ?, ?, ?, ?)
  `).run(
    record.endpointKey,
    record.requestUrl,
    JSON.stringify(record.params),
    record.statusCode,
    JSON.stringify(record.response)
  );
}

async function handleQuery(req, res) {
  try {
    const body = await readJsonBody(req);
    const endpointKey = String(body.endpointKey || '').trim();
    const params = typeof body.params === 'object' && body.params ? body.params : {};
    const { endpoint, requestUrl } = buildBrapiUrl(endpointKey, params);

    const apiResponse = await fetch(requestUrl);
    const json = await apiResponse.json();

    saveQueryHistory({
      endpointKey: endpoint.key,
      requestUrl,
      params,
      statusCode: apiResponse.status,
      response: json
    });

    sendJson(res, apiResponse.ok ? 200 : apiResponse.status, {
      endpoint: endpoint.path,
      requestUrl,
      status: apiResponse.status,
      data: json
    });
  } catch (error) {
    sendJson(res, 400, { error: error.message });
  }
}

function handleHistory(reqUrl, res) {
  const limit = Math.min(Number(reqUrl.searchParams.get('limit') || 20), 100);
  const rows = db
    .prepare(
      `SELECT id, endpoint_key, request_url, params_json, status_code, response_json, created_at
       FROM query_history
       ORDER BY id DESC
       LIMIT ?`
    )
    .all(limit)
    .map((row) => ({
      id: row.id,
      endpointKey: row.endpoint_key,
      requestUrl: row.request_url,
      params: JSON.parse(row.params_json),
      statusCode: row.status_code,
      response: JSON.parse(row.response_json),
      createdAt: row.created_at
    }));

  sendJson(res, 200, { items: rows });
}

const server = http.createServer(async (req, res) => {
  const reqUrl = new URL(req.url, `http://${req.headers.host}`);

  if (req.method === 'GET' && reqUrl.pathname === '/') {
    sendFile(res, path.join(__dirname, 'public', 'index.html'));
    return;
  }

  if (req.method === 'GET' && reqUrl.pathname === '/api/endpoints') {
    sendJson(res, 200, { endpoints: ENDPOINTS });
    return;
  }

  if (req.method === 'POST' && reqUrl.pathname === '/api/query') {
    await handleQuery(req, res);
    return;
  }

  if (req.method === 'GET' && reqUrl.pathname === '/api/history') {
    handleHistory(reqUrl, res);
    return;
  }

  sendJson(res, 404, { error: 'Rota não encontrada.' });
});

server.listen(PORT, () => {
  console.log(`Servidor iniciado em http://localhost:${PORT}`);
});
