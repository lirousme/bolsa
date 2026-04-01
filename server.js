const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');

const PORT = Number(process.env.PORT || 3000);
const SYMBOL = 'CXSE3';

function loadEnv() {
  const envPath = path.join(__dirname, '.env');
  if (!fs.existsSync(envPath)) return;

  const content = fs.readFileSync(envPath, 'utf8');
  for (const line of content.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const idx = trimmed.indexOf('=');
    if (idx === -1) continue;
    const key = trimmed.slice(0, idx).trim();
    const value = trimmed.slice(idx + 1).trim();
    if (!(key in process.env)) process.env[key] = value;
  }
}

function sendJson(res, status, payload) {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}

function sendFile(res, filePath, contentType) {
  fs.readFile(filePath, (err, data) => {
    if (err) {
      sendJson(res, 404, { error: 'Arquivo não encontrado' });
      return;
    }
    res.writeHead(200, { 'Content-Type': contentType });
    res.end(data);
  });
}

async function handleVolumeHistory(reqUrl, res) {
  const token = process.env.TOKEN_BRAPI;
  if (!token) {
    sendJson(res, 500, { error: 'TOKEN_BRAPI não configurado no arquivo .env' });
    return;
  }

  const range = reqUrl.searchParams.get('range') || '7d';
  const interval = reqUrl.searchParams.get('interval') || '1d';
  const apiUrl = `https://brapi.dev/api/quote/${SYMBOL}?range=${encodeURIComponent(range)}&interval=${encodeURIComponent(interval)}&token=${encodeURIComponent(token)}`;

  try {
    const response = await fetch(apiUrl);
    const data = await response.json();

    if (!response.ok) {
      sendJson(res, response.status, {
        error: 'Falha ao buscar dados na BRAPI',
        details: data
      });
      return;
    }

    const prices = data?.results?.[0]?.historicalDataPrice || [];
    const volumeHistory = prices
      .map((item) => ({
        date: new Date(item.date * 1000).toISOString().slice(0, 10),
        volume: item.volume || 0
      }))
      .sort((a, b) => a.date.localeCompare(b.date));

    sendJson(res, 200, { symbol: SYMBOL, days: 7, volumeHistory });
  } catch (error) {
    sendJson(res, 500, { error: 'Erro interno ao consultar BRAPI', details: error.message });
  }
}

loadEnv();

const server = http.createServer(async (req, res) => {
  const reqUrl = new URL(req.url, `http://${req.headers.host}`);

  if (req.method === 'GET' && reqUrl.pathname === '/') {
    sendFile(res, path.join(__dirname, 'public', 'index.html'), 'text/html; charset=utf-8');
    return;
  }

  if (req.method === 'GET' && reqUrl.pathname === '/api/volume-history') {
    await handleVolumeHistory(reqUrl, res);
    return;
  }

  sendJson(res, 404, { error: 'Rota não encontrada' });
});

server.listen(PORT, () => {
  console.log(`Servidor rodando em http://localhost:${PORT}`);
});
