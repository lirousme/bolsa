<?php

declare(strict_types=1);

header('Content-Type: application/json; charset=utf-8');

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['message' => 'Método não permitido. Use POST.']);
    exit;
}

try {
    $config = loadEnv(__DIR__ . '/../.env');

    $dbHost = required($config, 'DB_HOST');
    $dbName = required($config, 'DB_NAME');
    $dbUser = required($config, 'DB_USER');
    $dbPass = required($config, 'DB_PASS');
    $token = required($config, 'TOKEN_BRAPI');

    $body = json_decode(file_get_contents('php://input') ?: '{}', true);
    if (!is_array($body)) {
        throw new RuntimeException('JSON de entrada inválido.');
    }

    $tickers = normalizeTickers($body['tickers'] ?? null);
    if (count($tickers) === 0) {
        throw new RuntimeException('Informe pelo menos um ticker.');
    }

    if (count($tickers) > 20) {
        throw new RuntimeException('Máximo de 20 tickers por requisição neste endpoint.');
    }

    $results = fetchBrapi($tickers, $token);

    $pdo = new PDO(
        sprintf('mysql:host=%s;dbname=%s;charset=utf8mb4', $dbHost, $dbName),
        $dbUser,
        $dbPass,
        [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
    );

    $savedRows = upsertEmpresas($pdo, $results);

    echo json_encode([
        'message' => 'Empresas processadas com sucesso.',
        'salvos' => count($savedRows),
        'ignorados' => max(0, count($tickers) - count($savedRows)),
        'empresas' => $savedRows,
    ], JSON_UNESCAPED_UNICODE);
} catch (Throwable $e) {
    http_response_code(400);
    echo json_encode(['message' => $e->getMessage()], JSON_UNESCAPED_UNICODE);
}

function loadEnv(string $path): array
{
    if (!file_exists($path)) {
        throw new RuntimeException('.env não encontrado em ' . $path);
    }

    $env = [];
    $lines = file($path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    foreach ($lines as $line) {
        $line = trim($line);
        if ($line === '' || str_starts_with($line, '#')) {
            continue;
        }

        $eqPos = strpos($line, '=');
        if ($eqPos === false) {
            continue;
        }

        $key = trim(substr($line, 0, $eqPos));
        $value = trim(substr($line, $eqPos + 1));
        $env[$key] = trim($value, "\"'");
    }

    return $env;
}

function required(array $config, string $key): string
{
    $value = trim((string)($config[$key] ?? ''));
    if ($value === '') {
        throw new RuntimeException("Configuração obrigatória ausente: {$key}");
    }

    return $value;
}

function normalizeTickers(mixed $value): array
{
    $list = [];

    if (is_string($value)) {
        $list = explode(',', $value);
    } elseif (is_array($value)) {
        $list = $value;
    }

    $normalized = [];
    foreach ($list as $ticker) {
        $ticker = strtoupper(trim((string)$ticker));
        if ($ticker !== '') {
            $normalized[] = $ticker;
        }
    }

    return array_values(array_unique($normalized));
}

function fetchBrapi(array $tickers, string $token): array
{
    $url = 'https://brapi.dev/api/quote/' . rawurlencode(implode(',', $tickers))
        . '?modules=summaryProfile&token=' . rawurlencode($token);

    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_HTTPHEADER => [
            'Accept: application/json',
            'Authorization: Bearer ' . $token,
        ],
        CURLOPT_TIMEOUT => 30,
    ]);

    $response = curl_exec($ch);
    if ($response === false) {
        $err = curl_error($ch) ?: 'Erro desconhecido no cURL.';
        curl_close($ch);
        throw new RuntimeException('Falha ao consultar Brapi: ' . $err);
    }

    $status = curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
    curl_close($ch);

    $payload = json_decode($response, true);
    if (!is_array($payload)) {
        throw new RuntimeException('Resposta inválida da Brapi.');
    }

    if ($status < 200 || $status >= 300) {
        throw new RuntimeException((string)($payload['message'] ?? "Erro da Brapi ({$status})"));
    }

    return is_array($payload['results'] ?? null) ? $payload['results'] : [];
}

function upsertEmpresas(PDO $pdo, array $results): array
{
    $sql = <<<SQL
INSERT INTO empresas (
  ticker, address1, address2, address3, city, state, zip, country, phone, fax,
  website, industry, industryKey, industryDisp, sector, sectorKey, sectorDisp,
  longBusinessSummary, fullTimeEmployees, companyOfficers, twitter, name, startDate,
  description, logoUrl, cnpj
) VALUES (
  :ticker, :address1, :address2, :address3, :city, :state, :zip, :country, :phone, :fax,
  :website, :industry, :industryKey, :industryDisp, :sector, :sectorKey, :sectorDisp,
  :longBusinessSummary, :fullTimeEmployees, :companyOfficers, :twitter, :name, :startDate,
  :description, :logoUrl, :cnpj
)
ON DUPLICATE KEY UPDATE
  address1 = VALUES(address1),
  address2 = VALUES(address2),
  address3 = VALUES(address3),
  city = VALUES(city),
  state = VALUES(state),
  zip = VALUES(zip),
  country = VALUES(country),
  phone = VALUES(phone),
  fax = VALUES(fax),
  website = VALUES(website),
  industry = VALUES(industry),
  industryKey = VALUES(industryKey),
  industryDisp = VALUES(industryDisp),
  sector = VALUES(sector),
  sectorKey = VALUES(sectorKey),
  sectorDisp = VALUES(sectorDisp),
  longBusinessSummary = VALUES(longBusinessSummary),
  fullTimeEmployees = VALUES(fullTimeEmployees),
  companyOfficers = VALUES(companyOfficers),
  twitter = VALUES(twitter),
  name = VALUES(name),
  startDate = VALUES(startDate),
  description = VALUES(description),
  logoUrl = VALUES(logoUrl),
  cnpj = VALUES(cnpj)
SQL;

    $stmt = $pdo->prepare($sql);
    $savedRows = [];

    foreach ($results as $item) {
        if (!is_array($item) || !is_array($item['summaryProfile'] ?? null)) {
            continue;
        }

        $profile = $item['summaryProfile'];
        $ticker = strtoupper(trim((string)($item['symbol'] ?? '')));
        if ($ticker === '') {
            continue;
        }

        $data = [
            ':ticker' => $ticker,
            ':address1' => valueOrNull($profile, 'address1'),
            ':address2' => valueOrNull($profile, 'address2'),
            ':address3' => valueOrNull($profile, 'address3'),
            ':city' => valueOrNull($profile, 'city'),
            ':state' => valueOrNull($profile, 'state'),
            ':zip' => valueOrNull($profile, 'zip'),
            ':country' => valueOrNull($profile, 'country'),
            ':phone' => valueOrNull($profile, 'phone'),
            ':fax' => valueOrNull($profile, 'fax'),
            ':website' => valueOrNull($profile, 'website'),
            ':industry' => valueOrNull($profile, 'industry'),
            ':industryKey' => valueOrNull($profile, 'industryKey'),
            ':industryDisp' => valueOrNull($profile, 'industryDisp'),
            ':sector' => valueOrNull($profile, 'sector'),
            ':sectorKey' => valueOrNull($profile, 'sectorKey'),
            ':sectorDisp' => valueOrNull($profile, 'sectorDisp'),
            ':longBusinessSummary' => valueOrNull($profile, 'longBusinessSummary'),
            ':fullTimeEmployees' => is_numeric($profile['fullTimeEmployees'] ?? null) ? (int)$profile['fullTimeEmployees'] : null,
            ':companyOfficers' => isset($profile['companyOfficers']) ? json_encode($profile['companyOfficers'], JSON_UNESCAPED_UNICODE) : null,
            ':twitter' => valueOrNull($profile, 'twitter'),
            ':name' => valueOrNull($profile, 'name'),
            ':startDate' => toDateOrNull($profile['startDate'] ?? null),
            ':description' => valueOrNull($profile, 'description'),
            ':logoUrl' => valueOrNull($profile, 'logoUrl'),
            ':cnpj' => valueOrNull($profile, 'cnpj'),
        ];

        $stmt->execute($data);

        $savedRows[] = [
            'ticker' => $ticker,
            'name' => $data[':name'],
            'industry' => $data[':industry'],
            'sector' => $data[':sector'],
            'city' => $data[':city'],
            'state' => $data[':state'],
            'website' => $data[':website'],
        ];
    }

    return $savedRows;
}

function valueOrNull(array $arr, string $key): ?string
{
    if (!array_key_exists($key, $arr) || $arr[$key] === null) {
        return null;
    }

    $value = trim((string)$arr[$key]);
    return $value === '' ? null : $value;
}

function toDateOrNull(mixed $value): ?string
{
    if ($value === null) {
        return null;
    }

    $date = trim((string)$value);
    if ($date === '') {
        return null;
    }

    $dt = DateTime::createFromFormat('Y-m-d', $date);
    return $dt ? $dt->format('Y-m-d') : null;
}
