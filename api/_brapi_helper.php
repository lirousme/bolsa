<?php

declare(strict_types=1);

function carregarEnvArquivo(string $caminho): void
{
    if (!file_exists($caminho)) {
        responderJsonApi(500, ['erro' => 'Arquivo .env não encontrado.']);
    }

    $linhas = file($caminho, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    if ($linhas === false) {
        responderJsonApi(500, ['erro' => 'Não foi possível ler o arquivo .env.']);
    }

    foreach ($linhas as $linha) {
        $linha = trim($linha);
        if ($linha === '' || str_starts_with($linha, '#') || strpos($linha, '=') === false) {
            continue;
        }

        [$chave, $valor] = explode('=', $linha, 2);
        $chave = trim($chave);
        $valor = trim($valor, " \t\n\r\0\x0B\"'");

        putenv("{$chave}={$valor}");
        $_ENV[$chave] = $valor;
    }
}

function responderJsonApi(int $codigo, array $dados): void
{
    http_response_code($codigo);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode($dados, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    exit;
}

function obterConexaoBanco(): PDO
{
    carregarEnvArquivo(__DIR__ . '/../.env');

    $dbHost = getenv('DB_HOST') ?: '';
    $dbName = getenv('DB_NAME') ?: '';
    $dbUser = getenv('DB_USER') ?: '';
    $dbPass = getenv('DB_PASS') ?: '';

    if ($dbHost === '' || $dbName === '' || $dbUser === '') {
        responderJsonApi(500, ['erro' => 'Credenciais de banco incompletas no .env.']);
    }

    try {
        return new PDO(
            "mysql:host={$dbHost};dbname={$dbName};charset=utf8mb4",
            $dbUser,
            $dbPass,
            [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            ]
        );
    } catch (PDOException $e) {
        responderJsonApi(500, ['erro' => 'Falha ao conectar no banco.', 'detalhes' => $e->getMessage()]);
    }
}

function obterTickers(PDO $pdo): array
{
    $stmt = $pdo->query("SELECT ticker FROM tickers WHERE ticker IS NOT NULL AND ticker <> '' ORDER BY id ASC");
    $linhas = $stmt->fetchAll();

    $tickers = [];
    foreach ($linhas as $linha) {
        $ticker = strtoupper(trim((string) ($linha['ticker'] ?? '')));
        if ($ticker !== '') {
            $tickers[$ticker] = true;
        }
    }

    return array_keys($tickers);
}

function chamarBrapi(string $token, array $tickersLote, string $queryString): array
{
    $tickersEmTexto = implode(',', $tickersLote);
    $url = "https://brapi.dev/api/quote/{$tickersEmTexto}";
    if ($queryString !== '') {
        $url .= '?' . $queryString;
    }

    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_HTTPHEADER, [
        "Authorization: Bearer {$token}"
    ]);

    $resposta = curl_exec($ch);

    if ($resposta === false) {
        $erro = curl_error($ch);
        curl_close($ch);

        return [
            'sucesso' => false,
            'http_code' => 0,
            'resposta' => null,
            'resposta_bruta' => null,
            'erro' => $erro,
            'url' => $url,
        ];
    }

    $httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);

    $json = json_decode($resposta, true);

    return [
        'sucesso' => $httpCode >= 200 && $httpCode < 300,
        'http_code' => $httpCode,
        'resposta' => $json,
        'resposta_bruta' => $resposta,
        'erro' => null,
        'url' => $url,
    ];
}

function processarEndpointBrapi(array $queryParams): void
{
    $pdo = obterConexaoBanco();
    $tickers = obterTickers($pdo);

    if (count($tickers) === 0) {
        responderJsonApi(422, ['erro' => 'Nenhum ticker encontrado na tabela tickers.']);
    }

    $token = getenv('TOKEN_BRAPI') ?: '';
    if ($token === '') {
        responderJsonApi(500, ['erro' => 'TOKEN_BRAPI não encontrado no .env.']);
    }

    $lotes = array_chunk($tickers, 20);
    $totalLotes = count($lotes);
    $loteAtual = isset($_GET['lote']) ? (int) $_GET['lote'] : 1;

    if ($loteAtual < 1 || $loteAtual > $totalLotes) {
        responderJsonApi(422, [
            'erro' => 'Lote inválido.',
            'lote_informado' => $loteAtual,
            'total_lotes' => $totalLotes,
        ]);
    }

    $queryString = http_build_query($queryParams);
    $tickersLote = $lotes[$loteAtual - 1];

    $resultado = chamarBrapi($token, $tickersLote, $queryString);

    if (!$resultado['sucesso']) {
        responderJsonApi(502, [
            'erro' => 'Falha ao consultar BRAPI.',
            'http_code' => $resultado['http_code'],
            'detalhes' => $resultado['erro'] ?: $resultado['resposta_bruta'],
            'lote_atual' => $loteAtual,
            'total_lotes' => $totalLotes,
            'tickers_lote' => $tickersLote,
            'url' => $resultado['url'],
        ]);
    }

    responderJsonApi(200, [
        'lote_atual' => $loteAtual,
        'total_lotes' => $totalLotes,
        'lotes_restantes' => $totalLotes - $loteAtual,
        'tickers_lote' => $tickersLote,
        'url' => $resultado['url'],
        'dados' => $resultado['resposta'],
    ]);
}
