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
    $stmt = $pdo->query("SELECT id, ticker FROM tickers WHERE ticker IS NOT NULL AND ticker <> '' ORDER BY id ASC");
    $linhas = $stmt->fetchAll();

    $tickers = [];
    $tickerParaId = [];

    foreach ($linhas as $linha) {
        $ticker = strtoupper(trim((string) ($linha['ticker'] ?? '')));
        $idTicker = (int) ($linha['id'] ?? 0);

        if ($ticker === '' || $idTicker <= 0) {
            continue;
        }

        if (!isset($tickerParaId[$ticker])) {
            $tickerParaId[$ticker] = $idTicker;
            $tickers[] = $ticker;
        }
    }

    return [
        'tickers' => $tickers,
        'ticker_para_id' => $tickerParaId,
    ];
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

function camelCaseParaSnakeCase(string $texto): string
{
    $texto = preg_replace('/([a-z0-9])([A-Z])/', '$1_$2', $texto) ?? $texto;

    return strtolower($texto);
}

function tabelaExiste(PDO $pdo, string $nomeTabela): bool
{
    static $cache = [];

    if (isset($cache[$nomeTabela])) {
        return $cache[$nomeTabela];
    }

    $stmt = $pdo->prepare('SHOW TABLES LIKE :nome_tabela');
    $stmt->execute([':nome_tabela' => $nomeTabela]);

    $cache[$nomeTabela] = (bool) $stmt->fetchColumn();

    return $cache[$nomeTabela];
}

function obterColunasTabela(PDO $pdo, string $nomeTabela): array
{
    static $cache = [];

    if (isset($cache[$nomeTabela])) {
        return $cache[$nomeTabela];
    }

    $stmt = $pdo->query("SHOW COLUMNS FROM `{$nomeTabela}`");
    $linhas = $stmt->fetchAll();

    $colunas = [];
    foreach ($linhas as $linha) {
        $coluna = (string) ($linha['Field'] ?? '');
        if ($coluna !== '') {
            $colunas[$coluna] = true;
        }
    }

    $cache[$nomeTabela] = $colunas;

    return $colunas;
}

function arrayAssociativo(array $dados): bool
{
    return array_keys($dados) !== range(0, count($dados) - 1);
}

function extrairRegistrosTicker(array $tickerResultado, string $nomeEndpoint): array
{
    if (array_key_exists($nomeEndpoint, $tickerResultado)) {
        $valor = $tickerResultado[$nomeEndpoint];
        if (is_array($valor)) {
            if ($valor === []) {
                return [];
            }

            return arrayAssociativo($valor) ? [$valor] : $valor;
        }

        return [];
    }

    $candidatos = [];
    foreach ($tickerResultado as $chave => $valor) {
        if (in_array($chave, ['symbol', 'currency', 'marketCap', 'shortName', 'longName', 'logourl'], true)) {
            continue;
        }

        if (is_array($valor)) {
            $candidatos[] = $valor;
        }
    }

    foreach ($candidatos as $valor) {
        if ($valor === []) {
            continue;
        }

        if (arrayAssociativo($valor)) {
            return [$valor];
        }

        if (isset($valor[0]) && is_array($valor[0])) {
            return $valor;
        }
    }

    return [array_filter(
        $tickerResultado,
        static fn ($valor, $chave): bool => is_scalar($valor) || $valor === null,
        ARRAY_FILTER_USE_BOTH
    )];
}

function inserirRespostaNoBanco(PDO $pdo, array $resposta, string $nomeEndpoint, array $tickerParaId): array
{
    $nomeTabela = camelCaseParaSnakeCase($nomeEndpoint);

    if (!tabelaExiste($pdo, $nomeTabela)) {
        return [
            'tabela' => $nomeTabela,
            'inseridos' => 0,
            'ignorados' => 0,
            'observacao' => "Tabela '{$nomeTabela}' não encontrada.",
        ];
    }

    $colunasTabela = obterColunasTabela($pdo, $nomeTabela);
    if (!isset($colunasTabela['id_ticker'])) {
        return [
            'tabela' => $nomeTabela,
            'inseridos' => 0,
            'ignorados' => 0,
            'observacao' => "Tabela '{$nomeTabela}' sem coluna obrigatória id_ticker.",
        ];
    }

    $resultados = $resposta['results'] ?? [];
    if (!is_array($resultados)) {
        return [
            'tabela' => $nomeTabela,
            'inseridos' => 0,
            'ignorados' => 0,
            'observacao' => 'Resposta da BRAPI sem campo results válido.',
        ];
    }

    $inseridos = 0;
    $ignorados = 0;
    $observacoes = [];

    foreach ($resultados as $tickerResultado) {
        if (!is_array($tickerResultado)) {
            $ignorados++;
            continue;
        }

        $symbol = strtoupper(trim((string) ($tickerResultado['symbol'] ?? '')));
        $idTicker = $tickerParaId[$symbol] ?? null;

        if (!$idTicker) {
            $ignorados++;
            $observacoes[] = "Ticker '{$symbol}' não encontrado na tabela tickers.";
            continue;
        }

        $registros = extrairRegistrosTicker($tickerResultado, $nomeEndpoint);

        foreach ($registros as $registro) {
            if (!is_array($registro)) {
                $ignorados++;
                continue;
            }

            $dadosInsercao = ['id_ticker' => $idTicker];

            foreach ($registro as $atributo => $valor) {
                $coluna = camelCaseParaSnakeCase((string) $atributo);
                if (isset($colunasTabela[$coluna]) && $coluna !== 'id') {
                    $dadosInsercao[$coluna] = is_array($valor) ? json_encode($valor, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) : $valor;
                }
            }

            if (isset($colunasTabela['symbol']) && !isset($dadosInsercao['symbol'])) {
                $dadosInsercao['symbol'] = $symbol;
            }

            if (count($dadosInsercao) <= 1) {
                $ignorados++;
                continue;
            }

            $colunas = array_keys($dadosInsercao);
            $placeholders = array_map(static fn (string $coluna): string => ':' . $coluna, $colunas);

            $sql = sprintf(
                'INSERT INTO `%s` (%s) VALUES (%s)',
                $nomeTabela,
                implode(', ', array_map(static fn (string $coluna): string => "`{$coluna}`", $colunas)),
                implode(', ', $placeholders)
            );

            $stmt = $pdo->prepare($sql);
            $stmt->execute(array_combine($placeholders, array_values($dadosInsercao)));
            $inseridos++;
        }
    }

    return [
        'tabela' => $nomeTabela,
        'inseridos' => $inseridos,
        'ignorados' => $ignorados,
        'observacao' => $observacoes ? implode(' | ', array_unique($observacoes)) : null,
    ];
}

function processarEndpointBrapi(array $queryParams, ?string $nomeEndpoint = null): void
{
    $pdo = obterConexaoBanco();
    $dadosTickers = obterTickers($pdo);
    $tickers = $dadosTickers['tickers'];
    $tickerParaId = $dadosTickers['ticker_para_id'];

    if (count($tickers) === 0) {
        responderJsonApi(422, ['erro' => 'Nenhum ticker encontrado na tabela tickers.']);
    }

    $token = getenv('TOKEN_BRAPI') ?: '';
    if ($token === '') {
        responderJsonApi(500, ['erro' => 'TOKEN_BRAPI não encontrado no .env.']);
    }

    $nomeEndpointEfetivo = $nomeEndpoint ?: ((string) ($queryParams['modules'] ?? 'dados_endpoint'));

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

    $resumoPersistencia = inserirRespostaNoBanco(
        $pdo,
        is_array($resultado['resposta']) ? $resultado['resposta'] : [],
        $nomeEndpointEfetivo,
        $tickerParaId
    );

    responderJsonApi(200, [
        'lote_atual' => $loteAtual,
        'total_lotes' => $totalLotes,
        'lotes_restantes' => $totalLotes - $loteAtual,
        'tickers_lote' => $tickersLote,
        'url' => $resultado['url'],
        'endpoint' => $nomeEndpointEfetivo,
        'persistencia' => $resumoPersistencia,
        'dados' => $resultado['resposta'],
    ]);
}
