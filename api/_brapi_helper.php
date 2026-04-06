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
    foreach ($linhas as $linha) {
        $ticker = strtoupper(trim((string) ($linha['ticker'] ?? '')));
        if ($ticker !== '') {
            $tickers[$ticker] = (int) $linha['id'];
        }
    }

    return $tickers;
}

function camelParaSnake(string $valor): string
{
    $valorSemEspaco = preg_replace('/\s+/', '', $valor) ?? $valor;
    $snake = preg_replace('/(?<!^)[A-Z]/', '_$0', $valorSemEspaco);
    return strtolower((string) $snake);
}

function buscarColunasTabela(PDO $pdo, string $tabela): array
{
    $stmt = $pdo->prepare("SHOW COLUMNS FROM `{$tabela}`");
    $stmt->execute();
    $linhas = $stmt->fetchAll();

    $colunas = [];
    foreach ($linhas as $linha) {
        $campo = (string) ($linha['Field'] ?? '');
        if ($campo !== '') {
            $colunas[$campo] = true;
        }
    }

    return $colunas;
}

function prepararLinhasParaInsercao(mixed $dadosDoModulo): array
{
    if (is_array($dadosDoModulo) && array_is_list($dadosDoModulo)) {
        return $dadosDoModulo;
    }

    if (is_array($dadosDoModulo)) {
        return [$dadosDoModulo];
    }

    return [];
}

function registroJaExiste(PDO $pdo, string $tabela, array $colunas, array $valores): bool
{
    if (count($colunas) === 0 || count($valores) === 0) {
        return false;
    }

    $colunas = selecionarColunasParaDeduplicacao($colunas, $valores);

    $condicoes = [];
    foreach ($colunas as $coluna) {
        $placeholder = ':' . $coluna;
        if (!array_key_exists($placeholder, $valores)) {
            continue;
        }

        $condicoes[] = "`{$coluna}` <=> {$placeholder}";
    }

    if (count($condicoes) === 0) {
        return false;
    }

    $sql = "SELECT 1 FROM `{$tabela}` WHERE " . implode(' AND ', $condicoes) . ' LIMIT 1';
    $stmt = $pdo->prepare($sql);
    $stmt->execute($valores);

    return (bool) $stmt->fetchColumn();
}

function selecionarColunasParaDeduplicacao(array $colunas, array $valores): array
{
    $colunasDisponiveis = array_values(array_unique($colunas));
    if (count($colunasDisponiveis) === 0) {
        return [];
    }

    $possuiColuna = static function (string $coluna) use ($colunasDisponiveis, $valores): bool {
        return in_array($coluna, $colunasDisponiveis, true) && array_key_exists(':' . $coluna, $valores);
    };

    if (!$possuiColuna('id_ticker')) {
        return $colunasDisponiveis;
    }

    $camposPrioritarios = [
        'date',
        'datetime',
        'fiscal_date_ending',
        'as_of_date',
        'reference_date',
        'payment_date',
        'approved_on',
        'last_dividend_date',
        'ex_dividend_date',
        'record_date',
    ];

    foreach ($camposPrioritarios as $campo) {
        if ($possuiColuna($campo)) {
            return ['id_ticker', $campo];
        }
    }

    if ($possuiColuna('period') && $possuiColuna('type')) {
        return ['id_ticker', 'period', 'type'];
    }

    return $colunasDisponiveis;
}

function salvarRespostaModuloEmTabela(PDO $pdo, array $resultadoBrapi, array $mapaTickers, string $modulo, string $tabela): array
{
    $colunasTabela = buscarColunasTabela($pdo, $tabela);

    if (empty($colunasTabela)) {
        throw new RuntimeException("Tabela de destino não encontrada ou sem colunas: {$tabela}");
    }

    $totalInseridos = 0;
    $totalIgnorados = 0;
    $totalSemTicker = 0;

    $results = $resultadoBrapi['results'] ?? [];
    if (!is_array($results)) {
        return [
            'tabela' => $tabela,
            'inseridos' => 0,
            'ignorados' => 0,
            'sem_ticker' => 0,
        ];
    }

    foreach ($results as $resultadoTicker) {
        if (!is_array($resultadoTicker)) {
            continue;
        }

        $ticker = strtoupper(trim((string) ($resultadoTicker['symbol'] ?? '')));
        $idTicker = $mapaTickers[$ticker] ?? null;
        if (!$idTicker) {
            $totalSemTicker++;
            continue;
        }

        $dadosDoModulo = $resultadoTicker[$modulo] ?? null;
        $linhas = prepararLinhasParaInsercao($dadosDoModulo);

        foreach ($linhas as $linha) {
            if (!is_array($linha)) {
                $totalIgnorados++;
                continue;
            }

            $colunas = [];
            $valores = [];

            if (isset($colunasTabela['id_ticker'])) {
                $colunas[] = 'id_ticker';
                $valores[':id_ticker'] = (int) $idTicker;
            }

            foreach ($linha as $atributo => $valor) {
                $coluna = camelParaSnake((string) $atributo);
                if (!isset($colunasTabela[$coluna])) {
                    continue;
                }

                $placeholder = ':' . $coluna;
                $colunas[] = $coluna;
                $valores[$placeholder] = $valor;
            }

            if (count($colunas) === 0 || (count($colunas) === 1 && $colunas[0] === 'id_ticker')) {
                $totalIgnorados++;
                continue;
            }

            if (registroJaExiste($pdo, $tabela, $colunas, $valores)) {
                $totalIgnorados++;
                continue;
            }

            $colunasSql = implode(', ', array_map(static fn ($c) => "`{$c}`", $colunas));
            $placeholdersSql = implode(', ', array_keys($valores));
            $sql = "INSERT INTO `{$tabela}` ({$colunasSql}) VALUES ({$placeholdersSql})";

            $stmt = $pdo->prepare($sql);
            $stmt->execute($valores);
            $totalInseridos++;
        }
    }

    return [
        'tabela' => $tabela,
        'inseridos' => $totalInseridos,
        'ignorados' => $totalIgnorados,
        'sem_ticker' => $totalSemTicker,
    ];
}

function salvarRespostaNoBanco(PDO $pdo, array $resultadoBrapi, array $mapaTickers, string $modulo): array
{
    $tabela = camelParaSnake($modulo);
    return salvarRespostaModuloEmTabela($pdo, $resultadoBrapi, $mapaTickers, $modulo, $tabela);
}

function salvarRespostaDividendosNoBanco(PDO $pdo, array $resultadoBrapi, array $mapaTickers): array
{
    $tabela = 'cash_dividends';
    $colunasTabela = buscarColunasTabela($pdo, $tabela);

    if (empty($colunasTabela)) {
        throw new RuntimeException("Tabela de destino não encontrada ou sem colunas: {$tabela}");
    }

    $totalInseridos = 0;
    $totalIgnorados = 0;
    $totalSemTicker = 0;

    $results = $resultadoBrapi['results'] ?? [];
    if (!is_array($results)) {
        return [
            'tabela' => $tabela,
            'inseridos' => 0,
            'ignorados' => 0,
            'sem_ticker' => 0,
        ];
    }

    foreach ($results as $resultadoTicker) {
        if (!is_array($resultadoTicker)) {
            continue;
        }

        $ticker = strtoupper(trim((string) ($resultadoTicker['symbol'] ?? '')));
        $idTicker = $mapaTickers[$ticker] ?? null;
        if (!$idTicker) {
            $totalSemTicker++;
            continue;
        }

        $dadosDividendos = $resultadoTicker['dividendsData']['cashDividends'] ?? [];
        if (!is_array($dadosDividendos) || !array_is_list($dadosDividendos)) {
            $totalIgnorados++;
            continue;
        }

        foreach ($dadosDividendos as $linha) {
            if (!is_array($linha)) {
                $totalIgnorados++;
                continue;
            }

            $colunas = [];
            $valores = [];

            if (isset($colunasTabela['id_ticker'])) {
                $colunas[] = 'id_ticker';
                $valores[':id_ticker'] = (int) $idTicker;
            }

            foreach ($linha as $atributo => $valor) {
                $coluna = camelParaSnake((string) $atributo);
                if (!isset($colunasTabela[$coluna])) {
                    continue;
                }

                $placeholder = ':' . $coluna;
                $colunas[] = $coluna;
                $valores[$placeholder] = $valor;
            }

            if (count($colunas) === 0 || (count($colunas) === 1 && $colunas[0] === 'id_ticker')) {
                $totalIgnorados++;
                continue;
            }

            if (registroJaExiste($pdo, $tabela, $colunas, $valores)) {
                $totalIgnorados++;
                continue;
            }

            $colunasSql = implode(', ', array_map(static fn ($c) => "`{$c}`", $colunas));
            $placeholdersSql = implode(', ', array_keys($valores));
            $sql = "INSERT INTO `{$tabela}` ({$colunasSql}) VALUES ({$placeholdersSql})";

            $stmt = $pdo->prepare($sql);
            $stmt->execute($valores);
            $totalInseridos++;
        }
    }

    return [
        'tabela' => $tabela,
        'inseridos' => $totalInseridos,
        'ignorados' => $totalIgnorados,
        'sem_ticker' => $totalSemTicker,
    ];
}

function salvarRespostaFundamentalsNoBanco(PDO $pdo, array $resultadoBrapi, array $mapaTickers): array
{
    $tabela = 'fundamentals';
    $colunasTabela = buscarColunasTabela($pdo, $tabela);

    if (empty($colunasTabela)) {
        throw new RuntimeException("Tabela de destino não encontrada ou sem colunas: {$tabela}");
    }

    $totalInseridos = 0;
    $totalIgnorados = 0;
    $totalSemTicker = 0;

    $results = $resultadoBrapi['results'] ?? [];
    if (!is_array($results)) {
        return [
            'tabela' => $tabela,
            'inseridos' => 0,
            'ignorados' => 0,
            'sem_ticker' => 0,
        ];
    }

    foreach ($results as $resultadoTicker) {
        if (!is_array($resultadoTicker)) {
            $totalIgnorados++;
            continue;
        }

        $ticker = strtoupper(trim((string) ($resultadoTicker['symbol'] ?? '')));
        $idTicker = $mapaTickers[$ticker] ?? null;
        if (!$idTicker) {
            $totalSemTicker++;
            continue;
        }

        $colunas = [];
        $valores = [];

        if (isset($colunasTabela['id_ticker'])) {
            $colunas[] = 'id_ticker';
            $valores[':id_ticker'] = (int) $idTicker;
        }

        foreach ($resultadoTicker as $atributo => $valor) {
            if (is_array($valor) || is_object($valor)) {
                continue;
            }

            $coluna = camelParaSnake((string) $atributo);
            if (!isset($colunasTabela[$coluna])) {
                continue;
            }

            $placeholder = ':' . $coluna;
            $colunas[] = $coluna;
            $valores[$placeholder] = $valor;
        }

        if (count($colunas) === 0 || (count($colunas) === 1 && $colunas[0] === 'id_ticker')) {
            $totalIgnorados++;
            continue;
        }

        if (registroJaExiste($pdo, $tabela, $colunas, $valores)) {
            $totalIgnorados++;
            continue;
        }

        $colunasSql = implode(', ', array_map(static fn ($c) => "`{$c}`", $colunas));
        $placeholdersSql = implode(', ', array_keys($valores));
        $sql = "INSERT INTO `{$tabela}` ({$colunasSql}) VALUES ({$placeholdersSql})";

        $stmt = $pdo->prepare($sql);
        $stmt->execute($valores);
        $totalInseridos++;
    }

    return [
        'tabela' => $tabela,
        'inseridos' => $totalInseridos,
        'ignorados' => $totalIgnorados,
        'sem_ticker' => $totalSemTicker,
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

function processarEndpointDividendos(): void
{
    $pdo = obterConexaoBanco();
    $mapaTickers = obterTickers($pdo);
    $tickers = array_keys($mapaTickers);

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

    $tickersLote = $lotes[$loteAtual - 1];
    $resultado = chamarBrapi($token, $tickersLote, http_build_query(['dividends' => 'true']));

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

    try {
        $resumoPersistencia = salvarRespostaDividendosNoBanco($pdo, (array) $resultado['resposta'], $mapaTickers);
    } catch (Throwable $e) {
        responderJsonApi(500, [
            'erro' => 'Falha ao salvar dados no banco.',
            'detalhes' => $e->getMessage(),
            'lote_atual' => $loteAtual,
            'total_lotes' => $totalLotes,
            'tickers_lote' => $tickersLote,
        ]);
    }

    responderJsonApi(200, [
        'lote_atual' => $loteAtual,
        'total_lotes' => $totalLotes,
        'lotes_restantes' => $totalLotes - $loteAtual,
        'tickers_lote' => $tickersLote,
        'url' => $resultado['url'],
        'persistencia' => $resumoPersistencia,
        'dados' => $resultado['resposta'],
    ]);
}

function processarEndpointBrapi(array $queryParams): void
{
    $pdo = obterConexaoBanco();
    $mapaTickers = obterTickers($pdo);
    $tickers = array_keys($mapaTickers);

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

    $modulo = (string) ($queryParams['modules'] ?? '');
    if ($modulo === '') {
        responderJsonApi(500, ['erro' => 'Módulo não informado para processamento do endpoint.']);
    }

    try {
        $resumoPersistencia = salvarRespostaNoBanco($pdo, (array) $resultado['resposta'], $mapaTickers, $modulo);
    } catch (Throwable $e) {
        responderJsonApi(500, [
            'erro' => 'Falha ao salvar dados no banco.',
            'detalhes' => $e->getMessage(),
            'lote_atual' => $loteAtual,
            'total_lotes' => $totalLotes,
            'tickers_lote' => $tickersLote,
        ]);
    }

    responderJsonApi(200, [
        'lote_atual' => $loteAtual,
        'total_lotes' => $totalLotes,
        'lotes_restantes' => $totalLotes - $loteAtual,
        'tickers_lote' => $tickersLote,
        'url' => $resultado['url'],
        'persistencia' => $resumoPersistencia,
        'dados' => $resultado['resposta'],
    ]);
}

function processarEndpointFundamentals(): void
{
    $pdo = obterConexaoBanco();
    $mapaTickers = obterTickers($pdo);
    $tickers = array_keys($mapaTickers);

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

    $tickersLote = $lotes[$loteAtual - 1];
    $resultado = chamarBrapi($token, $tickersLote, http_build_query(['fundamental' => 'true']));

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

    try {
        $resumoPersistencia = salvarRespostaFundamentalsNoBanco($pdo, (array) $resultado['resposta'], $mapaTickers);
    } catch (Throwable $e) {
        responderJsonApi(500, [
            'erro' => 'Falha ao salvar dados no banco.',
            'detalhes' => $e->getMessage(),
            'lote_atual' => $loteAtual,
            'total_lotes' => $totalLotes,
            'tickers_lote' => $tickersLote,
        ]);
    }

    responderJsonApi(200, [
        'lote_atual' => $loteAtual,
        'total_lotes' => $totalLotes,
        'lotes_restantes' => $totalLotes - $loteAtual,
        'tickers_lote' => $tickersLote,
        'url' => $resultado['url'],
        'persistencia' => $resumoPersistencia,
        'dados' => $resultado['resposta'],
    ]);
}

function processarEndpointHistoricoCotacao(array $variacoes): void
{
    $variacaoSelecionada = (string) ($_GET['variacao'] ?? '1mo');
    if (!isset($variacoes[$variacaoSelecionada])) {
        $variacaoSelecionada = '1mo';
    }

    $configuracao = $variacoes[$variacaoSelecionada] ?? null;
    if (!is_array($configuracao)) {
        responderJsonApi(500, ['erro' => 'Configuração de variação inválida para histórico de cotações.']);
    }

    $range = (string) ($configuracao['range'] ?? '');
    $interval = (string) ($configuracao['interval'] ?? '');
    $tabela = (string) ($configuracao['tabela'] ?? '');
    if ($range === '' || $interval === '' || $tabela === '') {
        responderJsonApi(500, ['erro' => 'Configuração incompleta para histórico de cotações.']);
    }

    $pdo = obterConexaoBanco();
    $mapaTickers = obterTickers($pdo);
    $tickers = array_keys($mapaTickers);

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

    $tickersLote = $lotes[$loteAtual - 1];
    $resultado = chamarBrapi(
        $token,
        $tickersLote,
        http_build_query([
            'range' => $range,
            'interval' => $interval,
            'fundamental' => 'true',
        ])
    );

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

    try {
        $resumoPersistencia = salvarRespostaModuloEmTabela(
            $pdo,
            (array) $resultado['resposta'],
            $mapaTickers,
            'historicalDataPrice',
            $tabela
        );
    } catch (Throwable $e) {
        responderJsonApi(500, [
            'erro' => 'Falha ao salvar dados no banco.',
            'detalhes' => $e->getMessage(),
            'lote_atual' => $loteAtual,
            'total_lotes' => $totalLotes,
            'tickers_lote' => $tickersLote,
        ]);
    }

    responderJsonApi(200, [
        'lote_atual' => $loteAtual,
        'total_lotes' => $totalLotes,
        'lotes_restantes' => $totalLotes - $loteAtual,
        'tickers_lote' => $tickersLote,
        'url' => $resultado['url'],
        'variacao' => $variacaoSelecionada,
        'range' => $range,
        'interval' => $interval,
        'persistencia' => $resumoPersistencia,
        'dados' => $resultado['resposta'],
    ]);
}
