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

function resolverNomeColuna(string $atributo, string $tabela, array $colunasTabela): ?string
{
    $colunaPadrao = camelParaSnake($atributo);
    if (isset($colunasTabela[$colunaPadrao])) {
        return $colunaPadrao;
    }

    $aliasBalanceSheet = [
        'financial_assets_measured_at_fair_value_through_profit_or_loss' => 'financial_assets_fv_profit_loss',
        'long_term_financial_investments_measured_at_fair_value_through_income' => 'long_term_fin_inv_fv_income',
        'financial_investments_measured_at_amortized_cost' => 'financial_investments_amortized_cost',
        'financial_investments_fv_through_oci' => 'financial_investments_fv_oci',
        'financial_liabilities_measured_at_fair_value_through_income' => 'financial_liab_fv_income',
    ];

    $aliasPorTabela = [
        'balance_sheet_history' => $aliasBalanceSheet,
        'balance_sheet_history_quarterly' => $aliasBalanceSheet,
    ];

    $variacoesNomeColuna = [$colunaPadrao];
    $colunaComSiglasNormalizadas = str_replace(
        ['_f_v_', '_o_c_i_', '_p_l_'],
        ['_fv_', '_oci_', '_pl_'],
        $colunaPadrao
    );
    if ($colunaComSiglasNormalizadas !== $colunaPadrao) {
        $variacoesNomeColuna[] = $colunaComSiglasNormalizadas;
    }

    $aliasTabela = $aliasPorTabela[$tabela] ?? [];
    foreach ($variacoesNomeColuna as $variacao) {
        if (isset($aliasTabela[$variacao]) && isset($colunasTabela[$aliasTabela[$variacao]])) {
            return $aliasTabela[$variacao];
        }
    }

    return null;
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

function normalizarValorComparacao(mixed $valor): string
{
    if ($valor === null) {
        return '__NULL__';
    }

    if (is_bool($valor)) {
        return $valor ? '1' : '0';
    }

    if (is_int($valor) || is_string($valor)) {
        return (string) $valor;
    }

    if (is_float($valor)) {
        $normalizado = number_format($valor, 12, '.', '');
        return rtrim(rtrim($normalizado, '0'), '.');
    }

    return json_encode($valor, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) ?: '';
}

function normalizarValorDedupePorTabelaColuna(string $tabela, string $coluna, mixed $valor): mixed
{
    if ($tabela !== 'fundamentals' || $coluna !== 'regular_market_time') {
        return $valor;
    }

    if ($valor === null) {
        return null;
    }

    $valorTexto = trim((string) $valor);
    if ($valorTexto === '') {
        return '';
    }

    if (preg_match('/^(\d{4}-\d{2}-\d{2})/', $valorTexto, $matches) === 1) {
        return $matches[1];
    }

    if (preg_match('/^\d+$/', $valorTexto) === 1) {
        $timestamp = (int) $valorTexto;
        if (strlen($valorTexto) >= 13) {
            $timestamp = (int) floor($timestamp / 1000);
        }

        return gmdate('Y-m-d', $timestamp);
    }

    try {
        $data = new DateTimeImmutable($valorTexto);
        return $data->format('Y-m-d');
    } catch (Exception) {
        return $valorTexto;
    }
}

function carregarRegistrosExistentesPorTicker(PDO $pdo, string $tabela, int $idTicker): array
{
    $sql = "SELECT * FROM `{$tabela}` WHERE `id_ticker` = :id_ticker";
    $stmt = $pdo->prepare($sql);
    $stmt->execute([':id_ticker' => $idTicker]);
    $registros = $stmt->fetchAll();

    return is_array($registros) ? $registros : [];
}

function registroJaExistePorComparacaoLocal(string $tabela, array $registrosExistentes, array $valoresPorColuna): bool
{
    if (count($valoresPorColuna) === 0) {
        return false;
    }

    foreach ($registrosExistentes as $registroExistente) {
        if (!is_array($registroExistente)) {
            continue;
        }

        $todosIguais = true;
        foreach ($valoresPorColuna as $coluna => $valorNovo) {
            if (!array_key_exists($coluna, $registroExistente)) {
                $todosIguais = false;
                break;
            }

            $valorExistente = normalizarValorDedupePorTabelaColuna($tabela, $coluna, $registroExistente[$coluna]);
            $valorNovoNormalizado = normalizarValorDedupePorTabelaColuna($tabela, $coluna, $valorNovo);

            if (normalizarValorComparacao($valorExistente) !== normalizarValorComparacao($valorNovoNormalizado)) {
                $todosIguais = false;
                break;
            }
        }

        if ($todosIguais) {
            return true;
        }
    }

    return false;
}

function obterColunasChaveDedupe(string $tabela, array $colunasTabela): array
{
    $chavesPorTabela = [
        'fundamentals' => ['id_ticker', 'regular_market_time'],
        'historical_data_price_1d' => ['id_ticker', 'date'],
        'historical_data_price_5d' => ['id_ticker', 'date'],
        'historical_data_price_1mo' => ['id_ticker', 'date'],
        'historical_data_price_3mo' => ['id_ticker', 'date'],
        'historical_data_price_6mo' => ['id_ticker', 'date'],
        'historical_data_price_1y' => ['id_ticker', 'date'],
        'historical_data_price_5y' => ['id_ticker', 'date'],
        'historical_data_price_max' => ['id_ticker', 'date'],
        'cash_dividends' => ['id_ticker', 'payment_date', 'label', 'rate'],
        'balance_sheet_history' => ['id_ticker', 'end_date'],
        'balance_sheet_history_quarterly' => ['id_ticker', 'end_date'],
        'cashflow_history' => ['id_ticker', 'end_date'],
        'cashflow_history_quarterly' => ['id_ticker', 'end_date'],
        'default_key_statistics' => ['id_ticker', 'book_value'],
        'default_key_statistics_history' => ['id_ticker', 'end_date'],
        'default_key_statistics_history_quarterly' => ['id_ticker', 'end_date'],
        'financial_data' => ['id_ticker', 'ebitda'],
        'financial_data_history' => ['id_ticker', 'end_date'],
        'financial_data_history_quarterly' => ['id_ticker', 'end_date'],
        'income_statement_history' => ['id_ticker', 'end_date'],
        'income_statement_history_quarterly' => ['id_ticker', 'end_date'],
        'summary_profile' => ['id_ticker', 'full_time_employees'],
        'value_added_history' => ['id_ticker', 'end_date'],
        'value_added_history_quarterly' => ['id_ticker', 'end_date'],
    ];

    $chaves = $chavesPorTabela[$tabela] ?? [];
    if (!is_array($chaves) || count($chaves) === 0) {
        return [];
    }

    $chavesValidas = [];
    foreach ($chaves as $chave) {
        if (isset($colunasTabela[$chave])) {
            $chavesValidas[] = $chave;
        }
    }

    return $chavesValidas;
}

function montarValoresParaComparacao(array $colunas, array $valores, array $colunasChave): array
{
    $colunasComparacao = count($colunasChave) > 0 ? $colunasChave : $colunas;

    $valoresPorColuna = [];
    foreach ($colunasComparacao as $coluna) {
        $placeholder = ':' . $coluna;
        if (array_key_exists($placeholder, $valores)) {
            $valoresPorColuna[$coluna] = $valores[$placeholder];
        }
    }

    return $valoresPorColuna;
}

function salvarRespostaModuloEmTabela(PDO $pdo, array $resultadoBrapi, array $mapaTickers, string $modulo, string $tabela): array
{
    $colunasTabela = buscarColunasTabela($pdo, $tabela);
    $colunasChaveDedupe = obterColunasChaveDedupe($tabela, $colunasTabela);

    if (empty($colunasTabela)) {
        throw new RuntimeException("Tabela de destino não encontrada ou sem colunas: {$tabela}");
    }

    $totalInseridos = 0;
    $totalIgnorados = 0;
    $totalSemTicker = 0;
    $cacheRegistrosExistentes = [];

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

        if (!array_key_exists($idTicker, $cacheRegistrosExistentes)) {
            $cacheRegistrosExistentes[$idTicker] = carregarRegistrosExistentesPorTicker($pdo, $tabela, (int) $idTicker);
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
                $coluna = resolverNomeColuna((string) $atributo, $tabela, $colunasTabela);
                if ($coluna === null) {
                    continue;
                }

                $placeholder = ':' . $coluna;
                if (array_key_exists($placeholder, $valores)) {
                    continue;
                }

                $colunas[] = $coluna;
                $valores[$placeholder] = $valor;
            }

            if (count($colunas) === 0 || (count($colunas) === 1 && $colunas[0] === 'id_ticker')) {
                $totalIgnorados++;
                continue;
            }

            $valoresPorColuna = montarValoresParaComparacao($colunas, $valores, $colunasChaveDedupe);

            if (registroJaExistePorComparacaoLocal($tabela, $cacheRegistrosExistentes[$idTicker], $valoresPorColuna)) {
                $totalIgnorados++;
                continue;
            }

            $colunasSql = implode(', ', array_map(static fn ($c) => "`{$c}`", $colunas));
            $placeholdersSql = implode(', ', array_keys($valores));
            $sql = "INSERT INTO `{$tabela}` ({$colunasSql}) VALUES ({$placeholdersSql})";

            $stmt = $pdo->prepare($sql);
            $stmt->execute($valores);
            $cacheRegistrosExistentes[$idTicker][] = $valoresPorColuna;
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
    $colunasChaveDedupe = obterColunasChaveDedupe($tabela, $colunasTabela);

    if (empty($colunasTabela)) {
        throw new RuntimeException("Tabela de destino não encontrada ou sem colunas: {$tabela}");
    }

    $totalInseridos = 0;
    $totalIgnorados = 0;
    $totalSemTicker = 0;
    $cacheRegistrosExistentes = [];

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

        if (!array_key_exists($idTicker, $cacheRegistrosExistentes)) {
            $cacheRegistrosExistentes[$idTicker] = carregarRegistrosExistentesPorTicker($pdo, $tabela, (int) $idTicker);
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
                if (array_key_exists($placeholder, $valores)) {
                    continue;
                }

                $colunas[] = $coluna;
                $valores[$placeholder] = $valor;
            }

            if (count($colunas) === 0 || (count($colunas) === 1 && $colunas[0] === 'id_ticker')) {
                $totalIgnorados++;
                continue;
            }

            $valoresPorColuna = montarValoresParaComparacao($colunas, $valores, $colunasChaveDedupe);

            if (registroJaExistePorComparacaoLocal($tabela, $cacheRegistrosExistentes[$idTicker], $valoresPorColuna)) {
                $totalIgnorados++;
                continue;
            }

            $colunasSql = implode(', ', array_map(static fn ($c) => "`{$c}`", $colunas));
            $placeholdersSql = implode(', ', array_keys($valores));
            $sql = "INSERT INTO `{$tabela}` ({$colunasSql}) VALUES ({$placeholdersSql})";

            $stmt = $pdo->prepare($sql);
            $stmt->execute($valores);
            $cacheRegistrosExistentes[$idTicker][] = $valoresPorColuna;
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
    $colunasChaveDedupe = obterColunasChaveDedupe($tabela, $colunasTabela);

    if (empty($colunasTabela)) {
        throw new RuntimeException("Tabela de destino não encontrada ou sem colunas: {$tabela}");
    }

    $totalInseridos = 0;
    $totalIgnorados = 0;
    $totalSemTicker = 0;
    $cacheRegistrosExistentes = [];

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

        if (!array_key_exists($idTicker, $cacheRegistrosExistentes)) {
            $cacheRegistrosExistentes[$idTicker] = carregarRegistrosExistentesPorTicker($pdo, $tabela, (int) $idTicker);
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
            if (array_key_exists($placeholder, $valores)) {
                continue;
            }

            $colunas[] = $coluna;
            $valores[$placeholder] = $valor;
        }

        if (count($colunas) === 0 || (count($colunas) === 1 && $colunas[0] === 'id_ticker')) {
            $totalIgnorados++;
            continue;
        }

        $valoresPorColuna = montarValoresParaComparacao($colunas, $valores, $colunasChaveDedupe);

        if (registroJaExistePorComparacaoLocal($tabela, $cacheRegistrosExistentes[$idTicker], $valoresPorColuna)) {
            $totalIgnorados++;
            continue;
        }

        $colunasSql = implode(', ', array_map(static fn ($c) => "`{$c}`", $colunas));
        $placeholdersSql = implode(', ', array_keys($valores));
        $sql = "INSERT INTO `{$tabela}` ({$colunasSql}) VALUES ({$placeholdersSql})";

        $stmt = $pdo->prepare($sql);
        $stmt->execute($valores);
        $cacheRegistrosExistentes[$idTicker][] = $valoresPorColuna;
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
