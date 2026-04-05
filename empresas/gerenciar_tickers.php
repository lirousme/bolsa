<?php
header('Content-Type: application/json; charset=utf-8');

function responderJson(int $codigo, array $dados): void
{
    http_response_code($codigo);
    echo json_encode($dados, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    exit;
}

function carregarEnv(string $caminho): void
{
    if (!file_exists($caminho)) {
        responderJson(500, ['erro' => 'Arquivo .env não encontrado na raiz do projeto.']);
    }

    $linhas = file($caminho, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    foreach ($linhas as $linha) {
        $linha = trim($linha);
        if ($linha === '' || strpos($linha, '#') === 0 || strpos($linha, '=') === false) {
            continue;
        }

        [$chave, $valor] = explode('=', $linha, 2);
        $chave = trim($chave);
        $valor = trim($valor, " \t\n\r\0\x0B\"'");

        putenv("{$chave}={$valor}");
        $_ENV[$chave] = $valor;
    }
}

function normalizarTicker(string $ticker): string
{
    $tickerLimpo = trim($ticker);
    $tickerSemAcentos = iconv('UTF-8', 'ASCII//TRANSLIT//IGNORE', $tickerLimpo);

    if ($tickerSemAcentos === false) {
        $tickerSemAcentos = $tickerLimpo;
    }

    $tickerSemAcentos = preg_replace('/[^A-Za-z0-9]/', '', (string) $tickerSemAcentos);
    $tickerMaiusculo = mb_strtoupper((string) $tickerSemAcentos, 'UTF-8');

    return trim($tickerMaiusculo);
}

carregarEnv(__DIR__ . '/../.env');

$dbHost = getenv('DB_HOST');
$dbName = getenv('DB_NAME');
$dbUser = getenv('DB_USER');
$dbPass = getenv('DB_PASS');

if (!$dbHost || !$dbName || !$dbUser) {
    responderJson(500, ['erro' => 'Credenciais de banco incompletas no arquivo .env.']);
}

try {
    $pdo = new PDO(
        "mysql:host={$dbHost};dbname={$dbName};charset=utf8mb4",
        $dbUser,
        $dbPass,
        [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        ]
    );
} catch (PDOException $e) {
    responderJson(500, ['erro' => 'Falha ao conectar no banco de dados.', 'detalhes' => $e->getMessage()]);
}

$metodo = $_SERVER['REQUEST_METHOD'] ?? 'GET';

if ($metodo === 'GET') {
    $idEmpresa = isset($_GET['id_empresa']) ? (int) $_GET['id_empresa'] : 0;

    if ($idEmpresa <= 0) {
        responderJson(422, ['erro' => 'Informe um id_empresa válido.']);
    }

    try {
        $stmtEmpresa = $pdo->prepare('SELECT id, nome_com_caracteres_especiais, nome_sem_caracteres_especiais FROM empresas WHERE id = :id LIMIT 1');
        $stmtEmpresa->execute([':id' => $idEmpresa]);
        $empresa = $stmtEmpresa->fetch();

        if (!$empresa) {
            responderJson(404, ['erro' => 'Empresa não encontrada.']);
        }

        $stmtTickers = $pdo->prepare('SELECT id, ticker, id_empresa FROM tickers WHERE id_empresa = :id_empresa ORDER BY ticker ASC');
        $stmtTickers->execute([':id_empresa' => $idEmpresa]);
        $tickers = $stmtTickers->fetchAll();

        responderJson(200, [
            'empresa' => $empresa,
            'tickers' => $tickers,
        ]);
    } catch (PDOException $e) {
        responderJson(500, ['erro' => 'Não foi possível listar os tickers da empresa.', 'detalhes' => $e->getMessage()]);
    }
}

if ($metodo === 'POST') {
    $conteudo = file_get_contents('php://input');
    $dados = json_decode($conteudo, true);

    $idEmpresa = isset($dados['id_empresa']) ? (int) $dados['id_empresa'] : 0;
    $tickerInformado = isset($dados['ticker']) ? (string) $dados['ticker'] : '';

    if ($idEmpresa <= 0) {
        responderJson(422, ['erro' => 'Informe um id_empresa válido.']);
    }

    if (trim($tickerInformado) === '') {
        responderJson(422, ['erro' => 'Informe o ticker.']);
    }

    $tickerNormalizado = normalizarTicker($tickerInformado);
    if ($tickerNormalizado === '') {
        responderJson(422, ['erro' => 'Ticker inválido após normalização.']);
    }

    try {
        $stmtEmpresa = $pdo->prepare('SELECT id, nome_sem_caracteres_especiais, nome_com_caracteres_especiais FROM empresas WHERE id = :id LIMIT 1');
        $stmtEmpresa->execute([':id' => $idEmpresa]);
        $empresa = $stmtEmpresa->fetch();

        if (!$empresa) {
            responderJson(404, ['erro' => 'Empresa não encontrada.']);
        }

        $stmtTickerExistente = $pdo->prepare('SELECT id FROM tickers WHERE id_empresa = :id_empresa AND ticker = :ticker LIMIT 1');
        $stmtTickerExistente->execute([
            ':id_empresa' => $idEmpresa,
            ':ticker' => $tickerNormalizado,
        ]);

        if ($stmtTickerExistente->fetch()) {
            responderJson(409, ['erro' => 'Este ticker já está cadastrado para a empresa.']);
        }

        $stmtInserir = $pdo->prepare('INSERT INTO tickers (ticker, id_empresa) VALUES (:ticker, :id_empresa)');
        $stmtInserir->execute([
            ':ticker' => $tickerNormalizado,
            ':id_empresa' => $idEmpresa,
        ]);

        $caminhoEmpresa = __DIR__ . '/' . $empresa['nome_sem_caracteres_especiais'];
        if (!is_dir($caminhoEmpresa)) {
            if (!mkdir($caminhoEmpresa, 0775, true) && !is_dir($caminhoEmpresa)) {
                responderJson(500, ['erro' => 'Ticker cadastrado, mas não foi possível criar o diretório da empresa.']);
            }
        }

        $caminhoTicker = $caminhoEmpresa . '/' . $tickerNormalizado;
        if (!is_dir($caminhoTicker) && !mkdir($caminhoTicker, 0775, true) && !is_dir($caminhoTicker)) {
            responderJson(500, ['erro' => 'Ticker cadastrado, mas não foi possível criar o diretório do ticker.']);
        }

        responderJson(201, [
            'mensagem' => 'Ticker cadastrado com sucesso.',
            'ticker' => [
                'id' => (int) $pdo->lastInsertId(),
                'ticker' => $tickerNormalizado,
                'id_empresa' => $idEmpresa,
            ],
            'empresa' => [
                'id' => (int) $empresa['id'],
                'nome_com_caracteres_especiais' => $empresa['nome_com_caracteres_especiais'],
                'nome_sem_caracteres_especiais' => $empresa['nome_sem_caracteres_especiais'],
            ],
        ]);
    } catch (PDOException $e) {
        responderJson(500, ['erro' => 'Não foi possível cadastrar o ticker.', 'detalhes' => $e->getMessage()]);
    }
}

responderJson(405, ['erro' => 'Método não permitido.']);
