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

$metodo = $_SERVER['REQUEST_METHOD'] ?? 'GET';
if ($metodo !== 'GET') {
    responderJson(405, ['erro' => 'Método não permitido.']);
}

$idTicker = isset($_GET['id_ticker']) ? (int) $_GET['id_ticker'] : 0;
if ($idTicker <= 0) {
    responderJson(422, ['erro' => 'Informe um id_ticker válido.']);
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

try {
    $stmt = $pdo->prepare(
        'SELECT id_ticker, earnings_per_share
         FROM default_key_statistics
         WHERE id_ticker = :id_ticker
         LIMIT 1'
    );
    $stmt->execute([':id_ticker' => $idTicker]);
    $registro = $stmt->fetch();

    if (!$registro || $registro['earnings_per_share'] === null) {
        responderJson(404, ['erro' => 'Não foi encontrado earnings_per_share para este ticker.']);
    }

    $earningsPerShare = (float) $registro['earnings_per_share'];
    $precoJusto = $earningsPerShare * 10;

    responderJson(200, [
        'id_ticker' => (int) $registro['id_ticker'],
        'earnings_per_share' => $earningsPerShare,
        'multiplicador' => 10,
        'preco_justo' => $precoJusto,
        'formula' => 'preco_justo = earnings_per_share * 10',
    ]);
} catch (PDOException $e) {
    responderJson(500, ['erro' => 'Não foi possível calcular o preço justo.', 'detalhes' => $e->getMessage()]);
}
