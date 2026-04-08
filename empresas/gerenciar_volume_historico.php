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
        'SELECT id_ticker,
                DATE(regular_market_time) AS market_date,
                SUM(COALESCE(regular_market_volume, 0)) AS total_volume
         FROM fundamentals
         WHERE id_ticker = :id_ticker
           AND regular_market_time IS NOT NULL
         GROUP BY id_ticker, DATE(regular_market_time)
         ORDER BY market_date ASC'
    );
    $stmt->execute([':id_ticker' => $idTicker]);
    $registros = $stmt->fetchAll();

    $historico = array_map(
        static function (array $registro): array {
            return [
                'id_ticker' => (int) $registro['id_ticker'],
                'market_date' => $registro['market_date'],
                'regular_market_volume' => $registro['total_volume'] !== null ? (float) $registro['total_volume'] : null,
            ];
        },
        $registros
    );

    responderJson(200, [
        'id_ticker' => $idTicker,
        'historico' => $historico,
        'total_registros' => count($historico),
    ]);
} catch (PDOException $e) {
    responderJson(500, ['erro' => 'Não foi possível carregar o histórico de volume.', 'detalhes' => $e->getMessage()]);
}
