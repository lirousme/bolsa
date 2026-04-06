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

$periodos = [
    '1d' => ['label' => '1 Dia', 'tabela' => 'historical_data_price_1d'],
    '5d' => ['label' => '5 Dias', 'tabela' => 'historical_data_price_5d'],
    '1mo' => ['label' => '1 Mês', 'tabela' => 'historical_data_price_1mo'],
    '3mo' => ['label' => '3 Meses', 'tabela' => 'historical_data_price_3mo'],
    '6mo' => ['label' => '6 Meses', 'tabela' => 'historical_data_price_6mo'],
    '1y' => ['label' => '1 Ano', 'tabela' => 'historical_data_price_1y'],
    '5y' => ['label' => '5 Anos', 'tabela' => 'historical_data_price_5y'],
    'max' => ['label' => 'Máximo', 'tabela' => 'historical_data_price_max'],
];

try {
    $resultado = [];

    foreach ($periodos as $chave => $periodo) {
        $sql = sprintf(
            'SELECT MIN(low) AS menor_preco, AVG((high + low) / 2) AS preco_medio, MAX(high) AS maior_preco, COUNT(*) AS total_registros FROM %s WHERE id_ticker = :id_ticker AND high IS NOT NULL AND low IS NOT NULL',
            $periodo['tabela']
        );

        $stmt = $pdo->prepare($sql);
        $stmt->execute([':id_ticker' => $idTicker]);
        $dados = $stmt->fetch();

        $totalRegistros = (int) ($dados['total_registros'] ?? 0);

        $resultado[] = [
            'periodo' => $chave,
            'label' => $periodo['label'],
            'tabela' => $periodo['tabela'],
            'menor_preco' => $totalRegistros > 0 ? (float) $dados['menor_preco'] : null,
            'preco_medio' => $totalRegistros > 0 ? (float) $dados['preco_medio'] : null,
            'maior_preco' => $totalRegistros > 0 ? (float) $dados['maior_preco'] : null,
            'total_registros' => $totalRegistros,
        ];
    }

    responderJson(200, [
        'id_ticker' => $idTicker,
        'cards' => $resultado,
    ]);
} catch (PDOException $e) {
    responderJson(500, ['erro' => 'Não foi possível carregar o histórico de cotações.', 'detalhes' => $e->getMessage()]);
}
