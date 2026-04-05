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
    try {
        $stmt = $pdo->query('SELECT id, nome, estrutura_da_resposta, caminho FROM end_points ORDER BY id DESC');
        $endPoints = $stmt->fetchAll();

        responderJson(200, ['end_points' => $endPoints]);
    } catch (PDOException $e) {
        responderJson(500, ['erro' => 'Não foi possível listar os end points.', 'detalhes' => $e->getMessage()]);
    }
}

if ($metodo === 'POST') {
    $conteudo = file_get_contents('php://input');
    $dados = json_decode($conteudo, true);

    $nome = isset($dados['nome']) ? trim((string) $dados['nome']) : '';
    $estruturaDaResposta = isset($dados['estrutura_da_resposta']) ? trim((string) $dados['estrutura_da_resposta']) : '';
    $caminho = isset($dados['caminho']) ? trim((string) $dados['caminho']) : '';

    if ($nome === '') {
        responderJson(422, ['erro' => 'Informe o nome do end point.']);
    }

    if ($estruturaDaResposta === '') {
        responderJson(422, ['erro' => 'Informe a estrutura da resposta.']);
    }

    if ($caminho === '') {
        responderJson(422, ['erro' => 'Informe o caminho da API.']);
    }

    try {
        $stmt = $pdo->prepare(
            'INSERT INTO end_points (nome, estrutura_da_resposta, caminho) VALUES (:nome, :estrutura_da_resposta, :caminho)'
        );
        $stmt->execute([
            ':nome' => $nome,
            ':estrutura_da_resposta' => $estruturaDaResposta,
            ':caminho' => $caminho,
        ]);

        responderJson(201, [
            'mensagem' => 'End point cadastrado com sucesso.',
            'end_point' => [
                'id' => (int) $pdo->lastInsertId(),
                'nome' => $nome,
                'estrutura_da_resposta' => $estruturaDaResposta,
                'caminho' => $caminho,
            ],
        ]);
    } catch (PDOException $e) {
        responderJson(500, ['erro' => 'Não foi possível cadastrar o end point.', 'detalhes' => $e->getMessage()]);
    }
}

responderJson(405, ['erro' => 'Método não permitido.']);
