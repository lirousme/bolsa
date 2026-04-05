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

function normalizarNomeEmpresa(string $nome): string
{
    $nomeNormalizado = trim(mb_strtolower($nome, 'UTF-8'));

    $nomeSemAcentos = iconv('UTF-8', 'ASCII//TRANSLIT//IGNORE', $nomeNormalizado);
    if ($nomeSemAcentos === false) {
        $nomeSemAcentos = $nomeNormalizado;
    }

    $nomeSemAcentos = preg_replace('/[^a-z0-9\s]/', '', $nomeSemAcentos);
    $nomeSemAcentos = preg_replace('/\s+/', '_', trim((string) $nomeSemAcentos));

    return (string) $nomeSemAcentos;
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
        $sql = 'SELECT id, nome_com_caracteres_especiais, nome_sem_caracteres_especiais FROM empresas ORDER BY nome_com_caracteres_especiais ASC';
        $empresas = $pdo->query($sql)->fetchAll();
        responderJson(200, ['empresas' => $empresas]);
    } catch (PDOException $e) {
        responderJson(500, ['erro' => 'Não foi possível listar as empresas.', 'detalhes' => $e->getMessage()]);
    }
}

if ($metodo === 'POST') {
    $conteudo = file_get_contents('php://input');
    $dados = json_decode($conteudo, true);

    $nomeComCaracteres = isset($dados['nome_com_caracteres_especiais'])
        ? trim((string) $dados['nome_com_caracteres_especiais'])
        : '';

    if ($nomeComCaracteres === '') {
        responderJson(422, ['erro' => 'Informe o nome da empresa.']);
    }

    $nomeSemCaracteres = normalizarNomeEmpresa($nomeComCaracteres);

    if ($nomeSemCaracteres === '') {
        responderJson(422, ['erro' => 'Não foi possível gerar o nome sem caracteres especiais.']);
    }

    try {
        $stmt = $pdo->prepare(
            'INSERT INTO empresas (nome_com_caracteres_especiais, nome_sem_caracteres_especiais) VALUES (:nome_com, :nome_sem)'
        );
        $stmt->execute([
            ':nome_com' => $nomeComCaracteres,
            ':nome_sem' => $nomeSemCaracteres,
        ]);

        $idInserido = (int) $pdo->lastInsertId();

        $diretorioEmpresa = __DIR__ . '/' . $nomeSemCaracteres;
        if (!is_dir($diretorioEmpresa) && !mkdir($diretorioEmpresa, 0775, true) && !is_dir($diretorioEmpresa)) {
            responderJson(500, ['erro' => 'Empresa cadastrada, mas não foi possível criar o diretório da empresa.']);
        }

        responderJson(201, [
            'mensagem' => 'Empresa cadastrada com sucesso.',
            'empresa' => [
                'id' => $idInserido,
                'nome_com_caracteres_especiais' => $nomeComCaracteres,
                'nome_sem_caracteres_especiais' => $nomeSemCaracteres,
            ],
        ]);
    } catch (PDOException $e) {
        responderJson(500, ['erro' => 'Não foi possível cadastrar a empresa.', 'detalhes' => $e->getMessage()]);
    }
}

responderJson(405, ['erro' => 'Método não permitido.']);
