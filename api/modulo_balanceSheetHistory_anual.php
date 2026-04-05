<?php

// Função simples para carregar variáveis do arquivo .env
function carregarEnv($caminho) {
    if (!file_exists($caminho)) {
        die("Erro: Arquivo .env não encontrado.\n");
    }

    $linhas = file($caminho, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    foreach ($linhas as $linha) {
        if (strpos(trim($linha), '#') === 0) continue;
        if (strpos($linha, '=') !== false) {
            list($chave, $valor) = explode('=', $linha, 2);
            $chave = trim($chave);
            $valor = trim($valor, '"\'');
            putenv(sprintf('%s=%s', $chave, $valor));
            $_ENV[$chave] = $valor;
        }
    }
}

// 1. Carrega o arquivo .env
carregarEnv(__DIR__ . '/.env');

// 2. Obtém o token
$token = getenv('TOKEN_BRAPI');

if (!$token) {
    die("Erro: TOKEN_BRAPI não foi encontrado no arquivo .env\n");
}

// 3. Configura a URL e inicializa o cURL
$url = "https://brapi.dev/api/quote/PETR4,VALE3?modules=balanceSheetHistory";
$ch = curl_init();

// 4. Define as opções do cURL
curl_setopt($ch, CURLOPT_URL, $url);
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
curl_setopt($ch, CURLOPT_HTTPHEADER, [
    "Authorization: Bearer $token"
]);

// 5. Executa a requisição
$resposta = curl_exec($ch);

// 6. Verifica por erros
if(curl_errno($ch)){
    echo 'Erro na requisição cURL: ' . curl_error($ch);
} else {
    $http_code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    
    if ($http_code == 200) {
        $dados = json_decode($resposta, true);
        
        // Estrutura o HTML para exibir o JSON na tela do usuário
        echo "<h2 style='font-family: sans-serif;'>Dados Recebidos:</h2>";
        
        // O uso da tag <pre> mantém a indentação do JSON na tela do navegador
        // json_encode com JSON_PRETTY_PRINT estrutura o array de volta para um JSON indentado
        echo "<pre style='background: #1e1e1e; color: #dcdcdc; padding: 15px; border-radius: 8px; font-size: 14px; overflow-x: auto;'>";
        echo json_encode($dados, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        echo "</pre>";
        
    } else {
        echo "A API retornou um erro. Código HTTP: $http_code\n";
        echo "Resposta da API: $resposta\n";
    }
}

// 7. Fecha a conexão
curl_close($ch);

?>