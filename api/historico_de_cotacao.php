<?php

declare(strict_types=1);

require_once __DIR__ . '/_brapi_helper.php';

processarEndpointBrapi([
    'range' => '5d',
    'interval' => '1d',
    'fundamental' => 'true',
], 'historico_de_cotacao');
