<?php

declare(strict_types=1);

require_once __DIR__ . '/_brapi_helper.php';

processarEndpointHistoricoCotacao([
    '1d' => [
        'range' => '1d',
        'interval' => '1m',
        'tabela' => 'historical_data_price_1d',
    ],
    '5d' => [
        'range' => '5d',
        'interval' => '5m',
        'tabela' => 'historical_data_price_5d',
    ],
    '1mo' => [
        'range' => '1mo',
        'interval' => '1d',
        'tabela' => 'historical_data_price_1mo',
    ],
    '3mo' => [
        'range' => '3mo',
        'interval' => '1d',
        'tabela' => 'historical_data_price_3mo',
    ],
    '6mo' => [
        'range' => '6mo',
        'interval' => '1d',
        'tabela' => 'historical_data_price_6mo',
    ],
    '1y' => [
        'range' => '1y',
        'interval' => '1wk',
        'tabela' => 'historical_data_price_1y',
    ],
    '5y' => [
        'range' => '5y',
        'interval' => '1wk',
        'tabela' => 'historical_data_price_5y',
    ],
    'max' => [
        'range' => 'max',
        'interval' => '1mo',
        'tabela' => 'historical_data_price_max',
    ],
]);
