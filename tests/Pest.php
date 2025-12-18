<?php

declare(strict_types=1);

use FoundryCo\Snowflake\Tests\TestCase;

/*
|--------------------------------------------------------------------------
| Test Case
|--------------------------------------------------------------------------
*/

uses(TestCase::class)->in('Feature', 'Integration');
uses()->in('Unit');

/*
|--------------------------------------------------------------------------
| Expectations
|--------------------------------------------------------------------------
*/

expect()->extend('toBeValidSql', function () {
    return $this->toBeString()
        ->not->toBeEmpty();
});

/*
|--------------------------------------------------------------------------
| Functions
|--------------------------------------------------------------------------
*/

function createMockResponse(array $data = [], int $status = 200): array
{
    return [
        'status' => $status,
        'data' => $data,
    ];
}
