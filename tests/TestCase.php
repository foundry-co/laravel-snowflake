<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Tests;

use FoundryCo\Snowflake\SnowflakeServiceProvider;
use Orchestra\Testbench\TestCase as BaseTestCase;

abstract class TestCase extends BaseTestCase
{
    protected function getPackageProviders($app): array
    {
        return [
            SnowflakeServiceProvider::class,
        ];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('database.default', 'snowflake');
        $app['config']->set('database.connections.snowflake', [
            'driver' => 'snowflake',
            'account' => 'test-account',
            'warehouse' => 'TEST_WH',
            'database' => 'TEST_DB',
            'schema' => 'PUBLIC',
            'role' => 'SYSADMIN',
            'auth' => [
                'jwt' => [
                    'user' => 'TEST_USER',
                    'private_key_path' => __DIR__ . '/Fixtures/test_rsa_key.pem',
                ],
            ],
        ]);
    }
}
