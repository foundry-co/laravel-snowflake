<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Tests;

use FoundryCo\Snowflake\SnowflakeServiceProvider;
use Orchestra\Testbench\TestCase as BaseTestCase;

abstract class TestCase extends BaseTestCase
{
    /**
     * Get package providers.
     *
     * @param  \Illuminate\Foundation\Application  $app
     * @return array<int, class-string>
     */
    protected function getPackageProviders($app): array
    {
        return [
            SnowflakeServiceProvider::class,
        ];
    }

    /**
     * Define environment setup.
     *
     * @param  \Illuminate\Foundation\Application  $app
     */
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
            'prefix' => '',
            'auth' => [
                'method' => 'jwt',
                'jwt' => [
                    'user' => 'TEST_USER',
                    'private_key_path' => __DIR__ . '/Fixtures/test_rsa_key.pem',
                    'private_key_passphrase' => null,
                ],
            ],
            'timeout' => 0,
            'async_polling_interval' => 100,
        ]);
    }
}
