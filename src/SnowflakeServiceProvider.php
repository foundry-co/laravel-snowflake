<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake;

use FoundryCo\Snowflake\Connection\SnowflakeConnection;
use FoundryCo\Snowflake\Connection\SnowflakeConnector;
use Illuminate\Database\Connection;
use Illuminate\Support\ServiceProvider;

class SnowflakeServiceProvider extends ServiceProvider
{
    public function boot(): void
    {
        Connection::resolverFor('snowflake', function ($connection, $database, $prefix, $config) {
            if ($connection instanceof \Closure) {
                $connection = $connection();
            }

            return new SnowflakeConnection($connection, $database, '', $config);
        });

        $this->app['db']->extend('snowflake', function ($config, $name) {
            $config['name'] = $name;

            $connector = new SnowflakeConnector;
            $client = $connector->connect($config);

            return new SnowflakeConnection($client, $config['database'] ?? '', '', $config);
        });
    }
}
