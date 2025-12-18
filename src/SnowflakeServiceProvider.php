<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake;

use FoundryCo\Snowflake\Connection\SnowflakeConnection;
use FoundryCo\Snowflake\Connection\SnowflakeConnector;
use Illuminate\Database\Connection;
use Illuminate\Support\ServiceProvider;

/**
 * Laravel service provider for the Snowflake database driver.
 *
 * This provider registers the Snowflake driver with Laravel's
 * DatabaseManager, enabling seamless use of Snowflake connections.
 */
class SnowflakeServiceProvider extends ServiceProvider
{
    /**
     * Register the service provider.
     */
    public function register(): void
    {
        $this->mergeConfigFrom(
            __DIR__ . '/../config/snowflake.php',
            'database.connections.snowflake'
        );
    }

    /**
     * Bootstrap the service provider.
     */
    public function boot(): void
    {
        $this->publishes([
            __DIR__ . '/../config/snowflake.php' => config_path('snowflake.php'),
        ], 'snowflake-config');

        $this->registerSnowflakeDriver();
    }

    /**
     * Register the Snowflake database driver.
     */
    protected function registerSnowflakeDriver(): void
    {
        // Register the connection resolver
        Connection::resolverFor('snowflake', function ($connection, $database, $prefix, $config) {
            // If a closure is passed, it will create the client lazily
            if ($connection instanceof \Closure) {
                $connection = $connection();
            }

            return new SnowflakeConnection($connection, $database, $prefix, $config);
        });

        // Register the connector in the database manager
        $this->app->resolving('db', function ($db) {
            $db->extend('snowflake', function ($config, $name) {
                $config['name'] = $name;

                // Merge with default config if using 'snowflake' connection
                if (isset($config['driver']) && $config['driver'] === 'snowflake') {
                    $config = array_merge(
                        config('database.connections.snowflake', []),
                        $config
                    );
                }

                // Create the API client via connector
                $connector = new SnowflakeConnector;
                $client = $connector->connect($config);

                return new SnowflakeConnection(
                    $client,
                    $config['database'] ?? '',
                    $config['prefix'] ?? '',
                    $config
                );
            });
        });
    }
}
