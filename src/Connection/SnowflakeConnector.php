<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Connection;

use FoundryCo\Snowflake\Client\SnowflakeApiClient;
use Illuminate\Database\Connectors\ConnectorInterface;

/**
 * Connector for creating Snowflake API client instances.
 *
 * This connector creates an API client instead of a PDO connection
 * since we're using Snowflake's REST API.
 */
class SnowflakeConnector implements ConnectorInterface
{
    /**
     * Establish a database connection.
     *
     * Note: For Snowflake REST API, we return the API client instead of PDO.
     * The SnowflakeConnection class handles wrapping this appropriately.
     *
     * @return SnowflakeApiClient
     */
    public function connect(array $config): SnowflakeApiClient
    {
        return new SnowflakeApiClient($config);
    }
}
