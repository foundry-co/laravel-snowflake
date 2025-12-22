<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Connection;

use FoundryCo\Snowflake\Client\SnowflakeApiClient;
use Illuminate\Database\Connectors\ConnectorInterface;

class SnowflakeConnector implements ConnectorInterface
{
    public function connect(array $config): SnowflakeApiClient
    {
        return new SnowflakeApiClient($config);
    }
}
