<?php

return [
    /*
    |--------------------------------------------------------------------------
    | Snowflake Account Identifier
    |--------------------------------------------------------------------------
    |
    | Your Snowflake account identifier. This can be in the format:
    | - Organization-Account (e.g., "myorg-myaccount")
    | - Account.Region (legacy format, e.g., "xy12345.us-east-1")
    |
    */
    'account' => env('SNOWFLAKE_ACCOUNT'),

    /*
    |--------------------------------------------------------------------------
    | Warehouse
    |--------------------------------------------------------------------------
    |
    | The default warehouse to use for query execution. This can be overridden
    | per-query using the useWarehouse() method on the connection.
    |
    */
    'warehouse' => env('SNOWFLAKE_WAREHOUSE'),

    /*
    |--------------------------------------------------------------------------
    | Database
    |--------------------------------------------------------------------------
    |
    | The default database to use for queries.
    |
    */
    'database' => env('SNOWFLAKE_DATABASE'),

    /*
    |--------------------------------------------------------------------------
    | Schema
    |--------------------------------------------------------------------------
    |
    | The default schema within the database. Defaults to PUBLIC.
    |
    */
    'schema' => env('SNOWFLAKE_SCHEMA', 'PUBLIC'),

    /*
    |--------------------------------------------------------------------------
    | Role
    |--------------------------------------------------------------------------
    |
    | The default role to use for queries. If not specified, the user's
    | default role will be used.
    |
    */
    'role' => env('SNOWFLAKE_ROLE'),

    /*
    |--------------------------------------------------------------------------
    | Authentication
    |--------------------------------------------------------------------------
    |
    | Snowflake REST API authentication configuration. Supports JWT key-pair
    | authentication (recommended) and OAuth.
    |
    */
    'auth' => [
        // Authentication method: 'jwt' or 'oauth'
        'method' => env('SNOWFLAKE_AUTH_METHOD', 'jwt'),

        // JWT Key-Pair Authentication
        // Use either private_key_path (file) OR private_key (content directly)
        'jwt' => [
            'user' => env('SNOWFLAKE_USER'),
            // Path to private key file (PEM format)
            'private_key_path' => env('SNOWFLAKE_PRIVATE_KEY_PATH'),
            // OR: Private key content directly (useful for 1Password, secrets managers)
            'private_key' => env('SNOWFLAKE_PRIVATE_KEY'),
            'private_key_passphrase' => env('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'),
        ],

        // OAuth Authentication
        'oauth' => [
            'token_endpoint' => env('SNOWFLAKE_OAUTH_TOKEN_ENDPOINT'),
            'client_id' => env('SNOWFLAKE_OAUTH_CLIENT_ID'),
            'client_secret' => env('SNOWFLAKE_OAUTH_CLIENT_SECRET'),
            'scope' => env('SNOWFLAKE_OAUTH_SCOPE', 'session:role-any'),
            'refresh_token' => env('SNOWFLAKE_OAUTH_REFRESH_TOKEN'),
        ],
    ],

    /*
    |--------------------------------------------------------------------------
    | Query Timeout
    |--------------------------------------------------------------------------
    |
    | Maximum time in seconds to wait for a query to complete. Set to 0 for
    | no timeout (Snowflake default is 2 days).
    |
    */
    'timeout' => env('SNOWFLAKE_QUERY_TIMEOUT', 0),

    /*
    |--------------------------------------------------------------------------
    | Async Polling Interval
    |--------------------------------------------------------------------------
    |
    | When a query runs asynchronously, this is the interval in milliseconds
    | between status checks.
    |
    */
    'async_polling_interval' => env('SNOWFLAKE_POLLING_INTERVAL', 500),

    /*
    |--------------------------------------------------------------------------
    | Table Prefix
    |--------------------------------------------------------------------------
    |
    | Optional prefix to add to all table names.
    |
    */
    'prefix' => env('SNOWFLAKE_TABLE_PREFIX', ''),

    /*
    |--------------------------------------------------------------------------
    | Session Parameters
    |--------------------------------------------------------------------------
    |
    | Optional session parameters to set when establishing a connection.
    | These are sent with each query request.
    |
    */
    'session_parameters' => [
        // 'QUERY_TAG' => 'laravel-app',
        // 'TIMEZONE' => 'UTC',
    ],
];
