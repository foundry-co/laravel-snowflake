<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Connection;

use Closure;
use Exception;
use FoundryCo\Snowflake\Client\Exceptions\QueryException as SnowflakeQueryException;
use FoundryCo\Snowflake\Client\SnowflakeApiClient;
use FoundryCo\Snowflake\Query\Grammars\SnowflakeGrammar as QueryGrammar;
use FoundryCo\Snowflake\Query\Processors\SnowflakeProcessor;
use FoundryCo\Snowflake\Schema\Grammars\SnowflakeSchemaGrammar;
use FoundryCo\Snowflake\Schema\SnowflakeSchemaBuilder;
use Generator;
use Illuminate\Database\Connection;
use Illuminate\Database\QueryException;

/**
 * Snowflake database connection using REST API.
 *
 * This connection class wraps the Snowflake REST API client
 * and integrates with Laravel's database abstraction layer.
 */
class SnowflakeConnection extends Connection
{
    protected SnowflakeApiClient $client;
    protected ?string $currentWarehouse = null;
    protected ?string $currentRole = null;
    protected ?string $currentSchema = null;

    /**
     * Create a new Snowflake connection instance.
     *
     * @param  SnowflakeApiClient|Closure  $pdo  The API client or closure to create it
     */
    public function __construct($pdo, string $database = '', string $tablePrefix = '', array $config = [])
    {
        // Laravel expects PDO, but we pass our API client
        // The parent constructor stores it in $this->pdo
        parent::__construct($pdo, $database, $tablePrefix, $config);

        $this->currentWarehouse = $config['warehouse'] ?? null;
        $this->currentRole = $config['role'] ?? null;
        $this->currentSchema = $config['schema'] ?? 'PUBLIC';

        // Get the actual client instance
        $this->client = $this->getPdo();
    }

    /**
     * Get the default query grammar instance.
     */
    protected function getDefaultQueryGrammar(): QueryGrammar
    {
        $grammar = new QueryGrammar;
        $grammar->setTablePrefix($this->tablePrefix);

        return $grammar;
    }

    /**
     * Get the default schema grammar instance.
     */
    protected function getDefaultSchemaGrammar(): SnowflakeSchemaGrammar
    {
        $grammar = new SnowflakeSchemaGrammar;
        $grammar->setTablePrefix($this->tablePrefix);

        return $grammar;
    }

    /**
     * Get the default post processor instance.
     */
    protected function getDefaultPostProcessor(): SnowflakeProcessor
    {
        return new SnowflakeProcessor;
    }

    /**
     * Get the schema builder instance.
     */
    public function getSchemaBuilder(): SnowflakeSchemaBuilder
    {
        if ($this->schemaGrammar === null) {
            $this->useDefaultSchemaGrammar();
        }

        return new SnowflakeSchemaBuilder($this);
    }

    /**
     * Get the schema grammar used by the connection.
     */
    public function getSchemaGrammar(): SnowflakeSchemaGrammar
    {
        return $this->schemaGrammar ?? $this->getDefaultSchemaGrammar();
    }

    /**
     * Get the query context for API calls.
     */
    protected function getQueryContext(): array
    {
        return [
            'database' => $this->database,
            'schema' => $this->currentSchema,
            'warehouse' => $this->currentWarehouse,
            'role' => $this->currentRole,
        ];
    }

    // =====================================
    // Query Execution Methods
    // =====================================

    /**
     * Run a select statement against the database.
     */
    public function select($query, $bindings = [], $useReadPdo = true): array
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return [];
            }

            $result = $this->client->execute($query, $bindings, $this->getQueryContext());

            return $result->fetchAll();
        });
    }

    /**
     * Run a select statement and return a generator.
     */
    public function cursor($query, $bindings = [], $useReadPdo = true): Generator
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return;
            }

            $result = $this->client->execute($query, $bindings, $this->getQueryContext());

            foreach ($result->getResultSet()->rows() as $row) {
                yield $row;
            }
        });
    }

    /**
     * Execute an SQL statement and return the boolean result.
     */
    public function statement($query, $bindings = []): bool
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return true;
            }

            $this->client->execute($query, $bindings, $this->getQueryContext());

            return true;
        });
    }

    /**
     * Run an SQL statement and get the number of rows affected.
     */
    public function affectingStatement($query, $bindings = []): int
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return 0;
            }

            $result = $this->client->execute($query, $bindings, $this->getQueryContext());

            return $result->getRowCount();
        });
    }

    /**
     * Run a raw, unprepared query against the PDO connection.
     */
    public function unprepared($query): bool
    {
        return $this->run($query, [], function ($query) {
            if ($this->pretending()) {
                return true;
            }

            $this->client->execute($query, [], $this->getQueryContext());

            return true;
        });
    }

    /**
     * Run an insert statement against the database.
     */
    public function insert($query, $bindings = []): bool
    {
        return $this->statement($query, $bindings);
    }

    /**
     * Run an update statement against the database.
     */
    public function update($query, $bindings = []): int
    {
        return $this->affectingStatement($query, $bindings);
    }

    /**
     * Run a delete statement against the database.
     */
    public function delete($query, $bindings = []): int
    {
        return $this->affectingStatement($query, $bindings);
    }

    // =====================================
    // Transaction Support
    // =====================================

    /**
     * Start a new database transaction.
     */
    public function beginTransaction(): void
    {
        $this->createTransaction();

        $this->transactions++;

        $this->fireConnectionEvent('beganTransaction');
    }

    /**
     * Create a transaction within the database.
     */
    protected function createTransaction(): void
    {
        if ($this->transactions === 0) {
            $this->unprepared('BEGIN TRANSACTION');
        }
    }

    /**
     * Commit the active database transaction.
     */
    public function commit(): void
    {
        if ($this->transactions === 1) {
            $this->unprepared('COMMIT');
        }

        $this->transactions = max(0, $this->transactions - 1);

        $this->fireConnectionEvent('committed');
    }

    /**
     * Rollback the active database transaction.
     */
    public function rollBack($toLevel = null): void
    {
        $toLevel = $toLevel ?? $this->transactions - 1;

        if ($toLevel < 0 || $toLevel >= $this->transactions) {
            return;
        }

        $this->performRollBack($toLevel);

        $this->transactions = $toLevel;

        $this->fireConnectionEvent('rollingBack');
    }

    /**
     * Perform a rollback within the database.
     */
    protected function performRollBack($toLevel): void
    {
        if ($toLevel === 0) {
            $this->unprepared('ROLLBACK');
        }
        // Snowflake doesn't support savepoints
    }

    // =====================================
    // Context Switching
    // =====================================

    /**
     * Switch to a different warehouse.
     */
    public function useWarehouse(string $warehouse): static
    {
        $this->currentWarehouse = $warehouse;

        return $this;
    }

    /**
     * Switch to a different role.
     */
    public function useRole(string $role): static
    {
        $this->currentRole = $role;

        return $this;
    }

    /**
     * Switch to a different schema.
     */
    public function useSchema(string $schema): static
    {
        $this->currentSchema = $schema;

        return $this;
    }

    /**
     * Get the current warehouse.
     */
    public function getWarehouse(): ?string
    {
        return $this->currentWarehouse;
    }

    /**
     * Get the current role.
     */
    public function getRole(): ?string
    {
        return $this->currentRole;
    }

    /**
     * Get the current schema.
     */
    public function getSchema(): ?string
    {
        return $this->currentSchema;
    }

    // =====================================
    // Driver Info
    // =====================================

    /**
     * Get the driver name.
     */
    public function getDriverName(): string
    {
        return 'snowflake';
    }

    /**
     * Get the driver title.
     */
    public function getDriverTitle(): string
    {
        return 'Snowflake';
    }

    // =====================================
    // Exception Handling
    // =====================================

    /**
     * Handle a query exception.
     */
    protected function runQueryCallback($query, $bindings, Closure $callback): mixed
    {
        try {
            return $callback($query, $bindings);
        } catch (SnowflakeQueryException $e) {
            throw new QueryException(
                $this->getDriverName(),
                $query,
                $this->prepareBindings($bindings),
                $e
            );
        }
    }

    /**
     * Determine if the given exception is a unique constraint violation.
     */
    protected function isUniqueConstraintError(Exception $exception): bool
    {
        $message = $exception->getMessage();

        return str_contains($message, 'duplicate key value') ||
               str_contains($message, 'Duplicate entry') ||
               str_contains($message, 'unique constraint');
    }

    // =====================================
    // API Client Access
    // =====================================

    /**
     * Get the underlying API client.
     */
    public function getClient(): SnowflakeApiClient
    {
        return $this->client;
    }

    /**
     * Get the current PDO connection (returns API client for type compatibility).
     *
     * @return SnowflakeApiClient
     */
    public function getPdo(): SnowflakeApiClient
    {
        if ($this->pdo instanceof Closure) {
            $this->pdo = call_user_func($this->pdo);
        }

        return $this->pdo;
    }

    /**
     * Get the current PDO connection used for reading (same as getPdo for Snowflake).
     *
     * @return SnowflakeApiClient
     */
    public function getReadPdo(): SnowflakeApiClient
    {
        return $this->getPdo();
    }

    // =====================================
    // Unsupported Features
    // =====================================

    /**
     * Disconnect from the underlying connection.
     *
     * No-op for REST API connections.
     */
    public function disconnect(): void
    {
        // No persistent connection to close
    }

    /**
     * Reconnect to the database.
     *
     * No-op for REST API connections.
     */
    public function reconnect(): void
    {
        // API client handles token refresh automatically
    }
}
