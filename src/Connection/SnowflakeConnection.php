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

class SnowflakeConnection extends Connection
{
    protected SnowflakeApiClient $client;
    protected ?string $currentWarehouse = null;
    protected ?string $currentRole = null;
    protected ?string $currentSchema = null;

    public function __construct($pdo, string $database = '', string $tablePrefix = '', array $config = [])
    {
        parent::__construct($pdo, $database, $tablePrefix, $config);

        $this->currentWarehouse = $config['warehouse'] ?? null;
        $this->currentRole = $config['role'] ?? null;
        $this->currentSchema = $config['schema'] ?? 'PUBLIC';
        $this->client = $this->getPdo();
    }

    protected function getDefaultQueryGrammar(): QueryGrammar
    {
        return new QueryGrammar;
    }

    protected function getDefaultSchemaGrammar(): SnowflakeSchemaGrammar
    {
        return new SnowflakeSchemaGrammar;
    }

    protected function getDefaultPostProcessor(): SnowflakeProcessor
    {
        return new SnowflakeProcessor;
    }

    public function getSchemaBuilder(): SnowflakeSchemaBuilder
    {
        if ($this->schemaGrammar === null) {
            $this->useDefaultSchemaGrammar();
        }

        return new SnowflakeSchemaBuilder($this);
    }

    public function getSchemaGrammar(): SnowflakeSchemaGrammar
    {
        return $this->schemaGrammar ?? $this->getDefaultSchemaGrammar();
    }

    protected function getQueryContext(): array
    {
        return [
            'database' => $this->database,
            'schema' => $this->currentSchema,
            'warehouse' => $this->currentWarehouse,
            'role' => $this->currentRole,
        ];
    }

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

    public function insert($query, $bindings = []): bool
    {
        return $this->statement($query, $bindings);
    }

    public function update($query, $bindings = []): int
    {
        return $this->affectingStatement($query, $bindings);
    }

    public function delete($query, $bindings = []): int
    {
        return $this->affectingStatement($query, $bindings);
    }

    public function beginTransaction(): void
    {
        $this->createTransaction();
        $this->transactions++;
        $this->fireConnectionEvent('beganTransaction');
    }

    protected function createTransaction(): void
    {
        if ($this->transactions === 0) {
            $this->unprepared('BEGIN TRANSACTION');
        }
    }

    public function commit(): void
    {
        if ($this->transactions === 1) {
            $this->unprepared('COMMIT');
        }

        $this->transactions = max(0, $this->transactions - 1);
        $this->fireConnectionEvent('committed');
    }

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

    protected function performRollBack($toLevel): void
    {
        if ($toLevel === 0) {
            $this->unprepared('ROLLBACK');
        }
    }

    public function useWarehouse(string $warehouse): static
    {
        $this->currentWarehouse = $warehouse;

        return $this;
    }

    public function useRole(string $role): static
    {
        $this->currentRole = $role;

        return $this;
    }

    public function useSchema(string $schema): static
    {
        $this->currentSchema = $schema;

        return $this;
    }

    public function getWarehouse(): ?string
    {
        return $this->currentWarehouse;
    }

    public function getRole(): ?string
    {
        return $this->currentRole;
    }

    public function getSchema(): ?string
    {
        return $this->currentSchema;
    }

    public function getDriverName(): string
    {
        return 'snowflake';
    }

    public function getDriverTitle(): string
    {
        return 'Snowflake';
    }

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

    protected function isUniqueConstraintError(Exception $exception): bool
    {
        $message = $exception->getMessage();

        return str_contains($message, 'duplicate key value') ||
               str_contains($message, 'Duplicate entry') ||
               str_contains($message, 'unique constraint');
    }

    public function getClient(): SnowflakeApiClient
    {
        return $this->client;
    }

    public function getPdo(): SnowflakeApiClient
    {
        if ($this->pdo instanceof Closure) {
            $this->pdo = call_user_func($this->pdo);
        }

        return $this->pdo;
    }

    public function getReadPdo(): SnowflakeApiClient
    {
        return $this->getPdo();
    }

    public function disconnect(): void
    {
    }

    public function reconnect(): void
    {
    }
}
