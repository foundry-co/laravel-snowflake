<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Schema;

use Closure;
use Illuminate\Database\Schema\Builder;

/**
 * Snowflake schema builder for database structure operations.
 */
class SnowflakeSchemaBuilder extends Builder
{
    /**
     * Create a new database.
     */
    public function createDatabase(string $name): bool
    {
        return $this->connection->statement(
            $this->grammar->compileCreateDatabase($name, $this->connection)
        );
    }

    /**
     * Drop a database if it exists.
     */
    public function dropDatabaseIfExists(string $name): bool
    {
        return $this->connection->statement(
            $this->grammar->compileDropDatabaseIfExists($name)
        );
    }

    /**
     * Create a new schema within the current database.
     */
    public function createSchema(string $name): bool
    {
        return $this->connection->statement(
            sprintf('CREATE SCHEMA IF NOT EXISTS %s', $this->grammar->wrapTable($name))
        );
    }

    /**
     * Drop a schema if it exists.
     */
    public function dropSchemaIfExists(string $name): bool
    {
        return $this->connection->statement(
            sprintf('DROP SCHEMA IF EXISTS %s', $this->grammar->wrapTable($name))
        );
    }

    /**
     * Get all tables in the database.
     *
     * @return array<int, array{name: string, schema: string|null, size: int|null, rows: int|null, comment: string|null}>
     */
    public function getTables(): array
    {
        $database = $this->connection->getDatabaseName();
        $schema = $this->connection->getConfig('schema');

        return $this->connection->getPostProcessor()->processTables(
            $this->connection->selectFromWriteConnection(
                $this->grammar->compileTables($database, $schema)
            )
        );
    }

    /**
     * Get all views in the database.
     *
     * @return array<int, array{name: string, schema: string|null, definition: string|null}>
     */
    public function getViews(): array
    {
        $database = $this->connection->getDatabaseName();
        $schema = $this->connection->getConfig('schema');

        return $this->connection->getPostProcessor()->processViews(
            $this->connection->selectFromWriteConnection(
                $this->grammar->compileViews($database, $schema)
            )
        );
    }

    /**
     * Get all columns for a table.
     *
     * @return array<int, array{name: string, type_name: string, type: string, nullable: bool, default: string|null, auto_increment: bool, comment: string|null, generation: array|null}>
     */
    public function getColumns(string $table): array
    {
        $table = $this->connection->getTablePrefix() . $table;
        $database = $this->connection->getDatabaseName();
        $schema = $this->connection->getConfig('schema') ?? 'PUBLIC';

        return $this->connection->getPostProcessor()->processColumns(
            $this->connection->selectFromWriteConnection(
                $this->grammar->compileColumns($database, $schema, $table)
            )
        );
    }

    /**
     * Get indexes for a table.
     *
     * Note: Snowflake doesn't have traditional indexes - returns clustering info instead.
     *
     * @return array<int, array{name: string, columns: array, type: string, unique: bool, primary: bool}>
     */
    public function getIndexes(string $table): array
    {
        // Snowflake doesn't have traditional indexes
        // Could query clustering keys here if needed
        return [];
    }

    /**
     * Get foreign keys for a table.
     *
     * @return array<int, array{name: string, columns: array, foreign_schema: string, foreign_table: string, foreign_columns: array, on_update: string, on_delete: string}>
     */
    public function getForeignKeys(string $table): array
    {
        $table = $this->connection->getTablePrefix() . $table;
        $database = $this->connection->getDatabaseName();
        $schema = $this->connection->getConfig('schema') ?? 'PUBLIC';

        return $this->connection->getPostProcessor()->processForeignKeys(
            $this->connection->selectFromWriteConnection(
                $this->grammar->compileForeignKeys($database, $schema, $table)
            )
        );
    }

    /**
     * Check if a table exists.
     */
    public function hasTable($table): bool
    {
        $table = $this->connection->getTablePrefix() . $table;
        $schema = $this->connection->getConfig('schema') ?? 'PUBLIC';

        $result = $this->connection->selectFromWriteConnection(
            $this->grammar->compileTableExists(),
            [$schema, $table]
        );

        return count($result) > 0 && ((array) $result[0])['COUNT(*)'] > 0;
    }

    /**
     * Check if a column exists on a table.
     */
    public function hasColumn($table, $column): bool
    {
        $columns = array_map(
            fn ($col) => strtolower($col['name']),
            $this->getColumns($table)
        );

        return in_array(strtolower($column), $columns, true);
    }

    /**
     * Check if columns exist on a table.
     */
    public function hasColumns($table, array $columns): bool
    {
        $tableColumns = array_map(
            fn ($col) => strtolower($col['name']),
            $this->getColumns($table)
        );

        foreach ($columns as $column) {
            if (! in_array(strtolower($column), $tableColumns, true)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Drop all tables in the database.
     */
    public function dropAllTables(): void
    {
        $tables = $this->getTables();

        if (empty($tables)) {
            return;
        }

        foreach ($tables as $table) {
            $this->drop($table['name']);
        }
    }

    /**
     * Drop all views in the database.
     */
    public function dropAllViews(): void
    {
        $views = $this->getViews();

        foreach ($views as $view) {
            $this->connection->statement(
                sprintf('DROP VIEW IF EXISTS %s', $this->grammar->wrapTable($view['name']))
            );
        }
    }

    /**
     * Create a new table blueprint.
     */
    protected function createBlueprint($table, ?Closure $callback = null): SnowflakeBlueprint
    {
        $prefix = $this->connection->getConfig('prefix_indexes')
            ? $this->connection->getConfig('prefix')
            : '';

        return new SnowflakeBlueprint($table, $callback, $prefix);
    }

    /**
     * Get the column type for a given column name.
     */
    public function getColumnType(string $table, string $column, bool $fullDefinition = false): string
    {
        $columns = $this->getColumns($table);

        foreach ($columns as $col) {
            if (strtolower($col['name']) === strtolower($column)) {
                return $fullDefinition ? $col['type'] : $col['type_name'];
            }
        }

        return '';
    }

    /**
     * Get the column listing for a table.
     *
     * @return array<int, string>
     */
    public function getColumnListing($table): array
    {
        return array_map(
            fn ($col) => $col['name'],
            $this->getColumns($table)
        );
    }
}
