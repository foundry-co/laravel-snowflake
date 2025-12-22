<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Schema\Grammars;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Schema\Grammars\Grammar;
use Illuminate\Support\Fluent;

class SnowflakeSchemaGrammar extends Grammar
{
    protected $modifiers = ['Collate', 'Nullable', 'Default', 'Comment'];

    public function compileCreateDatabase($name): string
    {
        return sprintf('CREATE DATABASE IF NOT EXISTS %s', $this->wrapValue($name));
    }

    public function compileDropDatabaseIfExists($name): string
    {
        return sprintf('DROP DATABASE IF EXISTS %s', $this->wrapValue($name));
    }

    public function compileCreate(Blueprint $blueprint, Fluent $command): string
    {
        return sprintf(
            'CREATE TABLE %s (%s)',
            $this->wrapTable($blueprint),
            implode(', ', $this->getColumns($blueprint))
        );
    }

    public function compileAdd(Blueprint $blueprint, Fluent $command): string
    {
        $columns = $this->prefixArray('ADD COLUMN', $this->getColumns($blueprint));

        return 'ALTER TABLE ' . $this->wrapTable($blueprint) . ' ' . implode(', ', $columns);
    }

    public function compileDropColumn(Blueprint $blueprint, Fluent $command): string
    {
        $columns = $this->prefixArray('DROP COLUMN', $this->wrapArray($command->columns));

        return 'ALTER TABLE ' . $this->wrapTable($blueprint) . ' ' . implode(', ', $columns);
    }

    public function compileRenameColumn(Blueprint $blueprint, Fluent $command): string
    {
        return sprintf(
            'ALTER TABLE %s RENAME COLUMN %s TO %s',
            $this->wrapTable($blueprint),
            $this->wrap($command->from),
            $this->wrap($command->to)
        );
    }

    public function compileChange(Blueprint $blueprint, Fluent $command): array
    {
        $changes = [];

        foreach ($blueprint->getChangedColumns() as $column) {
            $sql = sprintf(
                'ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s',
                $this->wrapTable($blueprint),
                $this->wrap($column->name),
                $this->getType($column)
            );

            $changes[] = $sql;
        }

        return $changes;
    }

    public function compilePrimary(Blueprint $blueprint, Fluent $command): string
    {
        return sprintf(
            'ALTER TABLE %s ADD PRIMARY KEY (%s)',
            $this->wrapTable($blueprint),
            $this->columnize($command->columns)
        );
    }

    public function compileUnique(Blueprint $blueprint, Fluent $command): string
    {
        return sprintf(
            'ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)',
            $this->wrapTable($blueprint),
            $this->wrap($command->index),
            $this->columnize($command->columns)
        );
    }

    public function compileIndex(Blueprint $blueprint, Fluent $command): ?string
    {
        return null;
    }

    public function compileForeign(Blueprint $blueprint, Fluent $command): string
    {
        $sql = sprintf(
            'ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)',
            $this->wrapTable($blueprint),
            $this->wrap($command->index),
            $this->columnize($command->columns),
            $this->wrapTable($command->on),
            $this->columnize((array) $command->references)
        );

        if (! is_null($command->onDelete)) {
            $sql .= " ON DELETE {$command->onDelete}";
        }

        if (! is_null($command->onUpdate)) {
            $sql .= " ON UPDATE {$command->onUpdate}";
        }

        return $sql;
    }

    public function compileDropForeign(Blueprint $blueprint, Fluent $command): string
    {
        return sprintf(
            'ALTER TABLE %s DROP CONSTRAINT %s',
            $this->wrapTable($blueprint),
            $this->wrap($command->index)
        );
    }

    public function compileDropPrimary(Blueprint $blueprint, Fluent $command): string
    {
        return sprintf(
            'ALTER TABLE %s DROP PRIMARY KEY',
            $this->wrapTable($blueprint)
        );
    }

    public function compileDropUnique(Blueprint $blueprint, Fluent $command): string
    {
        return sprintf(
            'ALTER TABLE %s DROP CONSTRAINT %s',
            $this->wrapTable($blueprint),
            $this->wrap($command->index)
        );
    }

    public function compileDrop(Blueprint $blueprint, Fluent $command): string
    {
        return 'DROP TABLE ' . $this->wrapTable($blueprint);
    }

    public function compileDropIfExists(Blueprint $blueprint, Fluent $command): string
    {
        return 'DROP TABLE IF EXISTS ' . $this->wrapTable($blueprint);
    }

    public function compileRename(Blueprint $blueprint, Fluent $command): string
    {
        return sprintf(
            'ALTER TABLE %s RENAME TO %s',
            $this->wrapTable($blueprint),
            $this->wrapTable($command->to)
        );
    }

    public function compileClusterBy(Blueprint $blueprint, Fluent $command): string
    {
        return sprintf(
            'ALTER TABLE %s CLUSTER BY (%s)',
            $this->wrapTable($blueprint),
            $this->columnize($command->columns)
        );
    }

    public function compileDataRetention(Blueprint $blueprint, Fluent $command): string
    {
        return sprintf(
            'ALTER TABLE %s SET DATA_RETENTION_TIME_IN_DAYS = %d',
            $this->wrapTable($blueprint),
            $command->days
        );
    }

    public function compileSequence(Blueprint $blueprint, Fluent $command): string
    {
        return sprintf(
            'CREATE SEQUENCE IF NOT EXISTS %s START WITH %d INCREMENT BY %d',
            $this->wrap($command->name),
            $command->start,
            $command->increment
        );
    }

    public function compileTables($schema): string
    {
        return "SELECT TABLE_NAME AS \"name\", TABLE_SCHEMA AS \"schema\", BYTES AS \"size\", ROW_COUNT AS \"rows\", COMMENT AS \"comment\" " .
            "FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '{$schema}'";
    }

    public function compileViews($schema): string
    {
        return "SELECT TABLE_NAME AS \"name\", TABLE_SCHEMA AS \"schema\", VIEW_DEFINITION AS \"definition\" " .
            "FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{$schema}'";
    }

    public function compileColumns($schema, $table): string
    {
        return "SELECT COLUMN_NAME AS \"name\", DATA_TYPE AS \"type_name\", IS_NULLABLE AS \"nullable\", COLUMN_DEFAULT AS \"default\", COMMENT AS \"comment\", " .
            "NUMERIC_PRECISION AS \"precision\", NUMERIC_SCALE AS \"scale\", CHARACTER_MAXIMUM_LENGTH AS \"length\" " .
            "FROM INFORMATION_SCHEMA.COLUMNS " .
            "WHERE TABLE_SCHEMA = '{$schema}' AND TABLE_NAME = '{$table}' " .
            'ORDER BY ORDINAL_POSITION';
    }

    public function compileForeignKeys($schema, $table): string
    {
        return "SELECT " .
            'tc.CONSTRAINT_NAME AS "name", ' .
            'kcu.COLUMN_NAME AS "columns", ' .
            'ccu.TABLE_SCHEMA AS "foreign_schema", ' .
            'ccu.TABLE_NAME AS "foreign_table", ' .
            'ccu.COLUMN_NAME AS "foreign_columns", ' .
            'rc.UPDATE_RULE AS "on_update", rc.DELETE_RULE AS "on_delete" ' .
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc " .
            "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu " .
            'ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME ' .
            "JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc " .
            'ON tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME ' .
            "JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu " .
            'ON rc.UNIQUE_CONSTRAINT_NAME = ccu.CONSTRAINT_NAME ' .
            "WHERE tc.TABLE_SCHEMA = '{$schema}' AND tc.TABLE_NAME = '{$table}' " .
            "AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'";
    }

    public function compileTableExists($schema, $table): string
    {
        return "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{$schema}' AND TABLE_NAME = '{$table}'";
    }

    protected function typeChar(Fluent $column): string
    {
        return "CHAR({$column->length})";
    }

    protected function typeString(Fluent $column): string
    {
        $length = $column->length ?? 255;

        return "VARCHAR({$length})";
    }

    protected function typeTinyText(Fluent $column): string
    {
        return 'VARCHAR(255)';
    }

    protected function typeText(Fluent $column): string
    {
        return 'TEXT';
    }

    protected function typeMediumText(Fluent $column): string
    {
        return 'TEXT';
    }

    protected function typeLongText(Fluent $column): string
    {
        return 'TEXT';
    }

    protected function typeInteger(Fluent $column): string
    {
        return 'INTEGER';
    }

    protected function typeBigInteger(Fluent $column): string
    {
        return 'BIGINT';
    }

    protected function typeMediumInteger(Fluent $column): string
    {
        return 'INTEGER';
    }

    protected function typeSmallInteger(Fluent $column): string
    {
        return 'SMALLINT';
    }

    protected function typeTinyInteger(Fluent $column): string
    {
        return 'TINYINT';
    }

    protected function typeFloat(Fluent $column): string
    {
        return 'FLOAT';
    }

    protected function typeDouble(Fluent $column): string
    {
        return 'DOUBLE';
    }

    protected function typeDecimal(Fluent $column): string
    {
        return "NUMBER({$column->total}, {$column->places})";
    }

    protected function typeNumber(Fluent $column): string
    {
        $precision = $column->precision ?? 38;
        $scale = $column->scale ?? 0;

        return "NUMBER({$precision}, {$scale})";
    }

    protected function typeBoolean(Fluent $column): string
    {
        return 'BOOLEAN';
    }

    protected function typeEnum(Fluent $column): string
    {
        return sprintf(
            'VARCHAR(%d)',
            max(array_map('strlen', $column->allowed)) + 10
        );
    }

    protected function typeJson(Fluent $column): string
    {
        return 'VARIANT';
    }

    protected function typeJsonb(Fluent $column): string
    {
        return 'VARIANT';
    }

    protected function typeDate(Fluent $column): string
    {
        return 'DATE';
    }

    protected function typeDateTime(Fluent $column): string
    {
        return 'TIMESTAMP_NTZ';
    }

    protected function typeDateTimeTz(Fluent $column): string
    {
        return 'TIMESTAMP_TZ';
    }

    protected function typeTime(Fluent $column): string
    {
        return 'TIME';
    }

    protected function typeTimeTz(Fluent $column): string
    {
        return 'TIME';
    }

    protected function typeTimestamp(Fluent $column): string
    {
        return 'TIMESTAMP_NTZ';
    }

    protected function typeTimestampTz(Fluent $column): string
    {
        return 'TIMESTAMP_TZ';
    }

    protected function typeTimestampNtz(Fluent $column): string
    {
        return 'TIMESTAMP_NTZ';
    }

    protected function typeTimestampLtz(Fluent $column): string
    {
        return 'TIMESTAMP_LTZ';
    }

    protected function typeYear(Fluent $column): string
    {
        return 'INTEGER';
    }

    protected function typeBinary(Fluent $column): string
    {
        return 'BINARY';
    }

    protected function typeUuid(Fluent $column): string
    {
        return 'VARCHAR(36)';
    }

    protected function typeUlid(Fluent $column): string
    {
        return 'CHAR(26)';
    }

    protected function typeIpAddress(Fluent $column): string
    {
        return 'VARCHAR(45)';
    }

    protected function typeMacAddress(Fluent $column): string
    {
        return 'VARCHAR(17)';
    }

    protected function typeVariant(Fluent $column): string
    {
        return 'VARIANT';
    }

    protected function typeObject(Fluent $column): string
    {
        return 'OBJECT';
    }

    protected function typeArray(Fluent $column): string
    {
        return 'ARRAY';
    }

    protected function typeGeography(Fluent $column): string
    {
        return 'GEOGRAPHY';
    }

    protected function typeGeometry(Fluent $column): string
    {
        return 'GEOMETRY';
    }

    protected function typeIdentity(Fluent $column): string
    {
        $start = $column->start ?? 1;
        $increment = $column->increment ?? 1;

        return "INTEGER IDENTITY({$start}, {$increment})";
    }

    protected function modifyNullable(Blueprint $blueprint, Fluent $column): ?string
    {
        if ($column->nullable === false) {
            return ' NOT NULL';
        }

        return ' NULL';
    }

    protected function modifyDefault(Blueprint $blueprint, Fluent $column): ?string
    {
        if (! is_null($column->default)) {
            return ' DEFAULT ' . $this->getDefaultValue($column->default);
        }

        return null;
    }

    protected function modifyCollate(Blueprint $blueprint, Fluent $column): ?string
    {
        if (! is_null($column->collation)) {
            return " COLLATE '{$column->collation}'";
        }

        return null;
    }

    protected function modifyComment(Blueprint $blueprint, Fluent $column): ?string
    {
        if (! is_null($column->comment)) {
            return " COMMENT '" . str_replace("'", "''", $column->comment) . "'";
        }

        return null;
    }

    public function wrapTable($table): string
    {
        if ($table instanceof Blueprint) {
            $table = $table->getTable();
        }

        return '"' . str_replace('"', '""', $table) . '"';
    }

    protected function wrapValue($value): string
    {
        if ($value === '*') {
            return $value;
        }

        return '"' . str_replace('"', '""', (string) $value) . '"';
    }

    public function supportsSchemaTransactions(): bool
    {
        return false;
    }
}
