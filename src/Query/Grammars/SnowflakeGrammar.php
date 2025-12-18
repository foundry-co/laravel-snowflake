<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Query\Grammars;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar;

/**
 * Snowflake SQL query grammar.
 *
 * Handles the compilation of query builder calls to Snowflake SQL dialect.
 */
class SnowflakeGrammar extends Grammar
{
    /**
     * Snowflake-specific operators.
     *
     * @var string[]
     */
    protected $operators = [
        '=', '<', '>', '<=', '>=', '<>', '!=',
        'like', 'not like',
        'ilike', 'not ilike', // Case-insensitive LIKE
        'rlike', 'not rlike', // Regex LIKE
        '&', '|', '<<', '>>', // Bitwise
    ];

    /**
     * Compile a select statement.
     */
    public function compileSelect(Builder $query): string
    {
        return parent::compileSelect($query);
    }

    /**
     * Compile an insert statement into SQL.
     */
    public function compileInsert(Builder $query, array $values): string
    {
        return parent::compileInsert($query, $values);
    }

    /**
     * Compile an insert and get ID statement.
     *
     * Note: Snowflake doesn't have RETURNING for INSERT, but we use ULIDs
     * generated client-side so this isn't needed.
     */
    public function compileInsertGetId(Builder $query, $values, $sequence): string
    {
        return $this->compileInsert($query, $values);
    }

    /**
     * Compile an insert ignore statement.
     *
     * Snowflake doesn't have INSERT IGNORE - we need to use MERGE instead
     * or handle duplicates at the application level.
     */
    public function compileInsertOrIgnore(Builder $query, array $values): string
    {
        // For simple cases, just use regular INSERT
        // The caller should handle constraint violations
        return $this->compileInsert($query, $values);
    }

    /**
     * Compile an upsert statement using MERGE.
     */
    public function compileUpsert(Builder $query, array $values, array $uniqueBy, array $update): string
    {
        $table = $this->wrapTable($query->from);

        // Get column names from first row
        $columns = array_keys(reset($values) ?: []);

        // Build source values for MERGE
        $sourceRows = [];
        foreach ($values as $record) {
            $row = [];
            foreach ($columns as $column) {
                $row[] = $this->parameter($record[$column] ?? null);
            }
            $sourceRows[] = '(' . implode(', ', $row) . ')';
        }

        $wrappedColumns = array_map([$this, 'wrap'], $columns);
        $columnList = implode(', ', $wrappedColumns);

        // Build the ON condition
        $onConditions = [];
        foreach ($uniqueBy as $col) {
            $wrapped = $this->wrap($col);
            $onConditions[] = "target.{$wrapped} = source.{$wrapped}";
        }
        $onClause = implode(' AND ', $onConditions);

        // Build the UPDATE SET clause
        $updateCols = [];
        foreach ($update as $col) {
            $wrapped = $this->wrap($col);
            $updateCols[] = "{$wrapped} = source.{$wrapped}";
        }
        $updateClause = implode(', ', $updateCols);

        // Build source column references for INSERT
        $sourceColumns = array_map(fn ($col) => "source.{$this->wrap($col)}", $columns);
        $sourceColumnList = implode(', ', $sourceColumns);

        return "MERGE INTO {$table} AS target " .
            "USING (SELECT * FROM VALUES " . implode(', ', $sourceRows) . " AS temp({$columnList})) AS source " .
            "ON {$onClause} " .
            "WHEN MATCHED THEN UPDATE SET {$updateClause} " .
            "WHEN NOT MATCHED THEN INSERT ({$columnList}) VALUES ({$sourceColumnList})";
    }

    /**
     * Compile a truncate statement.
     */
    public function compileTruncate(Builder $query): array
    {
        return ['TRUNCATE TABLE ' . $this->wrapTable($query->from) => []];
    }

    /**
     * Compile the lock clause.
     *
     * Snowflake is append-only and doesn't support traditional row locking.
     */
    protected function compileLock(Builder $query, $value): string
    {
        return '';
    }

    /**
     * Wrap a value in keyword identifiers.
     *
     * Snowflake uses double quotes for identifiers.
     */
    public function wrap($value, $prefixAlias = false): string
    {
        if ($this->isExpression($value)) {
            return $this->getValue($value);
        }

        if (str_contains(strtolower((string) $value), ' as ')) {
            return $this->wrapAliasedValue($value, $prefixAlias);
        }

        return $this->wrapSegments(explode('.', (string) $value));
    }

    /**
     * Wrap a single value.
     *
     * Uses double quotes for Snowflake identifiers. Identifiers are
     * case-insensitive in Snowflake unless quoted.
     */
    protected function wrapValue($value): string
    {
        if ($value === '*') {
            return $value;
        }

        // Double quotes for identifiers, escape double quotes by doubling them
        return '"' . str_replace('"', '""', (string) $value) . '"';
    }

    /**
     * Compile a "where date" clause.
     */
    protected function whereDate(Builder $query, $where): string
    {
        $value = $this->parameter($where['value']);

        return $this->wrap($where['column']) . '::DATE ' . $where['operator'] . ' ' . $value;
    }

    /**
     * Compile a "where time" clause.
     */
    protected function whereTime(Builder $query, $where): string
    {
        $value = $this->parameter($where['value']);

        return $this->wrap($where['column']) . '::TIME ' . $where['operator'] . ' ' . $value;
    }

    /**
     * Compile a "where year" clause.
     */
    protected function whereYear(Builder $query, $where): string
    {
        $value = $this->parameter($where['value']);

        return 'YEAR(' . $this->wrap($where['column']) . ') ' . $where['operator'] . ' ' . $value;
    }

    /**
     * Compile a "where month" clause.
     */
    protected function whereMonth(Builder $query, $where): string
    {
        $value = $this->parameter($where['value']);

        return 'MONTH(' . $this->wrap($where['column']) . ') ' . $where['operator'] . ' ' . $value;
    }

    /**
     * Compile a "where day" clause.
     */
    protected function whereDay(Builder $query, $where): string
    {
        $value = $this->parameter($where['value']);

        return 'DAY(' . $this->wrap($where['column']) . ') ' . $where['operator'] . ' ' . $value;
    }

    /**
     * Compile a "where JSON contains" clause.
     *
     * Uses Snowflake's ARRAY_CONTAINS for array checks.
     */
    protected function whereJsonContains(Builder $query, $where): string
    {
        $column = $this->wrap($where['column']);
        $value = $this->parameter($where['value']);

        return "ARRAY_CONTAINS({$value}::VARIANT, {$column})";
    }

    /**
     * Compile a "where JSON length" clause.
     */
    protected function whereJsonLength(Builder $query, $where): string
    {
        $column = $this->wrap($where['column']);
        $value = $this->parameter($where['value']);

        return "ARRAY_SIZE({$column}) {$where['operator']} {$value}";
    }

    /**
     * Compile a JSON path for Snowflake.
     *
     * Snowflake uses :path or ['path'] notation for JSON access.
     */
    protected function wrapJsonSelector($value): string
    {
        // Split column and JSON path
        $parts = explode('->', (string) $value);
        $column = array_shift($parts);

        $wrapped = $this->wrap($column);

        foreach ($parts as $part) {
            // Remove surrounding quotes if present
            $part = trim($part, "'\"");

            if (is_numeric($part)) {
                $wrapped .= "[{$part}]";
            } else {
                $wrapped .= ":{$part}";
            }
        }

        return $wrapped;
    }

    /**
     * Compile a "LIMIT" clause.
     */
    protected function compileLimit(Builder $query, $limit): string
    {
        return 'LIMIT ' . (int) $limit;
    }

    /**
     * Compile an "OFFSET" clause.
     */
    protected function compileOffset(Builder $query, $offset): string
    {
        return 'OFFSET ' . (int) $offset;
    }

    /**
     * Compile a random statement.
     *
     * Snowflake uses RANDOM() function.
     */
    public function compileRandom($seed): string
    {
        return 'RANDOM()';
    }

    /**
     * Get the grammar's bitwise operators.
     */
    public function getBitwiseOperators(): array
    {
        return ['&', '|', '<<', '>>'];
    }

    /**
     * Compile an exists statement.
     */
    public function compileExists(Builder $query): string
    {
        return 'SELECT EXISTS(' . $this->compileSelect($query) . ') AS "exists"';
    }

    /**
     * Compile the SQL needed to retrieve all table names.
     */
    public function compileTableExists(): string
    {
        return "SELECT COUNT(*) AS \"exists\" FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
    }

    /**
     * Compile a query to get column listing.
     */
    public function compileColumnListing(): string
    {
        return 'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION';
    }

    /**
     * Determine if the grammar supports savepoints.
     */
    public function supportsSavepoints(): bool
    {
        return false; // Snowflake doesn't support savepoints
    }
}
