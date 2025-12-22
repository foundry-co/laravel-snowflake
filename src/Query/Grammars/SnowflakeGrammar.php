<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Query\Grammars;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar;

class SnowflakeGrammar extends Grammar
{
    protected $operators = [
        '=', '<', '>', '<=', '>=', '<>', '!=',
        'like', 'not like',
        'ilike', 'not ilike',
        'rlike', 'not rlike',
        '&', '|', '<<', '>>',
    ];

    public function compileSelect(Builder $query): string
    {
        return parent::compileSelect($query);
    }

    public function compileInsert(Builder $query, array $values): string
    {
        return parent::compileInsert($query, $values);
    }

    public function compileInsertGetId(Builder $query, $values, $sequence): string
    {
        return $this->compileInsert($query, $values);
    }

    public function compileInsertOrIgnore(Builder $query, array $values): string
    {
        return $this->compileInsert($query, $values);
    }

    public function compileUpsert(Builder $query, array $values, array $uniqueBy, array $update): string
    {
        $table = $this->wrapTable($query->from);
        $columns = array_keys(reset($values) ?: []);

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

        $onConditions = [];
        foreach ($uniqueBy as $col) {
            $wrapped = $this->wrap($col);
            $onConditions[] = "target.{$wrapped} = source.{$wrapped}";
        }
        $onClause = implode(' AND ', $onConditions);

        $updateCols = [];
        foreach ($update as $col) {
            $wrapped = $this->wrap($col);
            $updateCols[] = "{$wrapped} = source.{$wrapped}";
        }
        $updateClause = implode(', ', $updateCols);

        $sourceColumns = array_map(fn ($col) => "source.{$this->wrap($col)}", $columns);
        $sourceColumnList = implode(', ', $sourceColumns);

        return "MERGE INTO {$table} AS target " .
            "USING (SELECT * FROM VALUES " . implode(', ', $sourceRows) . " AS temp({$columnList})) AS source " .
            "ON {$onClause} " .
            "WHEN MATCHED THEN UPDATE SET {$updateClause} " .
            "WHEN NOT MATCHED THEN INSERT ({$columnList}) VALUES ({$sourceColumnList})";
    }

    public function compileTruncate(Builder $query): array
    {
        return ['TRUNCATE TABLE ' . $this->wrapTable($query->from) => []];
    }

    protected function compileLock(Builder $query, $value): string
    {
        return '';
    }

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

    protected function wrapValue($value): string
    {
        if ($value === '*') {
            return $value;
        }

        return '"' . str_replace('"', '""', (string) $value) . '"';
    }

    protected function whereDate(Builder $query, $where): string
    {
        $value = $this->parameter($where['value']);

        return $this->wrap($where['column']) . '::DATE ' . $where['operator'] . ' ' . $value;
    }

    protected function whereTime(Builder $query, $where): string
    {
        $value = $this->parameter($where['value']);

        return $this->wrap($where['column']) . '::TIME ' . $where['operator'] . ' ' . $value;
    }

    protected function whereYear(Builder $query, $where): string
    {
        $value = $this->parameter($where['value']);

        return 'YEAR(' . $this->wrap($where['column']) . ') ' . $where['operator'] . ' ' . $value;
    }

    protected function whereMonth(Builder $query, $where): string
    {
        $value = $this->parameter($where['value']);

        return 'MONTH(' . $this->wrap($where['column']) . ') ' . $where['operator'] . ' ' . $value;
    }

    protected function whereDay(Builder $query, $where): string
    {
        $value = $this->parameter($where['value']);

        return 'DAY(' . $this->wrap($where['column']) . ') ' . $where['operator'] . ' ' . $value;
    }

    protected function whereJsonContains(Builder $query, $where): string
    {
        $column = $this->wrap($where['column']);
        $value = $this->parameter($where['value']);

        return "ARRAY_CONTAINS({$value}::VARIANT, {$column})";
    }

    protected function whereJsonLength(Builder $query, $where): string
    {
        $column = $this->wrap($where['column']);
        $value = $this->parameter($where['value']);

        return "ARRAY_SIZE({$column}) {$where['operator']} {$value}";
    }

    protected function wrapJsonSelector($value): string
    {
        $parts = explode('->', (string) $value);
        $column = array_shift($parts);
        $wrapped = $this->wrap($column);

        foreach ($parts as $part) {
            $part = trim($part, "'\"");

            if (is_numeric($part)) {
                $wrapped .= "[{$part}]";
            } else {
                $wrapped .= ":{$part}";
            }
        }

        return $wrapped;
    }

    protected function compileLimit(Builder $query, $limit): string
    {
        return 'LIMIT ' . (int) $limit;
    }

    protected function compileOffset(Builder $query, $offset): string
    {
        return 'OFFSET ' . (int) $offset;
    }

    public function compileRandom($seed): string
    {
        return 'RANDOM()';
    }

    public function getBitwiseOperators(): array
    {
        return ['&', '|', '<<', '>>'];
    }

    public function compileExists(Builder $query): string
    {
        return 'SELECT EXISTS(' . $this->compileSelect($query) . ') AS "exists"';
    }

    public function compileTableExists(): string
    {
        return "SELECT COUNT(*) AS \"exists\" FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
    }

    public function compileColumnListing(): string
    {
        return 'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION';
    }

    public function supportsSavepoints(): bool
    {
        return false;
    }
}
