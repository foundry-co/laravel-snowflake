<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Query\Processors;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Processors\Processor;

/**
 * Snowflake query result processor.
 */
class SnowflakeProcessor extends Processor
{
    /**
     * Process an "insert get ID" query.
     *
     * Since Snowflake doesn't support RETURNING and we use ULIDs,
     * the ID should already be set in the values.
     */
    public function processInsertGetId(Builder $query, $sql, $values, $sequence = null): string|int
    {
        $query->getConnection()->insert($sql, $values);

        // Return the ID from the values if present
        $idColumn = $sequence ?? 'id';

        if (is_array($values) && isset($values[$idColumn])) {
            return $values[$idColumn];
        }

        // Return empty string as we can't get last insert ID from Snowflake REST API
        return '';
    }

    /**
     * Process the results of a column listing query.
     */
    public function processColumnListing($results): array
    {
        return array_map(function ($result) {
            $result = (object) $result;

            return $result->COLUMN_NAME ?? $result->column_name ?? '';
        }, $results);
    }

    /**
     * Process the results of a tables query.
     *
     * @param  array  $results
     * @return array<int, array{name: string, schema: string|null, size: int|null, rows: int|null, comment: string|null}>
     */
    public function processTables($results): array
    {
        return array_map(function ($result) {
            $result = (object) $result;

            return [
                'name' => $result->TABLE_NAME ?? $result->name ?? '',
                'schema' => $result->TABLE_SCHEMA ?? $result->schema_name ?? null,
                'size' => isset($result->BYTES) ? (int) $result->BYTES : null,
                'rows' => isset($result->ROW_COUNT) ? (int) $result->ROW_COUNT : null,
                'comment' => $result->COMMENT ?? $result->comment ?? null,
            ];
        }, $results);
    }

    /**
     * Process the results of a views query.
     *
     * @param  array  $results
     * @return array<int, array{name: string, schema: string|null, definition: string|null}>
     */
    public function processViews($results): array
    {
        return array_map(function ($result) {
            $result = (object) $result;

            return [
                'name' => $result->TABLE_NAME ?? $result->name ?? '',
                'schema' => $result->TABLE_SCHEMA ?? $result->schema_name ?? null,
                'definition' => $result->VIEW_DEFINITION ?? $result->definition ?? null,
            ];
        }, $results);
    }

    /**
     * Process the results of a columns query.
     *
     * @param  array  $results
     * @return array<int, array{name: string, type_name: string, type: string, nullable: bool, default: string|null, auto_increment: bool, comment: string|null, generation: array|null}>
     */
    public function processColumns($results): array
    {
        return array_map(function ($result) {
            $result = (object) $result;

            $columnDefault = $result->COLUMN_DEFAULT ?? $result->column_default ?? null;

            return [
                'name' => $result->COLUMN_NAME ?? $result->column_name ?? '',
                'type_name' => $result->DATA_TYPE ?? $result->data_type ?? '',
                'type' => $this->normalizeColumnType($result),
                'nullable' => ($result->IS_NULLABLE ?? $result->is_nullable ?? 'YES') === 'YES',
                'default' => $columnDefault,
                'auto_increment' => $columnDefault !== null && str_contains((string) $columnDefault, 'IDENTITY'),
                'comment' => $result->COMMENT ?? $result->comment ?? null,
                'generation' => null, // Snowflake doesn't expose generation expressions the same way
            ];
        }, $results);
    }

    /**
     * Normalize column type information.
     */
    private function normalizeColumnType(object $result): string
    {
        $type = strtoupper($result->DATA_TYPE ?? $result->data_type ?? '');

        // Add precision/scale for numeric types
        if (in_array($type, ['NUMBER', 'DECIMAL', 'NUMERIC'])) {
            $precision = $result->NUMERIC_PRECISION ?? $result->numeric_precision ?? null;
            $scale = $result->NUMERIC_SCALE ?? $result->numeric_scale ?? null;

            if ($precision !== null) {
                $type .= "({$precision}" . ($scale !== null ? ",{$scale}" : '') . ')';
            }
        }

        // Add length for string types
        if (in_array($type, ['VARCHAR', 'CHAR', 'STRING', 'TEXT'])) {
            $length = $result->CHARACTER_MAXIMUM_LENGTH ?? $result->character_maximum_length ?? null;

            if ($length !== null) {
                $type .= "({$length})";
            }
        }

        return $type;
    }

    /**
     * Process the results of an indexes query.
     *
     * Note: Snowflake doesn't have traditional indexes - it uses clustering keys.
     *
     * @param  array  $results
     * @return array<int, array{name: string, columns: array, type: string, unique: bool, primary: bool}>
     */
    public function processIndexes($results): array
    {
        return array_map(function ($result) {
            $result = (object) $result;

            return [
                'name' => $result->INDEX_NAME ?? $result->name ?? '',
                'columns' => $this->parseColumnList($result->COLUMNS ?? $result->columns ?? ''),
                'type' => 'clustering', // Snowflake uses clustering instead of indexes
                'unique' => false,
                'primary' => false,
            ];
        }, $results);
    }

    /**
     * Process the results of a foreign keys query.
     *
     * @param  array  $results
     * @return array<int, array{name: string, columns: array, foreign_schema: string, foreign_table: string, foreign_columns: array, on_update: string, on_delete: string}>
     */
    public function processForeignKeys($results): array
    {
        return array_map(function ($result) {
            $result = (object) $result;

            return [
                'name' => $result->FK_NAME ?? $result->constraint_name ?? '',
                'columns' => [$result->FK_COLUMN_NAME ?? $result->fk_column_name ?? ''],
                'foreign_schema' => $result->PK_SCHEMA_NAME ?? $result->pk_schema_name ?? '',
                'foreign_table' => $result->PK_TABLE_NAME ?? $result->pk_table_name ?? '',
                'foreign_columns' => [$result->PK_COLUMN_NAME ?? $result->pk_column_name ?? ''],
                'on_update' => $result->UPDATE_RULE ?? $result->update_rule ?? 'NO ACTION',
                'on_delete' => $result->DELETE_RULE ?? $result->delete_rule ?? 'NO ACTION',
            ];
        }, $results);
    }

    /**
     * Parse a comma-separated column list.
     */
    private function parseColumnList(string $columns): array
    {
        if (empty($columns)) {
            return [];
        }

        return array_map('trim', explode(',', $columns));
    }
}
