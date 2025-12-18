<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Schema;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Schema\ColumnDefinition;

/**
 * Snowflake-specific blueprint with additional column types and features.
 */
class SnowflakeBlueprint extends Blueprint
{
    // =====================================
    // Primary Key Helpers
    // =====================================

    /**
     * Create the default primary key column.
     *
     * For Snowflake, we default to ULID (26-char string) which is:
     * - Time-sortable (good for clustering)
     * - Generated client-side (no sequence needed)
     * - Distributed-safe
     */
    public function id($column = 'id'): ColumnDefinition
    {
        return $this->ulidPrimary($column);
    }

    /**
     * Create a ULID column as the primary key.
     */
    public function ulidPrimary(string $column = 'id'): ColumnDefinition
    {
        return $this->char($column, 26)->primary();
    }

    /**
     * Create a UUID column as the primary key.
     */
    public function uuidPrimary(string $column = 'id'): ColumnDefinition
    {
        return $this->uuid($column)->primary();
    }

    /**
     * Create a ULID column (not primary key).
     */
    public function ulid($column = 'ulid', $length = 26): ColumnDefinition
    {
        return $this->addColumn('ulid', $column);
    }

    // =====================================
    // Snowflake Semi-Structured Types
    // =====================================

    /**
     * Create a VARIANT column for semi-structured data.
     *
     * VARIANT can hold any data type including nested structures.
     * Use for JSON, Avro, XML, or mixed-type data.
     */
    public function variant(string $column): ColumnDefinition
    {
        return $this->addColumn('variant', $column);
    }

    /**
     * Create an OBJECT column for key-value pairs.
     *
     * OBJECT is similar to JSON object - keys are strings, values are VARIANT.
     */
    public function object(string $column): ColumnDefinition
    {
        return $this->addColumn('object', $column);
    }

    /**
     * Create an ARRAY column.
     *
     * ARRAY contains ordered sequences of VARIANT values.
     */
    public function array(string $column): ColumnDefinition
    {
        return $this->addColumn('array', $column);
    }

    // =====================================
    // Snowflake Timestamp Types
    // =====================================

    /**
     * Create a TIMESTAMP_NTZ column (no timezone).
     *
     * Stores timestamp without timezone information.
     * This is the default timestamp type in Snowflake.
     */
    public function timestampNtz(string $column): ColumnDefinition
    {
        return $this->addColumn('timestampNtz', $column);
    }

    /**
     * Create a TIMESTAMP_LTZ column (local timezone).
     *
     * Stores timestamp in UTC and displays in session timezone.
     */
    public function timestampLtz(string $column): ColumnDefinition
    {
        return $this->addColumn('timestampLtz', $column);
    }

    /**
     * Create a TIMESTAMP_TZ column (with timezone).
     *
     * Stores timestamp with associated timezone offset.
     */
    public function timestampTz($column, $precision = null): ColumnDefinition
    {
        return $this->addColumn('timestampTz', $column);
    }

    // =====================================
    // Snowflake Numeric Types
    // =====================================

    /**
     * Create a NUMBER column with precision and scale.
     *
     * Snowflake's NUMBER type supports up to 38 digits of precision.
     */
    public function number(string $column, int $precision = 38, int $scale = 0): ColumnDefinition
    {
        return $this->addColumn('number', $column, compact('precision', 'scale'));
    }

    // =====================================
    // Snowflake Geospatial Types
    // =====================================

    /**
     * Create a GEOGRAPHY column.
     *
     * Stores geospatial data on a spherical earth model.
     */
    public function geography($column, $subtype = null, $srid = 4326): ColumnDefinition
    {
        return $this->addColumn('geography', $column);
    }

    /**
     * Create a GEOMETRY column.
     *
     * Stores geospatial data on a planar coordinate system.
     */
    public function geometry($column, $subtype = null, $srid = 0): ColumnDefinition
    {
        return $this->addColumn('geometry', $column);
    }

    // =====================================
    // Identity Columns (Auto-increment alternative)
    // =====================================

    /**
     * Create an identity column (auto-incrementing integer).
     *
     * Note: For most use cases, ULIDs are recommended instead.
     */
    public function identity(string $column = 'id', int $start = 1, int $increment = 1): ColumnDefinition
    {
        return $this->addColumn('identity', $column, compact('start', 'increment'));
    }

    // =====================================
    // Snowflake Table Properties
    // =====================================

    /**
     * Add a clustering key to the table.
     *
     * Clustering keys help optimize query performance by organizing
     * data in micro-partitions based on the specified columns.
     *
     * @param  array|string  $columns
     */
    public function clusterBy(array|string $columns): void
    {
        $columns = is_array($columns) ? $columns : func_get_args();

        $this->addCommand('clusterBy', compact('columns'));
    }

    /**
     * Set the data retention time for Time Travel.
     *
     * Data retention allows you to query historical data and
     * restore dropped objects.
     */
    public function dataRetentionDays(int $days): void
    {
        $this->addCommand('dataRetention', compact('days'));
    }

    /**
     * Create a sequence for generating unique values.
     *
     * Sequences are standalone database objects that generate
     * unique numeric values.
     */
    public function sequence(string $name, int $start = 1, int $increment = 1): void
    {
        $this->addCommand('sequence', compact('name', 'start', 'increment'));
    }

    // =====================================
    // Convenience Methods
    // =====================================

    /**
     * Create nullable timestamps (created_at and updated_at).
     *
     * Uses TIMESTAMP_NTZ for Snowflake compatibility.
     */
    public function timestamps($precision = null): void
    {
        $this->timestampNtz('created_at')->nullable();
        $this->timestampNtz('updated_at')->nullable();
    }

    /**
     * Create nullable timestamps with timezone.
     */
    public function timestampsTz($precision = null): void
    {
        $this->timestampTz('created_at')->nullable();
        $this->timestampTz('updated_at')->nullable();
    }

    /**
     * Create a soft delete column.
     *
     * Uses TIMESTAMP_NTZ for Snowflake compatibility.
     */
    public function softDeletes($column = 'deleted_at', $precision = null): ColumnDefinition
    {
        return $this->timestampNtz($column)->nullable();
    }

    /**
     * Create a soft delete column with timezone.
     */
    public function softDeletesTz($column = 'deleted_at', $precision = null): ColumnDefinition
    {
        return $this->timestampTz($column)->nullable();
    }

    /**
     * Add a foreign ULID column.
     */
    public function foreignUlid($column, $length = 26): ColumnDefinition
    {
        return $this->char($column, $length);
    }
}
