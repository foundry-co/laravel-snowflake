<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Eloquent\Concerns;

use Illuminate\Database\Eloquent\Concerns\HasUlids;
use Illuminate\Support\Str;

/**
 * Trait for Eloquent models using Snowflake.
 *
 * Combines ULID support with Snowflake-specific timestamp handling.
 * Use this trait on any model that connects to Snowflake.
 */
trait UsesSnowflake
{
    use HasUlids;

    /**
     * Initialize the trait.
     */
    public function initializeUsesSnowflake(): void
    {
        // Ensure the model knows it's not auto-incrementing
        $this->incrementing = false;
        $this->keyType = 'string';
    }

    /**
     * Generate a new ULID for the model.
     *
     * Uses lowercase for better compatibility with case-insensitive
     * Snowflake identifiers.
     */
    public function newUniqueId(): string
    {
        return strtolower((string) Str::ulid());
    }

    /**
     * Get the columns that should receive a unique identifier.
     */
    public function uniqueIds(): array
    {
        return [$this->getKeyName()];
    }

    /**
     * Get the auto-incrementing key type.
     */
    public function getKeyType(): string
    {
        return 'string';
    }

    /**
     * Get whether the primary key is incrementing.
     */
    public function getIncrementing(): bool
    {
        return false;
    }

    /**
     * Get the format for database stored dates.
     *
     * Snowflake TIMESTAMP_NTZ uses microsecond precision.
     */
    public function getDateFormat(): string
    {
        return 'Y-m-d H:i:s.u';
    }

    /**
     * Convert a DateTime to a storable string.
     */
    public function fromDateTime($value): ?string
    {
        return empty($value) ? null : $this->asDateTime($value)->format($this->getDateFormat());
    }

    /**
     * Get a fresh timestamp for the model.
     */
    public function freshTimestampString(): string
    {
        return $this->freshTimestamp()->format($this->getDateFormat());
    }
}
