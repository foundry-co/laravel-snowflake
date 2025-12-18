<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Casts;

use Illuminate\Contracts\Database\Eloquent\CastsAttributes;
use Illuminate\Database\Eloquent\Model;

/**
 * Cast for Snowflake VARIANT columns.
 *
 * Handles conversion between PHP arrays/objects and Snowflake's VARIANT type.
 *
 * Usage in model:
 * ```php
 * protected $casts = [
 *     'metadata' => VariantCast::class,
 * ];
 * ```
 */
class VariantCast implements CastsAttributes
{
    /**
     * Cast the given value from the database.
     *
     * @param  array<string, mixed>  $attributes
     */
    public function get(Model $model, string $key, mixed $value, array $attributes): mixed
    {
        if ($value === null) {
            return null;
        }

        // If already decoded (by TypeConverter), return as-is
        if (is_array($value) || is_object($value)) {
            return $value;
        }

        // Otherwise, decode from JSON string
        $decoded = json_decode((string) $value, true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            return $value;
        }

        return $decoded;
    }

    /**
     * Prepare the given value for storage.
     *
     * @param  array<string, mixed>  $attributes
     */
    public function set(Model $model, string $key, mixed $value, array $attributes): mixed
    {
        if ($value === null) {
            return null;
        }

        // Convert to JSON for storage
        if (is_array($value) || is_object($value)) {
            return json_encode($value);
        }

        return $value;
    }
}
