<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Casts;

use Carbon\Carbon;
use Carbon\CarbonInterface;
use Illuminate\Contracts\Database\Eloquent\CastsAttributes;
use Illuminate\Database\Eloquent\Model;

/**
 * Cast for Snowflake TIMESTAMP columns.
 *
 * Handles TIMESTAMP_NTZ, TIMESTAMP_LTZ, and TIMESTAMP_TZ types.
 *
 * Usage in model:
 * ```php
 * protected $casts = [
 *     'processed_at' => SnowflakeTimestamp::class,
 *     'event_time' => SnowflakeTimestamp::class.':ltz',
 *     'created_at_tz' => SnowflakeTimestamp::class.':tz',
 * ];
 * ```
 */
class SnowflakeTimestamp implements CastsAttributes
{
    /**
     * The timestamp type: 'ntz', 'ltz', or 'tz'.
     */
    protected string $type;

    /**
     * Create a new cast instance.
     */
    public function __construct(string $type = 'ntz')
    {
        $this->type = strtolower($type);
    }

    /**
     * Cast the given value from the database.
     *
     * @param  array<string, mixed>  $attributes
     */
    public function get(Model $model, string $key, mixed $value, array $attributes): ?CarbonInterface
    {
        if ($value === null) {
            return null;
        }

        // If already a Carbon instance, return as-is
        if ($value instanceof CarbonInterface) {
            return $value;
        }

        // Parse Snowflake timestamp format
        return $this->parseTimestamp((string) $value);
    }

    /**
     * Prepare the given value for storage.
     *
     * @param  array<string, mixed>  $attributes
     */
    public function set(Model $model, string $key, mixed $value, array $attributes): ?string
    {
        if ($value === null) {
            return null;
        }

        if ($value instanceof \DateTimeInterface) {
            return $value->format('Y-m-d H:i:s.u');
        }

        if (is_string($value)) {
            return Carbon::parse($value)->format('Y-m-d H:i:s.u');
        }

        return null;
    }

    /**
     * Parse a timestamp string from Snowflake.
     */
    protected function parseTimestamp(string $value): CarbonInterface
    {
        // Handle standard datetime format
        if (preg_match('/^\d{4}-\d{2}-\d{2}/', $value)) {
            return match ($this->type) {
                'ltz' => Carbon::parse($value),
                'tz' => Carbon::parse($value),
                default => Carbon::parse($value, 'UTC'),
            };
        }

        // Handle epoch seconds format (from REST API)
        if (preg_match('/^-?\d+(\.\d+)?( -?\d+)?$/', $value)) {
            return $this->parseEpochTimestamp($value);
        }

        // Fallback to standard parsing
        return Carbon::parse($value);
    }

    /**
     * Parse epoch-based timestamp format from REST API.
     */
    protected function parseEpochTimestamp(string $value): CarbonInterface
    {
        $parts = explode(' ', $value);
        $timestampParts = explode('.', $parts[0]);

        $seconds = (int) $timestampParts[0];
        $nanoseconds = isset($timestampParts[1]) ? (int) str_pad($timestampParts[1], 9, '0') : 0;
        $microseconds = (int) floor($nanoseconds / 1000);

        $timestamp = Carbon::createFromTimestamp($seconds, 'UTC')
            ->addMicroseconds($microseconds);

        // Handle timezone offset for TIMESTAMP_TZ
        if (isset($parts[1]) && $this->type === 'tz') {
            $offsetMinutes = (int) $parts[1];
            $hours = abs((int) ($offsetMinutes / 60));
            $mins = abs($offsetMinutes % 60);
            $sign = $offsetMinutes >= 0 ? '+' : '-';
            $timezone = sprintf('%s%02d:%02d', $sign, $hours, $mins);
            $timestamp = $timestamp->setTimezone($timezone);
        }

        return $timestamp;
    }
}
