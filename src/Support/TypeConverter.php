<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Support;

use Carbon\Carbon;
use Carbon\CarbonInterface;

/**
 * Converts Snowflake JSON response values to appropriate PHP types.
 *
 * Snowflake REST API returns all values as JSON strings. This class handles
 * the conversion to native PHP types based on column metadata.
 */
final class TypeConverter
{
    /**
     * Cast a value from Snowflake's JSON string format to the appropriate PHP type.
     */
    public function cast(mixed $value, string $type, array $metadata = []): mixed
    {
        if ($value === null) {
            return null;
        }

        $type = strtoupper($type);

        return match ($type) {
            // Integer types
            'INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'BYTEINT' => $this->castInteger($value),

            // Fixed-point numbers
            'FIXED', 'NUMBER', 'DECIMAL', 'NUMERIC' => $this->castNumber($value, $metadata),

            // Floating-point numbers
            'FLOAT', 'FLOAT4', 'FLOAT8', 'DOUBLE', 'DOUBLE PRECISION', 'REAL' => $this->castFloat($value),

            // Boolean
            'BOOLEAN' => $this->castBoolean($value),

            // String types
            'TEXT', 'VARCHAR', 'STRING', 'CHAR', 'CHARACTER' => (string) $value,

            // Date and time types
            'DATE' => $this->castDate($value),
            'TIME' => $this->castTime($value),
            'TIMESTAMP_NTZ', 'DATETIME' => $this->castTimestampNtz($value),
            'TIMESTAMP_LTZ' => $this->castTimestampLtz($value),
            'TIMESTAMP_TZ' => $this->castTimestampTz($value),

            // Binary types
            'BINARY', 'VARBINARY' => $this->castBinary($value),

            // Semi-structured types
            'VARIANT', 'OBJECT', 'ARRAY' => $this->castSemiStructured($value),

            // Geospatial types
            'GEOGRAPHY', 'GEOMETRY' => $this->castGeospatial($value),

            // Unknown type - return as-is
            default => $value,
        };
    }

    /**
     * Cast to integer, handling large values.
     */
    private function castInteger(string $value): int|string
    {
        // For values that exceed PHP_INT_MAX, keep as string
        if (bccomp($value, (string) PHP_INT_MAX) > 0 || bccomp($value, (string) PHP_INT_MIN) < 0) {
            return $value;
        }

        return (int) $value;
    }

    /**
     * Cast NUMBER type based on scale.
     */
    private function castNumber(string $value, array $metadata): int|float|string
    {
        $scale = $metadata['scale'] ?? 0;

        // If no decimal places, treat as integer
        if ($scale === 0) {
            return $this->castInteger($value);
        }

        // For decimals, return as float
        // Note: For high-precision applications, you may want to keep as string
        return (float) $value;
    }

    /**
     * Cast to float.
     */
    private function castFloat(string $value): float
    {
        return (float) $value;
    }

    /**
     * Cast to boolean.
     */
    private function castBoolean(string $value): bool
    {
        return strtolower($value) === 'true' || $value === '1';
    }

    /**
     * Cast DATE from epoch days.
     *
     * Snowflake returns DATE as the number of days since Unix epoch (1970-01-01).
     */
    private function castDate(string $value): CarbonInterface
    {
        $days = (int) $value;

        return Carbon::createFromTimestamp($days * 86400, 'UTC')->startOfDay();
    }

    /**
     * Cast TIME from fractional seconds since midnight.
     */
    private function castTime(string $value): string
    {
        $seconds = (float) $value;

        $hours = (int) floor($seconds / 3600);
        $minutes = (int) floor(($seconds % 3600) / 60);
        $secs = fmod($seconds, 60);

        return sprintf('%02d:%02d:%012.9f', $hours, $minutes, $secs);
    }

    /**
     * Cast TIMESTAMP_NTZ from epoch seconds.nanoseconds.
     *
     * TIMESTAMP_NTZ (No Time Zone) represents a timestamp without timezone.
     */
    private function castTimestampNtz(string $value): CarbonInterface
    {
        [$seconds, $nanoseconds] = $this->parseTimestamp($value);
        $microseconds = (int) floor($nanoseconds / 1000);

        return Carbon::createFromTimestamp($seconds, 'UTC')
            ->addMicroseconds($microseconds);
    }

    /**
     * Cast TIMESTAMP_LTZ from epoch seconds.nanoseconds.
     *
     * TIMESTAMP_LTZ (Local Time Zone) represents a timestamp in local timezone.
     */
    private function castTimestampLtz(string $value): CarbonInterface
    {
        [$seconds, $nanoseconds] = $this->parseTimestamp($value);
        $microseconds = (int) floor($nanoseconds / 1000);

        return Carbon::createFromTimestamp($seconds)
            ->addMicroseconds($microseconds);
    }

    /**
     * Cast TIMESTAMP_TZ from "seconds.nanoseconds offset_minutes" format.
     *
     * TIMESTAMP_TZ includes timezone offset information.
     */
    private function castTimestampTz(string $value): CarbonInterface
    {
        $parts = explode(' ', $value);
        [$seconds, $nanoseconds] = $this->parseTimestamp($parts[0]);
        $microseconds = (int) floor($nanoseconds / 1000);

        $timestamp = Carbon::createFromTimestamp($seconds, 'UTC')
            ->addMicroseconds($microseconds);

        // Apply timezone offset if present
        if (isset($parts[1])) {
            $offsetMinutes = (int) $parts[1];
            $timezone = $this->offsetMinutesToTimezone($offsetMinutes);
            $timestamp = $timestamp->setTimezone($timezone);
        }

        return $timestamp;
    }

    /**
     * Parse timestamp into seconds and nanoseconds.
     *
     * @return array{int, int} [seconds, nanoseconds]
     */
    private function parseTimestamp(string $value): array
    {
        if (str_contains($value, '.')) {
            $parts = explode('.', $value);
            $seconds = (int) $parts[0];
            $nanoStr = str_pad($parts[1] ?? '0', 9, '0', STR_PAD_RIGHT);
            $nanoseconds = (int) substr($nanoStr, 0, 9);

            return [$seconds, $nanoseconds];
        }

        return [(int) $value, 0];
    }

    /**
     * Convert offset minutes to timezone string.
     */
    private function offsetMinutesToTimezone(int $minutes): string
    {
        $hours = abs((int) ($minutes / 60));
        $mins = abs($minutes % 60);
        $sign = $minutes >= 0 ? '+' : '-';

        return sprintf('%s%02d:%02d', $sign, $hours, $mins);
    }

    /**
     * Cast BINARY from hex string.
     */
    private function castBinary(string $value): string
    {
        $decoded = hex2bin($value);

        return $decoded !== false ? $decoded : $value;
    }

    /**
     * Cast semi-structured types (VARIANT, OBJECT, ARRAY) from JSON.
     */
    private function castSemiStructured(string $value): mixed
    {
        $decoded = json_decode($value, true);

        // If JSON decode fails, return the original string
        if (json_last_error() !== JSON_ERROR_NONE) {
            return $value;
        }

        return $decoded;
    }

    /**
     * Cast geospatial types from GeoJSON or WKT.
     */
    private function castGeospatial(string $value): mixed
    {
        // Try to decode as GeoJSON
        $decoded = json_decode($value, true);

        if (json_last_error() === JSON_ERROR_NONE) {
            return $decoded;
        }

        // Return as-is (might be WKT format)
        return $value;
    }

    /**
     * Convert a PHP value to Snowflake SQL literal for binding.
     */
    public function toSqlLiteral(mixed $value): string
    {
        if ($value === null) {
            return 'NULL';
        }

        if (is_bool($value)) {
            return $value ? 'TRUE' : 'FALSE';
        }

        if (is_int($value) || is_float($value)) {
            return (string) $value;
        }

        if ($value instanceof \DateTimeInterface) {
            return "'" . $value->format('Y-m-d H:i:s.u') . "'";
        }

        if (is_array($value) || is_object($value)) {
            return "PARSE_JSON('" . str_replace("'", "''", json_encode($value)) . "')";
        }

        // String - escape single quotes by doubling them
        return "'" . str_replace("'", "''", (string) $value) . "'";
    }
}
