<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Client\Exceptions;

/**
 * Exception thrown when authentication with Snowflake fails.
 */
class AuthenticationException extends SnowflakeException
{
    public static function invalidCredentials(string $message = 'Invalid credentials'): self
    {
        return new self($message, 401);
    }

    public static function tokenExpired(): self
    {
        return new self('Authentication token has expired', 401);
    }

    public static function configurationError(string $detail): self
    {
        return new self("Authentication configuration error: {$detail}", 500);
    }
}
