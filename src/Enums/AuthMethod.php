<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Enums;

enum AuthMethod: string
{
    case Jwt = 'jwt';
    case OAuth = 'oauth';

    /**
     * Get the authorization token type header value for Snowflake API.
     */
    public function tokenType(): string
    {
        return match ($this) {
            self::Jwt => 'KEYPAIR_JWT',
            self::OAuth => 'OAUTH',
        };
    }

    /**
     * Create from string, with validation.
     */
    public static function fromString(string $method): self
    {
        return self::tryFrom(strtolower($method))
            ?? throw new \InvalidArgumentException("Unknown authentication method: {$method}. Use 'jwt' or 'oauth'.");
    }
}
