<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Auth;

use FoundryCo\Snowflake\Enums\AuthMethod;

interface TokenProvider
{
    /**
     * Get a valid access token.
     *
     * Implementations should handle caching and automatic refresh.
     */
    public function getToken(): string;

    /**
     * Get the authentication method used by this provider.
     */
    public function getAuthMethod(): AuthMethod;

    /**
     * Force a token refresh, invalidating any cached token.
     */
    public function refresh(): void;

    /**
     * Check if the current token is still valid.
     */
    public function isValid(): bool;
}
