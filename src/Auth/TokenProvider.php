<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Auth;

interface TokenProvider
{
    public function getToken(): string;

    public function getTokenType(): string;

    public function refresh(): void;

    public function isValid(): bool;
}
