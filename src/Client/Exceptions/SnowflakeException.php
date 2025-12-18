<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Client\Exceptions;

use Exception;

/**
 * Base exception for all Snowflake-related errors.
 */
class SnowflakeException extends Exception
{
    public function __construct(
        string $message = '',
        int $code = 0,
        ?Exception $previous = null,
        protected ?string $sqlState = null,
        protected ?string $statementHandle = null,
    ) {
        parent::__construct($message, $code, $previous);
    }

    /**
     * Get the SQL state code from Snowflake.
     */
    public function getSqlState(): ?string
    {
        return $this->sqlState;
    }

    /**
     * Get the statement handle for debugging.
     */
    public function getStatementHandle(): ?string
    {
        return $this->statementHandle;
    }
}
