<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Client\Exceptions;

/**
 * Exception thrown when a Snowflake query fails.
 */
class QueryException extends SnowflakeException
{
    public function __construct(
        string $message,
        protected string $sql,
        protected array $bindings = [],
        int $code = 0,
        ?\Exception $previous = null,
        ?string $sqlState = null,
        ?string $statementHandle = null,
    ) {
        parent::__construct($message, $code, $previous, $sqlState, $statementHandle);
    }

    /**
     * Create from a Snowflake API error response.
     */
    public static function fromApiResponse(array $response, string $sql, array $bindings = []): self
    {
        $message = $response['message'] ?? 'Query execution failed';
        $code = isset($response['code']) ? (int) $response['code'] : 0;
        $sqlState = $response['sqlState'] ?? null;
        $statementHandle = $response['statementHandle'] ?? null;

        return new self(
            message: $message,
            sql: $sql,
            bindings: $bindings,
            code: $code,
            sqlState: $sqlState,
            statementHandle: $statementHandle,
        );
    }

    /**
     * Get the SQL query that caused the error.
     */
    public function getSql(): string
    {
        return $this->sql;
    }

    /**
     * Get the bindings used with the query.
     */
    public function getBindings(): array
    {
        return $this->bindings;
    }

    /**
     * Get a formatted error message including the SQL.
     */
    public function getFormattedMessage(): string
    {
        $message = $this->getMessage();

        if ($this->sqlState) {
            $message .= " [SQLSTATE: {$this->sqlState}]";
        }

        $message .= "\n\nSQL: {$this->sql}";

        if (! empty($this->bindings)) {
            $message .= "\n\nBindings: " . json_encode($this->bindings);
        }

        return $message;
    }
}
