<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Client;

use Closure;

/**
 * Represents the result of a Snowflake query execution.
 */
final class SnowflakeResult
{
    private ?ResultSet $resultSet = null;

    public function __construct(
        private readonly array $response,
        private readonly Closure $partitionFetcher,
    ) {}

    /**
     * Get the statement handle.
     */
    public function getStatementHandle(): string
    {
        return $this->response['statementHandle'] ?? '';
    }

    /**
     * Get the total number of rows affected or returned.
     */
    public function getRowCount(): int
    {
        return $this->response['resultSetMetaData']['numRows'] ?? 0;
    }

    /**
     * Get the number of rows in the first partition.
     */
    public function getRowsReturnedInFirstPartition(): int
    {
        return count($this->response['data'] ?? []);
    }

    /**
     * Check if the query returned any rows.
     */
    public function hasRows(): bool
    {
        return $this->getRowCount() > 0;
    }

    /**
     * Get column metadata.
     */
    public function getColumnMeta(): array
    {
        return $this->response['resultSetMetaData']['rowType'] ?? [];
    }

    /**
     * Get partition information.
     */
    public function getPartitionInfo(): array
    {
        return $this->response['resultSetMetaData']['partitionInfo'] ?? [];
    }

    /**
     * Get the number of partitions.
     */
    public function getPartitionCount(): int
    {
        $partitionInfo = $this->getPartitionInfo();

        return empty($partitionInfo) ? 1 : count($partitionInfo);
    }

    /**
     * Get the result set for iterating over rows.
     */
    public function getResultSet(): ResultSet
    {
        if ($this->resultSet === null) {
            $this->resultSet = new ResultSet(
                initialData: $this->response['data'] ?? [],
                columnMeta: $this->getColumnMeta(),
                partitionInfo: $this->getPartitionInfo(),
                statementHandle: $this->getStatementHandle(),
                partitionFetcher: $this->partitionFetcher,
            );
        }

        return $this->resultSet;
    }

    /**
     * Get all rows as an array.
     *
     * @return array<int, object>
     */
    public function fetchAll(): array
    {
        return $this->getResultSet()->toArray();
    }

    /**
     * Get the first row or null if empty.
     */
    public function fetchOne(): ?object
    {
        return $this->getResultSet()->first();
    }

    /**
     * Get statistics about the query execution.
     */
    public function getStats(): array
    {
        return [
            'rowCount' => $this->getRowCount(),
            'partitionCount' => $this->getPartitionCount(),
            'statementHandle' => $this->getStatementHandle(),
        ];
    }

    /**
     * Check if this is a SELECT query result (has data).
     */
    public function isSelectResult(): bool
    {
        return isset($this->response['data']) || isset($this->response['resultSetMetaData']['rowType']);
    }

    /**
     * Get the raw response data.
     */
    public function getRawResponse(): array
    {
        return $this->response;
    }
}
