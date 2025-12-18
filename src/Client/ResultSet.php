<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Client;

use Closure;
use FoundryCo\Snowflake\Support\TypeConverter;
use Generator;
use IteratorAggregate;
use Traversable;

/**
 * Represents a result set from a Snowflake query with lazy partition loading.
 *
 * @implements IteratorAggregate<int, object>
 */
final class ResultSet implements IteratorAggregate
{
    private readonly TypeConverter $typeConverter;

    /**
     * @param array $initialData The first partition data
     * @param array $columnMeta Column metadata from resultSetMetaData.rowType
     * @param array $partitionInfo Partition information for fetching additional data
     * @param string $statementHandle The statement handle for fetching partitions
     * @param Closure $partitionFetcher Callback to fetch additional partitions
     */
    public function __construct(
        private readonly array $initialData,
        private readonly array $columnMeta,
        private readonly array $partitionInfo,
        private readonly string $statementHandle,
        private readonly Closure $partitionFetcher,
    ) {
        $this->typeConverter = new TypeConverter;
    }

    /**
     * Get the column metadata.
     */
    public function getColumns(): array
    {
        return $this->columnMeta;
    }

    /**
     * Get column names.
     *
     * @return array<string>
     */
    public function getColumnNames(): array
    {
        return array_map(fn (array $col) => $col['name'], $this->columnMeta);
    }

    /**
     * Get the total number of partitions.
     */
    public function getPartitionCount(): int
    {
        return max(1, count($this->partitionInfo));
    }

    /**
     * Iterate over all rows across all partitions.
     *
     * @return Traversable<int, object>
     */
    public function getIterator(): Traversable
    {
        return $this->rows();
    }

    /**
     * Get a generator that yields all rows across all partitions.
     *
     * This is memory-efficient for large result sets as it only loads
     * one partition at a time.
     *
     * @return Generator<int, object>
     */
    public function rows(): Generator
    {
        $rowIndex = 0;

        // Yield rows from the initial (first) partition
        foreach ($this->initialData as $row) {
            yield $rowIndex++ => $this->transformRow($row);
        }

        // Fetch and yield rows from additional partitions
        for ($i = 1; $i < $this->getPartitionCount(); $i++) {
            $partitionData = ($this->partitionFetcher)($this->statementHandle, $i);

            foreach ($partitionData as $row) {
                yield $rowIndex++ => $this->transformRow($row);
            }
        }
    }

    /**
     * Get all rows as an array.
     *
     * Warning: This loads all data into memory. For large result sets,
     * use the rows() generator instead.
     *
     * @return array<int, object>
     */
    public function toArray(): array
    {
        return iterator_to_array($this->rows(), false);
    }

    /**
     * Get the first row or null if empty.
     */
    public function first(): ?object
    {
        foreach ($this->rows() as $row) {
            return $row;
        }

        return null;
    }

    /**
     * Transform a raw row array into an object with proper types.
     */
    private function transformRow(array $row): object
    {
        $result = new \stdClass;

        foreach ($this->columnMeta as $index => $column) {
            $name = $column['name'];
            $type = $column['type'];
            $rawValue = $row[$index] ?? null;

            $result->{$name} = $this->typeConverter->cast($rawValue, $type, $column);
        }

        return $result;
    }
}
