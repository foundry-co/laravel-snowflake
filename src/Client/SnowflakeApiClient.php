<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Client;

use Closure;
use FoundryCo\Snowflake\Auth\JwtTokenProvider;
use FoundryCo\Snowflake\Auth\OAuthTokenProvider;
use FoundryCo\Snowflake\Auth\TokenProvider;
use FoundryCo\Snowflake\Client\Exceptions\AuthenticationException;
use FoundryCo\Snowflake\Client\Exceptions\QueryException;
use FoundryCo\Snowflake\Client\Exceptions\SnowflakeException;
use FoundryCo\Snowflake\Enums\AuthMethod;
use FoundryCo\Snowflake\Support\TypeConverter;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Str;

/**
 * Snowflake REST API client using Laravel's HTTP facade.
 */
final class SnowflakeApiClient
{
    private readonly string $baseUrl;
    private readonly TokenProvider $tokenProvider;
    private readonly TypeConverter $typeConverter;

    public function __construct(
        private readonly array $config,
    ) {
        $this->baseUrl = $this->buildBaseUrl();
        $this->tokenProvider = $this->createTokenProvider();
        $this->typeConverter = new TypeConverter;
    }

    /**
     * Execute a SQL statement.
     */
    public function execute(string $sql, array $bindings = [], array $context = []): SnowflakeResult
    {
        $requestId = (string) Str::uuid();
        $interpolatedSql = $this->interpolateBindings($sql, $bindings);

        $payload = [
            'statement' => $interpolatedSql,
            'timeout' => $this->config['timeout'] ?? 0,
            'database' => $context['database'] ?? $this->config['database'],
            'schema' => $context['schema'] ?? $this->config['schema'] ?? 'PUBLIC',
            'warehouse' => $context['warehouse'] ?? $this->config['warehouse'],
        ];

        if (! empty($context['role'] ?? $this->config['role'])) {
            $payload['role'] = $context['role'] ?? $this->config['role'];
        }

        // Add session parameters if configured
        if (! empty($this->config['session_parameters'])) {
            $payload['parameters'] = $this->config['session_parameters'];
        }

        try {
            $response = Http::withHeaders($this->getAuthHeaders())
                ->timeout(0) // No HTTP timeout - Snowflake handles query timeout
                ->post("{$this->baseUrl}/api/v2/statements?requestId={$requestId}", $payload);

            return $this->handleResponse($response, $sql, $bindings, $requestId);
        } catch (\Illuminate\Http\Client\ConnectionException $e) {
            throw new SnowflakeException("Failed to connect to Snowflake: {$e->getMessage()}", 0, $e);
        }
    }

    /**
     * Handle the API response.
     */
    private function handleResponse(
        \Illuminate\Http\Client\Response $response,
        string $sql,
        array $bindings,
        string $requestId,
    ): SnowflakeResult {
        $data = $response->json() ?? [];

        // Check for authentication errors
        if ($response->status() === 401 || $response->status() === 403) {
            throw AuthenticationException::invalidCredentials($data['message'] ?? 'Authentication failed');
        }

        // Check for query errors (422 = SQL execution error)
        if ($response->status() === 422) {
            throw QueryException::fromApiResponse($data, $sql, $bindings);
        }

        // Check for other errors
        if (! $response->successful() && $response->status() !== 202) {
            throw new SnowflakeException(
                $data['message'] ?? "Request failed with status {$response->status()}",
                $response->status(),
            );
        }

        // Handle async query (202 = in progress)
        if ($response->status() === 202 || isset($data['statementStatusUrl'])) {
            return $this->pollForCompletion($data['statementHandle'] ?? '', $sql, $bindings);
        }

        return new SnowflakeResult($data, $this->createPartitionFetcher());
    }

    /**
     * Poll for async query completion.
     */
    private function pollForCompletion(string $statementHandle, string $sql, array $bindings): SnowflakeResult
    {
        $interval = $this->config['async_polling_interval'] ?? 500;
        $maxAttempts = 7200; // 1 hour max with 500ms interval

        for ($attempt = 0; $attempt < $maxAttempts; $attempt++) {
            usleep($interval * 1000);

            $response = Http::withHeaders($this->getAuthHeaders())
                ->get("{$this->baseUrl}/api/v2/statements/{$statementHandle}");

            $data = $response->json() ?? [];

            // Check for errors
            if ($response->status() === 422) {
                throw QueryException::fromApiResponse($data, $sql, $bindings);
            }

            if (! $response->successful() && $response->status() !== 202) {
                throw new SnowflakeException(
                    $data['message'] ?? "Request failed with status {$response->status()}",
                    $response->status(),
                );
            }

            // Check if complete
            if ($this->isQueryComplete($data)) {
                return new SnowflakeResult($data, $this->createPartitionFetcher());
            }
        }

        // Try to cancel the query
        $this->cancelStatement($statementHandle);

        throw new SnowflakeException("Query timed out after polling for {$maxAttempts} attempts");
    }

    /**
     * Fetch a specific partition of results.
     */
    public function fetchPartition(string $statementHandle, int $partitionIndex): array
    {
        $response = Http::withHeaders($this->getAuthHeaders())
            ->get("{$this->baseUrl}/api/v2/statements/{$statementHandle}", [
                'partition' => $partitionIndex,
            ]);

        if (! $response->successful()) {
            throw new SnowflakeException("Failed to fetch partition {$partitionIndex}: {$response->status()}");
        }

        return $response->json('data') ?? [];
    }

    /**
     * Cancel a running statement.
     */
    public function cancelStatement(string $statementHandle): bool
    {
        try {
            $response = Http::withHeaders($this->getAuthHeaders())
                ->post("{$this->baseUrl}/api/v2/statements/{$statementHandle}/cancel");

            return $response->successful();
        } catch (\Exception) {
            return false;
        }
    }

    /**
     * Get authentication headers.
     */
    private function getAuthHeaders(): array
    {
        return [
            'Authorization' => "Bearer {$this->tokenProvider->getToken()}",
            'X-Snowflake-Authorization-Token-Type' => $this->tokenProvider->getAuthMethod()->tokenType(),
            'Content-Type' => 'application/json',
            'Accept' => 'application/json',
            'User-Agent' => 'FoundryCo-Laravel-Snowflake/1.0',
        ];
    }

    /**
     * Build the base URL for Snowflake API.
     */
    private function buildBaseUrl(): string
    {
        $account = $this->config['account'] ?? throw new SnowflakeException('Snowflake account is required');

        // Check if account already contains region (legacy format)
        if (str_contains($account, '.')) {
            return "https://{$account}.snowflakecomputing.com";
        }

        return "https://{$account}.snowflakecomputing.com";
    }

    /**
     * Create the appropriate token provider based on config.
     */
    private function createTokenProvider(): TokenProvider
    {
        $method = AuthMethod::fromString($this->config['auth']['method'] ?? 'jwt');

        return match ($method) {
            AuthMethod::Jwt => JwtTokenProvider::fromConfig($this->config),
            AuthMethod::OAuth => OAuthTokenProvider::fromConfig($this->config),
        };
    }

    /**
     * Interpolate bindings into the SQL query.
     *
     * Note: Snowflake REST API doesn't support prepared statements with placeholders,
     * so we must interpolate values directly into the SQL.
     */
    private function interpolateBindings(string $sql, array $bindings): string
    {
        if (empty($bindings)) {
            return $sql;
        }

        $index = 0;

        return preg_replace_callback('/\?/', function () use ($bindings, &$index) {
            $value = $bindings[$index++] ?? null;

            return $this->typeConverter->toSqlLiteral($value);
        }, $sql);
    }

    /**
     * Check if a query has completed.
     */
    private function isQueryComplete(array $data): bool
    {
        // Query is complete if we have data or explicit success status
        if (isset($data['data'])) {
            return true;
        }

        // Check for success code
        $code = $data['code'] ?? null;
        if ($code === '090001' || $code === 'success') {
            return true;
        }

        return false;
    }

    /**
     * Create a closure for fetching partitions.
     */
    private function createPartitionFetcher(): Closure
    {
        return fn (string $handle, int $partition) => $this->fetchPartition($handle, $partition);
    }

    /**
     * Get the token provider (for testing/debugging).
     */
    public function getTokenProvider(): TokenProvider
    {
        return $this->tokenProvider;
    }
}
