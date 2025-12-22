<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Client;

use Closure;
use FoundryCo\Snowflake\Auth\JwtTokenProvider;
use FoundryCo\Snowflake\Client\Exceptions\AuthenticationException;
use FoundryCo\Snowflake\Client\Exceptions\QueryException;
use FoundryCo\Snowflake\Client\Exceptions\SnowflakeException;
use FoundryCo\Snowflake\Support\TypeConverter;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Str;

final class SnowflakeApiClient
{
    private readonly string $baseUrl;
    private readonly JwtTokenProvider $tokenProvider;
    private readonly TypeConverter $typeConverter;

    public function __construct(
        private readonly array $config,
    ) {
        $this->baseUrl = $this->buildBaseUrl();
        $this->tokenProvider = JwtTokenProvider::fromConfig($config);
        $this->typeConverter = new TypeConverter;
    }

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

        try {
            $response = Http::withHeaders($this->getAuthHeaders())
                ->timeout(0)
                ->post("{$this->baseUrl}/api/v2/statements?requestId={$requestId}", $payload);

            return $this->handleResponse($response, $sql, $bindings, $requestId);
        } catch (\Illuminate\Http\Client\ConnectionException $e) {
            throw new SnowflakeException("Failed to connect to Snowflake: {$e->getMessage()}", 0, $e);
        }
    }

    private function handleResponse(
        \Illuminate\Http\Client\Response $response,
        string $sql,
        array $bindings,
        string $requestId,
    ): SnowflakeResult {
        $data = $response->json() ?? [];

        if ($response->status() === 401 || $response->status() === 403) {
            throw AuthenticationException::invalidCredentials($data['message'] ?? 'Authentication failed');
        }

        if ($response->status() === 422) {
            throw QueryException::fromApiResponse($data, $sql, $bindings);
        }

        if (! $response->successful() && $response->status() !== 202) {
            throw new SnowflakeException(
                $data['message'] ?? "Request failed with status {$response->status()}",
                $response->status(),
            );
        }

        if ($response->status() === 202 || isset($data['statementStatusUrl'])) {
            return $this->pollForCompletion($data['statementHandle'] ?? '', $sql, $bindings);
        }

        return new SnowflakeResult($data, $this->createPartitionFetcher());
    }

    private function pollForCompletion(string $statementHandle, string $sql, array $bindings): SnowflakeResult
    {
        $interval = $this->config['async_polling_interval'] ?? 500;
        $maxAttempts = 7200;

        for ($attempt = 0; $attempt < $maxAttempts; $attempt++) {
            usleep($interval * 1000);

            $response = Http::withHeaders($this->getAuthHeaders())
                ->get("{$this->baseUrl}/api/v2/statements/{$statementHandle}");

            $data = $response->json() ?? [];

            if ($response->status() === 422) {
                throw QueryException::fromApiResponse($data, $sql, $bindings);
            }

            if (! $response->successful() && $response->status() !== 202) {
                throw new SnowflakeException(
                    $data['message'] ?? "Request failed with status {$response->status()}",
                    $response->status(),
                );
            }

            if ($this->isQueryComplete($data)) {
                return new SnowflakeResult($data, $this->createPartitionFetcher());
            }
        }

        $this->cancelStatement($statementHandle);

        throw new SnowflakeException("Query timed out after polling for {$maxAttempts} attempts");
    }

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

    private function getAuthHeaders(): array
    {
        return [
            'Authorization' => "Bearer {$this->tokenProvider->getToken()}",
            'X-Snowflake-Authorization-Token-Type' => $this->tokenProvider->getTokenType(),
            'Content-Type' => 'application/json',
            'Accept' => 'application/json',
            'User-Agent' => 'FoundryCo-Laravel-Snowflake/1.0',
        ];
    }

    private function buildBaseUrl(): string
    {
        $account = $this->config['account'] ?? throw new SnowflakeException('Snowflake account is required');

        return "https://{$account}.snowflakecomputing.com";
    }

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

    private function isQueryComplete(array $data): bool
    {
        if (isset($data['data'])) {
            return true;
        }

        $code = $data['code'] ?? null;

        return $code === '090001' || $code === 'success';
    }

    private function createPartitionFetcher(): Closure
    {
        return fn (string $handle, int $partition) => $this->fetchPartition($handle, $partition);
    }

    public function getTokenProvider(): JwtTokenProvider
    {
        return $this->tokenProvider;
    }
}
