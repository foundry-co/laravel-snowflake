<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Auth;

use FoundryCo\Snowflake\Client\Exceptions\AuthenticationException;
use FoundryCo\Snowflake\Enums\AuthMethod;
use Illuminate\Support\Facades\Http;

final class OAuthTokenProvider implements TokenProvider
{
    private ?string $cachedToken = null;
    private ?int $tokenExpiry = null;
    private ?string $refreshToken;

    /**
     * Buffer time in seconds before expiry to refresh the token.
     */
    private const REFRESH_BUFFER_SECONDS = 300;

    public function __construct(
        private readonly string $tokenEndpoint,
        private readonly string $clientId,
        private readonly string $clientSecret,
        private readonly string $scope = 'session:role-any',
        ?string $refreshToken = null,
    ) {
        $this->refreshToken = $refreshToken;
    }

    /**
     * Create from configuration array.
     */
    public static function fromConfig(array $config): self
    {
        $oauth = $config['auth']['oauth'] ?? [];

        $tokenEndpoint = $oauth['token_endpoint'] ?? throw new AuthenticationException('OAuth token endpoint is required');
        $clientId = $oauth['client_id'] ?? throw new AuthenticationException('OAuth client ID is required');
        $clientSecret = $oauth['client_secret'] ?? throw new AuthenticationException('OAuth client secret is required');
        $scope = $oauth['scope'] ?? 'session:role-any';
        $refreshToken = $oauth['refresh_token'] ?? null;

        return new self($tokenEndpoint, $clientId, $clientSecret, $scope, $refreshToken);
    }

    public function getToken(): string
    {
        if ($this->shouldRefresh()) {
            $this->refresh();
        }

        return $this->cachedToken;
    }

    public function getAuthMethod(): AuthMethod
    {
        return AuthMethod::OAuth;
    }

    public function refresh(): void
    {
        if ($this->refreshToken !== null) {
            $this->refreshWithToken();
        } else {
            $this->authenticateWithClientCredentials();
        }
    }

    public function isValid(): bool
    {
        if ($this->cachedToken === null || $this->tokenExpiry === null) {
            return false;
        }

        return time() < ($this->tokenExpiry - self::REFRESH_BUFFER_SECONDS);
    }

    /**
     * Authenticate using client credentials grant.
     */
    private function authenticateWithClientCredentials(): void
    {
        $response = Http::asForm()
            ->post($this->tokenEndpoint, [
                'grant_type' => 'client_credentials',
                'client_id' => $this->clientId,
                'client_secret' => $this->clientSecret,
                'scope' => $this->scope,
            ]);

        $this->handleTokenResponse($response);
    }

    /**
     * Refresh using refresh token grant.
     */
    private function refreshWithToken(): void
    {
        $response = Http::asForm()
            ->post($this->tokenEndpoint, [
                'grant_type' => 'refresh_token',
                'client_id' => $this->clientId,
                'client_secret' => $this->clientSecret,
                'refresh_token' => $this->refreshToken,
            ]);

        $this->handleTokenResponse($response);
    }

    /**
     * Handle the OAuth token response.
     */
    private function handleTokenResponse(\Illuminate\Http\Client\Response $response): void
    {
        if (! $response->successful()) {
            $error = $response->json('error_description') ?? $response->json('error') ?? 'Unknown error';
            throw new AuthenticationException("OAuth authentication failed: {$error}");
        }

        $data = $response->json();

        $this->cachedToken = $data['access_token'] ?? throw new AuthenticationException('No access token in response');
        $this->tokenExpiry = time() + ($data['expires_in'] ?? 3600);

        // Update refresh token if a new one was provided
        if (isset($data['refresh_token'])) {
            $this->refreshToken = $data['refresh_token'];
        }
    }

    /**
     * Check if token needs refresh.
     */
    private function shouldRefresh(): bool
    {
        return ! $this->isValid();
    }
}
