<?php

declare(strict_types=1);

namespace FoundryCo\Snowflake\Auth;

use Firebase\JWT\JWT;
use FoundryCo\Snowflake\Client\Exceptions\AuthenticationException;
use FoundryCo\Snowflake\Enums\AuthMethod;
use OpenSSLAsymmetricKey;

final class JwtTokenProvider implements TokenProvider
{
    private ?string $cachedToken = null;
    private ?int $tokenExpiry = null;

    /**
     * Buffer time in seconds before expiry to refresh the token.
     */
    private const REFRESH_BUFFER_SECONDS = 300;

    /**
     * Maximum token lifetime in seconds (Snowflake limit is 1 hour).
     */
    private const TOKEN_LIFETIME_SECONDS = 3600;

    public function __construct(
        private readonly string $account,
        private readonly string $user,
        private readonly ?string $privateKeyPath = null,
        private readonly ?string $privateKey = null,
        private readonly ?string $privateKeyPassphrase = null,
    ) {
        if ($this->privateKeyPath === null && $this->privateKey === null) {
            throw new AuthenticationException('Either private_key_path or private_key must be provided');
        }
    }

    /**
     * Create from configuration array.
     */
    public static function fromConfig(array $config): self
    {
        $account = $config['account'] ?? throw new AuthenticationException('Snowflake account is required');
        $user = $config['auth']['jwt']['user'] ?? throw new AuthenticationException('Snowflake user is required for JWT auth');

        $privateKeyPath = $config['auth']['jwt']['private_key_path'] ?? null;
        $privateKey = $config['auth']['jwt']['private_key'] ?? null;
        $passphrase = $config['auth']['jwt']['private_key_passphrase'] ?? null;

        if ($privateKeyPath === null && $privateKey === null) {
            throw new AuthenticationException('Either private_key_path or private_key is required for JWT auth');
        }

        return new self($account, $user, $privateKeyPath, $privateKey, $passphrase);
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
        return AuthMethod::Jwt;
    }

    public function refresh(): void
    {
        $privateKey = $this->loadPrivateKey();
        $publicKeyFingerprint = $this->getPublicKeyFingerprint($privateKey);

        // Normalize account identifier (replace dots with hyphens for org-account format)
        $accountIdentifier = strtoupper(str_replace('.', '-', $this->account));
        $userIdentifier = strtoupper($this->user);

        $now = time();
        $expiry = $now + self::TOKEN_LIFETIME_SECONDS;

        $payload = [
            'iss' => "{$accountIdentifier}.{$userIdentifier}.SHA256:{$publicKeyFingerprint}",
            'sub' => "{$accountIdentifier}.{$userIdentifier}",
            'iat' => $now,
            'exp' => $expiry,
        ];

        $this->cachedToken = JWT::encode($payload, $privateKey, 'RS256');
        $this->tokenExpiry = $expiry;
    }

    public function isValid(): bool
    {
        if ($this->cachedToken === null || $this->tokenExpiry === null) {
            return false;
        }

        return time() < ($this->tokenExpiry - self::REFRESH_BUFFER_SECONDS);
    }

    /**
     * Load the private key from content or file.
     */
    private function loadPrivateKey(): OpenSSLAsymmetricKey
    {
        // Use direct key content if provided (e.g., from 1Password)
        if ($this->privateKey !== null) {
            $keyContent = $this->privateKey;
        } else {
            // Fall back to reading from file
            if (! file_exists($this->privateKeyPath)) {
                throw new AuthenticationException("Private key file not found: {$this->privateKeyPath}");
            }

            $keyContent = file_get_contents($this->privateKeyPath);

            if ($keyContent === false) {
                throw new AuthenticationException("Failed to read private key file: {$this->privateKeyPath}");
            }
        }

        $privateKey = openssl_pkey_get_private($keyContent, $this->privateKeyPassphrase ?? '');

        if ($privateKey === false) {
            $error = openssl_error_string();
            throw new AuthenticationException("Failed to load private key: {$error}");
        }

        return $privateKey;
    }

    /**
     * Calculate the SHA256 fingerprint of the public key.
     */
    private function getPublicKeyFingerprint(OpenSSLAsymmetricKey $privateKey): string
    {
        $details = openssl_pkey_get_details($privateKey);

        if ($details === false) {
            throw new AuthenticationException('Failed to extract public key details');
        }

        $publicKeyPem = $details['key'];
        $publicKeyDer = $this->pemToDer($publicKeyPem);

        return base64_encode(hash('sha256', $publicKeyDer, true));
    }

    /**
     * Convert PEM format to DER format.
     */
    private function pemToDer(string $pem): string
    {
        $lines = explode("\n", $pem);
        $der = '';

        foreach ($lines as $line) {
            $line = trim($line);
            if ($line !== '' && ! str_starts_with($line, '-----')) {
                $der .= $line;
            }
        }

        return base64_decode($der);
    }

    /**
     * Check if token needs refresh.
     */
    private function shouldRefresh(): bool
    {
        return ! $this->isValid();
    }
}
