<?php

declare(strict_types=1);

use FoundryCo\Snowflake\Auth\JwtTokenProvider;
use FoundryCo\Snowflake\Client\Exceptions\AuthenticationException;

describe('JwtTokenProvider', function () {
    beforeEach(function () {
        $this->provider = new JwtTokenProvider(
            account: 'test-account',
            user: 'test_user',
            privateKeyPath: __DIR__ . '/../../Fixtures/test_rsa_key.pem',
        );
    });

    it('returns keypair jwt token type', function () {
        expect($this->provider->getTokenType())->toBe('KEYPAIR_JWT');
    });

    it('generates a valid jwt token', function () {
        $token = $this->provider->getToken();

        expect($token)->toBeString();
        expect($token)->not->toBeEmpty();

        $parts = explode('.', $token);
        expect($parts)->toHaveCount(3);
    });

    it('caches the token', function () {
        $token1 = $this->provider->getToken();
        $token2 = $this->provider->getToken();

        expect($token1)->toBe($token2);
    });

    it('reports valid after token generation', function () {
        expect($this->provider->isValid())->toBeFalse();

        $this->provider->getToken();

        expect($this->provider->isValid())->toBeTrue();
    });

    it('refreshes token on demand', function () {
        $token1 = $this->provider->getToken();
        $this->provider->refresh();
        $token2 = $this->provider->getToken();

        expect($token2)->toBeString();
    });

    it('throws exception for missing private key', function () {
        $provider = new JwtTokenProvider(
            account: 'test-account',
            user: 'test_user',
            privateKeyPath: '/nonexistent/key.pem',
        );

        expect(fn () => $provider->getToken())
            ->toThrow(AuthenticationException::class, 'Private key file not found');
    });

    describe('fromConfig', function () {
        it('creates provider from config array', function () {
            $config = [
                'account' => 'my-account',
                'auth' => [
                    'jwt' => [
                        'user' => 'my_user',
                        'private_key_path' => __DIR__ . '/../../Fixtures/test_rsa_key.pem',
                    ],
                ],
            ];

            $provider = JwtTokenProvider::fromConfig($config);

            expect($provider)->toBeInstanceOf(JwtTokenProvider::class);
            expect($provider->getToken())->toBeString();
        });

        it('throws on missing account', function () {
            $config = [
                'auth' => [
                    'jwt' => [
                        'user' => 'my_user',
                        'private_key_path' => '/path/to/key.pem',
                    ],
                ],
            ];

            expect(fn () => JwtTokenProvider::fromConfig($config))
                ->toThrow(AuthenticationException::class, 'account is required');
        });

        it('throws on missing user', function () {
            $config = [
                'account' => 'my-account',
                'auth' => [
                    'jwt' => [
                        'private_key_path' => '/path/to/key.pem',
                    ],
                ],
            ];

            expect(fn () => JwtTokenProvider::fromConfig($config))
                ->toThrow(AuthenticationException::class, 'user is required');
        });
    });
});
