<?php

declare(strict_types=1);

use FoundryCo\Snowflake\Schema\Grammars\SnowflakeSchemaGrammar;
use FoundryCo\Snowflake\Schema\SnowflakeBlueprint;
use Illuminate\Database\Connection;
use Illuminate\Support\Fluent;

beforeEach(function () {
    $grammar = null;
    $connection = Mockery::mock(Connection::class);
    $connection->shouldReceive('getTablePrefix')->andReturn('');
    $connection->shouldReceive('getSchemaGrammar')->andReturnUsing(function () use (&$grammar) {
        return $grammar;
    });

    $grammar = new SnowflakeSchemaGrammar($connection);
    $this->connection = $connection;
    $this->grammar = $grammar;
});

describe('column types', function () {
    it('compiles variant type', function () {
        $column = new Fluent(['type' => 'variant', 'name' => 'data']);

        $method = new ReflectionMethod($this->grammar, 'typeVariant');
        $method->setAccessible(true);

        expect($method->invoke($this->grammar, $column))->toBe('VARIANT');
    });

    it('compiles object type', function () {
        $column = new Fluent(['type' => 'object', 'name' => 'metadata']);

        $method = new ReflectionMethod($this->grammar, 'typeObject');
        $method->setAccessible(true);

        expect($method->invoke($this->grammar, $column))->toBe('OBJECT');
    });

    it('compiles array type', function () {
        $column = new Fluent(['type' => 'array', 'name' => 'tags']);

        $method = new ReflectionMethod($this->grammar, 'typeArray');
        $method->setAccessible(true);

        expect($method->invoke($this->grammar, $column))->toBe('ARRAY');
    });

    it('compiles geography type', function () {
        $column = new Fluent(['type' => 'geography', 'name' => 'location']);

        $method = new ReflectionMethod($this->grammar, 'typeGeography');
        $method->setAccessible(true);

        expect($method->invoke($this->grammar, $column))->toBe('GEOGRAPHY');
    });

    it('compiles geometry type', function () {
        $column = new Fluent(['type' => 'geometry', 'name' => 'shape']);

        $method = new ReflectionMethod($this->grammar, 'typeGeometry');
        $method->setAccessible(true);

        expect($method->invoke($this->grammar, $column))->toBe('GEOMETRY');
    });
});

describe('timestamp types', function () {
    it('compiles timestamp_ntz', function () {
        $column = new Fluent(['type' => 'timestampNtz']);

        $method = new ReflectionMethod($this->grammar, 'typeTimestampNtz');
        $method->setAccessible(true);

        expect($method->invoke($this->grammar, $column))->toBe('TIMESTAMP_NTZ');
    });

    it('compiles timestamp_ltz', function () {
        $column = new Fluent(['type' => 'timestampLtz']);

        $method = new ReflectionMethod($this->grammar, 'typeTimestampLtz');
        $method->setAccessible(true);

        expect($method->invoke($this->grammar, $column))->toBe('TIMESTAMP_LTZ');
    });

    it('compiles timestamp_tz', function () {
        $column = new Fluent(['type' => 'timestampTz']);

        $method = new ReflectionMethod($this->grammar, 'typeTimestampTz');
        $method->setAccessible(true);

        expect($method->invoke($this->grammar, $column))->toBe('TIMESTAMP_TZ');
    });
});

describe('identity column', function () {
    it('compiles identity type with defaults', function () {
        $column = new Fluent(['type' => 'identity', 'start' => 1, 'increment' => 1]);

        $method = new ReflectionMethod($this->grammar, 'typeIdentity');
        $method->setAccessible(true);

        expect($method->invoke($this->grammar, $column))->toBe('INTEGER IDENTITY(1, 1)');
    });

    it('compiles identity type with custom start and increment', function () {
        $column = new Fluent(['type' => 'identity', 'start' => 100, 'increment' => 10]);

        $method = new ReflectionMethod($this->grammar, 'typeIdentity');
        $method->setAccessible(true);

        expect($method->invoke($this->grammar, $column))->toBe('INTEGER IDENTITY(100, 10)');
    });
});

describe('number type', function () {
    it('compiles number with precision and scale', function () {
        $column = new Fluent(['type' => 'number', 'precision' => 10, 'scale' => 2]);

        $method = new ReflectionMethod($this->grammar, 'typeNumber');
        $method->setAccessible(true);

        expect($method->invoke($this->grammar, $column))->toBe('NUMBER(10, 2)');
    });
});

describe('ulid type', function () {
    it('compiles ulid as char(26)', function () {
        $column = new Fluent(['type' => 'ulid']);

        $method = new ReflectionMethod($this->grammar, 'typeUlid');
        $method->setAccessible(true);

        expect($method->invoke($this->grammar, $column))->toBe('CHAR(26)');
    });
});

describe('json type', function () {
    it('compiles json as variant', function () {
        $column = new Fluent(['type' => 'json']);

        $method = new ReflectionMethod($this->grammar, 'typeJson');
        $method->setAccessible(true);

        expect($method->invoke($this->grammar, $column))->toBe('VARIANT');
    });
});

describe('clustering key', function () {
    it('compiles cluster by command', function () {
        $blueprint = new SnowflakeBlueprint($this->connection, 'users');
        $command = new Fluent(['columns' => ['created_at', 'id']]);

        $sql = $this->grammar->compileClusterBy($blueprint, $command);

        expect($sql)->toBe('ALTER TABLE "users" CLUSTER BY ("created_at", "id")');
    });
});

describe('data retention', function () {
    it('compiles data retention command', function () {
        $blueprint = new SnowflakeBlueprint($this->connection, 'users');
        $command = new Fluent(['days' => 30]);

        $sql = $this->grammar->compileDataRetention($blueprint, $command);

        expect($sql)->toBe('ALTER TABLE "users" SET DATA_RETENTION_TIME_IN_DAYS = 30');
    });
});

describe('sequence', function () {
    it('compiles sequence creation', function () {
        $blueprint = new SnowflakeBlueprint($this->connection, 'users');
        $command = new Fluent(['name' => 'users_id_seq', 'start' => 1, 'increment' => 1]);

        $sql = $this->grammar->compileSequence($blueprint, $command);

        expect($sql)->toBe('CREATE SEQUENCE IF NOT EXISTS "users_id_seq" START WITH 1 INCREMENT BY 1');
    });
});

describe('schema transactions', function () {
    it('does not support schema transactions', function () {
        expect($this->grammar->supportsSchemaTransactions())->toBeFalse();
    });
});
