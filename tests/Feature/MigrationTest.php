<?php

declare(strict_types=1);

use FoundryCo\Snowflake\Schema\Grammars\SnowflakeSchemaGrammar;
use FoundryCo\Snowflake\Schema\SnowflakeBlueprint;
use Illuminate\Database\Connection;
use Illuminate\Support\Facades\Schema;

describe('Schema Blueprint', function () {
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

    it('creates table with ulid primary key by default', function () {
        $blueprint = new SnowflakeBlueprint($this->connection, 'users');
        $blueprint->id();
        $blueprint->string('name');
        $blueprint->timestamps();

        $columns = $blueprint->getColumns();

        expect($columns)->toHaveCount(4);
        expect($columns[0]->type)->toBe('char');
        expect($columns[0]->length)->toBe(26);
    });

    it('supports variant column', function () {
        $blueprint = new SnowflakeBlueprint($this->connection, 'events');
        $blueprint->id();
        $blueprint->variant('payload');

        $columns = $blueprint->getColumns();

        expect($columns[1]->type)->toBe('variant');
    });

    it('supports object column', function () {
        $blueprint = new SnowflakeBlueprint($this->connection, 'settings');
        $blueprint->id();
        $blueprint->object('config');

        $columns = $blueprint->getColumns();

        expect($columns[1]->type)->toBe('object');
    });

    it('supports array column', function () {
        $blueprint = new SnowflakeBlueprint($this->connection, 'posts');
        $blueprint->id();
        $blueprint->array('tags');

        $columns = $blueprint->getColumns();

        expect($columns[1]->type)->toBe('array');
    });

    it('supports geography column', function () {
        $blueprint = new SnowflakeBlueprint($this->connection, 'locations');
        $blueprint->id();
        $blueprint->geography('coordinates');

        $columns = $blueprint->getColumns();

        expect($columns[1]->type)->toBe('geography');
    });

    it('supports timestamp_ntz column', function () {
        $blueprint = new SnowflakeBlueprint($this->connection, 'events');
        $blueprint->id();
        $blueprint->timestampNtz('occurred_at');

        $columns = $blueprint->getColumns();

        expect($columns[1]->type)->toBe('timestampNtz');
    });

    it('supports timestamp_tz column', function () {
        $blueprint = new SnowflakeBlueprint($this->connection, 'events');
        $blueprint->id();
        $blueprint->timestampTz('scheduled_at');

        $columns = $blueprint->getColumns();

        expect($columns[1]->type)->toBe('timestampTz');
    });

    it('supports number column with precision', function () {
        $blueprint = new SnowflakeBlueprint($this->connection, 'products');
        $blueprint->id();
        $blueprint->number('price', 10, 2);

        $columns = $blueprint->getColumns();

        expect($columns[1]->type)->toBe('number');
        expect($columns[1]->precision)->toBe(10);
        expect($columns[1]->scale)->toBe(2);
    });

    it('supports identity column', function () {
        $blueprint = new SnowflakeBlueprint($this->connection, 'counters');
        $blueprint->identity('counter_id', 100, 10);

        $columns = $blueprint->getColumns();

        expect($columns[0]->type)->toBe('identity');
        expect($columns[0]->start)->toBe(100);
        expect($columns[0]->increment)->toBe(10);
    });

    it('creates uuid primary key when specified', function () {
        $blueprint = new SnowflakeBlueprint($this->connection, 'users');
        $blueprint->uuidPrimary();
        $blueprint->string('name');

        $columns = $blueprint->getColumns();

        expect($columns[0]->type)->toBe('uuid');
    });

    it('supports foreign ulid', function () {
        $blueprint = new SnowflakeBlueprint($this->connection, 'posts');
        $blueprint->id();
        $blueprint->foreignUlid('user_id');

        $columns = $blueprint->getColumns();

        expect($columns[1]->type)->toBe('char');
        expect($columns[1]->length)->toBe(26);
    });
});

describe('Schema Builder pretend mode', function () {
    // These tests require full Laravel integration with a registered driver
    // Skip them for now as they require a more complex test setup
    it('generates create table sql', function () {
        // Test the grammar directly instead of through Schema facade
        $grammar = $this->grammar;
        $blueprint = new SnowflakeBlueprint($this->connection, 'users');
        $blueprint->id();
        $blueprint->string('name');
        $blueprint->string('email');
        $blueprint->timestamps();

        $sql = $grammar->compileCreate($blueprint, new \Illuminate\Support\Fluent);

        expect($sql)->toContain('CREATE TABLE');
        expect($sql)->toContain('"users"');
    });

    it('generates drop table sql', function () {
        $grammar = $this->grammar;
        $blueprint = new SnowflakeBlueprint($this->connection, 'users');

        $sql = $grammar->compileDropIfExists($blueprint, new \Illuminate\Support\Fluent);

        expect($sql)->toContain('DROP TABLE IF EXISTS');
    });

    it('generates alter table sql', function () {
        $grammar = $this->grammar;
        $blueprint = new SnowflakeBlueprint($this->connection, 'users');
        $blueprint->string('phone')->nullable();

        $sql = $grammar->compileAdd($blueprint, new \Illuminate\Support\Fluent);

        expect($sql)->toContain('ALTER TABLE');
        expect($sql)->toContain('ADD COLUMN');
    });

    it('generates rename table sql', function () {
        $grammar = $this->grammar;
        $blueprint = new SnowflakeBlueprint($this->connection, 'users');
        $command = new \Illuminate\Support\Fluent(['to' => 'members']);

        $sql = $grammar->compileRename($blueprint, $command);

        expect($sql)->toContain('ALTER TABLE');
        expect($sql)->toContain('RENAME TO');
    });

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
});

describe('Clustering', function () {
    beforeEach(function () {
        $grammar = null;
        $connection = Mockery::mock(Connection::class);
        $connection->shouldReceive('getTablePrefix')->andReturn('');
        $connection->shouldReceive('getSchemaGrammar')->andReturnUsing(function () use (&$grammar) {
            return $grammar;
        });

        $grammar = new SnowflakeSchemaGrammar($connection);
        $this->connection = $connection;
    });

    it('adds cluster by command', function () {
        $blueprint = new SnowflakeBlueprint($this->connection, 'events');
        $blueprint->id();
        $blueprint->timestampNtz('created_at');
        $blueprint->clusterBy(['created_at', 'id']);

        $commands = $blueprint->getCommands();

        $clusterCommand = collect($commands)->first(fn ($cmd) => $cmd->name === 'clusterBy');

        expect($clusterCommand)->not->toBeNull();
        expect($clusterCommand->columns)->toBe(['created_at', 'id']);
    });
});

describe('Data Retention', function () {
    beforeEach(function () {
        $grammar = null;
        $connection = Mockery::mock(Connection::class);
        $connection->shouldReceive('getTablePrefix')->andReturn('');
        $connection->shouldReceive('getSchemaGrammar')->andReturnUsing(function () use (&$grammar) {
            return $grammar;
        });

        $grammar = new SnowflakeSchemaGrammar($connection);
        $this->connection = $connection;
    });

    it('adds data retention command', function () {
        $blueprint = new SnowflakeBlueprint($this->connection, 'audit_logs');
        $blueprint->id();
        $blueprint->dataRetentionDays(90);

        $commands = $blueprint->getCommands();

        $retentionCommand = collect($commands)->first(fn ($cmd) => $cmd->name === 'dataRetention');

        expect($retentionCommand)->not->toBeNull();
        expect($retentionCommand->days)->toBe(90);
    });
});
