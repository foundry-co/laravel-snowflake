<?php

declare(strict_types=1);

use FoundryCo\Snowflake\Query\Grammars\SnowflakeGrammar;
use Illuminate\Database\Connection;
use Illuminate\Database\Query\Builder;

beforeEach(function () {
    $connection = Mockery::mock(Connection::class);
    $connection->shouldReceive('getTablePrefix')->andReturn('');
    $this->connection = $connection;
    $this->grammar = new SnowflakeGrammar($connection);
});

describe('identifier wrapping', function () {
    it('wraps column names with double quotes', function () {
        expect($this->grammar->wrap('column_name'))->toBe('"column_name"');
    });

    it('wraps qualified column names', function () {
        expect($this->grammar->wrap('table.column'))->toBe('"table"."column"');
    });

    it('does not wrap asterisk', function () {
        expect($this->grammar->wrap('*'))->toBe('*');
    });

    it('escapes double quotes in identifiers', function () {
        expect($this->grammar->wrap('col"umn'))->toBe('"col""umn"');
    });
});

describe('select compilation', function () {
    it('compiles basic select', function () {
        $builder = Mockery::mock(Builder::class);
        $builder->shouldReceive('getConnection')->andReturn($this->connection);
        $builder->columns = ['*'];
        $builder->from = 'users';
        $builder->wheres = [];
        $builder->groups = null;
        $builder->havings = null;
        $builder->orders = null;
        $builder->limit = null;
        $builder->offset = null;
        $builder->unions = null;
        $builder->unionLimit = null;
        $builder->unionOffset = null;
        $builder->unionOrders = null;
        $builder->lock = null;
        $builder->distinct = false;
        $builder->aggregate = null;
        $builder->joins = null;
        $builder->indexHint = null;
        $builder->bindings = ['select' => [], 'from' => [], 'join' => [], 'where' => [], 'groupBy' => [], 'having' => [], 'order' => [], 'union' => [], 'unionOrder' => []];

        $sql = $this->grammar->compileSelect($builder);

        expect($sql)->toContain('select');
        expect($sql)->toContain('"users"');
    });
});

describe('truncate compilation', function () {
    it('compiles truncate statement', function () {
        $builder = Mockery::mock(Builder::class);
        $builder->from = 'users';

        $result = $this->grammar->compileTruncate($builder);

        expect($result)->toHaveKey('TRUNCATE TABLE "users"');
    });
});

describe('operators', function () {
    it('includes snowflake-specific operators', function () {
        $reflection = new ReflectionClass($this->grammar);
        $property = $reflection->getProperty('operators');
        $operators = $property->getValue($this->grammar);

        expect($operators)->toContain('ilike');
        expect($operators)->toContain('rlike');
    });
});

describe('random compilation', function () {
    it('compiles random function', function () {
        expect($this->grammar->compileRandom('seed'))->toBe('RANDOM()');
    });
});

describe('savepoints', function () {
    it('does not support savepoints', function () {
        expect($this->grammar->supportsSavepoints())->toBeFalse();
    });
});
