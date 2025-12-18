<?php

declare(strict_types=1);

use FoundryCo\Snowflake\Query\Grammars\SnowflakeGrammar;
use Illuminate\Database\Connection;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Processors\Processor;

describe('QueryBuilder', function () {
    beforeEach(function () {
        $connection = Mockery::mock(Connection::class);
        $connection->shouldReceive('getTablePrefix')->andReturn('');
        $grammar = new SnowflakeGrammar($connection);
        $processor = new Processor;

        $this->connection = $connection;
        $this->grammar = $grammar;
        $this->builder = new Builder($connection, $grammar, $processor);
    });

    it('generates correct select sql', function () {
        $this->builder->from('users')->select('id', 'name');
        $sql = $this->grammar->compileSelect($this->builder);

        expect($sql)->toContain('select');
        expect($sql)->toContain('"users"');
    });

    it('generates correct where clause', function () {
        $this->builder->from('users')->where('active', true);
        $sql = $this->grammar->compileSelect($this->builder);

        expect($sql)->toContain('where');
        expect($sql)->toContain('"active"');
    });

    it('generates correct insert sql', function () {
        $this->builder->from('users');
        $sql = $this->grammar->compileInsert($this->builder, [
            'id' => '01ARZ3NDEKTSV4RRFFQ69G5FAV',
            'name' => 'Test User',
            'email' => 'test@example.com',
        ]);

        expect($sql)->toContain('insert into "users"');
    });

    it('generates correct update sql', function () {
        $this->builder->from('users')->where('id', '01ARZ3NDEKTSV4RRFFQ69G5FAV');
        $sql = $this->grammar->compileUpdate($this->builder, ['name' => 'Updated Name']);

        expect($sql)->toContain('update "users"');
        expect($sql)->toContain('set');
    });

    it('generates correct delete sql', function () {
        $this->builder->from('users')->where('id', '01ARZ3NDEKTSV4RRFFQ69G5FAV');
        $sql = $this->grammar->compileDelete($this->builder);

        expect($sql)->toContain('delete from "users"');
    });

    it('supports limit and offset', function () {
        $this->builder->from('users')->limit(10)->offset(20);
        $sql = $this->grammar->compileSelect($this->builder);

        expect(strtolower($sql))->toContain('limit 10');
        expect(strtolower($sql))->toContain('offset 20');
    });

    it('supports order by', function () {
        $this->builder->from('users')->orderBy('created_at', 'desc');
        $sql = $this->grammar->compileSelect($this->builder);

        expect($sql)->toContain('order by');
        expect($sql)->toContain('"created_at"');
        expect($sql)->toContain('desc');
    });

    it('supports joins', function () {
        $this->builder->from('users')
            ->join('posts', 'users.id', '=', 'posts.user_id')
            ->select('users.*', 'posts.title');
        $sql = $this->grammar->compileSelect($this->builder);

        expect(strtolower($sql))->toContain('inner join');
        expect($sql)->toContain('"posts"');
    });

    it('supports group by and having', function () {
        $this->builder->from('orders')
            ->selectRaw('user_id, count(*) as order_count')
            ->groupBy('user_id')
            ->having('order_count', '>', 5);
        $sql = $this->grammar->compileSelect($this->builder);

        expect(strtolower($sql))->toContain('group by');
        expect(strtolower($sql))->toContain('having');
    });

    it('supports aggregate functions', function () {
        $builder = new Builder($this->connection, $this->grammar, new Processor);
        $builder->from('users');
        $builder->aggregate = ['function' => 'count', 'columns' => ['*']];
        $sql = $this->grammar->compileSelect($builder);

        expect(strtolower($sql))->toContain('count(*)');
    });

    it('supports distinct', function () {
        $this->builder->from('users')->distinct()->select('email');
        $sql = $this->grammar->compileSelect($this->builder);

        expect(strtolower($sql))->toContain('select distinct');
    });
});
