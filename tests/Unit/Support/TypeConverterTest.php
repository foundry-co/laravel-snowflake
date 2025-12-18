<?php

declare(strict_types=1);

use Carbon\Carbon;
use FoundryCo\Snowflake\Support\TypeConverter;

beforeEach(function () {
    $this->converter = new TypeConverter;
});

describe('integer casting', function () {
    it('casts integers', function () {
        expect($this->converter->cast('42', 'INTEGER'))->toBe(42);
        expect($this->converter->cast('-100', 'BIGINT'))->toBe(-100);
        expect($this->converter->cast('0', 'SMALLINT'))->toBe(0);
    });

    it('keeps large integers as strings', function () {
        $largeInt = '99999999999999999999999999999999999999';
        expect($this->converter->cast($largeInt, 'INTEGER'))->toBe($largeInt);
    });
});

describe('float casting', function () {
    it('casts floats', function () {
        expect($this->converter->cast('3.14159', 'FLOAT'))->toBe(3.14159);
        expect($this->converter->cast('2.718', 'DOUBLE'))->toBe(2.718);
        expect($this->converter->cast('-0.5', 'REAL'))->toBe(-0.5);
    });
});

describe('boolean casting', function () {
    it('casts boolean true', function () {
        expect($this->converter->cast('true', 'BOOLEAN'))->toBeTrue();
        expect($this->converter->cast('TRUE', 'BOOLEAN'))->toBeTrue();
        expect($this->converter->cast('1', 'BOOLEAN'))->toBeTrue();
    });

    it('casts boolean false', function () {
        expect($this->converter->cast('false', 'BOOLEAN'))->toBeFalse();
        expect($this->converter->cast('FALSE', 'BOOLEAN'))->toBeFalse();
        expect($this->converter->cast('0', 'BOOLEAN'))->toBeFalse();
    });
});

describe('string casting', function () {
    it('casts strings', function () {
        expect($this->converter->cast('hello', 'VARCHAR'))->toBe('hello');
        expect($this->converter->cast('world', 'TEXT'))->toBe('world');
        expect($this->converter->cast('test', 'STRING'))->toBe('test');
    });
});

describe('null handling', function () {
    it('returns null for null values', function () {
        expect($this->converter->cast(null, 'INTEGER'))->toBeNull();
        expect($this->converter->cast(null, 'VARCHAR'))->toBeNull();
        expect($this->converter->cast(null, 'BOOLEAN'))->toBeNull();
    });
});

describe('number casting', function () {
    it('casts numbers without scale as integers', function () {
        $result = $this->converter->cast('42', 'NUMBER', ['scale' => 0]);
        expect($result)->toBe(42);
    });

    it('casts numbers with scale as floats', function () {
        $result = $this->converter->cast('42.50', 'NUMBER', ['scale' => 2]);
        expect($result)->toBe(42.50);
    });
});

describe('date casting', function () {
    it('casts dates from epoch days', function () {
        // 18262 days from epoch = 2020-01-01
        $result = $this->converter->cast('18262', 'DATE');

        expect($result)->toBeInstanceOf(Carbon::class);
        expect($result->format('Y-m-d'))->toBe('2020-01-01');
    });
});

describe('timestamp casting', function () {
    it('casts timestamp_ntz from epoch seconds', function () {
        // 1577836800 = 2020-01-01 00:00:00 UTC
        $result = $this->converter->cast('1577836800.000000000', 'TIMESTAMP_NTZ');

        expect($result)->toBeInstanceOf(Carbon::class);
        expect($result->format('Y-m-d H:i:s'))->toBe('2020-01-01 00:00:00');
    });

    it('casts timestamp with nanoseconds', function () {
        // With microseconds
        $result = $this->converter->cast('1577836800.123456789', 'TIMESTAMP_NTZ');

        expect($result)->toBeInstanceOf(Carbon::class);
        expect($result->format('Y-m-d H:i:s.u'))->toBe('2020-01-01 00:00:00.123456');
    });

    it('casts timestamp_tz with timezone offset', function () {
        // 1577836800 seconds + 300 minutes offset (UTC+5)
        $result = $this->converter->cast('1577836800.000000000 300', 'TIMESTAMP_TZ');

        expect($result)->toBeInstanceOf(Carbon::class);
    });
});

describe('variant casting', function () {
    it('casts variant to array', function () {
        $json = '{"key": "value", "number": 42}';
        $result = $this->converter->cast($json, 'VARIANT');

        expect($result)->toBeArray();
        expect($result['key'])->toBe('value');
        expect($result['number'])->toBe(42);
    });

    it('casts array type', function () {
        $json = '[1, 2, 3, "four"]';
        $result = $this->converter->cast($json, 'ARRAY');

        expect($result)->toBeArray();
        expect($result)->toBe([1, 2, 3, 'four']);
    });

    it('returns original string on invalid json', function () {
        $invalid = 'not valid json';
        $result = $this->converter->cast($invalid, 'VARIANT');

        expect($result)->toBe($invalid);
    });
});

describe('binary casting', function () {
    it('casts binary from hex', function () {
        $hex = bin2hex('hello');
        $result = $this->converter->cast($hex, 'BINARY');

        expect($result)->toBe('hello');
    });
});

describe('sql literal conversion', function () {
    it('converts null to NULL', function () {
        expect($this->converter->toSqlLiteral(null))->toBe('NULL');
    });

    it('converts booleans', function () {
        expect($this->converter->toSqlLiteral(true))->toBe('TRUE');
        expect($this->converter->toSqlLiteral(false))->toBe('FALSE');
    });

    it('converts integers', function () {
        expect($this->converter->toSqlLiteral(42))->toBe('42');
    });

    it('converts floats', function () {
        expect($this->converter->toSqlLiteral(3.14))->toBe('3.14');
    });

    it('converts strings with escaping', function () {
        expect($this->converter->toSqlLiteral('hello'))->toBe("'hello'");
        expect($this->converter->toSqlLiteral("it's"))->toBe("'it''s'");
    });

    it('converts datetime', function () {
        $date = new DateTime('2020-01-01 12:30:45.123456');
        $result = $this->converter->toSqlLiteral($date);

        expect($result)->toBe("'2020-01-01 12:30:45.123456'");
    });

    it('converts arrays to PARSE_JSON', function () {
        $result = $this->converter->toSqlLiteral(['key' => 'value']);

        expect($result)->toContain('PARSE_JSON');
        expect($result)->toContain('"key"');
    });
});
