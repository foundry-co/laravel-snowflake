<?php

declare(strict_types=1);

use FoundryCo\Snowflake\Eloquent\Concerns\UsesSnowflake;
use Illuminate\Database\Eloquent\Model;

// Test model using the trait
class TestUser extends Model
{
    use UsesSnowflake;

    protected $connection = 'snowflake';
    protected $table = 'users';
    protected $fillable = ['name', 'email'];
}

// Another test model
class TestPost extends Model
{
    use UsesSnowflake;

    protected $connection = 'snowflake';
    protected $table = 'posts';
    protected $fillable = ['title', 'content'];
}

describe('UsesSnowflake trait', function () {
    it('sets incrementing to false', function () {
        $user = new TestUser;

        expect($user->getIncrementing())->toBeFalse();
    });

    it('sets key type to string', function () {
        $user = new TestUser;

        expect($user->getKeyType())->toBe('string');
    });

    it('generates lowercase ulid for new models', function () {
        $user = new TestUser;
        $ulid = $user->newUniqueId();

        expect($ulid)->toBeString();
        expect(strlen($ulid))->toBe(26);
        expect($ulid)->toBe(strtolower($ulid));
    });

    it('uses correct date format', function () {
        $user = new TestUser;

        expect($user->getDateFormat())->toBe('Y-m-d H:i:s.u');
    });

    it('returns unique ids array', function () {
        $user = new TestUser;

        expect($user->uniqueIds())->toBe(['id']);
    });

    it('generates ulid matching expected pattern', function () {
        $post = new TestPost;
        $ulid = $post->newUniqueId();

        expect(strlen($ulid))->toBe(26);
        expect($ulid)->toMatch('/^[0-9a-z]{26}$/');
    });
});

describe('Model timestamp handling', function () {
    it('formats datetime with microseconds', function () {
        $user = new TestUser;
        $date = now();

        $formatted = $user->fromDateTime($date);

        expect($formatted)->toContain('.');
        expect(strlen($formatted))->toBeGreaterThan(19);
    });

    it('returns fresh timestamp with microseconds', function () {
        $user = new TestUser;

        $timestamp = $user->freshTimestampString();

        expect($timestamp)->toMatch('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}$/');
    });

    it('returns null for empty datetime', function () {
        $user = new TestUser;

        expect($user->fromDateTime(null))->toBeNull();
        expect($user->fromDateTime(''))->toBeNull();
    });
});
