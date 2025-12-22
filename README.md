# Laravel Snowflake

A Laravel database driver for Snowflake using the REST SQL API. No PHP extensions or ODBC drivers required.

## Features

- Pure PHP implementation using Snowflake's REST API
- Full Eloquent support with models and relationships
- Laravel Query Builder with Snowflake-specific SQL
- Migrations with Snowflake-specific column types
- ULID primary keys optimized for Snowflake clustering
- Native support for VARIANT, OBJECT, and ARRAY types
- Large result set streaming via partitions

## Requirements

- PHP 8.2+
- Laravel 12.0+
- Snowflake account with REST API access

## Installation

```bash
composer require foundry-co/laravel-snowflake
```

The package will auto-register its service provider.

## Configuration

### 1. Snowflake Account Setup

Set up key-pair authentication in Snowflake:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_key.p8 -nocrypt
openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub
```

Assign the public key to your Snowflake user:

```sql
ALTER USER your_user SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...';
```

### 2. Environment Variables

```env
SNOWFLAKE_ACCOUNT=your-account-identifier
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=MY_DATABASE
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_USER=your_username
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/snowflake_key.p8
SNOWFLAKE_ROLE=SYSADMIN
```

### 3. Database Configuration

Add the Snowflake connection to `config/database.php`:

```php
'connections' => [
    'snowflake' => [
        'driver' => 'snowflake',
        'account' => env('SNOWFLAKE_ACCOUNT'),
        'warehouse' => env('SNOWFLAKE_WAREHOUSE'),
        'database' => env('SNOWFLAKE_DATABASE'),
        'schema' => env('SNOWFLAKE_SCHEMA', 'PUBLIC'),
        'role' => env('SNOWFLAKE_ROLE'),
        'auth' => [
            'jwt' => [
                'user' => env('SNOWFLAKE_USER'),
                'private_key_path' => env('SNOWFLAKE_PRIVATE_KEY_PATH'),
                'private_key_passphrase' => env('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'),
            ],
        ],
    ],
],
```

You can also provide the private key content directly instead of a file path:

```php
'auth' => [
    'jwt' => [
        'user' => env('SNOWFLAKE_USER'),
        'private_key' => env('SNOWFLAKE_PRIVATE_KEY'),
    ],
],
```

## Usage

### Eloquent Models

Add the `UsesSnowflake` trait to any model that connects to Snowflake:

```php
use Illuminate\Database\Eloquent\Model;
use FoundryCo\Snowflake\Eloquent\Concerns\UsesSnowflake;

class User extends Model
{
    use UsesSnowflake;

    protected $connection = 'snowflake';
    protected $table = 'users';
}
```

The trait automatically generates ULID primary keys and handles Snowflake timestamp formats.

### Query Builder

```php
$users = DB::connection('snowflake')->table('users')->get();

DB::connection('snowflake')->table('users')->insert([
    'id' => Str::ulid()->toLower(),
    'name' => 'John Doe',
    'email' => 'john@example.com',
]);

DB::connection('snowflake')
    ->table('events')
    ->where('payload->type', 'purchase')
    ->get();
```

### Migrations

```php
use Illuminate\Database\Migrations\Migration;
use FoundryCo\Snowflake\Schema\SnowflakeBlueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'snowflake';

    public function up(): void
    {
        Schema::connection('snowflake')->create('users', function (SnowflakeBlueprint $table) {
            $table->id();
            $table->string('name');
            $table->string('email')->unique();
            $table->variant('preferences');
            $table->timestamps();
            $table->clusterBy(['created_at', 'id']);
        });
    }

    public function down(): void
    {
        Schema::connection('snowflake')->dropIfExists('users');
    }
};
```

### Snowflake Column Types

| Method | Snowflake Type |
|--------|---------------|
| `id()` | `CHAR(26)` |
| `variant()` | `VARIANT` |
| `object()` | `OBJECT` |
| `array()` | `ARRAY` |
| `geography()` | `GEOGRAPHY` |
| `geometry()` | `GEOMETRY` |
| `timestampNtz()` | `TIMESTAMP_NTZ` |
| `timestampLtz()` | `TIMESTAMP_LTZ` |
| `timestampTz()` | `TIMESTAMP_TZ` |
| `number()` | `NUMBER(p,s)` |
| `identity()` | `INTEGER IDENTITY` |

### Custom Casts

```php
use FoundryCo\Snowflake\Casts\VariantCast;
use FoundryCo\Snowflake\Casts\SnowflakeTimestamp;

class Event extends Model
{
    use UsesSnowflake;

    protected $connection = 'snowflake';

    protected $casts = [
        'payload' => VariantCast::class,
        'occurred_at' => SnowflakeTimestamp::class,
    ];
}
```

### Warehouse & Role Switching

```php
$connection = DB::connection('snowflake');

$connection->useWarehouse('ANALYTICS_WH');
$connection->useRole('ANALYST');
$connection->useSchema('STAGING');
```

### Transactions

```php
DB::connection('snowflake')->transaction(function ($db) {
    $db->table('accounts')->where('id', 1)->decrement('balance', 100);
    $db->table('accounts')->where('id', 2)->increment('balance', 100);
});
```

### Cursors

```php
foreach (DB::connection('snowflake')->table('events')->cursor() as $event) {
    // Process one row at a time
}
```

## Testing

```bash
composer test
```

## Limitations

- No savepoints (Snowflake limitation)
- No row locking (Snowflake is append-only)
- No traditional indexes (use clustering keys instead)

## License

MIT License. See [LICENSE](LICENSE) for details.
