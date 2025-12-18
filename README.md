# Laravel Snowflake

A fully-featured Laravel database driver for Snowflake using the REST SQL API. No PHP extensions or ODBC drivers required.

## Features

- **Pure PHP Implementation** - Uses Snowflake's REST API, no `pdo_snowflake` or ODBC required
- **Full Eloquent Support** - Models, relationships, and all Eloquent features work seamlessly
- **Laravel Query Builder** - Complete query builder support with Snowflake-specific SQL
- **Migrations** - Full schema builder with Snowflake-specific column types
- **ULID Primary Keys** - Time-sortable, distributed-safe IDs optimized for Snowflake clustering
- **Semi-Structured Data** - Native support for VARIANT, OBJECT, and ARRAY types
- **JWT & OAuth Authentication** - Secure authentication with key-pair or OAuth tokens
- **Large Result Sets** - Efficient partition-based streaming for large query results

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

Before using this package, you need to set up key-pair authentication in Snowflake:

```bash
# Generate a private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_key.p8 -nocrypt

# Extract the public key
openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub
```

Then assign the public key to your Snowflake user:

```sql
ALTER USER your_user SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...';
```

### 2. Environment Variables

Add these to your `.env` file:

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
        'prefix' => '',
        'auth' => [
            'method' => 'jwt',
            'jwt' => [
                'user' => env('SNOWFLAKE_USER'),
                'private_key_path' => env('SNOWFLAKE_PRIVATE_KEY_PATH'),
                'private_key_passphrase' => env('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'),
            ],
        ],
    ],
],
```

### OAuth Authentication (Alternative)

```php
'auth' => [
    'method' => 'oauth',
    'oauth' => [
        'token_endpoint' => env('SNOWFLAKE_OAUTH_TOKEN_ENDPOINT'),
        'client_id' => env('SNOWFLAKE_OAUTH_CLIENT_ID'),
        'client_secret' => env('SNOWFLAKE_OAUTH_CLIENT_SECRET'),
        'scope' => 'session:role-any',
    ],
],
```

### Using 1Password or Secrets Managers

Instead of storing your private key in a file, you can provide the key content directly. This is useful when using 1Password CLI, HashiCorp Vault, or other secrets managers.

```env
# Use private_key instead of private_key_path
SNOWFLAKE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEF...
-----END PRIVATE KEY-----"
```

With 1Password CLI, you can reference secrets directly:

```env
SNOWFLAKE_PRIVATE_KEY="op://vault/snowflake/private-key"
```

Then run your application with:

```bash
op run -- php artisan serve
```

Update your database configuration to use the key content:

```php
'jwt' => [
    'user' => env('SNOWFLAKE_USER'),
    'private_key' => env('SNOWFLAKE_PRIVATE_KEY'),  // Content instead of path
    'private_key_passphrase' => env('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'),
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
    protected $fillable = ['name', 'email'];
}
```

The trait automatically:
- Generates ULID primary keys (time-sortable, optimal for Snowflake clustering)
- Handles Snowflake timestamp formats with microsecond precision
- Sets `$incrementing = false` and `$keyType = 'string'`

### Query Builder

Use the query builder as you normally would:

```php
// Basic queries
$users = DB::connection('snowflake')->table('users')->get();

// Inserts (ULID will be generated if id not provided)
DB::connection('snowflake')->table('users')->insert([
    'id' => Str::ulid()->toLower(),
    'name' => 'John Doe',
    'email' => 'john@example.com',
]);

// Updates
DB::connection('snowflake')
    ->table('users')
    ->where('id', $id)
    ->update(['name' => 'Jane Doe']);

// JSON/Variant queries
DB::connection('snowflake')
    ->table('events')
    ->where('payload->type', 'purchase')
    ->get();
```

### Migrations

Create migrations with Snowflake-specific features:

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
            // ULID primary key (recommended)
            $table->id();

            // Standard columns
            $table->string('name');
            $table->string('email')->unique();

            // Snowflake semi-structured types
            $table->variant('preferences');    // VARIANT - any JSON data
            $table->object('metadata');        // OBJECT - key-value pairs
            $table->array('tags');             // ARRAY - ordered list

            // Snowflake timestamp types
            $table->timestampNtz('processed_at');  // No timezone
            $table->timestampLtz('local_time');    // Local timezone
            $table->timestampTz('event_time');     // With timezone

            // Geospatial types
            $table->geography('location');
            $table->geometry('shape');

            // High-precision numbers
            $table->number('balance', 18, 4);

            // Standard timestamps
            $table->timestamps();

            // Clustering for query performance
            $table->clusterBy(['created_at', 'id']);
        });
    }

    public function down(): void
    {
        Schema::connection('snowflake')->dropIfExists('users');
    }
};
```

### Available Column Types

| Method | Snowflake Type | Description |
|--------|---------------|-------------|
| `id()` | `CHAR(26)` | ULID primary key |
| `ulidPrimary()` | `CHAR(26)` | ULID primary key |
| `uuidPrimary()` | `VARCHAR(36)` | UUID primary key |
| `variant()` | `VARIANT` | Semi-structured data |
| `object()` | `OBJECT` | Key-value pairs |
| `array()` | `ARRAY` | Ordered list |
| `geography()` | `GEOGRAPHY` | Spherical coordinates |
| `geometry()` | `GEOMETRY` | Planar coordinates |
| `timestampNtz()` | `TIMESTAMP_NTZ` | Timestamp without timezone |
| `timestampLtz()` | `TIMESTAMP_LTZ` | Timestamp in local timezone |
| `timestampTz()` | `TIMESTAMP_TZ` | Timestamp with timezone |
| `number()` | `NUMBER(p,s)` | High-precision decimal |
| `identity()` | `INTEGER IDENTITY` | Auto-incrementing integer |

### Custom Casts

Use the included casts for proper type handling:

```php
use Illuminate\Database\Eloquent\Model;
use FoundryCo\Snowflake\Casts\VariantCast;
use FoundryCo\Snowflake\Casts\SnowflakeTimestamp;
use FoundryCo\Snowflake\Eloquent\Concerns\UsesSnowflake;

class Event extends Model
{
    use UsesSnowflake;

    protected $connection = 'snowflake';

    protected $casts = [
        'payload' => VariantCast::class,
        'occurred_at' => SnowflakeTimestamp::class,
        'scheduled_at' => SnowflakeTimestamp::class.':tz',
    ];
}
```

### Warehouse & Role Switching

Switch context at runtime:

```php
// Get the connection
$connection = DB::connection('snowflake');

// Switch warehouse
$connection->useWarehouse('ANALYTICS_WH');

// Switch role
$connection->useRole('ANALYST');

// Switch schema
$connection->useSchema('STAGING');

// Chain methods
$results = $connection
    ->useWarehouse('LARGE_WH')
    ->useRole('ADMIN')
    ->table('big_table')
    ->get();
```

### Transactions

Transactions work as expected:

```php
DB::connection('snowflake')->transaction(function ($db) {
    $db->table('accounts')->where('id', 1)->decrement('balance', 100);
    $db->table('accounts')->where('id', 2)->increment('balance', 100);
});

// Or manually
DB::connection('snowflake')->beginTransaction();
try {
    // ... operations
    DB::connection('snowflake')->commit();
} catch (\Exception $e) {
    DB::connection('snowflake')->rollBack();
    throw $e;
}
```

### Cursors for Large Results

Use cursors to efficiently process large result sets:

```php
foreach (DB::connection('snowflake')->table('events')->cursor() as $event) {
    // Process one row at a time
    // Results are fetched partition by partition
}
```

## Primary Keys: ULID vs UUID vs Identity

This package defaults to ULIDs for primary keys because they offer significant advantages for Snowflake:

### ULID (Recommended)
- **Time-sortable**: ULIDs are lexicographically sortable by creation time
- **Clustering benefit**: Records created near each other are stored near each other
- **Client-generated**: No round-trip to database needed
- **Distributed-safe**: No sequence contention

```php
// Default behavior
$table->id(); // Creates CHAR(26) ULID column
```

### UUID (Alternative)
- Use if you need compatibility with UUID-based systems
- Not time-sortable (random distribution)

```php
$table->uuidPrimary();
```

### Identity (Legacy)
- Snowflake's auto-increment equivalent
- Consider only for compatibility with existing schemas

```php
$table->identity('id', start: 1, increment: 1);
```

## Testing

```bash
composer test
```

## Limitations

- **No savepoints**: Snowflake doesn't support savepoints
- **No row locking**: Snowflake is append-only
- **No traditional indexes**: Use clustering keys instead
- **REST API only**: All queries go through the REST API

## License

MIT License. See [LICENSE](LICENSE) for details.
