# JSON/JSONB Support

cel2sql provides comprehensive support for PostgreSQL's JSON and JSONB data types, automatically converting CEL field access to PostgreSQL JSON path operations.

## Quick Start

```go
// Define schema with JSON/JSONB columns
userSchema := pg.Schema{
    {Name: "id", Type: "bigint"},
    {Name: "name", Type: "text"},
    {Name: "preferences", Type: "jsonb"},  // JSONB column
    {Name: "metadata", Type: "json"},      // JSON column
}

// CEL: Access JSON field
user.preferences.theme == "dark"

// Generated SQL:
user.preferences->>'theme' = 'dark'
```

## JSON vs JSONB

PostgreSQL has two JSON types:

| Type | Description | Performance | Use Case |
|------|-------------|-------------|----------|
| `json` | Stores exact text | Slower queries | Store exact formatting |
| `jsonb` | Binary format | Faster queries | Most use cases |

**Recommendation**: Use `jsonb` unless you need to preserve exact JSON formatting.

## Field Access

### Simple Field Access

```go
// CEL
user.preferences.theme
// SQL: user.preferences->>'theme'

// CEL with comparison
user.preferences.theme == "dark"
// SQL: user.preferences->>'theme' = 'dark'
```

### Nested Field Access

```go
// CEL: Access nested fields
user.profile.settings.notifications
// SQL: user.profile->'settings'->>'notifications'

// CEL: Comparison with nested field
user.profile.settings.notifications == "enabled"
// SQL: user.profile->'settings'->>'notifications' = 'enabled'
```

### Deep Nesting

cel2sql handles arbitrarily deep JSON paths:

```go
// CEL: 4+ levels of nesting
user.metadata.config.api.version
// SQL: user.metadata->'config'->'api'->>'version'
```

## Field Existence Checking

Use the `has()` macro to check if JSON fields exist:

```go
// Check if field exists
has(user.preferences.theme)
// SQL: user.preferences ? 'theme'

// Check nested field existence
has(user.profile.settings.notifications)
// SQL: user.profile->'settings' ? 'notifications'

// Combine existence check with value comparison
has(user.preferences.theme) && user.preferences.theme == "dark"
// SQL: user.preferences ? 'theme' AND user.preferences->>'theme' = 'dark'
```

## Numeric Fields

JSON stores all values as text, so numeric comparisons require casting:

```go
// Schema with numeric JSON field
productSchema := pg.Schema{
    {Name: "id", Type: "bigint"},
    {Name: "metadata", Type: "jsonb"},
}

// CEL: Numeric comparison
int(product.metadata.stock) > 10
// SQL: (product.metadata->>'stock')::bigint > 10

// CEL: Decimal comparison
double(product.metadata.price) >= 99.99
// SQL: (product.metadata->>'price')::double precision >= 99.99
```

## Array Operations

### JSON Arrays

```go
// Schema with JSON array
userSchema := pg.Schema{
    {Name: "id", Type: "bigint"},
    {Name: "tags", Type: "jsonb"},  // Array of strings
}

// Check if array contains element
user.tags.exists(tag, tag == "developer")
// SQL: EXISTS (SELECT 1 FROM jsonb_array_elements_text(user.tags) AS tag
//              WHERE user.tags IS NOT NULL
//              AND jsonb_typeof(user.tags) = 'array'
//              AND tag = 'developer')
```

### Array of Objects

```go
// Schema with array of JSON objects
orderSchema := pg.Schema{
    {Name: "id", Type: "bigint"},
    {Name: "items", Type: "jsonb"},  // Array of {name, price, quantity}
}

// Filter items by condition
order.items.filter(item, item.price > 10)
// SQL: ARRAY(SELECT item FROM jsonb_array_elements(order.items) AS item
//            WHERE order.items IS NOT NULL
//            AND jsonb_typeof(order.items) = 'array'
//            AND (item->>'price')::numeric > 10)
```

## Complete Example

```go
package main

import (
    "fmt"
    "log"

    "github.com/google/cel-go/cel"
    "github.com/spandigital/cel2sql/v2"
    "github.com/spandigital/cel2sql/v2/pg"
)

func main() {
    // Define schema with JSONB columns
    userSchema := pg.Schema{
        {Name: "id", Type: "bigint"},
        {Name: "email", Type: "text"},
        {Name: "preferences", Type: "jsonb"},
        {Name: "profile", Type: "jsonb"},
    }

    provider := pg.NewTypeProvider(map[string]pg.Schema{
        "User": userSchema,
    })

    env, err := cel.NewEnv(
        cel.CustomTypeProvider(provider),
        cel.Variable("user", cel.ObjectType("User")),
    )
    if err != nil {
        log.Fatal(err)
    }

    // Example 1: Simple JSON field access
    ast, _ := env.Compile(`user.preferences.theme == "dark"`)
    sql, _ := cel2sql.Convert(ast)
    fmt.Println(sql)
    // Output: user.preferences->>'theme' = 'dark'

    // Example 2: Nested JSON fields
    ast, _ = env.Compile(`user.profile.settings.language == "en"`)
    sql, _ = cel2sql.Convert(ast)
    fmt.Println(sql)
    // Output: user.profile->'settings'->>'language' = 'en'

    // Example 3: Field existence check
    ast, _ = env.Compile(`has(user.preferences.theme)`)
    sql, _ = cel2sql.Convert(ast)
    fmt.Println(sql)
    // Output: user.preferences ? 'theme'

    // Example 4: Combined conditions
    ast, _ = env.Compile(`has(user.preferences.theme) && user.preferences.theme == "dark"`)
    sql, _ = cel2sql.Convert(ast)
    fmt.Println(sql)
    // Output: user.preferences ? 'theme' AND user.preferences->>'theme' = 'dark'
}
```

## JSON Path Operators

cel2sql uses these PostgreSQL JSON operators:

| Operator | Description | Example |
|----------|-------------|---------|
| `->` | Get JSON object field | `data->'key'` |
| `->>` | Get JSON object field as text | `data->>'key'` |
| `?` | Does JSON contain key? | `data ? 'key'` |
| `@>` | Does JSON contain value? | `data @> '{"key":"value"}'` |

## Type Conversions

When working with JSON fields, you may need explicit type conversion:

```go
// String field (default)
user.preferences.theme == "dark"
// SQL: user.preferences->>'theme' = 'dark'

// Integer field
int(user.preferences.page_size) > 20
// SQL: (user.preferences->>'page_size')::bigint > 20

// Boolean field
bool(user.preferences.dark_mode) == true
// SQL: (user.preferences->>'dark_mode')::boolean IS TRUE

// Double field
double(user.preferences.font_size) >= 14.5
// SQL: (user.preferences->>'font_size')::double precision >= 14.5
```

## Complex Queries

### Multiple JSON Conditions

```go
// CEL: Multiple JSON field comparisons
user.preferences.theme == "dark" &&
user.preferences.language == "en" &&
has(user.profile.avatar)

// Generated SQL:
// user.preferences->>'theme' = 'dark'
// AND user.preferences->>'language' = 'en'
// AND user.profile ? 'avatar'
```

### Mixed JSON and Regular Fields

```go
// CEL: Combine JSON and regular column filters
user.email.endsWith("@company.com") &&
user.preferences.notifications == "enabled"

// Generated SQL:
// user.email LIKE '%@company.com'
// AND user.preferences->>'notifications' = 'enabled'
```

## Performance Tips

### 1. Use JSONB Over JSON

```go
// ✅ Preferred: JSONB is faster for queries
{Name: "data", Type: "jsonb"}

// ❌ Slower: JSON stores exact text
{Name: "data", Type: "json"}
```

### 2. Add GIN Indexes

For frequently queried JSONB columns:

```sql
-- Create GIN index for JSONB column
CREATE INDEX idx_user_preferences ON users USING GIN (preferences);

-- Index specific paths
CREATE INDEX idx_user_theme ON users ((preferences->>'theme'));
```

### 3. Existence Checks Before Comparisons

```go
// ✅ Good: Check existence first
has(user.preferences.theme) && user.preferences.theme == "dark"

// ❌ Less efficient: Direct comparison may fail on missing keys
user.preferences.theme == "dark"
```

## Common Patterns

### User Preferences

```go
schema := pg.Schema{
    {Name: "id", Type: "bigint"},
    {Name: "preferences", Type: "jsonb"},
}

// Dark mode users
user.preferences.theme == "dark"

// Language preference
user.preferences.language == "es"

// Notification settings
bool(user.preferences.notifications) == true
```

### Product Metadata

```go
schema := pg.Schema{
    {Name: "id", Type: "bigint"},
    {Name: "metadata", Type: "jsonb"},
}

// Products with specific attribute
product.metadata.color == "red"

// Products in stock
int(product.metadata.stock) > 0

// Products on sale
bool(product.metadata.on_sale) == true
```

### Analytics Events

```go
schema := pg.Schema{
    {Name: "id", Type: "bigint"},
    {Name: "event_data", Type: "jsonb"},
}

// Events from specific source
event.event_data.source == "mobile"

// High-value events
double(event.event_data.value) > 100.0
```

## Error Handling

Common JSON-related errors:

```go
// ❌ Wrong: Field doesn't exist in schema
user.invalid_json_field.key == "value"
// Error: unknown field 'invalid_json_field'

// ✅ Correct: Use fields defined in schema
user.preferences.key == "value"

// ❌ Wrong: Type mismatch
user.preferences.count == "5"  // count is number, not string
// May work but inefficient

// ✅ Correct: Use proper type conversion
int(user.preferences.count) == 5
```

## See Also

- [Getting Started Guide](getting-started.md)
- [Array Comprehensions](comprehensions.md)
- [Operators Reference](operators-reference.md)
