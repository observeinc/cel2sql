# Regex Pattern Matching

cel2sql supports CEL's `matches()` function with automatic conversion from RE2 (CEL's regex flavor) to PostgreSQL POSIX regex.

## Quick Start

```go
// Define schema
schema := pg.Schema{
    {Name: "email", Type: "text"},
    {Name: "phone", Type: "text"},
}

// CEL: Email validation
email.matches(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

// Generated SQL:
email ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
```

## Basic Syntax

### Method Style

```go
// CEL: Call matches() on string
field.matches(r"pattern")

// Generated SQL:
field ~ 'pattern'
```

### Function Style

```go
// CEL: Use matches() as function
matches(field, r"pattern")

// Generated SQL:
field ~ 'pattern'
```

## PostgreSQL Operators

| Operator | Description | CEL Example |
|----------|-------------|-------------|
| `~` | Case-sensitive match | `field.matches(r"pattern")` |
| `~*` | Case-insensitive match | `field.matches(r"(?i)pattern")` |
| `!~` | Case-sensitive non-match | `!field.matches(r"pattern")` |
| `!~*` | Case-insensitive non-match | `!field.matches(r"(?i)pattern")` |

## Common Patterns

### Email Validation

```go
// Simple email pattern
email.matches(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
// SQL: email ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

// Email from specific domain
email.matches(r"^.*@company\.com$")
// SQL: email ~ '^.*@company\.com$'
```

### Phone Numbers

```go
// US phone number (123-456-7890)
phone.matches(r"^\d{3}-\d{3}-\d{4}$")
// SQL: phone ~ '^[[:digit:]]{3}-[[:digit:]]{3}-[[:digit:]]{4}$'

// International format (+1-234-567-8900)
phone.matches(r"^\+\d{1,3}-\d{3}-\d{3}-\d{4}$")
// SQL: phone ~ '^\+[[:digit:]]{1,3}-[[:digit:]]{3}-[[:digit:]]{3}-[[:digit:]]{4}$'
```

### URLs

```go
// HTTP/HTTPS URLs
url.matches(r"^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
// SQL: url ~ '^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

// Specific domain
url.matches(r"^https://.*\.example\.com")
// SQL: url ~ '^https://.*\.example\.com'
```

### Usernames

```go
// Alphanumeric, 3-20 characters
username.matches(r"^[a-zA-Z0-9]{3,20}$")
// SQL: username ~ '^[a-zA-Z0-9]{3,20}$'

// Allow underscores and hyphens
username.matches(r"^[a-zA-Z0-9_-]{3,20}$")
// SQL: username ~ '^[a-zA-Z0-9_-]{3,20}$'
```

### Zip Codes

```go
// US ZIP code (12345 or 12345-6789)
zipcode.matches(r"^\d{5}(-\d{4})?$")
// SQL: zipcode ~ '^[[:digit:]]{5}(-[[:digit:]]{4})?$'
```

## Case-Insensitive Matching

Use the `(?i)` flag for case-insensitive matching:

```go
// CEL: Case-insensitive search
description.matches(r"(?i)urgent|priority|important")

// Generated SQL:
description ~* 'urgent|priority|important'
```

## Character Classes

CEL uses RE2 character classes, which are automatically converted:

| RE2 Class | POSIX Class | Description |
|-----------|-------------|-------------|
| `\d` | `[[:digit:]]` | Digits (0-9) |
| `\w` | `[[:alnum:]_]` | Word characters (a-z, A-Z, 0-9, _) |
| `\s` | `[[:space:]]` | Whitespace |
| `\D` | `[^[:digit:]]` | Non-digits |
| `\W` | `[^[:alnum:]_]` | Non-word characters |
| `\S` | `[^[:space:]]` | Non-whitespace |

### Examples

```go
// Digits only
code.matches(r"^\d+$")
// SQL: code ~ '^[[:digit:]]+$'

// Word characters
identifier.matches(r"^\w+$")
// SQL: identifier ~ '^[[:alnum:]_]+$'

// Contains whitespace
text.matches(r".*\s+.*")
// SQL: text ~ '.*[[:space:]]+.*'
```

## Word Boundaries

```go
// Match whole word "test"
text.matches(r"\btest\b")
// SQL: text ~ '\ytest\y'

// Word starting with "pre"
text.matches(r"\bpre\w+")
// SQL: text ~ '\ypre[[:alnum:]_]+'
```

## Anchors

| Anchor | Description | Example |
|--------|-------------|---------|
| `^` | Start of string | `^abc` matches "abc..." |
| `$` | End of string | `xyz$` matches "...xyz" |
| `\b` or `\y` | Word boundary | `\bword\b` matches "word" |

```go
// Start of string
field.matches(r"^Hello")
// SQL: field ~ '^Hello'

// End of string
field.matches(r"\.com$")
// SQL: field ~ '\.com$'

// Exact match
field.matches(r"^exact$")
// SQL: field ~ '^exact$'
```

## Quantifiers

| Quantifier | Description | Example |
|------------|-------------|---------|
| `*` | 0 or more | `a*` |
| `+` | 1 or more | `a+` |
| `?` | 0 or 1 | `a?` |
| `{n}` | Exactly n | `a{3}` |
| `{n,}` | n or more | `a{3,}` |
| `{n,m}` | Between n and m | `a{3,5}` |

```go
// One or more digits
field.matches(r"^\d+$")
// SQL: field ~ '^[[:digit:]]+$'

// 3 to 5 letters
field.matches(r"^[a-z]{3,5}$")
// SQL: field ~ '^[a-z]{3,5}$'

// Optional dash
field.matches(r"^ABC-?\d{4}$")
// SQL: field ~ '^ABC-?[[:digit:]]{4}$'
```

## Groups and Alternation

```go
// Alternation (OR)
field.matches(r"^(cat|dog|bird)$")
// SQL: field ~ '^(cat|dog|bird)$'

// Grouped pattern
field.matches(r"^(Mr|Ms|Dr)\.?\s+[A-Z][a-z]+$")
// SQL: field ~ '^(Mr|Ms|Dr)\.?[[:space:]]+[A-Z][a-z]+$'
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
    // Define schema
    contactSchema := pg.Schema{
        {Name: "id", Type: "bigint"},
        {Name: "email", Type: "text"},
        {Name: "phone", Type: "text"},
        {Name: "website", Type: "text"},
    }

    provider := pg.NewTypeProvider(map[string]pg.Schema{
        "Contact": contactSchema,
    })

    env, err := cel.NewEnv(
        cel.CustomTypeProvider(provider),
        cel.Variable("contact", cel.ObjectType("Contact")),
    )
    if err != nil {
        log.Fatal(err)
    }

    // Example 1: Email validation
    ast, _ := env.Compile(`contact.email.matches(r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$")`)
    sql, _ := cel2sql.Convert(ast)
    fmt.Println("Email:", sql)
    // Output: contact.email ~ '^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'

    // Example 2: Phone validation
    ast, _ = env.Compile(`contact.phone.matches(r"^\d{3}-\d{3}-\d{4}$")`)
    sql, _ = cel2sql.Convert(ast)
    fmt.Println("Phone:", sql)
    // Output: contact.phone ~ '^[[:digit:]]{3}-[[:digit:]]{3}-[[:digit:]]{4}$'

    // Example 3: Case-insensitive domain check
    ast, _ = env.Compile(`contact.website.matches(r"(?i).*\.com$")`)
    sql, _ = cel2sql.Convert(ast)
    fmt.Println("Website:", sql)
    // Output: contact.website ~* '.*\.com$'

    // Example 4: Combined conditions
    ast, _ = env.Compile(`
        contact.email.matches(r"@company\.com$") &&
        contact.phone.matches(r"^\+1-")
    `)
    sql, _ = cel2sql.Convert(ast)
    fmt.Println("Combined:", sql)
    // Output: contact.email ~ '@company\.com$' AND contact.phone ~ '^\+1-'
}
```

## Performance Tips

### 1. Use Anchors

```go
// ✅ Better: Anchored pattern
field.matches(r"^pattern$")

// ❌ Slower: Unanchored pattern
field.matches(r"pattern")
```

### 2. Keep Patterns Simple

```go
// ✅ Better: Simple pattern
field.matches(r"^[a-z]+$")

// ❌ Slower: Complex nested groups
field.matches(r"^((([a-z])+)+)+$")  // Catastrophic backtracking risk
```

### 3. Use Indexes When Possible

For frequently searched patterns, consider PostgreSQL indexes:

```sql
-- GIN index for trigram search (pg_trgm extension)
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX idx_field_trgm ON table USING GIN (field gin_trgm_ops);
```

## Common Patterns Reference

### Validation Patterns

```go
// Email
r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

// URL
r"^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"

// IPv4 Address
r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"

// Credit Card (basic)
r"^\d{4}-?\d{4}-?\d{4}-?\d{4}$"

// Date (YYYY-MM-DD)
r"^\d{4}-\d{2}-\d{2}$"

// Time (HH:MM:SS)
r"^\d{2}:\d{2}:\d{2}$"

// Hex Color
r"^#[0-9A-Fa-f]{6}$"

// UUID
r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
```

## Escaping Special Characters

Remember to escape regex special characters:

```go
// Escape dots for literal match
field.matches(r"example\.com")  // Literal dot
field.matches(r"example.com")   // Any character

// Common characters to escape: . * + ? ^ $ { } ( ) [ ] | \
```

## Error Handling

```go
// Invalid regex pattern
field.matches(r"[invalid")
// Error during compilation: invalid regex pattern

// Type mismatch
number_field.matches(r"pattern")
// Error: matches() requires string type

// Missing raw string prefix
field.matches("pattern")  // Works but CEL may interpret escapes
field.matches(r"pattern") // Better: raw string
```

## See Also

- [Getting Started Guide](getting-started.md)
- [Operators Reference](operators-reference.md)
- [PostgreSQL Regex Documentation](https://www.postgresql.org/docs/current/functions-matching.html)
