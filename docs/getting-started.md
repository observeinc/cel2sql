# Getting Started with cel2sql

This guide will help you get up and running with cel2sql in just a few minutes.

## What is cel2sql?

cel2sql converts CEL (Common Expression Language) expressions into PostgreSQL SQL. This is useful when you want to:

- Build dynamic filters for database queries
- Allow users to create custom filters safely
- Write type-safe query conditions
- Avoid SQL injection vulnerabilities

## Installation

```bash
go get github.com/spandigital/cel2sql/v2
```

## Your First cel2sql Program

Let's build a simple program that converts CEL expressions to SQL:

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
    // Step 1: Define your table schema
    // This matches your PostgreSQL table structure
    productSchema := pg.Schema{
        {Name: "id", Type: "bigint"},
        {Name: "name", Type: "text"},
        {Name: "price", Type: "double precision"},
        {Name: "in_stock", Type: "boolean"},
        {Name: "category", Type: "text"},
    }

    // Step 2: Create a type provider
    // This tells CEL about your database types
    provider := pg.NewTypeProvider(map[string]pg.Schema{
        "Product": productSchema,
    })

    // Step 3: Create CEL environment
    env, err := cel.NewEnv(
        cel.CustomTypeProvider(provider),
        cel.Variable("product", cel.ObjectType("Product")),
    )
    if err != nil {
        log.Fatalf("Failed to create CEL environment: %v", err)
    }

    // Step 4: Compile CEL expression
    celExpression := `product.price < 100 && product.in_stock`
    ast, issues := env.Compile(celExpression)
    if issues != nil && issues.Err() != nil {
        log.Fatalf("CEL compilation failed: %v", issues.Err())
    }

    // Step 5: Convert to SQL
    sqlWhere, err := cel2sql.Convert(ast)
    if err != nil {
        log.Fatalf("Failed to convert to SQL: %v", err)
    }

    // Step 6: Use in your SQL query
    query := "SELECT * FROM products WHERE " + sqlWhere
    fmt.Println("Generated SQL:")
    fmt.Println(query)
    // Output: SELECT * FROM products WHERE product.price < 100 AND product.in_stock IS TRUE
}
```

## Understanding the Steps

### 1. Define Your Schema

The schema tells cel2sql about your database table structure:

```go
schema := pg.Schema{
    {Name: "column_name", Type: "postgresql_type"},
    {Name: "column_name", Type: "postgresql_type", Repeated: true}, // for arrays
}
```

Common PostgreSQL types:
- `"text"` - Text/string columns
- `"bigint"` - Integer columns
- `"double precision"` - Decimal/float columns
- `"boolean"` - Boolean columns
- `"timestamp with time zone"` - Timestamp columns
- `"jsonb"` - JSON columns
- Set `Repeated: true` for array columns

### 2. Create Type Provider

The type provider connects your schema to CEL:

```go
provider := pg.NewTypeProvider(map[string]pg.Schema{
    "TableName": schema,
})
```

You can define multiple tables:

```go
provider := pg.NewTypeProvider(map[string]pg.Schema{
    "User": userSchema,
    "Product": productSchema,
    "Order": orderSchema,
})
```

### 3. Create CEL Environment

The CEL environment is where you define variables and compile expressions:

```go
env, err := cel.NewEnv(
    cel.CustomTypeProvider(provider),
    cel.Variable("product", cel.ObjectType("Product")),
)
```

### 4. Compile Expression

Compile your CEL expression to check for syntax errors:

```go
ast, issues := env.Compile(`product.price < 100`)
if issues != nil && issues.Err() != nil {
    // Handle compilation error
}
```

### 5. Convert to SQL

Convert the compiled CEL expression to SQL:

```go
sqlWhere, err := cel2sql.Convert(ast)
```

## Dynamic Schema Loading

Instead of manually defining schemas, you can load them directly from your PostgreSQL database:

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/google/cel-go/cel"
    "github.com/spandigital/cel2sql/v2"
    "github.com/spandigital/cel2sql/v2/pg"
)

func main() {
    ctx := context.Background()

    // Connect to PostgreSQL and create provider
    connString := "postgres://user:password@localhost:5432/mydb?sslmode=disable"
    provider, err := pg.NewTypeProviderWithConnection(ctx, connString)
    if err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }
    defer provider.Close()

    // Load table schema from database
    err = provider.LoadTableSchema(ctx, "products")
    if err != nil {
        log.Fatalf("Failed to load schema: %v", err)
    }

    // Create CEL environment with loaded schema
    env, err := cel.NewEnv(
        cel.CustomTypeProvider(provider),
        cel.Variable("product", cel.ObjectType("products")),
    )
    if err != nil {
        log.Fatalf("Failed to create environment: %v", err)
    }

    // Now use it like before
    ast, issues := env.Compile(`product.price < 100`)
    if issues != nil && issues.Err() != nil {
        log.Fatalf("Compilation error: %v", issues.Err())
    }

    sqlWhere, err := cel2sql.Convert(ast)
    if err != nil {
        log.Fatalf("Conversion error: %v", err)
    }

    fmt.Println(sqlWhere)
    // Output: product.price < 100
}
```

**Benefits of dynamic schema loading:**
- No manual schema definition needed
- Automatically stays in sync with database changes
- Supports composite types and complex structures
- Easier to maintain

## Common Examples

### Simple Comparisons

```go
// CEL
product.price > 50
// SQL: product.price > 50

// CEL
product.name == "Laptop"
// SQL: product.name = 'Laptop'
```

### Logical Operators

```go
// CEL
product.price < 100 && product.in_stock
// SQL: product.price < 100 AND product.in_stock IS TRUE

// CEL
product.category == "Electronics" || product.category == "Computers"
// SQL: product.category = 'Electronics' OR product.category = 'Computers'
```

### String Operations

```go
// CEL
product.name.startsWith("Mac")
// SQL: product.name LIKE 'Mac%'

// CEL
product.description.contains("wireless")
// SQL: POSITION('wireless' IN product.description) > 0
```

### Date Comparisons

```go
// CEL
product.created_at > timestamp("2024-01-01T00:00:00Z")
// SQL: product.created_at > CAST('2024-01-01T00:00:00Z' AS TIMESTAMP WITH TIME ZONE)
```

### Lists/Arrays

```go
// CEL
"electronics" in product.categories
// SQL: 'electronics' IN UNNEST(product.categories)

// CEL
size(product.tags) > 3
// SQL: ARRAY_LENGTH(product.tags, 1) > 3
```

## Error Handling

Always check for errors at each step:

```go
// Check CEL compilation
ast, issues := env.Compile(expression)
if issues != nil && issues.Err() != nil {
    return fmt.Errorf("CEL compilation error: %w", issues.Err())
}

// Check SQL conversion
sqlWhere, err := cel2sql.Convert(ast)
if err != nil {
    return fmt.Errorf("SQL conversion error: %w", err)
}
```

Common errors:
- **Unknown field**: Field not in schema definition
- **Type mismatch**: Wrong type for operation (e.g., comparing string to number)
- **Syntax error**: Invalid CEL expression syntax

## Next Steps

- Learn about [JSON/JSONB Support](json-support.md)
- Explore [Array Comprehensions](comprehensions.md)
- Try [Regex Matching](regex-matching.md)
- See more [Examples](../examples/)

## Best Practices

1. **Always validate user input** before compiling CEL expressions
2. **Use type providers** to catch type errors early
3. **Test your expressions** with sample data
4. **Use dynamic schema loading** for production applications
5. **Handle errors gracefully** with proper error messages
