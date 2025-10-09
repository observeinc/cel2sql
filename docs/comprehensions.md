# Array Comprehensions

cel2sql supports CEL comprehensions for powerful array operations. Comprehensions are converted to PostgreSQL-compatible SQL using `UNNEST()` and array functions.

## Overview

CEL comprehensions allow you to:
- Test if all elements satisfy a condition (`all()`)
- Check if any element satisfies a condition (`exists()`)
- Count exact matches (`exists_one()`)
- Filter arrays (`filter()`)
- Transform arrays (`map()`)

## Supported Comprehension Types

| CEL Expression | Description | SQL Pattern |
|----------------|-------------|-------------|
| `list.all(x, condition)` | All elements satisfy condition | `NOT EXISTS (SELECT 1 FROM UNNEST(list) AS x WHERE NOT (condition))` |
| `list.exists(x, condition)` | At least one element satisfies condition | `EXISTS (SELECT 1 FROM UNNEST(list) AS x WHERE condition)` |
| `list.exists_one(x, condition)` | Exactly one element satisfies condition | `(SELECT COUNT(*) FROM UNNEST(list) AS x WHERE condition) = 1` |
| `list.filter(x, condition)` | Return elements that satisfy condition | `ARRAY(SELECT x FROM UNNEST(list) AS x WHERE condition)` |
| `list.map(x, transform)` | Transform all elements | `ARRAY(SELECT transform FROM UNNEST(list) AS x)` |

## Basic Examples

### all() - Check All Elements

```go
// Define schema with array field
schema := pg.Schema{
    {Name: "scores", Type: "integer", Repeated: true},
}

// CEL: Check if all scores are passing
scores.all(s, s >= 60)

// Generated SQL:
// NOT EXISTS (SELECT 1 FROM UNNEST(scores) AS s WHERE NOT (s >= 60))
```

### exists() - Check Any Element

```go
// CEL: Check if any score is excellent
scores.exists(s, s >= 90)

// Generated SQL:
// EXISTS (SELECT 1 FROM UNNEST(scores) AS s WHERE s >= 90)
```

### exists_one() - Check Exactly One

```go
// CEL: Check if exactly one score is perfect
scores.exists_one(s, s == 100)

// Generated SQL:
// (SELECT COUNT(*) FROM UNNEST(scores) AS s WHERE s = 100) = 1
```

### filter() - Filter Elements

```go
// CEL: Get only passing scores
scores.filter(s, s >= 60)

// Generated SQL:
// ARRAY(SELECT s FROM UNNEST(scores) AS s WHERE s >= 60)
```

### map() - Transform Elements

```go
// CEL: Double all scores
scores.map(s, s * 2)

// Generated SQL:
// ARRAY(SELECT s * 2 FROM UNNEST(scores) AS s)
```

## Working with Structured Arrays

### Schema Definition

```go
// Define employee schema with array of skills
employeeSchema := pg.Schema{
    {Name: "id", Type: "bigint"},
    {Name: "name", Type: "text"},
    {Name: "age", Type: "integer"},
    {Name: "active", Type: "boolean"},
    {Name: "skills", Type: "text", Repeated: true},
    {Name: "scores", Type: "integer", Repeated: true},
}

provider := pg.NewTypeProvider(map[string]pg.Schema{
    "Employee": employeeSchema,
})
```

### Comprehension Examples

```go
// Check if all employees are adults
employees.all(e, e.age >= 18)
// SQL: NOT EXISTS (SELECT 1 FROM UNNEST(employees) AS e WHERE NOT (e.age >= 18))

// Find if any employee has Go skills
employees.exists(e, e.skills.exists(s, s == "Go"))
// SQL: EXISTS (SELECT 1 FROM UNNEST(employees) AS e WHERE EXISTS (SELECT 1 FROM UNNEST(e.skills) AS s WHERE s = 'Go'))

// Get active employees
employees.filter(e, e.active)
// SQL: ARRAY(SELECT e FROM UNNEST(employees) AS e WHERE e.active IS TRUE)

// Get all employee names
employees.map(e, e.name)
// SQL: ARRAY(SELECT e.name FROM UNNEST(employees) AS e)
```

## Chaining Comprehensions

You can chain multiple comprehensions for complex operations:

```go
// Get names of active employees with high scores
employees.filter(e, e.active && e.scores.all(s, s >= 80)).map(e, e.name)

// Generated SQL:
// ARRAY(SELECT e.name FROM UNNEST(
//     ARRAY(SELECT e FROM UNNEST(employees) AS e
//           WHERE e.active IS TRUE
//           AND NOT EXISTS (SELECT 1 FROM UNNEST(e.scores) AS s WHERE NOT (s >= 80)))
// ) AS e)
```

## Nested Comprehensions

Comprehensions can be nested to handle complex data structures:

```go
// Define nested schema
addressSchema := pg.Schema{
    {Name: "city", Type: "text"},
    {Name: "country", Type: "text"},
}

employeeSchema := pg.Schema{
    {Name: "name", Type: "text"},
    {Name: "address", Type: "composite", Schema: addressSchema},
    {Name: "skills", Type: "text", Repeated: true},
}

// Find employees in New York with Go skills
employees.filter(e, e.address.city == "New York" && e.skills.exists(s, s == "Go"))

// SQL: ARRAY(SELECT e FROM UNNEST(employees) AS e
//            WHERE e.address.city = 'New York'
//            AND EXISTS (SELECT 1 FROM UNNEST(e.skills) AS s WHERE s = 'Go'))
```

## JSON Array Comprehensions

cel2sql supports comprehensions on JSON/JSONB arrays:

```go
// Schema with JSONB array
userSchema := pg.Schema{
    {Name: "id", Type: "bigint"},
    {Name: "tags", Type: "jsonb"},  // JSON array
}

// Check if user has "developer" tag
user.tags.exists(tag, tag == "developer")

// Generated SQL:
// EXISTS (SELECT 1 FROM jsonb_array_elements_text(user.tags) AS tag
//         WHERE user.tags IS NOT NULL
//         AND jsonb_typeof(user.tags) = 'array'
//         AND tag = 'developer')
```

## Complex Examples

### Example 1: High-Performing Team

```go
// Find teams where all members have scores above 80
teams.filter(t, t.members.all(m, m.score > 80))
```

### Example 2: Mixed Skills

```go
// Find employees who know both Go and JavaScript
employees.filter(e,
    e.skills.exists(s, s == "Go") &&
    e.skills.exists(s, s == "JavaScript")
)
```

### Example 3: Complex Filtering and Mapping

```go
// Get emails of active users with admin role
users
    .filter(u, u.active && u.roles.exists(r, r == "admin"))
    .map(u, u.email)
```

## Performance Considerations

### 1. UNNEST with Large Arrays
PostgreSQL's `UNNEST()` is efficient, but consider:
- Add indexes on array columns for better performance
- Use partial indexes for frequently filtered values
- Monitor query plans with `EXPLAIN ANALYZE`

### 2. Nested Comprehensions
Nested comprehensions can generate complex SQL:
- Consider denormalizing data for frequently accessed patterns
- Use materialized views for expensive nested queries
- Cache results when appropriate

### 3. Map Operations
`map()` creates new arrays:
- Be mindful of memory usage with large arrays
- Consider streaming results in application code
- Use `LIMIT` when possible

## Best Practices

1. **Keep comprehensions simple** - Complex nested logic is hard to maintain
2. **Test with real data** - Verify performance with production-like datasets
3. **Use appropriate indexes** - Index columns used in comprehension predicates
4. **Monitor performance** - Use PostgreSQL's query analysis tools
5. **Consider alternatives** - Sometimes a JOIN is more efficient than a comprehension

## Error Handling

Common errors with comprehensions:

```go
// ❌ Wrong: Using field that doesn't exist
employees.filter(e, e.invalid_field == "value")
// Error: unknown field 'invalid_field'

// ✅ Correct: Use defined schema fields
employees.filter(e, e.name == "John")

// ❌ Wrong: Type mismatch
scores.filter(s, s == "high")  // scores are integers
// Error: type mismatch

// ✅ Correct: Use correct types
scores.filter(s, s > 80)
```

## See Also

- [Getting Started Guide](getting-started.md)
- [JSON/JSONB Support](json-support.md)
- [Operators Reference](operators-reference.md)
