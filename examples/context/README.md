# Context Usage Example

This example demonstrates how to use context with cel2sql for cancellation and timeout support.

## Features

- **Backward Compatible**: Works without context (existing code continues to work)
- **Timeout Support**: Set deadlines for conversion operations
- **Cancellation**: Ability to cancel long-running conversions
- **Multiple Options**: Combine context with other options like schemas

## Running the Example

```bash
cd examples/context
go run main.go
```

## Usage Patterns

### Without Context (Backward Compatible)

Existing code continues to work without any changes:

```go
sql, err := cel2sql.Convert(ast)
```

### With Timeout

Protect against long-running conversions:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

sql, err := cel2sql.Convert(ast, cel2sql.WithContext(ctx))
```

### With Cancellation

Allow conversions to be cancelled:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

// Later, in another goroutine or signal handler:
cancel()

sql, err := cel2sql.Convert(ast, cel2sql.WithContext(ctx))
```

### Combining Options

Use context with other options like schemas:

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

sql, err := cel2sql.Convert(ast,
    cel2sql.WithContext(ctx),
    cel2sql.WithSchemas(schemas))
```

## When to Use Context

### Timeout Protection

Use context with timeout when:
- Converting user-provided CEL expressions
- Processing expressions from external sources
- Running in web services with request timeouts
- Protecting against accidentally complex expressions

### Cancellation Support

Use context with cancellation when:
- Long-running batch operations
- User can cancel operations
- Graceful shutdown scenarios
- Request cancellation in HTTP handlers

## Error Handling

When a context is cancelled or times out, `Convert()` returns an error that wraps `context.Canceled` or `context.DeadlineExceeded`:

```go
sql, err := cel2sql.Convert(ast, cel2sql.WithContext(ctx))
if err != nil {
    if errors.Is(err, context.Canceled) {
        // Handle cancellation
    } else if errors.Is(err, context.DeadlineExceeded) {
        // Handle timeout
    } else {
        // Handle other errors
    }
}
```

## Performance Impact

Context checking is opt-in and has minimal performance impact:
- **No Context**: No overhead (nil check is fast)
- **With Context**: Minimal overhead at recursion points
- Only checks context at key points: `visit()`, `visitCall()`, `visitComprehension()`

## Best Practices

1. **Always use defer cancel()**: Prevent context leaks
   ```go
   ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
   defer cancel()
   ```

2. **Set reasonable timeouts**: Balance between too short (false failures) and too long (no protection)

3. **Handle cancellation gracefully**: Check for context errors and handle appropriately

4. **Combine with schemas**: Use both options for complete functionality
   ```go
   sql, err := cel2sql.Convert(ast,
       cel2sql.WithContext(ctx),
       cel2sql.WithSchemas(schemas))
   ```

## Related

- [Basic Example](../basic/) - Simple conversion without context
- [Load Table Schema Example](../load_table_schema/) - Dynamic schema loading
- [Comprehensions Example](../comprehensions/) - CEL comprehensions support
