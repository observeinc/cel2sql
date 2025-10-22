# Logging Example

This example demonstrates how to enable structured logging in cel2sql using Go's standard `log/slog` package.

## Overview

cel2sql supports optional structured logging that provides visibility into:
- JSON path detection decisions
- Comprehension type identification
- Schema lookups and field type detection
- Performance metrics (conversion duration)
- Regex pattern conversions (RE2 to POSIX)
- Operator mapping decisions
- Error contexts

## Running the Example

```bash
go run main.go
```

## Features Demonstrated

### 1. JSON Handler (Machine-Readable Logs)

The first example uses `slog.NewJSONHandler` for structured JSON output:

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelDebug,
}))
```

This is ideal for:
- Production environments
- Log aggregation systems (Elasticsearch, Splunk, etc.)
- Automated parsing and analysis

### 2. Text Handler (Human-Readable Logs)

The second example uses `slog.NewTextHandler` for human-friendly output:

```go
textLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelDebug,
}))
```

This is ideal for:
- Development and debugging
- Console output
- Quick troubleshooting

## Log Levels

cel2sql uses two log levels:

- **Debug**: Detailed conversion steps, operator mappings, schema lookups
- **Error**: Conversion failures with full context

## Zero Overhead

When logging is not enabled (the default), cel2sql uses `slog.DiscardHandler` which provides zero performance overhead.

```go
// Without logger - no overhead
sql, err := cel2sql.Convert(ast)

// With logger - detailed observability
sql, err := cel2sql.Convert(ast, cel2sql.WithLogger(logger))
```

## Log Events

The example will show logs for:

1. **JSON Path Detection**: When `metadata.active` is detected as a JSON field access
2. **Schema Lookups**: Field type checks for `metadata` and `tags`
3. **Comprehension Analysis**: The `exists()` comprehension on the `tags` array
4. **Regex Conversion**: The RE2 pattern `(?i)admin.*` converted to POSIX
5. **Operator Mapping**: CEL operators like `==` and `&&` converted to SQL
6. **Performance**: Total conversion duration

## Integration with Existing Logging

If your application already uses slog, you can pass your existing logger:

```go
// Use application logger
sql, err := cel2sql.Convert(ast,
    cel2sql.WithSchemas(schemas),
    cel2sql.WithLogger(appLogger))
```

## Custom Handlers

You can use any slog handler, including custom handlers:

```go
// Example with custom handler that filters by subsystem
type FilteredHandler struct {
    handler slog.Handler
}

customLogger := slog.New(&FilteredHandler{
    handler: slog.NewJSONHandler(os.Stdout, nil),
})

sql, err := cel2sql.Convert(ast, cel2sql.WithLogger(customLogger))
```

## See Also

- [Go slog documentation](https://pkg.go.dev/log/slog)
- [cel2sql documentation](../../README.md)
- [Context example](../context/) for timeout and cancellation support
