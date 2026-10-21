// Copyright 2026 Observe, Inc. Licensed under Apache 2.0.
// Tests for list- and map-valued JSON variables, added by Observe, Inc.
package cel2sql_test

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/observeinc/cel2sql/v3"
	dialectpkg "github.com/observeinc/cel2sql/v3/dialect"
	bigqueryDialect "github.com/observeinc/cel2sql/v3/dialect/bigquery"
	duckdbDialect "github.com/observeinc/cel2sql/v3/dialect/duckdb"
	mysqlDialect "github.com/observeinc/cel2sql/v3/dialect/mysql"
	postgresDialect "github.com/observeinc/cel2sql/v3/dialect/postgres"
	sqliteDialect "github.com/observeinc/cel2sql/v3/dialect/sqlite"
)

// newJSONContainerEnv declares tags with the object-tag shape, a map whose values
// are lists, beside the map<string, string> shape WithJSONVariables started with.
func newJSONContainerEnv(t *testing.T) *cel.Env {
	t.Helper()
	env, err := cel.NewEnv(
		cel.CustomTypeAdapter(types.DefaultTypeAdapter),
		cel.Variable("tags", cel.MapType(cel.StringType, cel.ListType(cel.StringType))),
		cel.Variable("counts", cel.MapType(cel.StringType, cel.ListType(cel.IntType))),
		cel.Variable("context", cel.MapType(cel.StringType, cel.StringType)),
		cel.Variable("name", cel.StringType),
	)
	require.NoError(t, err)
	return env
}

func convertJSONContainer(t *testing.T, expr string, opts ...cel2sql.ConvertOption) (*cel2sql.Result, error) {
	t.Helper()
	ast, issues := newJSONContainerEnv(t).Compile(expr)
	require.NoError(t, issues.Err())
	opts = append([]cel2sql.ConvertOption{cel2sql.WithJSONVariables("tags", "counts", "context")}, opts...)
	return cel2sql.ConvertParameterized(ast, opts...)
}

// guardEnv is the PostgreSQL array guard the converter wraps a read of tags.env in.
func guardEnv(body string) string {
	return `CASE WHEN jsonb_typeof(tags->'env') = 'array' THEN ` + body + ` END`
}

func TestJSONVariableContainers_PostgreSQL(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		wantSQL  string
		wantArgs []any
	}{
		{name: "key in map", expr: `"env" in tags`, wantSQL: `tags ? 'env'`},
		{name: "has on a key", expr: `has(tags.env)`, wantSQL: `tags ? 'env'`},
		{name: "non-identifier key in map", expr: `"k8s.cluster" in tags`, wantSQL: `tags ? 'k8s.cluster'`},
		{
			name:     "value in list, bracket",
			expr:     `"prod" in tags["env"]`,
			wantSQL:  guardEnv(`(tags->'env') ? ($1)`),
			wantArgs: []any{"prod"},
		},
		{
			name:     "value in list, dot",
			expr:     `"prod" in tags.env`,
			wantSQL:  guardEnv(`(tags->'env') ? ($1)`),
			wantArgs: []any{"prod"},
		},
		{
			// The webapp's objectTagFilterQuery.
			name: "key test and values of a list",
			expr: `"env" in tags && tags["env"].exists(v, v == "prod" || v == "dev")`,
			wantSQL: `tags ? 'env' AND ` + guardEnv(
				`EXISTS (SELECT 1 FROM jsonb_array_elements_text(tags->'env') AS v(v) WHERE v = $1 OR v = $2)`),
			wantArgs: []any{"prod", "dev"},
		},
		{
			// The source names its column, or the predicate's tags would be the
			// variable rather than the element.
			name:     "iteration variable named like the variable",
			expr:     `tags.env.exists(tags, tags == "prod")`,
			wantSQL:  guardEnv(`EXISTS (SELECT 1 FROM jsonb_array_elements_text(tags->'env') AS tags(tags) WHERE tags = $1)`),
			wantArgs: []any{"prod"},
		},
		{
			name:     "a scalar value is still read as text",
			expr:     `context.host == "web-1"`,
			wantSQL:  `context->>'host' = $1`,
			wantArgs: []any{"web-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertJSONContainer(t, tt.expr)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSQL, result.SQL)
			if tt.wantArgs != nil {
				assert.Equal(t, tt.wantArgs, result.Parameters)
			}
		})
	}
}

// TestJSONVariableContainers_Refused pins the uses of a list or map value, or of
// the variable itself, that have no SQL form here. Each used to convert to SQL
// that failed when it ran; now each is a conversion error.
func TestJSONVariableContainers_Refused(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		wantErr error
	}{
		{name: "comparing a list value", expr: `tags.env == ["prod"]`},
		{name: "indexing a list value", expr: `tags["env"][0] == "prod"`},
		{name: "size of a list value", expr: `size(tags["env"]) > 0`},
		{name: "membership in a non-string list", expr: `1 in counts.ids`},
		{name: "a key that is not a literal", expr: `name in tags`, wantErr: cel2sql.ErrInvalidFieldName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := convertJSONContainer(t, tt.expr)
			require.Error(t, err)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

// TestJSONVariableContainers_OtherDialects pins that a dialect without
// dialect.JSONContainerWriter takes none of the PostgreSQL forms: nothing is
// refused that it converted before, and nothing it writes uses jsonb.
func TestJSONVariableContainers_OtherDialects(t *testing.T) {
	containerForms := []string{
		`"env" in tags`,
		`has(tags.env)`,
		`"prod" in tags.env`,
		`tags.env.exists(v, v == "prod")`,
	}

	for _, d := range []dialectpkg.Dialect{mysqlDialect.New(), sqliteDialect.New(), duckdbDialect.New(), bigqueryDialect.New()} {
		t.Run(string(d.Name()), func(t *testing.T) {
			_, ok := dialectpkg.GetJSONContainerWriter(d)
			require.False(t, ok, "a dialect that opts in needs its own integration test before this one changes")

			for _, expr := range containerForms {
				ast, issues := newJSONContainerEnv(t).Compile(expr)
				require.NoError(t, issues.Err())
				// Convert inlines every value, so a ? in the SQL can only be the jsonb
				// operator and never this dialect's placeholder.
				sql, err := cel2sql.Convert(ast, cel2sql.WithDialect(d), cel2sql.WithJSONVariables("tags", "counts", "context"))
				require.NoError(t, err, expr)
				assert.NotContains(t, sql, " ? ", expr)
				assert.NotContains(t, sql, "jsonb", expr)
			}

			_, err := convertJSONContainer(t, `context.host == "web-1"`, cel2sql.WithDialect(d))
			assert.NoError(t, err, "a scalar read needs no container support")
		})
	}

	t.Run("postgresql opts in", func(t *testing.T) {
		_, ok := dialectpkg.GetJSONContainerWriter(postgresDialect.New())
		require.True(t, ok)
	})
}
