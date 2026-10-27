// Copyright 2026 Observe, Inc. Licensed under Apache 2.0.
// Tests for comprehension variable scoping, added by Observe, Inc.
package cel2sql_test

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/observeinc/cel2sql/v3"
	dialectpkg "github.com/observeinc/cel2sql/v3/dialect"
	bigqueryDialect "github.com/observeinc/cel2sql/v3/dialect/bigquery"
	duckdbDialect "github.com/observeinc/cel2sql/v3/dialect/duckdb"
	mysqlDialect "github.com/observeinc/cel2sql/v3/dialect/mysql"
	sqliteDialect "github.com/observeinc/cel2sql/v3/dialect/sqlite"
)

// scopeAliases maps each variable to a column of another name, as a caller that
// aliases its filter fields onto view columns does.
var scopeAliases = map[string]string{"name": "obj_name", "tags": "obj_tags", "items": "obj_items"}

func newScopeEnv(t *testing.T) *cel.Env {
	t.Helper()
	env, err := cel.NewEnv(
		cel.Variable("name", cel.StringType),
		cel.Variable("tags", cel.MapType(cel.StringType, cel.ListType(cel.StringType))),
		cel.Variable("items", cel.ListType(cel.StringType)),
	)
	require.NoError(t, err)
	return env
}

func convertScoped(t *testing.T, expr string, opts ...cel2sql.ConvertOption) (*cel2sql.Result, error) {
	t.Helper()
	ast, issues := newScopeEnv(t).Compile(expr)
	require.NoError(t, issues.Err())
	opts = append([]cel2sql.ConvertOption{
		cel2sql.WithColumnAliases(scopeAliases),
		cel2sql.WithJSONVariables("tags"),
	}, opts...)
	return cel2sql.ConvertParameterized(ast, opts...)
}

func TestComprehensionScope_PostgreSQL(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		wantSQL  string
		wantArgs []any
	}{
		{
			// The variable shadows the field: name is each key, not obj_name.
			name:     "variable named like a field",
			expr:     `tags.exists(name, name == "team")`,
			wantSQL:  `EXISTS (SELECT 1 FROM jsonb_object_keys(obj_tags) AS name(name) WHERE name = $1)`,
			wantArgs: []any{"team"},
		},
		{
			name:     "variable named like a field, over an array column",
			expr:     `items.exists(name, name == "x")`,
			wantSQL:  `EXISTS (SELECT 1 FROM UNNEST(obj_items) AS name WHERE name = $1)`,
			wantArgs: []any{"x"},
		},
		{
			// Written as obj_name the variable would capture the field name reads,
			// so it is written obj_name_.
			name:     "variable named like an aliased column",
			expr:     `tags.exists(obj_name, obj_name == "team" && name == "x")`,
			wantSQL:  `EXISTS (SELECT 1 FROM jsonb_object_keys(obj_tags) AS obj_name_(obj_name_) WHERE obj_name_ = $1 AND obj_name = $2)`,
			wantArgs: []any{"team", "x"},
		},
		{
			name: "variable named like an aliased column, in a select list",
			expr: `size(items.filter(obj_name, obj_name != name)) > 0`,
			wantSQL: `COALESCE(ARRAY_LENGTH(ARRAY(SELECT obj_name_ FROM UNNEST(obj_items) AS obj_name_ ` +
				`WHERE obj_name_ != obj_name), 1), 0) > $1`,
			wantArgs: []any{int64(0)},
		},
		{
			name:    "variable named like an aliased column, exists_one",
			expr:    `items.exists_one(obj_name, obj_name == name)`,
			wantSQL: `(SELECT COUNT(*) FROM UNNEST(obj_items) AS obj_name_ WHERE obj_name_ = obj_name) = 1`,
		},
		{
			// The range is outside the variable's scope, so tags there is the
			// variable tags, aliased, and only the predicate's tags is the element.
			name: "range not in the variable's scope",
			expr: `tags["env"].exists(tags, tags == "prod")`,
			wantSQL: `CASE WHEN jsonb_typeof(obj_tags->'env') = 'array' THEN ` +
				`EXISTS (SELECT 1 FROM jsonb_array_elements_text(obj_tags->'env') AS tags(tags) WHERE tags = $1) END`,
			wantArgs: []any{"prod"},
		},
		{
			name:    "an enclosing variable read from a nested comprehension",
			expr:    `items.exists(name, tags.exists(k, k == name))`,
			wantSQL: `EXISTS (SELECT 1 FROM UNNEST(obj_items) AS name WHERE EXISTS (SELECT 1 FROM jsonb_object_keys(obj_tags) AS k(k) WHERE k = name))`,
		},
		{
			// The inner variable may not reuse the name the outer one is written as.
			name: "a nested variable named like the outer one's SQL name",
			expr: `items.exists(obj_name, items.exists(obj_name_, obj_name_ == obj_name))`,
			wantSQL: `EXISTS (SELECT 1 FROM UNNEST(obj_items) AS obj_name_ WHERE ` +
				`EXISTS (SELECT 1 FROM UNNEST(obj_items) AS obj_name__ WHERE obj_name__ = obj_name_))`,
		},
		{
			name:     "transform of a variable named like a field",
			expr:     `items.map(name, name + "!").exists(v, v == "a!")`,
			wantSQL:  `EXISTS (SELECT 1 FROM UNNEST(ARRAY(SELECT name || $1 FROM UNNEST(obj_items) AS name)) AS v WHERE v = $2)`,
			wantArgs: []any{"!", "a!"},
		},
		{
			name:    "a variable that collides with nothing is written as before",
			expr:    `items.exists(i, i == name)`,
			wantSQL: `EXISTS (SELECT 1 FROM UNNEST(obj_items) AS i WHERE i = obj_name)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertScoped(t, tt.expr)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSQL, result.SQL)
			if tt.wantArgs != nil {
				assert.Equal(t, tt.wantArgs, result.Parameters)
			}
		})
	}
}

// The scoping does not depend on the dialect: no dialect aliases an iteration
// variable or lets one capture an aliased column.
func TestComprehensionScope_OtherDialects(t *testing.T) {
	for _, d := range []dialectpkg.Dialect{mysqlDialect.New(), sqliteDialect.New(), duckdbDialect.New(), bigqueryDialect.New()} {
		t.Run(string(d.Name()), func(t *testing.T) {
			ast, issues := newScopeEnv(t).Compile(`items.exists(name, name == "x")`)
			require.NoError(t, issues.Err())
			sql, err := cel2sql.Convert(ast, cel2sql.WithDialect(d), cel2sql.WithColumnAliases(scopeAliases))
			require.NoError(t, err)
			assert.Contains(t, sql, "name = 'x'")
			assert.NotContains(t, sql, "obj_name", "the variable must not be aliased")

			ast, issues = newScopeEnv(t).Compile(`items.exists(obj_name, obj_name == name)`)
			require.NoError(t, issues.Err())
			sql, err = cel2sql.Convert(ast, cel2sql.WithDialect(d), cel2sql.WithColumnAliases(scopeAliases))
			require.NoError(t, err)
			assert.Contains(t, sql, "obj_name_ = obj_name", "the variable must not capture the aliased column")
		})
	}
}
