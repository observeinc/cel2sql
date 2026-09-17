// Copyright 2026 Observe, Inc. Licensed under Apache 2.0.
// Tests for WithJSONVariables option added by Observe, Inc.
package cel2sql_test

import (
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/observeinc/cel2sql/v3"
)

func TestWithJSONVariables_DotNotation(t *testing.T) {
	env, err := cel.NewEnv(
		cel.CustomTypeAdapter(types.DefaultTypeAdapter),
		cel.Variable("status", cel.StringType),
		cel.Variable("tags", cel.MapType(cel.StringType, cel.StringType)),
	)
	require.NoError(t, err)

	tests := []struct {
		name     string
		expr     string
		wantSQL  string
		wantArgs []any
	}{
		{
			name:     "equality",
			expr:     `tags.color == "blue"`,
			wantSQL:  `tags->>'color' = $1`,
			wantArgs: []any{"blue"},
		},
		{
			name:     "not equal",
			expr:     `tags.color != "red"`,
			wantSQL:  `tags->>'color' != $1`,
			wantArgs: []any{"red"},
		},
		{
			name:     "contains",
			expr:     `tags.color.contains("lu")`,
			wantSQL:  `POSITION($1 IN tags->>'color') > 0`,
			wantArgs: []any{"lu"},
		},
		{
			name:     "startsWith",
			expr:     `tags.color.startsWith("bl")`,
			wantSQL:  `tags->>'color' LIKE 'bl%' ESCAPE E'\\'`,
			wantArgs: nil,
		},
		{
			name:     "endsWith",
			expr:     `tags.color.endsWith("ue")`,
			wantSQL:  `tags->>'color' LIKE '%ue' ESCAPE E'\\'`,
			wantArgs: nil,
		},
		{
			name:     "combined with flat variable",
			expr:     `status == "ok" && tags.color == "blue"`,
			wantSQL:  `status = $1 AND tags->>'color' = $2`,
			wantArgs: []any{"ok", "blue"},
		},
		{
			name:     "multiple keys",
			expr:     `tags.color == "blue" && tags.shape == "circle"`,
			wantSQL:  `tags->>'color' = $1 AND tags->>'shape' = $2`,
			wantArgs: []any{"blue", "circle"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, issues := env.Compile(tt.expr)
			require.NoError(t, issues.Err())

			result, err := cel2sql.ConvertParameterized(ast, cel2sql.WithJSONVariables("tags"))
			require.NoError(t, err)

			assert.Equal(t, tt.wantSQL, result.SQL)
			if tt.wantArgs != nil {
				assert.Equal(t, tt.wantArgs, result.Parameters)
			}
		})
	}
}

func TestWithJSONVariables_BracketNotation(t *testing.T) {
	env, err := cel.NewEnv(
		cel.CustomTypeAdapter(types.DefaultTypeAdapter),
		cel.Variable("metadata", cel.MapType(cel.StringType, cel.StringType)),
	)
	require.NoError(t, err)

	tests := []struct {
		name     string
		expr     string
		wantSQL  string
		wantArgs []any
	}{
		{
			name:     "equality",
			expr:     `metadata["key"] == "value"`,
			wantSQL:  `metadata->>'key' = $1`,
			wantArgs: []any{"value"},
		},
		{
			name:     "contains",
			expr:     `metadata["key"].contains("val")`,
			wantSQL:  `POSITION($1 IN metadata->>'key') > 0`,
			wantArgs: []any{"val"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, issues := env.Compile(tt.expr)
			require.NoError(t, issues.Err())

			result, err := cel2sql.ConvertParameterized(ast, cel2sql.WithJSONVariables("metadata"))
			require.NoError(t, err)

			assert.Equal(t, tt.wantSQL, result.SQL)
			if tt.wantArgs != nil {
				assert.Equal(t, tt.wantArgs, result.Parameters)
			}
		})
	}
}

// TestWithJSONVariables_NonIdentifierKeys covers keys that are not SQL
// identifiers. A JSON object key is data, and bracket notation is the only way to
// name one holding a space, a hyphen or a dot, so these have to convert: the key
// reaches SQL as a quoted operand, never as a bare column name.
func TestWithJSONVariables_NonIdentifierKeys(t *testing.T) {
	env, err := cel.NewEnv(
		cel.CustomTypeAdapter(types.DefaultTypeAdapter),
		cel.Variable("metadata", cel.MapType(cel.StringType, cel.StringType)),
	)
	require.NoError(t, err)

	tests := []struct {
		name     string
		expr     string
		wantSQL  string
		wantArgs []any
	}{
		{
			name:     "space",
			expr:     `metadata["has space"] == "value"`,
			wantSQL:  `metadata->>'has space' = $1`,
			wantArgs: []any{"value"},
		},
		{
			name:     "hyphen",
			expr:     `metadata["pod-name"] == "web-1"`,
			wantSQL:  `metadata->>'pod-name' = $1`,
			wantArgs: []any{"web-1"},
		},
		{
			name:     "dotted key is one key, not a nested path",
			expr:     `metadata["k8s.pod.name"] == "web-1"`,
			wantSQL:  `metadata->>'k8s.pod.name' = $1`,
			wantArgs: []any{"web-1"},
		},
		{
			name:     "leading digit",
			expr:     `metadata["2fa"] == "on"`,
			wantSQL:  `metadata->>'2fa' = $1`,
			wantArgs: []any{"on"},
		},
		{
			name:     "reserved SQL keyword",
			expr:     `metadata["select"] == "all"`,
			wantSQL:  `metadata->>'select' = $1`,
			wantArgs: []any{"all"},
		},
		{
			name:     "quote in key is escaped, not injected",
			expr:     `metadata["it's"] == "value"`,
			wantSQL:  `metadata->>'it''s' = $1`,
			wantArgs: []any{"value"},
		},
		{
			name:     "key longer than a PostgreSQL identifier",
			expr:     `metadata["` + strings.Repeat("k", 80) + `"] == "value"`,
			wantSQL:  `metadata->>'` + strings.Repeat("k", 80) + `' = $1`,
			wantArgs: []any{"value"},
		},
		{
			name:     "contains on a spaced key",
			expr:     `metadata["has space"].contains("alu")`,
			wantSQL:  `POSITION($1 IN metadata->>'has space') > 0`,
			wantArgs: []any{"alu"},
		},
		{
			name:     "double quote in key is JSON-escaped in the operand",
			expr:     `metadata["say \"hi\""] == "value"`,
			wantSQL:  `metadata->>'say "hi"' = $1`,
			wantArgs: []any{"value"},
		},
		{
			// PostgreSQL names the key directly rather than through a path, so a
			// backslash needs no escaping: its literals treat one as data.
			name:     "backslash in key",
			expr:     `metadata["back\\slash"] == "value"`,
			wantSQL:  `metadata->>'back\slash' = $1`,
			wantArgs: []any{"value"},
		},
		{
			name:     "two spaced keys compared to each other",
			expr:     `metadata["a b"] == metadata["c d"]`,
			wantSQL:  `metadata->>'a b' = metadata->>'c d'`,
			wantArgs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, issues := env.Compile(tt.expr)
			require.NoError(t, issues.Err())

			result, err := cel2sql.ConvertParameterized(ast, cel2sql.WithJSONVariables("metadata"))
			require.NoError(t, err)

			assert.Equal(t, tt.wantSQL, result.SQL)
			if tt.wantArgs != nil {
				assert.Equal(t, tt.wantArgs, result.Parameters)
			}
		})
	}
}

// TestWithJSONVariables_IdentifierRulesStillApplyToColumns pins the other half of
// the map-index branch: indexing something that is not a declared JSONB variable
// lowers to <operand>.<field>, where the key does name a column and so still has
// to be an identifier.
func TestWithJSONVariables_IdentifierRulesStillApplyToColumns(t *testing.T) {
	env, err := cel.NewEnv(
		cel.CustomTypeAdapter(types.DefaultTypeAdapter),
		cel.Variable("metadata", cel.MapType(cel.StringType, cel.StringType)),
	)
	require.NoError(t, err)

	ast, issues := env.Compile(`metadata["has space"] == "value"`)
	require.NoError(t, issues.Err())

	// metadata is not declared as a JSONB variable here, so the key would be
	// emitted unquoted as metadata.<key>.
	_, err = cel2sql.ConvertParameterized(ast)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid field name")
}

// TestWithJSONVariables_ComputedKeyRejected keeps the string-literal requirement:
// only a constant key can be rendered into the JSON operand.
func TestWithJSONVariables_ComputedKeyRejected(t *testing.T) {
	env, err := cel.NewEnv(
		cel.CustomTypeAdapter(types.DefaultTypeAdapter),
		cel.Variable("metadata", cel.MapType(cel.StringType, cel.StringType)),
		cel.Variable("wanted", cel.StringType),
	)
	require.NoError(t, err)

	ast, issues := env.Compile(`metadata[wanted] == "value"`)
	require.NoError(t, issues.Err())

	_, err = cel2sql.ConvertParameterized(ast, cel2sql.WithJSONVariables("metadata"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected string literal")
}

func TestWithJSONVariables_MultipleVariables(t *testing.T) {
	env, err := cel.NewEnv(
		cel.CustomTypeAdapter(types.DefaultTypeAdapter),
		cel.Variable("tags", cel.MapType(cel.StringType, cel.StringType)),
		cel.Variable("metadata", cel.MapType(cel.StringType, cel.StringType)),
	)
	require.NoError(t, err)

	ast, issues := env.Compile(`tags.color == "blue" && metadata.source == "api"`)
	require.NoError(t, issues.Err())

	result, err := cel2sql.ConvertParameterized(ast, cel2sql.WithJSONVariables("tags", "metadata"))
	require.NoError(t, err)

	assert.Equal(t, `tags->>'color' = $1 AND metadata->>'source' = $2`, result.SQL)
	assert.Equal(t, []any{"blue", "api"}, result.Parameters)
}

func TestWithJSONVariables_BackwardCompatible(t *testing.T) {
	env, err := cel.NewEnv(
		cel.CustomTypeAdapter(types.DefaultTypeAdapter),
		cel.Variable("data", cel.MapType(cel.StringType, cel.StringType)),
	)
	require.NoError(t, err)

	ast, issues := env.Compile(`data.key == "value"`)
	require.NoError(t, issues.Err())

	result, err := cel2sql.ConvertParameterized(ast)
	require.NoError(t, err)

	assert.Equal(t, `data.key = $1`, result.SQL, "without WithJSONVariables, should produce dot notation")
}

func TestWithJSONVariables_NonParameterized(t *testing.T) {
	env, err := cel.NewEnv(
		cel.CustomTypeAdapter(types.DefaultTypeAdapter),
		cel.Variable("props", cel.MapType(cel.StringType, cel.StringType)),
	)
	require.NoError(t, err)

	ast, issues := env.Compile(`props.key == "value"`)
	require.NoError(t, issues.Err())

	sql, err := cel2sql.Convert(ast, cel2sql.WithJSONVariables("props"))
	require.NoError(t, err)

	assert.Equal(t, `props->>'key' = 'value'`, sql)
}

func TestWithJSONVariables_OnlyDeclaredVarsAffected(t *testing.T) {
	env, err := cel.NewEnv(
		cel.CustomTypeAdapter(types.DefaultTypeAdapter),
		cel.Variable("tags", cel.MapType(cel.StringType, cel.StringType)),
		cel.Variable("other", cel.MapType(cel.StringType, cel.StringType)),
	)
	require.NoError(t, err)

	ast, issues := env.Compile(`tags.color == "blue" && other.key == "val"`)
	require.NoError(t, issues.Err())

	result, err := cel2sql.ConvertParameterized(ast, cel2sql.WithJSONVariables("tags"))
	require.NoError(t, err)

	assert.Equal(t, `tags->>'color' = $1 AND other.key = $2`, result.SQL,
		"only 'tags' should use JSONB operators; 'other' should use dot notation")
}

func TestWithColumnAliases(t *testing.T) {
	env, err := cel.NewEnv(
		cel.CustomTypeAdapter(types.DefaultTypeAdapter),
		cel.Variable("name", cel.StringType),
		cel.Variable("active", cel.BoolType),
	)
	require.NoError(t, err)

	aliases := map[string]string{
		"name":   "tbl_name",
		"active": "tbl_active",
	}

	tests := []struct {
		name     string
		expr     string
		wantSQL  string
		wantArgs []any
	}{
		{
			name:     "simple equality",
			expr:     `name == "Alice"`,
			wantSQL:  `tbl_name = $1`,
			wantArgs: []any{"Alice"},
		},
		{
			name:    "boolean",
			expr:    `active == true`,
			wantSQL: `tbl_active IS TRUE`,
		},
		{
			name:     "string contains",
			expr:     `name.contains("li")`,
			wantSQL:  `POSITION($1 IN tbl_name) > 0`,
			wantArgs: []any{"li"},
		},
		{
			name:     "combined",
			expr:     `name == "Alice" && active == true`,
			wantSQL:  `tbl_name = $1 AND tbl_active IS TRUE`,
			wantArgs: []any{"Alice"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, issues := env.Compile(tt.expr)
			require.NoError(t, issues.Err())

			result, err := cel2sql.ConvertParameterized(ast, cel2sql.WithColumnAliases(aliases))
			require.NoError(t, err)

			assert.Equal(t, tt.wantSQL, result.SQL)
			if tt.wantArgs != nil {
				assert.Equal(t, tt.wantArgs, result.Parameters)
			}
		})
	}
}

func TestWithColumnAliases_CombinedWithJSONVariables(t *testing.T) {
	env, err := cel.NewEnv(
		cel.CustomTypeAdapter(types.DefaultTypeAdapter),
		cel.Variable("status", cel.StringType),
		cel.Variable("tags", cel.MapType(cel.StringType, cel.StringType)),
	)
	require.NoError(t, err)

	ast, issues := env.Compile(`status == "ok" && tags.color == "blue"`)
	require.NoError(t, issues.Err())

	result, err := cel2sql.ConvertParameterized(ast,
		cel2sql.WithColumnAliases(map[string]string{
			"status": "tbl_status",
			"tags":   "tbl_tags",
		}),
		cel2sql.WithJSONVariables("tags"),
	)
	require.NoError(t, err)

	assert.Equal(t, `tbl_status = $1 AND tbl_tags->>'color' = $2`, result.SQL)
	assert.Equal(t, []any{"ok", "blue"}, result.Parameters)
}

func TestWithColumnAliases_BackwardCompatible(t *testing.T) {
	env, err := cel.NewEnv(
		cel.CustomTypeAdapter(types.DefaultTypeAdapter),
		cel.Variable("name", cel.StringType),
	)
	require.NoError(t, err)

	ast, issues := env.Compile(`name == "Alice"`)
	require.NoError(t, issues.Err())

	result, err := cel2sql.ConvertParameterized(ast)
	require.NoError(t, err)

	assert.Equal(t, `name = $1`, result.SQL, "without WithColumnAliases, should use original name")
}
