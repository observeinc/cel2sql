package cel2sql_test

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/observeinc/cel2sql/v3"
)

// qualifiedEnv declares dotted names as variables, so the checker resolves each
// one to that variable rather than to field access on a base value.
func qualifiedEnv(t *testing.T) *cel.Env {
	t.Helper()
	env, err := cel.NewEnv(
		cel.Variable("name", cel.StringType),
		cel.Variable("owner.id", cel.IntType),
		cel.Variable("owner.profile.email", cel.StringType),
		cel.Variable("parent.id", cel.IntType),
		cel.Variable("parent.name", cel.StringType),
	)
	require.NoError(t, err)
	return env
}

var qualifiedAliases = map[string]string{
	"name":                "tbl_name",
	"owner.id":            "tbl_owner_id",
	"owner.profile.email": "tbl_owner_email",
	"parent.id":           "tbl_parent_id",
	"parent.name":         "tbl_parent_name",
}

func TestQualifiedNameColumnAliases(t *testing.T) {
	env := qualifiedEnv(t)

	tests := []struct {
		name    string
		expr    string
		wantSQL string
	}{
		{
			name:    "qualified name resolves to its column",
			expr:    `owner.id == 7`,
			wantSQL: `tbl_owner_id = $1`,
		},
		{
			name:    "several qualified names sharing a prefix",
			expr:    `parent.id == 1 && parent.name == "Alice"`,
			wantSQL: `tbl_parent_id = $1 AND tbl_parent_name = $2`,
		},
		{
			name:    "deeper qualified name",
			expr:    `owner.profile.email == "alice@example.com"`,
			wantSQL: `tbl_owner_email = $1`,
		},
		{
			name:    "mixed with an unqualified name",
			expr:    `name == "Alice" || owner.id == 3`,
			wantSQL: `tbl_name = $1 OR tbl_owner_id = $2`,
		},
		{
			// The literal is a constant, never a selection, so it cannot be mistaken
			// for one -- the failure mode of rewriting the expression text.
			name:    "matching text inside a string literal is untouched",
			expr:    `name == "owner.id"`,
			wantSQL: `tbl_name = $1`,
		},
		{
			name:    "matching text mid-literal is untouched",
			expr:    `name == "see owner.id for details"`,
			wantSQL: `tbl_name = $1`,
		},
		{
			name:    "qualified name inside a function call",
			expr:    `name.matches("A") && owner.id == 7`,
			wantSQL: `tbl_name ~ 'A' AND tbl_owner_id = $1`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, issues := env.Compile(tt.expr)
			require.NoError(t, issues.Err())

			result, err := cel2sql.ConvertParameterized(ast, cel2sql.WithColumnAliases(qualifiedAliases))
			require.NoError(t, err)
			assert.Equal(t, tt.wantSQL, result.SQL)
		})
	}
}

// An undeclared field under a qualified prefix fails to compile, so it never
// reaches conversion and cannot produce SQL naming a column that does not exist.
func TestQualifiedNameColumnAliases_UndeclaredFieldFailsToCompile(t *testing.T) {
	env := qualifiedEnv(t)

	exprs := []string{
		`owner.name == "Alice"`,
		`owner.profile.phone == "555"`,
		`parent.description == "x"`,
		`owner == 1`,
	}
	for _, expr := range exprs {
		t.Run(expr, func(t *testing.T) {
			_, issues := env.Compile(expr)
			require.Error(t, issues.Err())
		})
	}
}

// Without a declaration, a dotted expression keeps its existing rendering as a
// qualified column reference.
func TestQualifiedNameColumnAliases_UndeclaredPrefixUnchanged(t *testing.T) {
	env, err := cel.NewEnv(cel.Variable("owner", cel.MapType(cel.StringType, cel.DynType)))
	require.NoError(t, err)

	ast, issues := env.Compile(`owner.id == 7`)
	require.NoError(t, issues.Err())

	result, err := cel2sql.ConvertParameterized(ast, cel2sql.WithColumnAliases(qualifiedAliases))
	require.NoError(t, err)
	assert.Equal(t, `owner.id = $1`, result.SQL)
}
