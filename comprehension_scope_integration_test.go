// Copyright 2026 Observe, Inc. Licensed under Apache 2.0.
// Integration tests for comprehension variable scoping, added by Observe, Inc.
package cel2sql_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/cel-go/common/types"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/observeinc/cel2sql/v3"
)

// scopeRows are rows of a table whose columns are reached through aliases,
// chosen so that a variable read where an element should be, or an element where
// a column should be, selects different rows.
var scopeRows = []struct {
	id    int
	name  string
	tags  string
	items []string
}{
	{1, "checkout", `{"team": ["sre"], "checkout": ["x"]}`, []string{"a", "checkout"}},
	{2, "team", `{"env": ["prod"]}`, []string{"team"}},
	{3, "x", `{}`, []string{}},
	{4, "env", `{"env": ["staging"], "team": ["ops"]}`, []string{"x", "team"}},
}

// scopeExprs name iteration variables after a field, after an aliased column,
// and after neither.
var scopeExprs = []string{
	`tags.exists(name, name == "team")`,
	`!tags.exists(name, name == "team")`,
	`items.exists(name, name == "team")`,
	`items.all(name, name != "x")`,
	`tags.exists(obj_name, obj_name == "env" && name == "team")`,
	`items.exists(obj_name, obj_name == name)`,
	`size(items.filter(obj_name, obj_name != name)) > 0`,
	`items.exists_one(obj_name, obj_name == "team")`,
	`tags["env"].exists(tags, tags == "prod")`,
	`items.exists(name, tags.exists(k, k == name))`,
	`items.exists(obj_name, items.exists(obj_name_, obj_name_ == obj_name && obj_name != name))`,
	`items.map(name, name + "!").exists(v, v == "team!")`,
	`items.exists(i, i == name)`,
}

// TestComprehensionScope_PostgreSQL_MatchesCEL converts each filter both ways a
// caller can and checks that PostgreSQL selects the rows cel-go says are true.
func TestComprehensionScope_PostgreSQL_MatchesCEL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	container, db := setupPostgresContainer(ctx, t)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}()

	_, err := db.Exec(`CREATE TABLE obj (id INT, obj_name TEXT, obj_tags JSONB, obj_items TEXT[])`)
	require.NoError(t, err)
	for _, row := range scopeRows {
		_, err = db.Exec(`INSERT INTO obj VALUES ($1, $2, $3::jsonb, $4)`, row.id, row.name, row.tags, pq.Array(row.items))
		require.NoError(t, err)
	}

	env := newScopeEnv(t)
	for _, expr := range scopeExprs {
		t.Run(expr, func(t *testing.T) {
			ast, issues := env.Compile(expr)
			require.NoError(t, issues.Err())

			prg, err := env.Program(ast)
			require.NoError(t, err)
			var want []int
			for _, row := range scopeRows {
				var tags map[string]any
				require.NoError(t, json.Unmarshal([]byte(row.tags), &tags))
				out, _, err := prg.Eval(map[string]any{"name": row.name, "tags": tags, "items": row.items})
				if err == nil && out == types.True {
					want = append(want, row.id)
				}
			}

			opts := []cel2sql.ConvertOption{cel2sql.WithColumnAliases(scopeAliases), cel2sql.WithJSONVariables("tags")}
			inlined, err := cel2sql.Convert(ast, opts...)
			require.NoError(t, err)
			bound, err := cel2sql.ConvertParameterized(ast, opts...)
			require.NoError(t, err)

			assert.Equal(t, want, sqlMatches(t, db, inlined, nil), "Convert\nSQL: %s", inlined)
			assert.Equal(t, want, sqlMatches(t, db, bound.SQL, bound.Parameters), "ConvertParameterized\nSQL: %s", bound.SQL)
		})
	}
}
