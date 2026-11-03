// Copyright 2026 Observe, Inc. Licensed under Apache 2.0.
// Integration tests for list-valued JSON expressions, added by Observe, Inc.
package cel2sql_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/observeinc/cel2sql/v3"
)

// jsonListResultExprs are list-valued expressions converted whole, as a caller
// counting each tag's keys or values across rows writes them.
var jsonListResultExprs = []string{
	`tags["env"]`,
	`tags.team`,
	`tags["k8s.cluster"]`,
	`tags["it's"]`,
	`("env" in tags ? tags["env"] : [])`,
	`("team" in tags ? tags["team"] : [])`,
	`(has(tags.env) ? tags["env"] : tags["team"])`,
	`tags.map(k, k)`,
}

// celLists is the in-memory answer: each row's list by id, nil where cel-go
// raises an error, as for an absent key, which the SQL writes as NULL. Lists are
// sorted, since a map's keys come in no order and a caller counting the
// elements does not read one.
func celLists(t *testing.T, env *cel.Env, ast *cel.Ast) map[int][]string {
	t.Helper()
	prg, err := env.Program(ast)
	require.NoError(t, err)
	lists := make(map[int][]string, len(jsonContainerRows))
	for _, row := range jsonContainerRows {
		var doc map[string]any
		require.NoError(t, json.Unmarshal([]byte(row.doc), &doc))
		out, _, err := prg.Eval(map[string]any{"tags": doc})
		if err != nil {
			lists[row.id] = nil
			continue
		}
		native, err := out.ConvertToNative(reflect.TypeOf([]string{}))
		require.NoError(t, err)
		list := native.([]string)
		slices.Sort(list)
		lists[row.id] = list
	}
	return lists
}

// sqlLists selects the converted expression for every row. It is read through
// array_to_json so that NULL and the empty array stay apart.
func sqlLists(t *testing.T, db *sql.DB, expr string, args []any) map[int][]string {
	t.Helper()
	// #nosec G202 - test asserting on generated SQL, not user input
	rows, err := db.Query("SELECT id, array_to_json("+expr+")::text FROM obj ORDER BY id", args...)
	require.NoError(t, err, "SQL must execute: %s", expr)
	defer func() { _ = rows.Close() }()
	lists := make(map[int][]string)
	for rows.Next() {
		var id int
		var js sql.NullString
		require.NoError(t, rows.Scan(&id, &js))
		if !js.Valid {
			lists[id] = nil
			continue
		}
		list := []string{}
		require.NoError(t, json.Unmarshal([]byte(js.String), &list))
		slices.Sort(list)
		lists[id] = list
	}
	require.NoError(t, rows.Err())
	return lists
}

// sqlCounts counts each element across rows the way a stats caller does,
// unnesting the array, so a row whose array is NULL contributes nothing.
func sqlCounts(t *testing.T, db *sql.DB, expr string, args []any) map[string]int {
	t.Helper()
	// #nosec G202 - test asserting on generated SQL, not user input
	rows, err := db.Query("SELECT value, COUNT(*) FROM obj CROSS JOIN LATERAL unnest("+expr+") AS value GROUP BY value", args...)
	require.NoError(t, err, "SQL must execute: %s", expr)
	defer func() { _ = rows.Close() }()
	counts := make(map[string]int)
	for rows.Next() {
		var value string
		var n int
		require.NoError(t, rows.Scan(&value, &n))
		counts[value] = n
	}
	require.NoError(t, rows.Err())
	return counts
}

// TestJSONVariableListResult_PostgreSQL_MatchesCEL converts each list-valued
// expression both ways a caller can and checks that PostgreSQL yields, row by
// row, the list cel-go evaluates, and NULL where cel-go raises an error; then
// that unnesting it counts the elements of the lists cel-go produced.
func TestJSONVariableListResult_PostgreSQL_MatchesCEL(t *testing.T) {
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

	_, err := db.Exec(`CREATE TABLE obj (id INT, tags JSONB)`)
	require.NoError(t, err)
	for _, row := range jsonContainerRows {
		_, err = db.Exec(`INSERT INTO obj VALUES ($1, $2::jsonb)`, row.id, row.doc)
		require.NoError(t, err)
	}

	env, err := cel.NewEnv(cel.Variable("tags", cel.MapType(cel.StringType, cel.ListType(cel.StringType))))
	require.NoError(t, err)

	for _, expr := range jsonListResultExprs {
		t.Run(expr, func(t *testing.T) {
			ast, issues := env.Compile(expr)
			require.NoError(t, issues.Err())
			want := celLists(t, env, ast)
			wantCounts := make(map[string]int)
			for _, list := range want {
				for _, v := range list {
					wantCounts[v]++
				}
			}

			inlined, err := cel2sql.Convert(ast, cel2sql.WithJSONVariables("tags"))
			require.NoError(t, err)
			bound, err := cel2sql.ConvertParameterized(ast, cel2sql.WithJSONVariables("tags"))
			require.NoError(t, err)

			assert.Equal(t, want, sqlLists(t, db, inlined, nil), "Convert\nSQL: %s", inlined)
			assert.Equal(t, want, sqlLists(t, db, bound.SQL, bound.Parameters), "ConvertParameterized\nSQL: %s", bound.SQL)
			assert.Equal(t, wantCounts, sqlCounts(t, db, inlined, nil), "Convert\nSQL: %s", inlined)
			assert.Equal(t, wantCounts, sqlCounts(t, db, bound.SQL, bound.Parameters), "ConvertParameterized\nSQL: %s", bound.SQL)
		})
	}
}
