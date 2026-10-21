// Copyright 2026 Observe, Inc. Licensed under Apache 2.0.
// Integration tests for list- and map-valued JSON variables, added by Observe, Inc.
package cel2sql_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/observeinc/cel2sql/v3"
	sqliteDialect "github.com/observeinc/cel2sql/v3/dialect/sqlite"
)

// jsonContainerRows are object-tag documents chosen to tell the forms apart: an
// absent key, an empty list, a list beside other keys, keys that are not
// identifiers, and values holding a quote or a backslash.
var jsonContainerRows = []struct {
	id  int
	doc string
}{
	{1, `{"env":["prod","dev"],"team":["sre"]}`},
	{2, `{"env":["staging"]}`},
	{3, `{}`},
	{4, `{"team":["platform","sre"],"k8s.cluster":["us-west-2"]}`},
	{5, `{"env":[]}`},
	{6, `{"it's":["o'brien"],"back\\slash":["a\\b"]}`},
	{7, `{"env":["prod"],"owner":["dev team"]}`},
	{8, `{"env":["prod","prod"],"team":[]}`},
}

// jsonContainerExprs are the filters an object-tag filter is built from. The test
// does not state their answers: cel-go evaluates each over every row, and the
// SQL has to select exactly the rows it says are true.
var jsonContainerExprs = []string{
	`"env" in tags`,
	`!("env" in tags)`,
	`has(tags.team)`,
	`"k8s.cluster" in tags`,
	`"prod" in tags["env"]`,
	`"prod" in tags.env`,
	`!("prod" in tags["env"])`,
	`"prod" in tags["env"] || "sre" in tags["team"]`,
	`"o'brien" in tags["it's"]`,
	`"a\\b" in tags["back\\slash"]`,
	// The webapp's objectTagFilterQuery, and its negation.
	`"env" in tags && tags["env"].exists(v, v == "prod" || v == "staging")`,
	`("env" in tags && tags["env"].exists(v, v == "prod")) && ("team" in tags && tags["team"].exists(v, v == "sre"))`,
	`!("env" in tags && tags["env"].exists(v, v == "prod"))`,
	// Substring searches over tag keys and values.
	`tags.exists(k, k.startsWith("k8s."))`,
	`"owner" in tags && tags["owner"].exists(v, v.contains("team"))`,
	// The webapp's ranked-search tag key test.
	`tags.map(k, k).exists(k, k == "team")`,
	`!tags["env"].exists(v, v == "dev")`,
	`tags["env"].all(v, v == "prod")`,
	// Iteration variables named like the variable or another column of the row.
	`tags["env"].exists(tags, tags == "prod")`,
	`tags["env"].exists(id, id == "prod")`,
	`tags.exists(tags, tags == "env")`,
	// The webapp's ranked-search tag values for datasets and dashboards.
	`tags.map(k, k).exists(k, k == "team") && ("team" in tags ? tags["team"] : []).exists(v, v.contains("platform"))`,
	`("team" in tags ? tags["team"] : []).exists(v, v.contains("sr"))`,
	`!("env" in tags ? tags["env"] : []).exists(v, v == "prod")`,
	`("env" in tags ? tags["env"] : []).all(v, v == "prod")`,
	`(has(tags.env) ? tags["env"] : tags["team"]).exists(v, v == "sre")`,
}

// celMatches is the in-memory answer: the rows for which the expression is true.
// An evaluation error, such as reading an absent key, is not a match, which is
// what an error's SQL counterpart, NULL, gives too.
func celMatches(t *testing.T, env *cel.Env, ast *cel.Ast) []int {
	t.Helper()
	prg, err := env.Program(ast)
	require.NoError(t, err)
	var ids []int
	for _, row := range jsonContainerRows {
		var doc map[string]any
		require.NoError(t, json.Unmarshal([]byte(row.doc), &doc))
		out, _, err := prg.Eval(map[string]any{"tags": doc})
		if err == nil && out == types.True {
			ids = append(ids, row.id)
		}
	}
	return ids
}

func sqlMatches(t *testing.T, db *sql.DB, condition string, args []any) []int {
	t.Helper()
	// #nosec G202 - test asserting on generated SQL, not user input
	rows, err := db.Query("SELECT id FROM obj WHERE "+condition+" ORDER BY id", args...)
	require.NoError(t, err, "SQL must execute: %s", condition)
	defer func() { _ = rows.Close() }()
	var ids []int
	for rows.Next() {
		var id int
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	return ids
}

// TestJSONVariableContainers_PostgreSQL_MatchesCEL converts each filter both ways
// a caller can and checks that PostgreSQL selects the rows cel-go says are true.
func TestJSONVariableContainers_PostgreSQL_MatchesCEL(t *testing.T) {
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

	for _, expr := range jsonContainerExprs {
		t.Run(expr, func(t *testing.T) {
			ast, issues := env.Compile(expr)
			require.NoError(t, issues.Err())
			want := celMatches(t, env, ast)

			inlined, err := cel2sql.Convert(ast, cel2sql.WithJSONVariables("tags"))
			require.NoError(t, err)
			bound, err := cel2sql.ConvertParameterized(ast, cel2sql.WithJSONVariables("tags"))
			require.NoError(t, err)

			assert.Equal(t, want, sqlMatches(t, db, inlined, nil), "Convert\nSQL: %s", inlined)
			assert.Equal(t, want, sqlMatches(t, db, bound.SQL, bound.Parameters), "ConvertParameterized\nSQL: %s", bound.SQL)
		})
	}
}

// TestJSONVariableContainers_SQLiteUnchanged runs filters over a list value that
// SQLite already converted correctly, reading it with json_extract, to pin that
// a dialect without dialect.JSONContainerWriter keeps converting them as before.
func TestJSONVariableContainers_SQLiteUnchanged(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	_, err = db.Exec(`CREATE TABLE obj (id INT, tags TEXT)`)
	require.NoError(t, err)
	for _, row := range jsonContainerRows {
		_, err = db.Exec(`INSERT INTO obj VALUES (?, ?)`, row.id, row.doc)
		require.NoError(t, err)
	}

	env, err := cel.NewEnv(cel.Variable("tags", cel.MapType(cel.StringType, cel.ListType(cel.StringType))))
	require.NoError(t, err)

	for _, expr := range []string{
		`"prod" in tags.env`,
		`size(tags.env) > 1`,
		`tags.env == ["prod"]`,
	} {
		t.Run(expr, func(t *testing.T) {
			ast, issues := env.Compile(expr)
			require.NoError(t, issues.Err())
			condition, err := cel2sql.Convert(ast, cel2sql.WithDialect(sqliteDialect.New()), cel2sql.WithJSONVariables("tags"))
			require.NoError(t, err)
			assert.Equal(t, celMatches(t, env, ast), sqlMatches(t, db, condition, nil), "SQL: %s", condition)
		})
	}
}
