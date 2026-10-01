package cel2sql_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/cel-go/cel"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/observeinc/cel2sql/v3"
)

// jsonVariableKeyCase is a filter over a flat JSON variable whose key is not an
// identifier, with the number of seeded rows it must match.
type jsonVariableKeyCase struct {
	name     string
	celExpr  string
	wantRows int
}

// jsonVariableKeyCases are the shapes a caller reaches for when the JSON keys are
// data — Kubernetes labels, OTel attributes, monitor group-by columns — rather
// than a schema someone chose to be SQL-shaped.
func jsonVariableKeyCases() []jsonVariableKeyCase {
	return []jsonVariableKeyCase{
		{name: "bare key", celExpr: `metadata["brand"] == "Acme"`, wantRows: 1},
		{name: "key with a space", celExpr: `metadata["has space"] == "web 01"`, wantRows: 1},
		{name: "key with a hyphen", celExpr: `metadata["pod-name"] == "web-1"`, wantRows: 1},
		{name: "dotted key is one key", celExpr: `metadata["k8s.pod.name"] == "checkout-7f9c"`, wantRows: 1},
		{name: "dotted key is not a nested path", celExpr: `metadata["k8s.pod.name"] == "nested"`, wantRows: 0},
		{name: "key holding a quote", celExpr: `metadata["it's"] == "quoted"`, wantRows: 1},
		{name: "reserved SQL keyword as key", celExpr: `metadata["select"] == "all"`, wantRows: 1},
		{name: "leading digit", celExpr: `metadata["2fa"] == "on"`, wantRows: 1},
		{name: "spaced key that does not match", celExpr: `metadata["has space"] == "nope"`, wantRows: 0},
		{name: "contains on a spaced key", celExpr: `metadata["has space"].contains("eb 0")`, wantRows: 1},
		{name: "absent key matches nothing", celExpr: `metadata["no such key"] == "x"`, wantRows: 0},
		{name: "key holding a double quote", celExpr: `metadata["say \"hi\""] == "E"`, wantRows: 1},
		{name: "key holding a backslash", celExpr: `metadata["back\\slash"] == "F"`, wantRows: 1},
		{name: "key holding a tab", celExpr: `metadata["tab\there"] == "G"`, wantRows: 1},
		{name: "key starting with a JSONPath root", celExpr: `metadata["$bar"] == "H"`, wantRows: 1},
		{name: "key that looks like a JSONPath", celExpr: `metadata["$.decoy"] == "I"`, wantRows: 1},
		{name: "empty key", celExpr: `metadata[""] == "J"`, wantRows: 1},
		{name: "key ending in a backslash", celExpr: `metadata["trail\\"] == "K"`, wantRows: 1},
		// A backslash before a quote is how a key would escape its literal when
		// standard_conforming_strings is off; written correctly it is inert data
		// naming a key the document does not have.
		{name: "key built to break out of its literal", celExpr: `metadata["\\' = '' OR true OR $1::text = '' --"] == "zzz"`, wantRows: 0},
	}
}

// jsonVariableKeysSeedJSON is one row's worth of keys, including a "k8s" object
// so that reading metadata["k8s.pod.name"] as a nested path would match "nested"
// instead of the flat key and the test would catch it. It also holds keys with a
// quote, a backslash and a tab, which the literal they are written into must not
// mangle, and keys that look like JSON paths or are empty, which must still be
// read as plain keys.
const jsonVariableKeysSeedJSON = `{"brand":"Acme","has space":"web 01","pod-name":"web-1",` +
	`"k8s.pod.name":"checkout-7f9c","k8s":{"pod":{"name":"nested"}},"it's":"quoted",` +
	`"select":"all","2fa":"on","say \"hi\"":"E","back\\slash":"F","tab\there":"G",` +
	`"$bar":"H","$.decoy":"I","":"J","trail\\":"K"}`

// newJSONVariableEnv declares metadata as a flat map, the shape
// cel2sql.WithJSONVariables serves.
func newJSONVariableEnv(t *testing.T) *cel.Env {
	t.Helper()
	env, err := cel.NewEnv(
		cel.Variable("metadata", cel.MapType(cel.StringType, cel.StringType)),
	)
	require.NoError(t, err)
	return env
}

// runJSONVariableKeyCases converts each filter both ways a caller can -- Convert,
// which inlines values, and ConvertParameterized, which binds them -- and runs the
// result against a live database, so SQL that parses but reads the wrong key
// still fails the test.
func runJSONVariableKeyCases(t *testing.T, db *sql.DB, table string, opts ...cel2sql.ConvertOption) {
	t.Helper()
	env := newJSONVariableEnv(t)

	for _, tc := range jsonVariableKeyCases() {
		t.Run(tc.name, func(t *testing.T) {
			ast, issues := env.Compile(tc.celExpr)
			require.NoError(t, issues.Err())

			inlined, err := cel2sql.Convert(ast, opts...)
			require.NoError(t, err, "conversion should succeed for %s", tc.celExpr)
			bound, err := cel2sql.ConvertParameterized(ast, opts...)
			require.NoError(t, err, "parameterized conversion should succeed for %s", tc.celExpr)

			for _, q := range []struct {
				mode      string
				condition string
				args      []any
			}{
				{"Convert", inlined, nil},
				{"ConvertParameterized", bound.SQL, bound.Parameters},
			} {
				// #nosec G202 - test asserting on generated SQL, not user input
				query := "SELECT COUNT(*) FROM " + table + " WHERE " + q.condition
				var got int
				require.NoError(t, db.QueryRow(query, q.args...).Scan(&got),
					"%s SQL must execute: %s", q.mode, query)
				assert.Equal(t, tc.wantRows, got, "%s row count for %s\nSQL: %s", q.mode, tc.celExpr, q.condition)
			}
		})
	}
}

// TestJSONVariableKeys_PostgreSQL runs the non-identifier keys against PostgreSQL,
// where the key is named directly in a ->> operand.
func TestJSONVariableKeys_PostgreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:17-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_PASSWORD": "password",
				"POSTGRES_DB":       "testdb",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		},
		Started: true,
	})
	require.NoError(t, err)
	defer func() {
		if termErr := container.Terminate(ctx); termErr != nil {
			t.Logf("failed to terminate container: %v", termErr)
		}
	}()

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "5432")
	require.NoError(t, err)

	db, err := sql.Open("postgres",
		"host="+host+" port="+port.Port()+" user=postgres password=password dbname=testdb sslmode=disable")
	require.NoError(t, err)
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Logf("failed to close db: %v", closeErr)
		}
	}()

	// One connection, so the SET below governs every query that follows it.
	db.SetMaxOpenConns(1)

	_, err = db.Exec(`CREATE TABLE obj (id INT, metadata JSONB)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO obj VALUES (1, $1::jsonb)`, jsonVariableKeysSeedJSON)
	require.NoError(t, err)

	// The default is on, but a role or database can turn it off, and that changes
	// what a backslash means inside a standard string literal. The keys have to
	// read the same, and stay inert, either way.
	for _, setting := range []string{"on", "off"} {
		t.Run("standard_conforming_strings="+setting, func(t *testing.T) {
			_, err := db.Exec("SET standard_conforming_strings = " + setting)
			require.NoError(t, err)
			runJSONVariableKeyCases(t, db, "obj", cel2sql.WithJSONVariables("metadata"))
		})
	}
}
