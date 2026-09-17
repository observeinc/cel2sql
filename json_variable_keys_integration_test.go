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
	_ "modernc.org/sqlite"

	"github.com/observeinc/cel2sql/v3"
	mysqlDialect "github.com/observeinc/cel2sql/v3/dialect/mysql"
	sqliteDialect "github.com/observeinc/cel2sql/v3/dialect/sqlite"
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
	}
}

// jsonVariableKeysSeedJSON is one row's worth of keys, including a "k8s" object so
// that reading metadata["k8s.pod.name"] as a nested path would match "nested"
// instead of the flat key and the test would catch it.
// jsonVariableKeysSeedJSON is one row's worth of keys, including a "k8s" object
// so that reading metadata["k8s.pod.name"] as a nested path would match "nested"
// instead of the flat key and the test would catch it. The last three keys hold a
// double quote, a backslash and a tab: characters a JSONPath member has to escape
// and the surrounding SQL literal may have to escape again.
const jsonVariableKeysSeedJSON = `{"brand":"Acme","has space":"web 01","pod-name":"web-1",` +
	`"k8s.pod.name":"checkout-7f9c","k8s":{"pod":{"name":"nested"}},"it's":"quoted",` +
	`"select":"all","2fa":"on","say \"hi\"":"E","back\\slash":"F","tab\there":"G"}`

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

// runJSONVariableKeyCases converts each filter for the given dialect and runs it
// against a live database, so a path that parses but reads the wrong key still
// fails the test.
func runJSONVariableKeyCases(t *testing.T, db *sql.DB, table string, opts ...cel2sql.ConvertOption) {
	t.Helper()
	env := newJSONVariableEnv(t)

	for _, tc := range jsonVariableKeyCases() {
		t.Run(tc.name, func(t *testing.T) {
			ast, issues := env.Compile(tc.celExpr)
			require.NoError(t, issues.Err())

			condition, err := cel2sql.Convert(ast, opts...)
			require.NoError(t, err, "conversion should succeed for %s", tc.celExpr)
			t.Logf("CEL %s -> SQL %s", tc.celExpr, condition)

			// #nosec G202 - test asserting on generated SQL, not user input
			query := "SELECT COUNT(*) FROM " + table + " WHERE " + condition
			var got int
			require.NoError(t, db.QueryRow(query).Scan(&got),
				"generated SQL must execute: %s", query)
			assert.Equal(t, tc.wantRows, got, "row count for %s\nSQL: %s", tc.celExpr, condition)
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

	_, err = db.Exec(`CREATE TABLE obj (id INT, metadata JSONB)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO obj VALUES (1, $1::jsonb)`, jsonVariableKeysSeedJSON)
	require.NoError(t, err)

	runJSONVariableKeyCases(t, db, "obj", cel2sql.WithJSONVariables("metadata"))
}

// TestJSONVariableKeys_MySQL runs the same keys against MySQL, where the key is a
// JSONPath member and has to be quoted to stay one key.
func TestJSONVariableKeys_MySQL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	container, db := setupMySQLContainer(ctx, t)
	defer func() {
		if termErr := container.Terminate(ctx); termErr != nil {
			t.Logf("failed to terminate container: %v", termErr)
		}
		if closeErr := db.Close(); closeErr != nil {
			t.Logf("failed to close db: %v", closeErr)
		}
	}()

	_, err := db.Exec("CREATE TABLE obj (id INT, metadata JSON)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO obj VALUES (1, ?)", jsonVariableKeysSeedJSON)
	require.NoError(t, err)

	runJSONVariableKeyCases(t, db, "obj",
		cel2sql.WithJSONVariables("metadata"),
		cel2sql.WithDialect(mysqlDialect.New()))
}

// TestJSONVariableKeys_SQLite runs the same keys against SQLite, the other
// JSONPath dialect, in memory and without a container.
func TestJSONVariableKeys_SQLite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Logf("failed to close db: %v", closeErr)
		}
	}()

	_, err = db.Exec(`CREATE TABLE obj (id INTEGER, metadata TEXT)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO obj VALUES (1, ?)`, jsonVariableKeysSeedJSON)
	require.NoError(t, err)

	runJSONVariableKeyCases(t, db, "obj",
		cel2sql.WithJSONVariables("metadata"),
		cel2sql.WithDialect(sqliteDialect.New()))
}
