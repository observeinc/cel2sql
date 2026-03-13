package testutil_test

import (
	"testing"

	"github.com/observeinc/cel2sql/v3/dialect"
	"github.com/observeinc/cel2sql/v3/testutil"
)

func TestSQLiteSharedCases(t *testing.T) {
	testutil.RunAllConvertTests(t, dialect.SQLite, testutil.SQLiteEnvFactory())
}

func TestSQLiteParameterizedSharedCases(t *testing.T) {
	testutil.RunAllParameterizedTests(t, dialect.SQLite, testutil.SQLiteEnvFactory())
}
