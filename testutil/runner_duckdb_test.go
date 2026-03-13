package testutil_test

import (
	"testing"

	"github.com/observeinc/cel2sql/v3/dialect"
	"github.com/observeinc/cel2sql/v3/testutil"
)

func TestDuckDBSharedCases(t *testing.T) {
	testutil.RunAllConvertTests(t, dialect.DuckDB, testutil.DuckDBEnvFactory())
}

func TestDuckDBParameterizedSharedCases(t *testing.T) {
	testutil.RunAllParameterizedTests(t, dialect.DuckDB, testutil.DuckDBEnvFactory())
}
