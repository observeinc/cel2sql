package testutil_test

import (
	"testing"

	"github.com/observeinc/cel2sql/v3/dialect"
	"github.com/observeinc/cel2sql/v3/testutil"
)

func TestBigQuerySharedCases(t *testing.T) {
	testutil.RunAllConvertTests(t, dialect.BigQuery, testutil.BigQueryEnvFactory())
}

func TestBigQueryParameterizedSharedCases(t *testing.T) {
	testutil.RunAllParameterizedTests(t, dialect.BigQuery, testutil.BigQueryEnvFactory())
}
