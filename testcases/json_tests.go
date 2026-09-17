package testcases

import "github.com/observeinc/cel2sql/v3/dialect"

// JSONTests returns test cases for JSON/JSONB field access and operations.
// These tests require the "json_schema" environment setup.
func JSONTests() []ConvertTestCase {
	return []ConvertTestCase{
		{
			Name:     "json_field_access",
			CELExpr:  `product.metadata.brand == "Acme"`,
			Category: CategoryJSON,
			EnvSetup: EnvWithJSON,
			WantSQL: map[dialect.Name]string{
				dialect.PostgreSQL: "product.metadata->>'brand' = 'Acme'",
				dialect.MySQL:      "product.metadata->>'$.brand' = 'Acme'",
				dialect.SQLite:     "json_extract(product.metadata, '$.brand') = 'Acme'",
				dialect.DuckDB:     "product.metadata->>'brand' = 'Acme'",
				dialect.BigQuery:   "JSON_VALUE(product.metadata, '$.brand') = 'Acme'",
			},
		},
		{
			Name:     "json_nested_access",
			CELExpr:  `product.metadata.specs.color == "red"`,
			Category: CategoryJSON,
			EnvSetup: EnvWithJSON,
			WantSQL: map[dialect.Name]string{
				dialect.PostgreSQL: "product.metadata->'specs'->>'color' = 'red'",
				dialect.MySQL:      "product.metadata->'$.specs'->>'$.color' = 'red'",
				dialect.SQLite:     "json_extract(json_extract(product.metadata, '$.specs'), '$.color') = 'red'",
				dialect.DuckDB:     "product.metadata->'specs'->>'color' = 'red'",
				dialect.BigQuery:   "JSON_VALUE(JSON_QUERY(product.metadata, '$.specs'), '$.color') = 'red'",
			},
		},
		{
			Name:     "json_variable_bare_key",
			CELExpr:  `metadata["brand"] == "Acme"`,
			Category: CategoryJSON,
			EnvSetup: EnvWithJSONVariables,
			WantSQL: map[dialect.Name]string{
				dialect.PostgreSQL: "metadata->>'brand' = 'Acme'",
				dialect.MySQL:      "metadata->>'$.brand' = 'Acme'",
				dialect.SQLite:     "json_extract(metadata, '$.brand') = 'Acme'",
				dialect.DuckDB:     "metadata->>'brand' = 'Acme'",
				dialect.BigQuery:   "JSON_VALUE(metadata, '$.brand') = 'Acme'",
			},
		},
		{
			// A key with a space is only nameable through bracket notation, and the
			// path dialects have to quote the member or the path ends at the space.
			Name:     "json_variable_key_with_space",
			CELExpr:  `metadata["has space"] == "Acme"`,
			Category: CategoryJSON,
			EnvSetup: EnvWithJSONVariables,
			WantSQL: map[dialect.Name]string{
				dialect.PostgreSQL: "metadata->>'has space' = 'Acme'",
				dialect.MySQL:      `metadata->>'$."has space"' = 'Acme'`,
				dialect.SQLite:     `json_extract(metadata, '$."has space"') = 'Acme'`,
				dialect.DuckDB:     "metadata->>'has space' = 'Acme'",
				dialect.BigQuery:   `JSON_VALUE(metadata, '$."has space"') = 'Acme'`,
			},
		},
		{
			// The dots belong to the key. Left unquoted, a path dialect would read
			// them as steps into nested objects and match nothing.
			Name:     "json_variable_dotted_key",
			CELExpr:  `metadata["k8s.pod.name"] == "web-1"`,
			Category: CategoryJSON,
			EnvSetup: EnvWithJSONVariables,
			WantSQL: map[dialect.Name]string{
				dialect.PostgreSQL: "metadata->>'k8s.pod.name' = 'web-1'",
				dialect.MySQL:      `metadata->>'$."k8s.pod.name"' = 'web-1'`,
				dialect.SQLite:     `json_extract(metadata, '$."k8s.pod.name"') = 'web-1'`,
				dialect.DuckDB:     "metadata->>'k8s.pod.name' = 'web-1'",
				dialect.BigQuery:   `JSON_VALUE(metadata, '$."k8s.pod.name"') = 'web-1'`,
			},
		},
		{
			// The key is quoted and escaped wherever it lands, so a quote in it
			// cannot close the literal it sits in.
			Name:     "json_variable_key_with_quote",
			CELExpr:  `metadata["it's"] == "Acme"`,
			Category: CategoryJSON,
			EnvSetup: EnvWithJSONVariables,
			WantSQL: map[dialect.Name]string{
				dialect.PostgreSQL: "metadata->>'it''s' = 'Acme'",
				dialect.MySQL:      `metadata->>'$."it''s"' = 'Acme'`,
				dialect.SQLite:     `json_extract(metadata, '$."it''s"') = 'Acme'`,
				dialect.DuckDB:     "metadata->>'it''s' = 'Acme'",
				dialect.BigQuery:   `JSON_VALUE(metadata, '$."it\'s"') = 'Acme'`,
			},
		},
		{
			// A quoted JSONPath member is a JSON string, so a double quote in the key
			// is escaped as JSON escapes it, and the dialects whose literals read a
			// backslash as an escape double it again on the way into the literal.
			Name:     "json_variable_key_with_double_quote",
			CELExpr:  `metadata["say \"hi\""] == "Acme"`,
			Category: CategoryJSON,
			EnvSetup: EnvWithJSONVariables,
			WantSQL: map[dialect.Name]string{
				dialect.PostgreSQL: `metadata->>'say "hi"' = 'Acme'`,
				dialect.MySQL:      `metadata->>'$."say \\"hi\\""' = 'Acme'`,
				dialect.SQLite:     `json_extract(metadata, '$."say \"hi\""') = 'Acme'`,
				dialect.DuckDB:     `metadata->>'say "hi"' = 'Acme'`,
				dialect.BigQuery:   `JSON_VALUE(metadata, '$."say \\"hi\\""') = 'Acme'`,
			},
		},
		{
			Name:     "json_has_field",
			CELExpr:  `has(product.metadata.brand)`,
			Category: CategoryJSON,
			EnvSetup: EnvWithJSON,
			WantSQL: map[dialect.Name]string{
				dialect.PostgreSQL: "product.metadata ? 'brand'",
				dialect.MySQL:      "JSON_CONTAINS_PATH(product.metadata, 'one', '$.brand')",
				dialect.SQLite:     "json_type(product.metadata, '$.brand') IS NOT NULL",
				dialect.DuckDB:     "json_exists(product.metadata, '$.brand')",
				dialect.BigQuery:   "JSON_VALUE(product.metadata, '$.brand') IS NOT NULL",
			},
		},
	}
}
