// Copyright 2026 Observe, Inc. Licensed under Apache 2.0.
// List- and map-valued JSON variable support added by Observe, Inc.

package cel2sql

import (
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	"github.com/observeinc/cel2sql/v3/dialect"
)

// A WithJSONVariables value is read as text, v->>'k', which is wrong for a list
// such as an object tag's values, map<string, list<string>>, and a test of the
// map's own keys did not parse at all. For PostgreSQL the forms an object-tag
// filter needs are written instead:
//
//	"k" in v, has(v.k)              v ? 'k'
//
// A dialect without dialect.JSONContainerWriter converts all of these exactly as
// it did before.

// isJSONVariableIdent reports whether expr is a bare JSON variable, v.
func (con *converter) isJSONVariableIdent(expr *exprpb.Expr) bool {
	ident := expr.GetIdentExpr()
	return ident != nil && con.isJSONVariable(ident.GetName())
}

// visitJSONVariableIn writes `lhs in rhs` when rhs is a JSON variable. handled is
// false otherwise, and in a dialect without dialect.JSONContainerWriter.
func (con *converter) visitJSONVariableIn(lhs, rhs *exprpb.Expr) (handled bool, err error) {
	if _, ok := dialect.GetJSONContainerWriter(con.dialect); !ok {
		return false, nil
	}
	if con.isJSONVariableIdent(rhs) {
		// "k" in v tests the map's keys. The key is named in the SQL, so it must be
		// a literal.
		key, err := con.extractJSONVariableKey(lhs)
		if err != nil {
			return true, err
		}
		return true, con.dialect.WriteJSONExistence(&con.str, true, key, func() error { return con.visit(rhs) })
	}
	return false, nil
}

// visitJSONVariableHas writes has(v.k) on a JSON variable. handled is false when
// the operand is not one, and in a dialect without dialect.JSONContainerWriter.
func (con *converter) visitJSONVariableHas(operand *exprpb.Expr, key string) (handled bool, err error) {
	if _, ok := dialect.GetJSONContainerWriter(con.dialect); !ok || !con.isJSONVariableIdent(operand) {
		return false, nil
	}
	return true, con.dialect.WriteJSONExistence(&con.str, true, key, func() error { return con.visit(operand) })
}
