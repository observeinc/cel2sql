// Copyright 2026 Observe, Inc. Licensed under Apache 2.0.
// Comprehension variable scoping added by Observe, Inc.

package cel2sql

import (
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// A comprehension's iteration variable is in scope in its predicate, transform
// and filter, not in its range, and inside them it shadows a declared variable of
// the same name, as CEL scopes it. In SQL the variable names the comprehension's
// source (AS v, or AS v(v)) and the body refers to it unqualified, beside the
// columns the body reads, which are unqualified too. Two things follow:
//
//   - A reference to the variable is written as the variable, never through
//     WithColumnAliases. With name aliased to obj_name,
//     tags.exists(name, name == "team") compares each key, not obj_name.
//   - The variable's SQL name must not be a column an alias writes, or that
//     column, read in the body, would resolve to the source rather than the row.
//     In tags.exists(obj_name, obj_name == "team" && name == "x") the variable
//     is written obj_name_, so name still reads the row's obj_name.
//
// A variable that collides with neither is written exactly as before.

// comprehensionVar is an iteration variable in scope.
type comprehensionVar struct {
	cel string // the variable as the expression names it
	sql string // the name it is written as
}

// comprehensionVarSQLName returns the name iteration variable name is written
// as: the name itself, unless an alias writes it or an enclosing variable of
// another name is written as it, in which case underscores are appended until
// neither does.
func (con *converter) comprehensionVarSQLName(name string) string {
	sqlName := name
	for con.comprehensionSQLNameTaken(sqlName, name) {
		sqlName += "_"
	}
	return sqlName
}

func (con *converter) comprehensionSQLNameTaken(sqlName, celName string) bool {
	for _, column := range con.columnAlias {
		if column == sqlName {
			return true
		}
	}
	for _, v := range con.comprehensionVars {
		if v.sql == sqlName && v.cel != celName {
			return true
		}
	}
	return false
}

// visitInComprehensionScope visits part of a comprehension's body, its
// predicate, transform or filter, with iteration variable celName in scope and
// written as sqlName.
func (con *converter) visitInComprehensionScope(celName, sqlName string, expr *exprpb.Expr) error {
	con.comprehensionVars = append(con.comprehensionVars, comprehensionVar{cel: celName, sql: sqlName})
	defer func() { con.comprehensionVars = con.comprehensionVars[:len(con.comprehensionVars)-1] }()
	return con.visit(expr)
}

// comprehensionVarInScope returns the SQL name of the innermost iteration
// variable in scope named name, if there is one.
func (con *converter) comprehensionVarInScope(name string) (string, bool) {
	for i := len(con.comprehensionVars) - 1; i >= 0; i-- {
		if con.comprehensionVars[i].cel == name {
			return con.comprehensionVars[i].sql, true
		}
	}
	return "", false
}
