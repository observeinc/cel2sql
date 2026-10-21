// Copyright 2026 Observe, Inc. Licensed under Apache 2.0.
// List- and map-valued JSON variable support added by Observe, Inc.

package cel2sql

import (
	"fmt"

	"github.com/google/cel-go/common/operators"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	"github.com/observeinc/cel2sql/v3/dialect"
)

// A WithJSONVariables value is read as text, v->>'k', which is wrong for a list
// such as an object tag's values, map<string, list<string>>. For PostgreSQL the
// forms an object-tag filter needs are written instead, with GUARD(a, b) standing
// for CASE WHEN jsonb_typeof(a) = 'array' THEN b END:
//
//	"k" in v, has(v.k)              v ? 'k'
//	x in v.k, list(string)          GUARD(v->'k', (v->'k') ? (x))
//	v.k.exists(e, p), list(string)  GUARD(v->'k', EXISTS (SELECT 1 FROM jsonb_array_elements_text(v->'k') AS e(e) WHERE p))
//
// The guard makes a read of an absent key NULL, as CEL raises an error for it, so
// !v.env.exists(e, e == "prod") does not match a row with no env. Any other use
// of a list value is refused. A dialect without dialect.JSONContainerWriter
// converts all of these exactly as it did before.

// jsonVariableValue is one read of a JSON variable's value, v.k or v["k"].
type jsonVariableValue struct {
	variable *exprpb.Expr // the JSON variable's identifier
	key      string
	typ      *exprpb.Type // the value's CEL type
}

// isJSONVariableIdent reports whether expr is a bare JSON variable, v.
func (con *converter) isJSONVariableIdent(expr *exprpb.Expr) bool {
	ident := expr.GetIdentExpr()
	return ident != nil && con.isJSONVariable(ident.GetName())
}

// jsonVariableValueOf recognizes v.k and v["k"] on a JSON variable. ok is false for
// any other expression. A recognized read whose key the dialect cannot render
// returns ok with the error, so the caller reports it rather than falling back.
func (con *converter) jsonVariableValueOf(expr *exprpb.Expr) (jsonVariableValue, bool, error) {
	if sel := expr.GetSelectExpr(); sel != nil && !sel.GetTestOnly() && con.isJSONVariableIdent(sel.GetOperand()) {
		if err := con.dialect.ValidateFieldName(sel.GetField()); err != nil {
			return jsonVariableValue{}, true, fmt.Errorf("%w: %w", ErrInvalidFieldName, err)
		}
		return jsonVariableValue{variable: sel.GetOperand(), key: sel.GetField(), typ: con.getType(expr)}, true, nil
	}
	if call := expr.GetCallExpr(); call != nil && call.GetFunction() == operators.Index &&
		len(call.GetArgs()) == 2 && con.isJSONVariableIdent(call.GetArgs()[0]) {
		key, err := con.extractJSONVariableKey(call.GetArgs()[1])
		if err != nil {
			return jsonVariableValue{}, true, err
		}
		return jsonVariableValue{variable: call.GetArgs()[0], key: key, typ: con.getType(expr)}, true, nil
	}
	return jsonVariableValue{}, false, nil
}

func isContainerType(typ *exprpb.Type) bool {
	return isListType(typ) || isMapType(typ)
}

func isStringListType(typ *exprpb.Type) bool {
	return isListType(typ) && typ.GetListType().GetElemType().GetPrimitive() == exprpb.Type_STRING
}

// refuseJSONContainerValue is the error for a list- or map-valued read in a
// position none of the forms above covers.
func refuseJSONContainerValue(v jsonVariableValue) error {
	return newConversionErrorf(errMsgUnsupportedType,
		"JSON variable key %q holds a list or map; it can only be tested with in or iterated with exists() or all()", v.key)
}

// writeJSONVariableContainer writes v->'k', the value kept as JSON so it can be
// iterated or tested rather than read as text.
func (con *converter) writeJSONVariableContainer(v jsonVariableValue) error {
	return con.dialect.WriteJSONFieldAccess(&con.str, func() error { return con.visit(v.variable) }, v.key, false)
}

// visitJSONVariableIn writes `lhs in rhs` when rhs is a JSON variable or a list
// value of one. handled is false when rhs is neither, and in a dialect without
// dialect.JSONContainerWriter.
func (con *converter) visitJSONVariableIn(lhs, rhs *exprpb.Expr) (handled bool, err error) {
	w, ok := dialect.GetJSONContainerWriter(con.dialect)
	if !ok {
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

	v, ok, err := con.jsonVariableValueOf(rhs)
	if !ok || err != nil {
		return ok, err
	}
	if !isContainerType(v.typ) {
		return false, nil
	}
	if !isStringListType(v.typ) {
		return true, refuseJSONContainerValue(v)
	}
	writeArray := func() error { return con.writeJSONVariableContainer(v) }
	return true, w.WriteJSONArrayGuard(&con.str, writeArray, func() error {
		return w.WriteJSONArrayContainsText(&con.str, writeArray, func() error { return con.visit(lhs) })
	})
}

// jsonListRange returns a writer for a comprehension range that is a JSON array,
// a list value of a JSON variable: v.k or v["k"]. ok is false for any other
// range; a list or map of something other than strings is refused.
func (con *converter) jsonListRange(expr *exprpb.Expr) (write func() error, ok bool, err error) {
	v, ok, err := con.jsonVariableValueOf(expr)
	if !ok || err != nil {
		return nil, ok, err
	}
	if !isContainerType(v.typ) {
		return nil, false, nil
	}
	if !isStringListType(v.typ) {
		return nil, true, refuseJSONContainerValue(v)
	}
	return func() error { return con.writeJSONVariableContainer(v) }, true, nil
}

// visitJSONVariableComprehension writes a comprehension over a JSON array range
// inside the array guard, so it is NULL when the key is absent rather than true
// for all() and false for exists(). handled is false for any other range, and in
// a dialect without dialect.JSONContainerWriter.
func (con *converter) visitJSONVariableComprehension(iterRange *exprpb.Expr, writeComprehension func() error) (handled bool, err error) {
	w, ok := dialect.GetJSONContainerWriter(con.dialect)
	if !ok {
		return false, nil
	}
	write, ok, err := con.jsonListRange(iterRange)
	if !ok || err != nil {
		return ok, err
	}
	return true, w.WriteJSONArrayGuard(&con.str, write, writeComprehension)
}

// writeJSONVariableComprehensionSource writes the FROM source of a comprehension
// over a JSON array range, aliased as iterVar. handled is false for any other
// range, and in a dialect without dialect.JSONContainerWriter.
func (con *converter) writeJSONVariableComprehensionSource(iterRange *exprpb.Expr, iterVar string) (handled bool, err error) {
	if _, ok := dialect.GetJSONContainerWriter(con.dialect); !ok {
		return false, nil
	}
	write, ok, err := con.jsonListRange(iterRange)
	if !ok || err != nil {
		return ok, err
	}
	if err := con.dialect.WriteJSONArrayElements(&con.str, true, true, write); err != nil {
		return true, err
	}
	con.writeJSONSourceAlias(iterVar)
	return true, nil
}

// writeJSONSourceAlias writes " AS v(v)", naming the source's one column after the
// iteration variable as well as the source itself. jsonb_array_elements_text
// calls its column value whatever the alias, so a bare v in the predicate finds
// no column of the source and resolves to any outer column named v instead:
// tags.env.exists(name, name == "prod") would compare the row's own name column.
func (con *converter) writeJSONSourceAlias(iterVar string) {
	con.str.WriteString(" AS ")
	con.str.WriteString(iterVar)
	con.str.WriteString("(")
	con.str.WriteString(iterVar)
	con.str.WriteString(")")
}

// visitJSONVariableHas writes has(v.k) on a JSON variable. handled is false when
// the operand is not one, and in a dialect without dialect.JSONContainerWriter.
func (con *converter) visitJSONVariableHas(operand *exprpb.Expr, key string) (handled bool, err error) {
	if _, ok := dialect.GetJSONContainerWriter(con.dialect); !ok || !con.isJSONVariableIdent(operand) {
		return false, nil
	}
	return true, con.dialect.WriteJSONExistence(&con.str, true, key, func() error { return con.visit(operand) })
}

// checkJSONVariableScalarRead refuses a list- or map-valued read reached as an
// ordinary value, where it would otherwise be written as text. The forms above
// write their reads themselves and never visit them, so reaching here means
// nothing claimed the value. A dialect without dialect.JSONContainerWriter keeps
// the read it wrote before.
func (con *converter) checkJSONVariableScalarRead(expr *exprpb.Expr) error {
	if _, ok := dialect.GetJSONContainerWriter(con.dialect); !ok {
		return nil
	}
	v, ok, err := con.jsonVariableValueOf(expr)
	if !ok || err != nil {
		return err
	}
	if isContainerType(v.typ) {
		return refuseJSONContainerValue(v)
	}
	return nil
}
