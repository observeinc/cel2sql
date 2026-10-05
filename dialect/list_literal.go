package dialect

import "strings"

// ListLiteralMembershipWriter is implemented by a dialect that writes membership
// in a list literal, x in [a, b], as a value list rather than an array.
//
// Written as an array, x = ANY(ARRAY[$1, $2]), the elements are typed with no
// reference to x: PostgreSQL resolves an untyped placeholder or quoted literal
// inside an ARRAY constructor as text, so a bigint column fails with "operator
// does not exist: bigint = text" and a uuid column against a list of strings
// fails the same way. In a value list, x IN ($1, $2), each element is resolved
// against x and takes its type, with the same NULL semantics as = ANY.
//
// Only PostgreSQL implements it today. Other dialects keep WriteArrayMembership.
type ListLiteralMembershipWriter interface {
	// WriteListLiteralMembership writes a test that the element is one of the
	// values, each written by its own callback. values is never empty.
	WriteListLiteralMembership(w *strings.Builder, writeElem func() error, values []func() error) error
}

// GetListLiteralMembershipWriter returns the dialect's ListLiteralMembershipWriter,
// if it implements one.
func GetListLiteralMembershipWriter(d Dialect) (ListLiteralMembershipWriter, bool) {
	v, ok := d.(ListLiteralMembershipWriter)
	return v, ok
}
