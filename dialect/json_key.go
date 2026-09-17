package dialect

// JSONKeyValidator is implemented by a dialect whose WriteJSONFieldAccess can
// render any JSON object key, not only one shaped like a SQL identifier.
//
// Bracket access on a variable declared with WithJSONVariables -- meta["k8s.pod.name"]
// -- names a key that is data, so it may hold a space, a dot, a quote or any other
// character. A dialect that implements this interface takes such a key and
// renders it safely itself, returning an error from ValidateJSONKey only for a
// key it cannot carry at all. A dialect that does not implement it keeps the
// default rule: the key must be a valid identifier, exactly as for a column.
//
// Only PostgreSQL implements it today. Each of the other dialects embeds the key
// in a JSON path whose quoting rules differ from engine to engine, so accepting
// arbitrary keys there needs its own per-dialect rendering and verification.
type JSONKeyValidator interface {
	// ValidateJSONKey reports whether key can be rendered by this dialect's
	// WriteJSONFieldAccess.
	ValidateJSONKey(key string) error
}

// GetJSONKeyValidator returns the dialect's JSONKeyValidator, if it implements one.
func GetJSONKeyValidator(d Dialect) (JSONKeyValidator, bool) {
	v, ok := d.(JSONKeyValidator)
	return v, ok
}
