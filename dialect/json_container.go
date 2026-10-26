// Copyright 2026 Observe, Inc. Licensed under Apache 2.0.

package dialect

import "strings"

// JSONContainerWriter is implemented by a dialect that can read a list value, or
// the keys, of a variable declared with WithJSONVariables, such as
// map<string, list<string>> object tags. A dialect that does not implement it
// converts those expressions exactly as it did before.
type JSONContainerWriter interface {
	// WriteJSONObjectKeys writes a set-returning call that yields each key of the
	// JSON object writeObject names as one text row, for use as a FROM source.
	WriteJSONObjectKeys(w *strings.Builder, writeObject func() error) error

	// WriteJSONArrayContainsText writes a test of whether the JSON array
	// writeArray names holds the string writeElem names as an element.
	WriteJSONArrayContainsText(w *strings.Builder, writeArray, writeElem func() error) error

	// WriteJSONEmptyArray writes an empty JSON array.
	WriteJSONEmptyArray(w *strings.Builder)

	// WriteJSONArrayGuard writes writeBody where writeArray names a JSON array,
	// and NULL where it names anything else, including a key that is absent.
	WriteJSONArrayGuard(w *strings.Builder, writeArray, writeBody func() error) error
}

// GetJSONContainerWriter returns the dialect's JSONContainerWriter, if it implements one.
func GetJSONContainerWriter(d Dialect) (JSONContainerWriter, bool) {
	w, ok := d.(JSONContainerWriter)
	return w, ok
}
