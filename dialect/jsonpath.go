package dialect

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
)

// jsonPathBareMember matches a key a JSONPath accepts after a plain dot. Any
// other key needs the quoted form, since a dot would otherwise read as a step
// into a nested object and a space would end the path.
var jsonPathBareMember = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// JSONPathMember renders key as a member accessor to append to a JSONPath root,
// including the leading dot: "brand" gives ".brand", "has space" gives
// `."has space"`, and "k8s.pod.name" gives `."k8s.pod.name"` rather than three
// nested steps.
//
// A quoted member is a JSON string, so it is written as one: a key holding a
// double quote, a backslash or a control character comes back escaped the way
// JSON escapes it. MySQL 8.0 and SQLite 3.51 both resolve those escapes to the
// original key; TestJSONVariableKeys_MySQL and TestJSONVariableKeys_SQLite query
// for such keys against live engines.
//
// The result still has to be escaped for the string literal it is embedded in,
// which is the caller\'s business: a dialect whose literals treat a backslash as
// an escape has to double the ones this adds. Dialects that name the key
// directly instead of building a path -- PostgreSQL\'s and DuckDB\'s ->>\'key\' --
// never come here.
func JSONPathMember(key string) string {
	if jsonPathBareMember.MatchString(key) {
		return "." + key
	}
	return "." + jsonString(key)
}

// jsonString renders s as a quoted JSON string. HTML escaping is off so that a
// key holding <, > or & stays readable in the generated SQL; the engines accept
// either form.
func jsonString(s string) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		// Encoding a Go string cannot fail: invalid UTF-8 is replaced rather than
		// rejected. Fall back to quoting the two characters that must not appear
		// raw, rather than panicking.
		return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s) + `"`
	}
	// Encoder.Encode appends a newline.
	return strings.TrimSuffix(buf.String(), "\n")
}
