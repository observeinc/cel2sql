package dialect_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/observeinc/cel2sql/v3/dialect"
)

func TestJSONPathMember(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "bare word", key: "brand", want: ".brand"},
		{name: "leading underscore", key: "_brand", want: "._brand"},
		{name: "digits after a letter", key: "addr2", want: ".addr2"},
		{name: "space", key: "has space", want: `."has space"`},
		{name: "hyphen", key: "pod-name", want: `."pod-name"`},
		{name: "dot stays one member", key: "k8s.pod.name", want: `."k8s.pod.name"`},
		{name: "leading digit", key: "2fa", want: `."2fa"`},
		{name: "single quote", key: "it's", want: `."it's"`},
		{name: "bracket", key: "a[0]", want: `."a[0]"`},
		{name: "wildcard", key: "*", want: `."*"`},
		{name: "unicode", key: "ключ", want: `."ключ"`},
		{name: "empty", key: "", want: `.""`},
		{name: "angle brackets stay readable", key: "a<b>c", want: `."a<b>c"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, dialect.JSONPathMember(tt.key))
		})
	}
}

// TestJSONPathMember_JSONEscapes covers the keys that need escaping inside the
// quoted member. A member is a JSON string, so these come back with JSON escapes,
// which MySQL and SQLite resolve back to the original key -- see the live-engine
// cases in TestJSONVariableKeys_MySQL and TestJSONVariableKeys_SQLite.
func TestJSONPathMember_JSONEscapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "double quote", key: `say "hi"`, want: `."say \"hi\""`},
		{name: "backslash", key: `back\slash`, want: `."back\\slash"`},
		{name: "quote and backslash", key: `q"and\slash`, want: `."q\"and\\slash"`},
		{name: "tab", key: "tab\there", want: `."tab\there"`},
		{name: "newline", key: "nl\nhere", want: `."nl\nhere"`},
		{name: "other control character", key: "bell\x07here", want: `."bell\u0007here"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, dialect.JSONPathMember(tt.key))
		})
	}
}
