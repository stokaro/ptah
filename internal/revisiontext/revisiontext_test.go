package revisiontext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/revisiontext"
)

// TestValidUTF8_KeepsValidTextByteForByte holds the half the revision table
// already depended on: an ordinary statement is recorded as written, so an
// assertion on a recorded statement — and Atlas-shaped parity with it — reads
// the same text the migration file carries.
func TestValidUTF8_KeepsValidTextByteForByte(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "ascii statement", value: "INSERT INTO users (id) VALUES (1);"},
		{name: "two byte rune", value: "INSERT INTO users (name) VALUES ('café');"},
		{name: "four byte rune", value: "INSERT INTO users (name) VALUES ('🙂');"},
		{name: "newlines and tabs", value: "CREATE TABLE a (\n\tid integer\n);"},
		{name: "backslash x already in the text", value: `SELECT '\xFF';`},
		{name: "encoded replacement character", value: "SELECT '�';"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(revisiontext.ValidUTF8(test.value), qt.Equals, test.value)
		})
	}
}

// TestValidUTF8_EscapesBytesThatAreNotUTF8 covers the bytes MySQL and MariaDB
// answer Error 1366 for. Each case pins where the escape lands, because a
// renderer that dropped the surrounding text would also produce valid UTF-8
// while losing the statement the operator has to recognize.
func TestValidUTF8_EscapesBytesThatAreNotUTF8(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{
			name:  "lone byte",
			value: "\xff",
			want:  `\xFF`,
		},
		{
			name:  "byte inside a literal",
			value: "INSERT INTO t (payload) VALUES ('\xff');",
			want:  `INSERT INTO t (payload) VALUES ('\xFF');`,
		},
		{
			name:  "byte at the end",
			value: "SELECT 1;\xff",
			want:  `SELECT 1;\xFF`,
		},
		{
			name:  "lone continuation byte",
			value: "SELECT '\x80';",
			want:  `SELECT '\x80';`,
		},
		{
			name:  "truncated two byte sequence",
			value: "SELECT '\xc3';",
			want:  `SELECT '\xC3';`,
		},
		{
			// 0xDE 0xAD is valid UTF-8 (U+07AD), so the bytes here are ones no
			// sequence can begin with.
			name:  "several bytes in a row",
			value: "VALUES ('\xff\xfe\xff')",
			want:  `VALUES ('\xFF\xFE\xFF')`,
		},
		{
			name:  "valid runes around an invalid byte",
			value: "VALUES ('café\xff🙂')",
			want:  `VALUES ('café\xFF🙂')`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(revisiontext.ValidUTF8(test.value), qt.Equals, test.want)
		})
	}
}
