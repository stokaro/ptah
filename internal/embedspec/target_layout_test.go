package embedspec_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedspec"
)

// withLayout is `complete` with a target.layout key, and with its target table
// moved off the source: the own-table layout refuses a target that names the
// relation it would go on to drop, so a fixture that changed only the layout
// would be refused for a reason this file is not about.
func withLayout(word string) []byte {
	document := strings.Replace(complete,
		"target:\n  schema: public\n  table: articles\n",
		"target:\n  schema: public\n  table: article_vectors\n  layout: "+word+"\n", 1)
	return []byte(document)
}

// TestParse_ReadsTheTargetLayoutHappyPath pins the two words an author writes
// and the one they can omit.
//
// The omitted case is not decoration. It is the promise that a specification
// written before this key existed still means what it meant: its vectors are
// columns on the relation it names, and nothing about it authorizes Ptah to
// create or drop a table.
func TestParse_ReadsTheTargetLayoutHappyPath(t *testing.T) {
	tests := []struct {
		name     string
		document []byte
		want     embedgen.TargetLayout
	}{
		{name: "own_table", document: withLayout("own_table"), want: embedgen.LayoutOwnTable},
		{name: "source_columns", document: withLayout("source_columns"), want: embedgen.LayoutSourceColumns},
		{name: "omitted", document: []byte(complete), want: embedgen.LayoutSourceColumns},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			loaded, err := embedspec.Parse(test.document, "spec.yaml")

			c.Assert(err, qt.IsNil)
			c.Assert(loaded.Spec.Target.Layout, qt.Equals, test.want)
		})
	}
}

// TestParse_RefusesALayoutItDoesNotAct is the refusal that keeps a misspelling
// from becoming the default.
//
// `own-table` is the misspelling to expect, and folding it to the zero value
// would put the generation's columns on the application's own rows after its
// author asked for a relation of the generation's own -- silently, and with a
// specification that reads as though it said so.
func TestParse_RefusesALayoutItDoesNotAct(t *testing.T) {
	tests := []struct {
		name    string
		word    string
		wantErr string
	}{
		{
			name:    "a hyphen for an underscore",
			word:    "own-table",
			wantErr: `spec.yaml: target.layout "own-table" is not one this build acts on; it has source_columns, own_table`,
		},
		{
			name:    "the Go spelling of the zero value",
			word:    `""`,
			wantErr: `spec.yaml: target.layout "" is not one this build acts on; it has source_columns, own_table`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := embedspec.Parse(withLayout(test.word), "spec.yaml")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
