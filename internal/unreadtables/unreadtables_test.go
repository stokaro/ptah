package unreadtables_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/unreadtables"
)

func unread(schema, name, reason string) catalog.UnreadTable {
	builder := objectidentity.NewBuilder(identifier.Semantics{})
	return catalog.UnreadTable{Identity: builder.TablePartsVerbatim(schema, name), Reason: reason}
}

func TestReport_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		tables []catalog.UnreadTable
		want   string
	}{
		{"no unread table", nil, ""},
		{"one table", []catalog.UnreadTable{unread("dir", "rep", "rep is a replica table")},
			"note: 1 table this read does not describe: dir.rep: rep is a replica table\n"},
		{"two tables", []catalog.UnreadTable{unread("dir", "a", "first reason"), unread("", "b", "second reason")},
			"note: 2 tables this read does not describe:\n  dir.a: first reason\n  b: second reason\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out strings.Builder
			unreadtables.Report(&out, &catalog.Database{UnreadTables: test.tables})
			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

// A nil stream or read writes nothing and does not panic.
func TestReport_NilInputs(t *testing.T) {
	c := qt.New(t)
	schema := &catalog.Database{UnreadTables: []catalog.UnreadTable{unread("dir", "rep", "reason")}}
	unreadtables.Report(nil, schema)
	var out strings.Builder
	unreadtables.Report(&out, nil)
	c.Assert(out.String(), qt.Equals, "")
}
