package ydbview_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbview"
)

func TestRealmQueryText_RemovesOnlyTheConnectionPrefix(t *testing.T) {
	const root = "/local/ptah_dev/run"
	const prefix = "PRAGMA TablePathPrefix('/local/ptah_dev/run');\n"
	const query = "SELECT id FROM items"
	tests := []struct {
		name, body, want string
	}{
		{name: "realm prefix", body: prefix + "\n" + query, want: "\n" + query},
		{name: "no prefix", body: query, want: query},
		{name: "different root", body: "PRAGMA TablePathPrefix('/local/other');\n" + query,
			want: "PRAGMA TablePathPrefix('/local/other');\n" + query},
		{name: "user pragma", body: prefix + "PRAGMA OrderedColumns;\n" + query,
			want: "PRAGMA OrderedColumns;\n" + query},
		{name: "repeated prefix", body: prefix + prefix + query, want: prefix + query},
		{name: "prefix in literal", body: "SELECT \"" + prefix + "\"", want: "SELECT \"" + prefix + "\""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbview.RealmQueryText(test.body, root), qt.Equals, test.want)
		})
	}
}
