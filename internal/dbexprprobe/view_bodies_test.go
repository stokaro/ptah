package dbexprprobe_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbexprprobe"
)

// A body that selects `*` is never put through the server: the server expands
// the star against the columns the database has now, and the declaration asks
// for the columns the desired tables declare (stokaro/ptah#4057).
func TestSelectsStar_FindsAStarItem(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "a lone star", body: "SELECT * FROM all_items()"},
		{name: "a star after a comma", body: "SELECT id, * FROM items"},
		{name: "a qualified star", body: "SELECT i.* FROM items i"},
		{name: "a distinct star", body: "SELECT DISTINCT * FROM items"},
		{name: "a star after ALL", body: "SELECT ALL * FROM items"},
		{name: "a distinct-on star", body: "SELECT DISTINCT ON (id) * FROM items"},
		{name: "a star in a subquery", body: "SELECT id FROM (SELECT * FROM items) s"},
		{name: "a lower-case select", body: "select * from items"},
		{name: "a composite field star", body: "SELECT (r).* FROM item_rows() r"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(dbexprprobe.SelectsStar(test.body), qt.IsTrue)
		})
	}
}

// The controls: a multiplication, count(*), and a star inside text are not a
// star item, so the body is put through the server.
func TestSelectsStar_IgnoresWhatIsNotAStarItem(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "no star", body: "SELECT id, title FROM public.all_items()"},
		{name: "count of all rows", body: "SELECT count(*) FROM items"},
		{name: "a multiplication", body: "SELECT id * 2 AS twice FROM items"},
		{name: "a star in a string", body: "SELECT '*' AS mark FROM items"},
		{name: "a star in a dollar quote", body: "SELECT $$*$$ AS mark FROM items"},
		{name: "a star in a quoted name", body: `SELECT "a*b" FROM items`},
		{name: "a star in a comment", body: "SELECT id /* * */ FROM items"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(dbexprprobe.SelectsStar(test.body), qt.IsFalse)
		})
	}
}
