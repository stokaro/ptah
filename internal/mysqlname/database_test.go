package mysqlname_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/mysqlname"
)

// The predicates are SQL a server evaluates, and each arm answers a server
// setting CI runs on only in part: the byte comparison is what keeps two
// databases apart on lower_case_table_names=0, and the fold arm is what lets a
// server with lower_case_table_names=2 match at all. CI runs no server with
// lower_case_table_names=2, and on 1 a session that selects R_LC reads
// DATABASE() = r_lc, the name the views hold (measured on MySQL 26.10.0 and
// MariaDB 12.3.3), so without the fold arm every engine test still passes.
// These pin the text a server receives.
func TestSameDatabase(t *testing.T) {
	c := qt.New(t)

	c.Assert(mysqlname.SameDatabase("sp.TABLE_SCHEMA", "?"), qt.Equals,
		"(sp.TABLE_SCHEMA = ? AND (@@lower_case_table_names <> 0 OR "+
			"CONVERT(sp.TABLE_SCHEMA USING utf8mb4) COLLATE utf8mb4_bin = CONVERT(? USING utf8mb4)))")
}

func TestContainsName(t *testing.T) {
	c := qt.New(t)

	c.Assert(mysqlname.ContainsName("view_definition", "?"), qt.Equals,
		"(INSTR(view_definition, ?) > 0 AND (@@lower_case_table_names <> 0 OR "+
			"INSTR(CONVERT(view_definition USING utf8mb4) COLLATE utf8mb4_bin, CONVERT(? USING utf8mb4) COLLATE utf8mb4_bin) > 0))")
}

// The folding read asks the setting the SQL predicates test, so a connection
// that records the answer reads the server the predicates read.
func TestFoldsNamesQuery(t *testing.T) {
	c := qt.New(t)

	c.Assert(mysqlname.FoldsNamesQuery, qt.Equals, "SELECT @@lower_case_table_names <> 0")
}

// A server that folds names reads two spellings that differ in case as one
// database, and a server that keeps names apart reads them as two.
func TestNames_Same(t *testing.T) {
	tests := []struct {
		name        string
		folds       bool
		left, right string
		want        bool
	}{
		{name: "the same bytes on a server that folds", folds: true, left: "fa", right: "fa", want: true},
		{name: "another case on a server that folds", folds: true, left: "FA", right: "fa", want: true},
		{name: "mixed case on a server that folds", folds: true, left: "Fa", right: "fA", want: true},
		{name: "another name on a server that folds", folds: true, left: "fb", right: "fa", want: false},
		{name: "the same bytes on a server that keeps case apart", left: "fa", right: "fa", want: true},
		{name: "another case on a server that keeps case apart", left: "FA", right: "fa", want: false},
		{name: "another name on a server that keeps case apart", left: "fb", right: "fa", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlname.NamesOf(test.folds).Same(test.left, test.right), qt.Equals, test.want)
		})
	}
}

// The zero rule is the answer where no connection says how the server
// compares: as bytes.
func TestNames_ZeroValue(t *testing.T) {
	c := qt.New(t)
	var names mysqlname.Names

	c.Assert(names.Same("fa", "fa"), qt.IsTrue)
	c.Assert(names.Same("FA", "fa"), qt.IsFalse)
}
