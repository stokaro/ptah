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
