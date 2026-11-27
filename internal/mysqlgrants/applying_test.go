package mysqlgrants_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/mysqlgrants"
)

// TestOptions_ApplyingRows asks which of a subject's database-level rows the
// server applies to the database qabc. Each row without a comment is a
// measurement: roles held the rows on MySQL 8.4.13 and 26.10.0 and on MariaDB
// 11.8.9 and 12.3.3, each row with its own privilege, and a session that
// activated them showed which privilege it held. A row that says it ranks
// equal is the order no measurement can establish, and then every name that
// shares the rank is returned.
func TestOptions_ApplyingRows(t *testing.T) {
	mysql := mysqlgrants.Options{}
	mariaDB := mysqlgrants.Options{MariaDB: true}
	tests := []struct {
		name    string
		options mysqlgrants.Options
		lookup  mysqlgrants.Lookup
		rows    []string
		want    []string
	}{
		{name: "MySQL role: the database's own row shadows a pattern", options: mysql, lookup: mysqlgrants.RoleLookup, rows: []string{"qa%", "qabc"}, want: []string{"qabc"}},
		{name: "MySQL role: the own row shadows an underscore pattern", options: mysql, lookup: mysqlgrants.RoleLookup, rows: []string{"q_bc", "qabc"}, want: []string{"qabc"}},
		{name: "MySQL role: the first pattern in byte order, an underscore first", options: mysql, lookup: mysqlgrants.RoleLookup, rows: []string{"qa%", "q_bc"}, want: []string{"q_bc"}},
		{name: "MySQL role: the first pattern in byte order, the shorter first", options: mysql, lookup: mysqlgrants.RoleLookup, rows: []string{"qab%", "qa%"}, want: []string{"qa%"}},
		{name: "MySQL role: the first pattern in byte order, a percent sign first", options: mysql, lookup: mysqlgrants.RoleLookup, rows: []string{"qa%", "q%"}, want: []string{"q%"}},
		{name: "MySQL role: byte order over specificity", options: mysql, lookup: mysqlgrants.RoleLookup, rows: []string{"qa%c", "q_b%"}, want: []string{"q_b%"}},
		{name: "MySQL role: byte order between two wildcards", options: mysql, lookup: mysqlgrants.RoleLookup, rows: []string{"q_%c", "q%_c"}, want: []string{"q%_c"}},
		{name: "MySQL role: a lone percent sign first", options: mysql, lookup: mysqlgrants.RoleLookup, rows: []string{"qa%", "%"}, want: []string{"%"}},
		{name: "MySQL role: an escaped name applies nowhere", options: mysql, lookup: mysqlgrants.RoleLookup, rows: []string{`qa\bc`}},
		{name: "MySQL role: an escaped name leaves the pattern", options: mysql, lookup: mysqlgrants.RoleLookup, rows: []string{`qa\bc`, "qa%"}, want: []string{"qa%"}},
		{name: "MySQL role: an escaped name does not shadow the own row", options: mysql, lookup: mysqlgrants.RoleLookup, rows: []string{`qa\bc`, "qabc"}, want: []string{"qabc"}},
		{name: "MySQL role: another case does not match", options: mysql, lookup: mysqlgrants.RoleLookup, rows: []string{"QA%"}},
		{name: "MySQL role: a pattern that reaches nothing here", options: mysql, lookup: mysqlgrants.RoleLookup, rows: []string{"xy%"}},
		{name: "MySQL role: one name held twice", options: mysql, lookup: mysqlgrants.RoleLookup, rows: []string{"qa%", "qa%"}, want: []string{"qa%"}},
		// Not measured: a fold the measured servers did not have.
		{name: "MySQL role, folded: another case matches", options: mysqlgrants.Options{FoldCase: true}, lookup: mysqlgrants.RoleLookup, rows: []string{"QA%"}, want: []string{"QA%"}},
		{
			// Not measured: the order as written and folded disagree.
			name: "MySQL role, folded: two orders rank equal", options: mysqlgrants.Options{FoldCase: true}, lookup: mysqlgrants.RoleLookup,
			rows: []string{"qa%", "QA_C"}, want: []string{"QA_C", "qa%"},
		},
		{name: "MySQL account: the later first wildcard", options: mysql, rows: []string{"q_bc", "qa%"}, want: []string{"qa%"}},
		{name: "MySQL account: the longer prefix", options: mysql, rows: []string{"qa%", "qab%"}, want: []string{"qab%"}},
		{name: "MySQL account: a prefix after an underscore", options: mysql, rows: []string{"q_bc", "qab_"}, want: []string{"qab_"}},
		{name: "MySQL account: the own row first", options: mysql, rows: []string{"qa%", "qabc"}, want: []string{"qabc"}},
		{name: "MySQL account: a pattern before a lone percent sign", options: mysql, rows: []string{"%", "%bc"}, want: []string{"%bc"}},
		{name: "MySQL account: an escaped name applies", options: mysql, rows: []string{`qa\bc`, "qa%"}, want: []string{`qa\bc`}},
		// Measured on MySQL 8.4.13 and 26.10.0, the server applied qa%.
		{name: "MySQL account: a first wildcard at one byte ranks equal", options: mysql, rows: []string{"qa%", "qa_c"}, want: []string{"qa%", "qa_c"}},
		{name: "MariaDB: the own row shadows a pattern", options: mariaDB, rows: []string{"qabc", "qa%"}, want: []string{"qabc"}},
		{name: "MariaDB: the longer prefix", options: mariaDB, rows: []string{"q%", "qa%"}, want: []string{"qa%"}},
		{name: "MariaDB: an underscore pattern over a percent sign", options: mariaDB, rows: []string{"qa%", "q_bc"}, want: []string{"q_bc"}},
		{name: "MariaDB: more literal bytes", options: mariaDB, rows: []string{"qa%", "qab%"}, want: []string{"qab%"}},
		{name: "MariaDB: the shorter prefix breaks a tie", options: mariaDB, rows: []string{"qa%", "q%c"}, want: []string{"q%c"}},
		{name: "MariaDB: a leading percent sign", options: mariaDB, rows: []string{"q%", "%bc"}, want: []string{"%bc"}},
		{name: "MariaDB: two underscores over a percent sign", options: mariaDB, rows: []string{"qa%", "q__c"}, want: []string{"q__c"}},
		{name: "MariaDB: an underscore before the prefix", options: mariaDB, rows: []string{"qab_", "q_bc"}, want: []string{"q_bc"}},
		{name: "MariaDB: a lone percent sign last", options: mariaDB, rows: []string{"%", "qa%"}, want: []string{"qa%"}},
		{name: "MariaDB: underscores over more literal bytes", options: mariaDB, rows: []string{"qab%", "qa__"}, want: []string{"qa__"}},
		{name: "MariaDB: four underscores over three letters", options: mariaDB, rows: []string{"qab%", "____"}, want: []string{"____"}},
		{name: "MariaDB: more letters among underscores", options: mariaDB, rows: []string{"q___", "qa__"}, want: []string{"qa__"}},
		{name: "MariaDB: one percent sign over two wildcards", options: mariaDB, rows: []string{"q_b%", "qa%c"}, want: []string{"qa%c"}},
		{name: "MariaDB: an underscore pattern over a middle percent sign", options: mariaDB, rows: []string{"q%bc", "qa_c"}, want: []string{"qa_c"}},
		{name: "MariaDB: a letter over an underscore before the percent sign", options: mariaDB, rows: []string{"q_%", "qa%"}, want: []string{"qa%"}},
		{name: "MariaDB: an escaped name is literal", options: mariaDB, rows: []string{"qa%", `qa\bc`}, want: []string{`qa\bc`}},
		{name: "MariaDB: the lookup kind changes nothing", options: mariaDB, lookup: mysqlgrants.RoleLookup, rows: []string{"q%", "qa%"}, want: []string{"qa%"}},
		// Measured on MariaDB 11.8.9 and 12.3.3, the server applied `%_bc`.
		{name: "MariaDB: equal weights rank equal", options: mariaDB, rows: []string{"_%bc", "%_bc"}, want: []string{"%_bc", "_%bc"}},
		// Measured on MariaDB 11.8.9 and 12.3.3, the server applied `q_%c`.
		{name: "MariaDB: equal weights and prefix rank equal", options: mariaDB, rows: []string{"q%_c", "q_%c"}, want: []string{"q%_c", "q_%c"}},
		// Measured on MariaDB 11.8.9 and 12.3.3, the server applied `qa\bc`.
		{name: "MariaDB: an escaped name ranks equal with the own row", options: mariaDB, rows: []string{"qabc", `qa\bc`}, want: []string{`qa\bc`, "qabc"}},
		{name: "partial_revokes: the own row alone", options: mysqlgrants.Options{LiteralDatabaseNames: true}, lookup: mysqlgrants.RoleLookup, rows: []string{"qa%", "qabc"}, want: []string{"qabc"}},
		{
			name: "partial_revokes, folded: two spellings rank equal", options: mysqlgrants.Options{LiteralDatabaseNames: true, FoldCase: true},
			lookup: mysqlgrants.RoleLookup, rows: []string{"qabc", "QABC"}, want: []string{"QABC", "qabc"},
		},
		{name: "no row", options: mysql, lookup: mysqlgrants.RoleLookup},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.options.ApplyingRows(test.lookup, "qabc", test.rows), qt.DeepEquals, test.want)
		})
	}
}

// TestOptions_ApplyingRows_PartialRevokes asks which rows MySQL applies with
// partial_revokes ON, where `%` and `_` are the characters themselves. Each
// row is a measurement on MySQL 8.4.13 and 26.10.0 with
// lower_case_table_names=0: roles or an account held the rows, and a session
// that activated them showed which privilege it held.
func TestOptions_ApplyingRows_PartialRevokes(t *testing.T) {
	literal := mysqlgrants.Options{LiteralDatabaseNames: true}
	tests := []struct {
		name     string
		lookup   mysqlgrants.Lookup
		database string
		rows     []string
		want     []string
	}{
		{name: "a role's row in another case on a name holding an underscore", lookup: mysqlgrants.RoleLookup, database: "x_y", rows: []string{"X_Y"}, want: []string{"X_Y"}},
		{name: "a role's row in another case on a name holding a percent sign", lookup: mysqlgrants.RoleLookup, database: "x%y", rows: []string{"X%Y"}, want: []string{"X%Y"}},
		{name: "an underscore is not a wildcard", lookup: mysqlgrants.RoleLookup, database: "xay", rows: []string{"X_Y"}},
		{name: "a percent sign is not a wildcard", lookup: mysqlgrants.RoleLookup, database: "xzzy", rows: []string{"X%Y"}},
		{name: "a role's row in another case on a name holding neither", lookup: mysqlgrants.RoleLookup, database: "xy", rows: []string{"XY"}},
		{name: "the spelling first in byte order shadows the database's own", lookup: mysqlgrants.RoleLookup, database: "x_y", rows: []string{"x_y", "X_Y"}, want: []string{"X_Y"}},
		{name: "two roles' rows on the database's own name", lookup: mysqlgrants.RoleLookup, database: "x_y", rows: []string{"x_y", "x_y"}, want: []string{"x_y"}},
		{name: "an account's row in another case", lookup: mysqlgrants.AccountLookup, database: "x_y", rows: []string{"X_Y"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(literal.ApplyingRows(test.lookup, test.database, test.rows), qt.DeepEquals, test.want)
		})
	}
}

// TestOptions_RoleRowReaches compares a name holding `%` or `_` without case
// only where MySQL reads the characters literally, under partial_revokes.
func TestOptions_RoleRowReaches(t *testing.T) {
	tests := []struct {
		name     string
		options  mysqlgrants.Options
		row      string
		database string
		want     bool
	}{
		{name: "partial_revokes: another case", options: mysqlgrants.Options{LiteralDatabaseNames: true}, row: "X_Y", database: "x_y", want: true},
		{name: "partial_revokes: no wildcard", options: mysqlgrants.Options{LiteralDatabaseNames: true}, row: "X_Y", database: "xay", want: false},
		{name: "partial_revokes: another case of a name holding neither", options: mysqlgrants.Options{LiteralDatabaseNames: true}, row: "XY", database: "xy", want: false},
		{name: "without partial_revokes: a wildcard", options: mysqlgrants.Options{}, row: "x_y", database: "xay", want: true},
		{name: "without partial_revokes: another case", options: mysqlgrants.Options{}, row: "X_Y", database: "x_y", want: false},
		{name: "MariaDB: another case", options: mysqlgrants.Options{LiteralDatabaseNames: true, MariaDB: true}, row: "X_Y", database: "x_y", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.options.RoleRowReaches(test.row, test.database), qt.Equals, test.want)
		})
	}
}

// TestOptions_ApplyingRows_LongMariaDBPattern returns both rows where a MariaDB
// pattern's literal bytes exceed the longest database name, 64: the server's
// unsigned arithmetic wraps there, so the order is unknown. No server was
// measured with such a pattern.
func TestOptions_ApplyingRows_LongMariaDBPattern(t *testing.T) {
	c := qt.New(t)
	long := strings.Repeat("é", 33)

	got := mysqlgrants.Options{MariaDB: true}.ApplyingRows(mysqlgrants.AccountLookup, long+"x", []string{"%x", long + "%"})

	c.Assert(got, qt.DeepEquals, []string{"%x", long + "%"})
}

// TestOptions_ApplyingRows_EscapedWildcard asks about the database qa_c, whose
// own row is a pattern to the server, beside the same name with the "_"
// escaped, which matches qa_c alone: both engines apply the escaped name,
// MySQL because it sorts first byte for byte and MariaDB because it matches
// fewer names. integration/schema_security_reach_e2e_test.go measures the
// shape on MySQL 8.4.13 and 26.10.0 and on MariaDB 11.8.9 and 12.3.3.
func TestOptions_ApplyingRows_EscapedWildcard(t *testing.T) {
	tests := []struct {
		name    string
		options mysqlgrants.Options
	}{
		{name: "MySQL", options: mysqlgrants.Options{}},
		{name: "MariaDB", options: mysqlgrants.Options{MariaDB: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := test.options.ApplyingRows(mysqlgrants.RoleLookup, "qa_c", []string{"qa_c", `qa\_c`})

			c.Assert(got, qt.DeepEquals, []string{`qa\_c`})
		})
	}
}
