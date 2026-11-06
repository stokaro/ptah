package mysqlgrants_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/mysqlgrants"
)

// The lines below are what MySQL 8.4 and MariaDB 12.3 printed for accounts given
// these grants, and the answers are what those servers then listed in
// information_schema.

// TestOnDatabase asks what an account holds on every table of database fx.
func TestOnDatabase(t *testing.T) {
	tests := []struct {
		name      string
		lines     []string
		options   mysqlgrants.Options
		database  string
		privilege string
		want      bool
	}{
		{
			name:      "a database-level row",
			lines:     []string{"GRANT USAGE ON *.* TO `a`@`%`", "GRANT SELECT ON `fx`.* TO `a`@`%`"},
			privilege: "SELECT",
			want:      true,
		},
		{
			name:      "a database-level row holds only what it names",
			lines:     []string{"GRANT SELECT ON `fx`.* TO `a`@`%`"},
			privilege: "TRIGGER",
			want:      false,
		},
		{
			name:      "a multi-word privilege",
			lines:     []string{"GRANT SELECT, SHOW VIEW, TRIGGER ON `fx`.* TO `a`@`%`"},
			privilege: "SHOW VIEW",
			want:      true,
		},
		{
			name:      "a row on another database",
			lines:     []string{"GRANT SELECT ON `other`.* TO `a`@`%`"},
			privilege: "SELECT",
			want:      false,
		},
		{
			name:      "a global row",
			lines:     []string{"GRANT SELECT ON *.* TO `a`@`%`"},
			privilege: "SELECT",
			want:      true,
		},
		{
			name:      "ALL PRIVILEGES on the database",
			lines:     []string{"GRANT ALL PRIVILEGES ON `fx`.* TO `a`@`%`"},
			privilege: "TRIGGER",
			want:      true,
		},
		{
			name:      "ALL PRIVILEGES globally, as MariaDB prints it with a password hash",
			lines:     []string{"GRANT ALL PRIVILEGES ON *.* TO `a`@`%` IDENTIFIED BY PASSWORD '*7B844B41A3799185EBF33B603FA8C632E65CA3EF' WITH GRANT OPTION"},
			privilege: "SHOW VIEW",
			want:      true,
		},
		{
			name:      "a dynamic privilege in a list without spaces",
			lines:     []string{"GRANT BACKUP_ADMIN,SHOW_ROUTINE,SYSTEM_USER ON *.* TO `a`@`%`"},
			privilege: "SHOW_ROUTINE",
			want:      true,
		},
		{
			name:      "WITH GRANT OPTION counts as a privilege",
			lines:     []string{"GRANT SELECT ON `fx`.* TO `a`@`%` WITH GRANT OPTION"},
			privilege: "GRANT OPTION",
			want:      true,
		},
		{
			name:      "a table-level row does not hold on the database",
			lines:     []string{"GRANT SELECT ON `fx`.`child` TO `a`@`%`"},
			privilege: "SELECT",
			want:      false,
		},
		{
			name:      "a routine-level row does not hold on the database",
			lines:     []string{"GRANT EXECUTE ON PROCEDURE `fx`.`p_noop` TO `a`@`%`"},
			privilege: "EXECUTE",
			want:      false,
		},
		{
			name:      "a wildcard row that matches",
			lines:     []string{"GRANT SELECT ON `f%`.* TO `a`@`%`"},
			privilege: "SELECT",
			want:      true,
		},
		{
			name:      "a single-character wildcard that matches",
			lines:     []string{"GRANT SELECT ON `f_`.* TO `a`@`%`"},
			privilege: "SELECT",
			want:      true,
		},
		{
			name:      "an escaped underscore is not a wildcard",
			lines:     []string{"GRANT SELECT ON `f\\_x`.* TO `a`@`%`"},
			database:  "fax",
			privilege: "SELECT",
			want:      false,
		},
		{
			name:      "an escaped underscore matches the name it spells, as the container images write it",
			lines:     []string{"GRANT ALL PRIVILEGES ON `ptah\\_test`.* TO `a`@`%`"},
			database:  "ptah_test",
			privilege: "SELECT",
			want:      true,
		},
		{
			name:      "a name is case-sensitive by default",
			lines:     []string{"GRANT SELECT ON `FX`.* TO `a`@`%`"},
			privilege: "SELECT",
			want:      false,
		},
		{
			name:      "a name folds with lower_case_table_names",
			lines:     []string{"GRANT SELECT ON `FX`.* TO `a`@`%`"},
			options:   mysqlgrants.Options{FoldCase: true},
			privilege: "SELECT",
			want:      true,
		},
		{
			name: "a literal row hides a wildcard row",
			lines: []string{
				"GRANT INSERT ON `fx`.* TO `a`@`%`",
				"GRANT SELECT, EXECUTE, SHOW VIEW, TRIGGER ON `f%`.* TO `a`@`%`",
			},
			privilege: "TRIGGER",
			want:      false,
		},
		{
			name: "the row whose wildcard comes later hides the other",
			lines: []string{
				"GRANT TRIGGER ON `f%`.* TO `a`@`%`",
				"GRANT SELECT ON `fx%`.* TO `a`@`%`",
			},
			privilege: "TRIGGER",
			want:      false,
		},
		{
			name: "the more specific row is the one that holds",
			lines: []string{
				"GRANT TRIGGER ON `f%`.* TO `a`@`%`",
				"GRANT SELECT ON `fx%`.* TO `a`@`%`",
			},
			privilege: "SELECT",
			want:      true,
		},
		{
			name: "rows that rank equal credit only what they share",
			lines: []string{
				"GRANT SELECT ON `f%`.* TO `a`@`%`",
				"GRANT TRIGGER ON `f_`.* TO `a`@`%`",
			},
			privilege: "TRIGGER",
			want:      false,
		},
		{
			name: "a global row adds to a database-level row",
			lines: []string{
				"GRANT TRIGGER ON *.* TO `a`@`%`",
				"GRANT SELECT ON `fx`.* TO `a`@`%`",
			},
			privilege: "TRIGGER",
			want:      true,
		},
		{
			name: "MariaDB adds an active role's rows, each grantee ranked apart",
			lines: []string{
				"GRANT `r`  TO `a`@`%`",
				"GRANT SELECT ON `fx%`.* TO `a`@`%`",
				"GRANT TRIGGER ON `f%`.* TO `r`",
				"SET DEFAULT ROLE `r` FOR `a`@`%`",
			},
			privilege: "TRIGGER",
			want:      true,
		},
		{
			name: "MariaDB holds what PUBLIC holds",
			lines: []string{
				"GRANT TRIGGER ON `fx`.* TO `a`@`%`",
				"GRANT SELECT ON `fx`.* TO PUBLIC",
			},
			privilege: "SELECT",
			want:      true,
		},
		{
			name: "a role that is not active prints nothing beyond its grant",
			lines: []string{
				"GRANT SELECT ON `fx`.* TO `a`@`%`",
				"GRANT `r`@`%` TO `a`@`%`",
			},
			options:   mysqlgrants.Options{MergedRoles: true},
			privilege: "TRIGGER",
			want:      false,
		},
		{
			name: "MySQL merges an active role's literal rows into the account's",
			lines: []string{
				"GRANT SELECT, TRIGGER ON `fx`.* TO `a`@`%`",
				"GRANT `r`@`%` TO `a`@`%`",
			},
			options:   mysqlgrants.Options{MergedRoles: true},
			privilege: "TRIGGER",
			want:      true,
		},
		{
			// Measured on MySQL 8.4: with the role active the server used `f%`,
			// which holds INSERT alone, and listed no trigger.
			name: "with a role granted on MySQL, wildcard rows do not rank by specificity",
			lines: []string{
				"GRANT INSERT ON `f%`.* TO `a`@`%`",
				"GRANT SELECT, TRIGGER ON `fx%`.* TO `a`@`%`",
				"GRANT `r`@`%` TO `a`@`%`",
			},
			options:   mysqlgrants.Options{MergedRoles: true},
			privilege: "TRIGGER",
			want:      false,
		},
		{
			name: "with a role granted on MySQL, a literal row still wins",
			lines: []string{
				"GRANT TRIGGER ON `fx`.* TO `a`@`%`",
				"GRANT SELECT ON `f%`.* TO `a`@`%`",
				"GRANT `r`@`%` TO `a`@`%`",
			},
			options:   mysqlgrants.Options{MergedRoles: true},
			privilege: "SELECT",
			want:      false,
		},
		{
			name:      "with partial_revokes on, a wildcard is an ordinary character",
			lines:     []string{"GRANT TRIGGER ON `f%`.* TO `a`@`%`"},
			options:   mysqlgrants.Options{LiteralDatabaseNames: true},
			privilege: "TRIGGER",
			want:      false,
		},
		{
			name: "a partial revoke takes a global privilege away on one database",
			lines: []string{
				"GRANT SELECT, SHOW VIEW, TRIGGER ON *.* TO `a`@`%`",
				"REVOKE TRIGGER ON `fx`.* FROM `a`@`%`",
			},
			options:   mysqlgrants.Options{LiteralDatabaseNames: true},
			privilege: "TRIGGER",
			want:      false,
		},
		{
			name: "a partial revoke leaves the rest of the global row",
			lines: []string{
				"GRANT SELECT, SHOW VIEW, TRIGGER ON *.* TO `a`@`%`",
				"REVOKE TRIGGER ON `fx`.* FROM `a`@`%`",
			},
			options:   mysqlgrants.Options{LiteralDatabaseNames: true},
			privilege: "SELECT",
			want:      true,
		},
		{
			name: "a partial revoke on another database takes nothing here",
			lines: []string{
				"GRANT TRIGGER ON *.* TO `a`@`%`",
				"REVOKE TRIGGER ON `other`.* FROM `a`@`%`",
			},
			options:   mysqlgrants.Options{LiteralDatabaseNames: true},
			privilege: "TRIGGER",
			want:      true,
		},
		{
			name:      "ANSI_QUOTES spells identifiers in double quotes",
			lines:     []string{`GRANT SELECT ON "fx".* TO "a"@"%"`},
			privilege: "SELECT",
			want:      true,
		},
		{
			name:      "a privilege on columns does not hold on the table or above",
			lines:     []string{"GRANT SELECT (`id`), INSERT ON `fx`.* TO `a`@`%`"},
			privilege: "SELECT",
			want:      false,
		},
		{
			name:      "the privilege beside one on columns does hold",
			lines:     []string{"GRANT SELECT (`id`, `name`), INSERT ON `fx`.* TO `a`@`%`"},
			privilege: "INSERT",
			want:      true,
		},
		{
			name:      "a proxy grant holds nothing",
			lines:     []string{"GRANT PROXY ON ''@'%' TO 'root'@'localhost' WITH GRANT OPTION"},
			privilege: "PROXY",
			want:      false,
		},
		{
			name:      "a line that is no grant holds nothing",
			lines:     []string{"SET DEFAULT ROLE `r` FOR `a`@`%`", "", "GRANT", "GRANT SELECT", "GRANT SELECT ON", "GRANT SELECT ON `fx`", "GRANT SELECT ON `fx`.*"},
			privilege: "SELECT",
			want:      false,
		},
		{
			name:      "a database whose name holds a dot",
			lines:     []string{"GRANT SELECT ON `f.x`.* TO `a`@`%`"},
			database:  "f.x",
			privilege: "SELECT",
			want:      true,
		},
		{
			name:      "a database whose name holds a backtick",
			lines:     []string{"GRANT SELECT ON `f``x`.* TO `a`@`%`"},
			database:  "f`x",
			privilege: "SELECT",
			want:      true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			grants := mysqlgrants.Parse(test.lines, test.options)
			c.Assert(grants.OnDatabase(databaseOr(test.database, "fx")).Holds(test.privilege), qt.Equals, test.want)
		})
	}
}

// databaseOr is the row's database, or the one most rows ask about.
func databaseOr(database, fallback string) string {
	if database == "" {
		return fallback
	}
	return database
}

// TestLikeSemantics holds the database-level pattern to the server's LIKE over a
// grid of patterns and names, including the ones a greedy match gets wrong.
func TestLikeSemantics(t *testing.T) {
	tests := []struct {
		pattern string
		name    string
		want    bool
	}{
		{pattern: "fx", name: "fx", want: true},
		{pattern: "fx", name: "fxy", want: false},
		{pattern: "fx", name: "f", want: false},
		{pattern: "%", name: "anything", want: true},
		{pattern: "%", name: "", want: true},
		{pattern: "_", name: "a", want: true},
		{pattern: "_", name: "ab", want: false},
		{pattern: "f%x", name: "fx", want: true},
		{pattern: "f%x", name: "faaax", want: true},
		{pattern: "f%x", name: "faaay", want: false},
		{pattern: "%x%y", name: "axbxcy", want: true},
		{pattern: "%x%y", name: "axbxc", want: false},
		{pattern: "a_c", name: "abc", want: true},
		{pattern: "a_c", name: "ac", want: false},
		{pattern: `a\_c`, name: "a_c", want: true},
		{pattern: `a\_c`, name: "abc", want: false},
		{pattern: `a\%c`, name: "a%c", want: true},
		{pattern: `a\%c`, name: "aXc", want: false},
		{pattern: `a\\c`, name: `a\c`, want: true},
		{pattern: `ptah\_rt\_%`, name: "ptah_rt_x", want: true},
		{pattern: `ptah\_rt\_%`, name: "ptahXrt_x", want: false},
		{pattern: "ab%%", name: "ab", want: true},
		{pattern: "abc", name: "ab", want: false},
	}

	for _, test := range tests {
		t.Run(test.pattern+" "+test.name, func(t *testing.T) {
			c := qt.New(t)
			grants := mysqlgrants.Parse([]string{"GRANT SELECT ON `" + test.pattern + "`.* TO `a`@`%`"}, mysqlgrants.Options{})
			c.Assert(grants.OnDatabase(test.name).Holds("SELECT"), qt.Equals, test.want)
		})
	}
}

// TestOnTable asks what an account holds on one table, at any level.
func TestOnTable(t *testing.T) {
	tests := []struct {
		name      string
		lines     []string
		options   mysqlgrants.Options
		table     string
		privilege string
		want      bool
	}{
		{
			name:      "a row on the table",
			lines:     []string{"GRANT TRIGGER ON `fx`.`child` TO `a`@`%`"},
			table:     "child",
			privilege: "TRIGGER",
			want:      true,
		},
		{
			name:      "a row on another table",
			lines:     []string{"GRANT TRIGGER ON `fx`.`child` TO `a`@`%`"},
			table:     "parent",
			privilege: "TRIGGER",
			want:      false,
		},
		{
			name:      "a row on the database",
			lines:     []string{"GRANT TRIGGER ON `fx`.* TO `a`@`%`"},
			table:     "parent",
			privilege: "TRIGGER",
			want:      true,
		},
		{
			name:      "a row on a table of another database",
			lines:     []string{"GRANT TRIGGER ON `other`.`child` TO `a`@`%`"},
			table:     "child",
			privilege: "TRIGGER",
			want:      false,
		},
		{
			name:      "ALL PRIVILEGES on the table",
			lines:     []string{"GRANT ALL PRIVILEGES ON `fx`.`child` TO `a`@`%`"},
			table:     "child",
			privilege: "TRIGGER",
			want:      true,
		},
		{
			name:      "a table name folds with lower_case_table_names",
			lines:     []string{"GRANT TRIGGER ON `fx`.`Child` TO `a`@`%`"},
			options:   mysqlgrants.Options{FoldCase: true},
			table:     "child",
			privilege: "TRIGGER",
			want:      true,
		},
		{
			name:      "a table name is not a pattern",
			lines:     []string{"GRANT TRIGGER ON `fx`.`c_ild` TO `a`@`%`"},
			table:     "child",
			privilege: "TRIGGER",
			want:      false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			grants := mysqlgrants.Parse(test.lines, test.options)
			c.Assert(grants.OnTable("fx", test.table).Holds(test.privilege), qt.Equals, test.want)
		})
	}
}

// TestGlobal separates what holds on every database from what holds on one.
func TestGlobal(t *testing.T) {
	tests := []struct {
		name      string
		lines     []string
		database  string
		privilege string
		want      bool
	}{
		{
			name:      "a global row",
			lines:     []string{"GRANT SHOW_ROUTINE ON *.* TO `a`@`%`"},
			privilege: "SHOW_ROUTINE",
			want:      true,
		},
		{
			name:      "ALL PRIVILEGES on a database is not global",
			lines:     []string{"GRANT ALL PRIVILEGES ON `fx`.* TO `a`@`%`"},
			privilege: "SHOW_ROUTINE",
			want:      false,
		},
		{
			name: "a partial revoke on the database asked about",
			lines: []string{
				"GRANT SELECT ON *.* TO `a`@`%`",
				"REVOKE SELECT ON `fx`.* FROM `a`@`%`",
			},
			database:  "fx",
			privilege: "SELECT",
			want:      false,
		},
		{
			name: "a partial revoke when no one database is asked about",
			lines: []string{
				"GRANT SELECT ON *.* TO `a`@`%`",
				"REVOKE SELECT ON `fx`.* FROM `a`@`%`",
			},
			privilege: "SELECT",
			want:      true,
		},
		{
			name: "a partial revoke of ALL PRIVILEGES",
			lines: []string{
				"GRANT ALL PRIVILEGES ON *.* TO `a`@`%`",
				"REVOKE ALL PRIVILEGES ON `fx`.* FROM `a`@`%`",
			},
			database:  "fx",
			privilege: "SELECT",
			want:      false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			grants := mysqlgrants.Parse(test.lines, mysqlgrants.Options{})
			c.Assert(grants.Global(test.database).Holds(test.privilege), qt.Equals, test.want)
		})
	}
}

// FuzzParse holds the parser to reading any line without a panic: the lines come
// from a server, and a shape it has not met must grant nothing rather than stop
// the read.
func FuzzParse(f *testing.F) {
	f.Add("GRANT SELECT (`id`), INSERT ON `fx`.`t` TO `a`@`%` WITH GRANT OPTION")
	f.Add("REVOKE ALL PRIVILEGES ON `fx`.* FROM 'a'@'%'")
	f.Add("GRANT `r`@`%`,`s`@`%` TO `a`@`%`")
	f.Add("GRANT EXECUTE ON PROCEDURE `fx`.`p` TO `a`@`%`")
	f.Add("GRANT SELECT ON \"f\"\"x\".* TO \"a\"")
	f.Add("GRANT ( ON")
	f.Fuzz(func(_ *testing.T, line string) {
		grants := mysqlgrants.Parse([]string{line}, mysqlgrants.Options{MergedRoles: true})
		_ = grants.OnTable("fx", "t").Holds("SELECT")
	})
}
