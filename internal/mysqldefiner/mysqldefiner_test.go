package mysqldefiner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/mysqldefiner"
)

func TestScan_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      mysqldefiner.Named
	}{
		{
			name:      "a view the planner writes",
			statement: "CREATE OR REPLACE DEFINER=`app`@`%` SQL SECURITY DEFINER VIEW `my_docs` AS\nSELECT 1",
			want:      mysqldefiner.Named{Object: "view my_docs", Definer: "app@%"},
		},
		{
			name:      "single quotes and an algorithm",
			statement: "CREATE ALGORITHM=MERGE DEFINER='app'@'10.0.%' VIEW v AS SELECT 1",
			want:      mysqldefiner.Named{Object: "view v", Definer: "app@10.0.%"},
		},
		{
			name:      "an unquoted account and a qualified procedure",
			statement: "CREATE DEFINER=app@localhost PROCEDURE shop.refresh() SELECT 1",
			want:      mysqldefiner.Named{Object: "procedure shop.refresh", Definer: "app@localhost"},
		},
		{
			name:      "a definer without a host is the account with host %",
			statement: "CREATE DEFINER = app TRIGGER t BEFORE INSERT ON x FOR EACH ROW SET @a = 1",
			want:      mysqldefiner.Named{Object: "trigger t", Definer: "app@%"},
		},
		{
			name:      "an aggregate function after a comment",
			statement: "/* keep */ CREATE DEFINER=\"app\"@\"%\" AGGREGATE FUNCTION f RETURNS INT SONAME 'x.so'",
			want:      mysqldefiner.Named{Object: "function f", Definer: "app@%"},
		},
		{
			name:      "an event that may exist",
			statement: "CREATE DEFINER=`app`@`%` EVENT IF NOT EXISTS ev ON SCHEDULE EVERY 1 DAY DO SELECT 1",
			want:      mysqldefiner.Named{Object: "event ev", Definer: "app@%"},
		},
		{
			name:      "ALTER of a view",
			statement: "ALTER DEFINER=`app`@`%` SQL SECURITY INVOKER VIEW v AS SELECT 1",
			want:      mysqldefiner.Named{Object: "view v", Definer: "app@%"},
		},
		{
			name:      "a doubled backtick",
			statement: "CREATE DEFINER=`a``b`@`%` VIEW v AS SELECT 1",
			want:      mysqldefiner.Named{Object: "view v", Definer: "a`b@%"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := mysqldefiner.Scan([]string{test.statement})

			c.Assert(got, qt.DeepEquals, []mysqldefiner.Named{test.want})
		})
	}
}

// Statements that name no account other than the one running them, and ones
// the scan cannot follow to an account, yield nothing.
func TestScan_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "no clause", statement: "CREATE OR REPLACE VIEW v AS SELECT 1"},
		{name: "CURRENT_USER", statement: "CREATE DEFINER=CURRENT_USER VIEW v AS SELECT 1"},
		{name: "CURRENT_USER()", statement: "CREATE DEFINER=CURRENT_USER() PROCEDURE p() SELECT 1"},
		{name: "a table", statement: "CREATE TABLE definer (id int)"},
		{name: "a select", statement: "SELECT 'CREATE DEFINER=`app`@`%` VIEW v AS SELECT 1'"},
		{name: "a clause the statement does not finish", statement: "CREATE DEFINER="},
		{name: "an empty user", statement: "CREATE DEFINER=''@'%' VIEW v AS SELECT 1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := mysqldefiner.Scan([]string{test.statement})

			c.Assert(got, qt.HasLen, 0)
		})
	}
}

// The lines are what plain SHOW GRANTS answers, measured: MySQL merges an
// active role's privileges into the account's line, MariaDB lists the role's
// grants on lines of their own.
func TestGlobalPrivileges_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		lines []string
		want  map[string]bool
	}{
		{
			name: "mysql with an active role",
			lines: []string{
				"GRANT USAGE ON *.* TO `dep_role`@`%`",
				"GRANT ALLOW_NONEXISTENT_DEFINER,SET_ANY_DEFINER ON *.* TO `dep_role`@`%`",
				"GRANT ALL PRIVILEGES ON `m`.* TO `dep_role`@`%`",
				"GRANT `r_definer`@`%` TO `dep_role`@`%`",
			},
			want: map[string]bool{"USAGE": true, "ALLOW_NONEXISTENT_DEFINER": true, "SET_ANY_DEFINER": true},
		},
		{
			name: "mariadb with a nested role",
			lines: []string{
				"GRANT `r_outer` TO `dep_nested`@`%`",
				"GRANT USAGE ON *.* TO `dep_nested`@`%` IDENTIFIED BY PASSWORD '*21DB'",
				"GRANT ALL PRIVILEGES ON `m`.* TO `dep_nested`@`%`",
				"GRANT `r_inner` TO `r_outer`",
				"GRANT USAGE ON *.* TO `r_outer`",
				"GRANT SET USER ON *.* TO `r_inner`",
				"SET DEFAULT ROLE `r_outer` FOR `dep_nested`@`%`",
			},
			want: map[string]bool{"USAGE": true, "SET USER": true},
		},
		{
			name:  "mariadb root",
			lines: []string{"GRANT ALL PRIVILEGES ON *.* TO `root`@`%` WITH GRANT OPTION"},
			want:  map[string]bool{"ALL PRIVILEGES": true},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, found := mysqldefiner.GlobalPrivileges(test.lines)

			c.Assert(found, qt.IsTrue)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// With no line on *.*, the answer is not a list of what the account holds,
// so the caller decides nothing from it.
func TestGlobalPrivileges_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, found := mysqldefiner.GlobalPrivileges([]string{"GRANT ALL PRIVILEGES ON `m`.* TO `x`@`%`"})

	c.Assert(found, qt.IsFalse)
	c.Assert(got, qt.HasLen, 0)
}

// Each row is a measured case: the privilege the account holds and whether
// the server accepted the statement naming another definer.
func TestDecide(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		privileges string
		existence  mysqldefiner.Existence
		want       mysqldefiner.Verdict
	}{
		{name: "mysql without a privilege", dialect: platform.MySQL, privileges: "USAGE",
			existence: mysqldefiner.Exists, want: mysqldefiner.Verdict{Decided: true, Missing: "SET_ANY_DEFINER"}},
		{name: "mysql without a privilege, missing definer", dialect: platform.MySQL, privileges: "USAGE",
			existence: mysqldefiner.Missing,
			want:      mysqldefiner.Verdict{Decided: true, Missing: "SET_ANY_DEFINER and ALLOW_NONEXISTENT_DEFINER"}},
		{name: "mysql SET_ANY_DEFINER, existing definer", dialect: platform.MySQL, privileges: "SET_ANY_DEFINER",
			existence: mysqldefiner.Exists, want: mysqldefiner.Verdict{Decided: true}},
		{name: "mysql SET_ANY_DEFINER, missing definer", dialect: platform.MySQL, privileges: "SET_ANY_DEFINER",
			existence: mysqldefiner.Missing, want: mysqldefiner.Verdict{Decided: true, Missing: "ALLOW_NONEXISTENT_DEFINER"}},
		{name: "mysql SET_ANY_DEFINER, existence unknown", dialect: platform.MySQL, privileges: "SET_ANY_DEFINER",
			existence: mysqldefiner.ExistenceUnknown, want: mysqldefiner.Verdict{}},
		{name: "mysql both privileges", dialect: platform.MySQL, privileges: "SET_ANY_DEFINER,ALLOW_NONEXISTENT_DEFINER",
			existence: mysqldefiner.Missing, want: mysqldefiner.Verdict{Decided: true}},
		{name: "mysql SUPER", dialect: platform.MySQL, privileges: "SUPER",
			existence: mysqldefiner.Missing, want: mysqldefiner.Verdict{Decided: true}},
		{name: "mariadb without a privilege", dialect: platform.MariaDB, privileges: "USAGE",
			existence: mysqldefiner.ExistenceUnknown, want: mysqldefiner.Verdict{Decided: true, Missing: "SET USER"}},
		{name: "mariadb SET USER", dialect: platform.MariaDB, privileges: "SET USER",
			existence: mysqldefiner.ExistenceUnknown, want: mysqldefiner.Verdict{Decided: true}},
		{name: "mariadb ALL PRIVILEGES", dialect: platform.MariaDB, privileges: "ALL PRIVILEGES",
			existence: mysqldefiner.ExistenceUnknown, want: mysqldefiner.Verdict{Decided: true}},
		{name: "mariadb SUPER alone is decided by the server's version", dialect: platform.MariaDB, privileges: "SUPER",
			existence: mysqldefiner.ExistenceUnknown, want: mysqldefiner.Verdict{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			privileges := make(map[string]bool)
			for privilege := range strings.SplitSeq(test.privileges, ",") {
				privileges[privilege] = true
			}

			got := mysqldefiner.Decide(test.dialect, privileges, test.existence)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestPrivilegeError(t *testing.T) {
	c := qt.New(t)
	err := &mysqldefiner.PrivilegeError{Missing: []mysqldefiner.MissingPrivilege{{
		Object: "view my_docs", Definer: "app@%", Account: "deployer@%", Privilege: "SET_ANY_DEFINER",
		Source: "20240102000000_change.sql",
	}}}

	c.Assert(err.Error(), qt.Equals, "refusing before any statement runs: view my_docs in "+
		"20240102000000_change.sql names the definer app@%, and the connected account deployer@% lacks "+
		"SET_ANY_DEFINER, which naming another account needs. Grant it with GRANT SET_ANY_DEFINER ON *.* "+
		"TO 'deployer'@'%', or apply as app@%")
}
