package mysqldefiner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/mysqldefiner"
)

// The servers the scan and the decision are asked about, by the version each
// reports.
var (
	mysql2610 = mysqldefiner.Server{Dialect: platform.MySQL, VersionID: 261000}
	mysql80   = mysqldefiner.Server{Dialect: platform.MySQL, VersionID: 80046}
	mysql57   = mysqldefiner.Server{Dialect: platform.MySQL, VersionID: 50744}
	mariadb   = mysqldefiner.Server{Dialect: platform.MariaDB, VersionID: 120303}
	unknown   = mysqldefiner.Server{Dialect: platform.MySQL}
)

func TestScan_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		server    mysqldefiner.Server
		statement string
		want      mysqldefiner.Named
	}{
		{
			name:      "a view the planner writes",
			server:    mysql2610,
			statement: "CREATE OR REPLACE DEFINER=`app`@`%` SQL SECURITY DEFINER VIEW `my_docs` AS\nSELECT 1",
			want:      mysqldefiner.Named{Object: "view my_docs", Definer: "app@%"},
		},
		{
			name:      "single quotes and an algorithm",
			server:    mysql2610,
			statement: "CREATE ALGORITHM=MERGE DEFINER='app'@'10.0.%' VIEW v AS SELECT 1",
			want:      mysqldefiner.Named{Object: "view v", Definer: "app@10.0.%"},
		},
		{
			name:      "an unquoted account and a qualified procedure",
			server:    mysql2610,
			statement: "CREATE DEFINER=app@localhost PROCEDURE shop.refresh() SELECT 1",
			want:      mysqldefiner.Named{Object: "procedure shop.refresh", Definer: "app@localhost"},
		},
		{
			name:      "a definer without a host is the account with host %",
			server:    mysql2610,
			statement: "CREATE DEFINER = app TRIGGER t BEFORE INSERT ON x FOR EACH ROW SET @a = 1",
			want:      mysqldefiner.Named{Object: "trigger t", Definer: "app@%", Hostless: true},
		},
		{
			name:   "a view as mysqldump writes it",
			server: mysql2610,
			statement: "/*!50001 CREATE ALGORITHM=UNDEFINED */\n/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */\n" +
				"/*!50001 VIEW `v` AS select 1 AS `1` */",
			want: mysqldefiner.Named{Object: "view v", Definer: "root@localhost"},
		},
		{
			name:   "a trigger as mysqldump writes it",
			server: mysql2610,
			statement: "/*!50003 CREATE*/ /*!50017 DEFINER=`app`@`%`*/ /*!50003 TRIGGER `t` BEFORE INSERT ON `x` " +
				"FOR EACH ROW SET @a = 1 */",
			want: mysqldefiner.Named{Object: "trigger t", Definer: "app@%"},
		},
		{
			name:      "a MariaDB package",
			server:    mysql2610,
			statement: "CREATE OR REPLACE DEFINER=`app`@`%` PACKAGE pkg AS FUNCTION f RETURN INT; END",
			want:      mysqldefiner.Named{Object: "package pkg", Definer: "app@%"},
		},
		{
			name:      "a MariaDB package body",
			server:    mysql2610,
			statement: "CREATE DEFINER=`app`@`%` PACKAGE BODY pkg AS FUNCTION f RETURN INT AS BEGIN RETURN 1; END; END",
			want:      mysqldefiner.Named{Object: "package body pkg", Definer: "app@%"},
		},
		{
			name:      "an aggregate function after a comment",
			server:    mysql2610,
			statement: "/* keep */ CREATE DEFINER=\"app\"@\"%\" AGGREGATE FUNCTION f RETURNS INT SONAME 'x.so'",
			want:      mysqldefiner.Named{Object: "function f", Definer: "app@%"},
		},
		{
			name:      "an event that may exist",
			server:    mysql2610,
			statement: "CREATE DEFINER=`app`@`%` EVENT IF NOT EXISTS ev ON SCHEDULE EVERY 1 DAY DO SELECT 1",
			want:      mysqldefiner.Named{Object: "event ev", Definer: "app@%"},
		},
		{
			name:      "ALTER of a view",
			server:    mysql2610,
			statement: "ALTER DEFINER=`app`@`%` SQL SECURITY INVOKER VIEW v AS SELECT 1",
			want:      mysqldefiner.Named{Object: "view v", Definer: "app@%"},
		},
		{
			name:      "a doubled backtick",
			server:    mysql2610,
			statement: "CREATE DEFINER=`a``b`@`%` VIEW v AS SELECT 1",
			want:      mysqldefiner.Named{Object: "view v", Definer: "a`b@%"},
		},
		{
			name:      "CURRENT_ROLE, an account on MySQL",
			server:    mysql2610,
			statement: "CREATE OR REPLACE DEFINER=CURRENT_ROLE VIEW v AS SELECT 1",
			want:      mysqldefiner.Named{Object: "view v", Definer: "CURRENT_ROLE@%", Hostless: true},
		},
		{
			name:      "CURRENT_ROLE, the current role on MariaDB, is host-less",
			server:    mariadb,
			statement: "CREATE OR REPLACE DEFINER=CURRENT_ROLE VIEW v AS SELECT 1",
			want:      mysqldefiner.Named{Object: "view v", Definer: "CURRENT_ROLE@%", Hostless: true},
		},
		{
			name:      "a comment gated on a version the server has reached",
			server:    mysql2610,
			statement: "CREATE /*!80023 DEFINER=`app`@`%` */ VIEW v AS SELECT 1",
			want:      mysqldefiner.Named{Object: "view v", Definer: "app@%"},
		},
		{
			name:      "a comment gated on the server's own version",
			server:    mysql80,
			statement: "CREATE /*!80046 DEFINER=`app`@`%` */ VIEW v AS SELECT 1",
			want:      mysqldefiner.Named{Object: "view v", Definer: "app@%"},
		},
		{
			name:      "a six-digit gate on MySQL",
			server:    mysql2610,
			statement: "CREATE /*!100100 DEFINER=`app`@`%` */ VIEW v AS SELECT 1",
			want:      mysqldefiner.Named{Object: "view v", Definer: "app@%"},
		},
		{
			name:      "a six-digit gate on MariaDB",
			server:    mariadb,
			statement: "CREATE /*!100100 DEFINER=`app`@`%` */ VIEW v AS SELECT 1",
			want:      mysqldefiner.Named{Object: "view v", Definer: "app@%"},
		},
		{
			name:      "a MariaDB comment on MariaDB",
			server:    mariadb,
			statement: "CREATE /*M!100100 DEFINER=`app`@`%` */ VIEW v AS SELECT 1",
			want:      mysqldefiner.Named{Object: "view v", Definer: "app@%"},
		},
		{
			name:      "a MariaDB comment gated on a MySQL 5.7 version, on MariaDB",
			server:    mariadb,
			statement: "CREATE /*M!50700 DEFINER=`app`@`%` */ VIEW v AS SELECT 1",
			want:      mysqldefiner.Named{Object: "view v", Definer: "app@%"},
		},
		{
			name:   "a view as mysqldump writes it, on MariaDB",
			server: mariadb,
			statement: "/*!50001 CREATE ALGORITHM=UNDEFINED */\n/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */\n" +
				"/*!50001 VIEW `v` AS select 1 AS `1` */",
			want: mysqldefiner.Named{Object: "view v", Definer: "root@localhost"},
		},
		{
			name:      "a view as mysqldump writes it, on a server of unknown version",
			server:    unknown,
			statement: "/*!50001 CREATE ALGORITHM=UNDEFINED */ /*!50013 DEFINER=`root`@`localhost` */ /*!50001 VIEW `v` AS select 1 */",
			want:      mysqldefiner.Named{Object: "view v", Definer: "root@localhost"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := mysqldefiner.Scan(test.server, []string{test.statement})

			c.Assert(got, qt.DeepEquals, []mysqldefiner.Named{test.want})
		})
	}
}

// Statements that name no account other than the one running them, ones whose
// clause the server does not run, and ones the scan cannot follow to an
// account, yield nothing.
func TestScan_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		server    mysqldefiner.Server
		statement string
	}{
		{name: "no clause", server: mysql2610, statement: "CREATE OR REPLACE VIEW v AS SELECT 1"},
		{name: "CURRENT_USER", server: mysql2610, statement: "CREATE DEFINER=CURRENT_USER VIEW v AS SELECT 1"},
		{name: "CURRENT_USER()", server: mysql2610, statement: "CREATE DEFINER=CURRENT_USER() PROCEDURE p() SELECT 1"},
		{name: "CURRENT_ROLE() on MariaDB", server: mariadb, statement: "CREATE DEFINER=CURRENT_ROLE() VIEW v AS SELECT 1"},
		{name: "a backslash in a quoted user", server: mysql2610, statement: "CREATE DEFINER='o\\'b'@'%' VIEW v AS SELECT 1"},
		{name: "a backslash in a quoted host", server: mysql2610, statement: "CREATE DEFINER=`app`@'h\\%' VIEW v AS SELECT 1"},
		{name: "a comment gated on a later version", server: mysql80,
			statement: "CREATE /*!80047 DEFINER=`app`@`%` */ VIEW v AS SELECT 1"},
		{name: "a MySQL 8 gate on MariaDB", server: mariadb, statement: "CREATE /*!80023 DEFINER=`app`@`%` */ VIEW v AS SELECT 1"},
		{name: "a MySQL 5.7 gate on MariaDB", server: mariadb, statement: "CREATE /*!50700 DEFINER=`app`@`%` */ VIEW v AS SELECT 1"},
		{name: "a gate from 5.7 on a server of unknown version", server: unknown,
			statement: "CREATE /*!50700 DEFINER=`app`@`%` */ VIEW v AS SELECT 1"},
		{name: "a six-digit gate after the server's version", server: mysql2610,
			statement: "CREATE /*!500130 DEFINER=`app`@`%` */ VIEW v AS SELECT 1"},
		{name: "a four-digit gate", server: mysql2610, statement: "CREATE /*!5013 DEFINER=`app`@`%` */ VIEW v AS SELECT 1"},
		{name: "a MariaDB comment on MySQL", server: mysql2610, statement: "CREATE /*M!100100 DEFINER=`app`@`%` */ VIEW v AS SELECT 1"},
		{name: "a lowercase m comment on MariaDB", server: mariadb,
			statement: "CREATE /*m!100100 DEFINER=`app`@`%` */ VIEW v AS SELECT 1"},
		{name: "a table", server: mysql2610, statement: "CREATE TABLE definer (id int)"},
		{name: "a select", server: mysql2610, statement: "SELECT 'CREATE DEFINER=`app`@`%` VIEW v AS SELECT 1'"},
		{name: "a clause the statement does not finish", server: mysql2610, statement: "CREATE DEFINER="},
		{name: "an empty user", server: mysql2610, statement: "CREATE DEFINER=''@'%' VIEW v AS SELECT 1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := mysqldefiner.Scan(test.server, []string{test.statement})

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
// the server accepted the statement naming another definer. The MySQL rows
// are MySQL 26.10 unless they say 8.0, which is 8.0.46.
func TestDecide(t *testing.T) {
	mysql := mysql2610
	tests := []struct {
		name       string
		server     mysqldefiner.Server
		privileges string
		existence  mysqldefiner.Existence
		want       mysqldefiner.Verdict
	}{
		{name: "mysql without a privilege", server: mysql, privileges: "USAGE",
			existence: mysqldefiner.Exists, want: mysqldefiner.Verdict{Decided: true, Missing: "SET_ANY_DEFINER"}},
		{name: "mysql without a privilege, missing definer", server: mysql, privileges: "USAGE",
			existence: mysqldefiner.Missing,
			want:      mysqldefiner.Verdict{Decided: true, Missing: "SET_ANY_DEFINER and ALLOW_NONEXISTENT_DEFINER"}},
		{name: "mysql SET_ANY_DEFINER, existing definer", server: mysql, privileges: "SET_ANY_DEFINER",
			existence: mysqldefiner.Exists, want: mysqldefiner.Verdict{Decided: true}},
		{name: "mysql SET_ANY_DEFINER, missing definer", server: mysql, privileges: "SET_ANY_DEFINER",
			existence: mysqldefiner.Missing, want: mysqldefiner.Verdict{Decided: true, Missing: "ALLOW_NONEXISTENT_DEFINER"}},
		{name: "mysql SET_ANY_DEFINER, existence unknown", server: mysql, privileges: "SET_ANY_DEFINER",
			existence: mysqldefiner.ExistenceUnknown, want: mysqldefiner.Verdict{}},
		{name: "mysql both privileges", server: mysql, privileges: "SET_ANY_DEFINER,ALLOW_NONEXISTENT_DEFINER",
			existence: mysqldefiner.Missing, want: mysqldefiner.Verdict{Decided: true}},
		{name: "mysql SUPER", server: mysql, privileges: "SUPER",
			existence: mysqldefiner.Missing, want: mysqldefiner.Verdict{Decided: true}},
		{name: "mariadb without a privilege", server: mariadb, privileges: "USAGE",
			existence: mysqldefiner.ExistenceUnknown, want: mysqldefiner.Verdict{Decided: true, Missing: "SET USER"}},
		{name: "mariadb SET USER", server: mariadb, privileges: "SET USER",
			existence: mysqldefiner.ExistenceUnknown, want: mysqldefiner.Verdict{Decided: true}},
		{name: "mariadb ALL PRIVILEGES", server: mariadb, privileges: "ALL PRIVILEGES",
			existence: mysqldefiner.ExistenceUnknown, want: mysqldefiner.Verdict{Decided: true}},
		{name: "mariadb SUPER alone is decided by the server's version", server: mariadb, privileges: "SUPER",
			existence: mysqldefiner.ExistenceUnknown, want: mysqldefiner.Verdict{}},
		{name: "mysql ALLOW_NONEXISTENT_DEFINER alone, existing definer", server: mysql,
			privileges: "ALLOW_NONEXISTENT_DEFINER", existence: mysqldefiner.Exists,
			want: mysqldefiner.Verdict{Decided: true, Missing: "SET_ANY_DEFINER"}},
		{name: "mysql 8.0 without a privilege", server: mysql80, privileges: "USAGE",
			existence: mysqldefiner.ExistenceUnknown, want: mysqldefiner.Verdict{Decided: true, Missing: "SET_USER_ID"}},
		{name: "mysql 8.0 SET_USER_ID, missing definer", server: mysql80, privileges: "SET_USER_ID",
			existence: mysqldefiner.ExistenceUnknown, want: mysqldefiner.Verdict{Decided: true}},
		{name: "mysql 5.7 without a privilege", server: mysql57, privileges: "USAGE",
			existence: mysqldefiner.ExistenceUnknown, want: mysqldefiner.Verdict{}},
		{name: "mysql 5.7 SUPER", server: mysql57, privileges: "SUPER",
			existence: mysqldefiner.ExistenceUnknown, want: mysqldefiner.Verdict{Decided: true}},
		{name: "mysql of an unknown version is read as 8.2 or later", server: unknown, privileges: "USAGE",
			existence: mysqldefiner.Exists, want: mysqldefiner.Verdict{Decided: true, Missing: "SET_ANY_DEFINER"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			privileges := make(map[string]bool)
			for privilege := range strings.SplitSeq(test.privileges, ",") {
				privileges[privilege] = true
			}

			got := mysqldefiner.Decide(test.server, privileges, test.existence)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestVersionID(t *testing.T) {
	tests := []struct {
		version string
		want    int
	}{
		{version: "8.0.46", want: 80046},
		{version: "8.1.0", want: 80100},
		{version: "8.2.0", want: 80200},
		{version: "8.4.11", want: 80411},
		{version: "26.10.0", want: 261000},
		{version: "5.7.44", want: 50744},
		{version: "8.0.46-0ubuntu1", want: 80046},
		{version: "12.3.3-MariaDB-ubu2404", want: 120303},
		{version: "11.4.2-MariaDB-log", want: 110402},
		{version: "8.0", want: 80000},
		{version: "8.x.1", want: 0},
		{version: "8-beta.1", want: 0},
		{version: "garbage", want: 0},
		{version: "", want: 0},
	}

	for _, test := range tests {
		t.Run(test.version, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqldefiner.VersionID(test.version), qt.Equals, test.want)
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
