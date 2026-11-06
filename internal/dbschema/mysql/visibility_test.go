package mysql_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/internal/readscope"
)

// visibleCatalog answers a reader as a server whose database fx holds what the
// fields say. Every question it does not model answers no rows, which is how a
// catalog view answers for what the account may not see.
type visibleCatalog struct {
	selected  any
	databases []string
	tables    []string
	views     [][]driver.Value
	triggers  [][]driver.Value
	routines  [][]driver.Value
}

func (v visibleCatalog) answer(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, "SELECT DATABASE()"):
		return dbtest.QueryResult{Columns: []string{"DATABASE()"}, Rows: [][]driver.Value{{v.selected}}}, nil
	case strings.Contains(query, "FROM information_schema.SCHEMATA"):
		rows := make([][]driver.Value, 0, len(v.databases))
		for _, database := range v.databases {
			rows = append(rows, []driver.Value{database, "utf8mb4", "utf8mb4_0900_ai_ci"})
		}
		return dbtest.QueryResult{Columns: []string{"SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME"}, Rows: rows}, nil
	case strings.Contains(query, "TABLE_TYPE = 'BASE TABLE'"):
		rows := make([][]driver.Value, 0, len(v.tables))
		for _, table := range v.tables {
			rows = append(rows, []driver.Value{table, "BASE TABLE", "", "utf8mb4_0900_ai_ci", "Dynamic"})
		}
		return dbtest.QueryResult{Columns: []string{"TABLE_NAME", "TABLE_TYPE", "TABLE_COMMENT", "TABLE_COLLATION", "ROW_FORMAT"}, Rows: rows}, nil
	case strings.Contains(query, "FROM information_schema.VIEWS"):
		return dbtest.QueryResult{Columns: []string{"TABLE_NAME", "VIEW_DEFINITION", "CHECK_OPTION"}, Rows: v.views}, nil
	case strings.Contains(query, "FROM information_schema.TRIGGERS"):
		return dbtest.QueryResult{Columns: []string{
			"TRIGGER_NAME", "EVENT_OBJECT_TABLE", "ACTION_TIMING", "EVENT_MANIPULATION", "ACTION_ORIENTATION", "ACTION_STATEMENT",
		}, Rows: v.triggers}, nil
	case strings.Contains(query, "FROM information_schema.ROUTINES"):
		return dbtest.QueryResult{Columns: []string{
			"ROUTINE_NAME", "DTD_IDENTIFIER", "IS_DETERMINISTIC", "SQL_DATA_ACCESS", "SECURITY_TYPE",
			"DEFINER", "CURRENT_USER()", "ROUTINE_DEFINITION", "ROUTINE_COMMENT", "ROUTINE_TYPE",
		}, Rows: v.routines}, nil
	}
	return dbtest.QueryResult{}, nil
}

// procedureRow is a procedure the account may list, with its body or without.
func procedureRow(name string, body any) []driver.Value {
	return []driver.Value{name, nil, "NO", "CONTAINS SQL", "DEFINER", "owner@%", "app@%", body, "", "PROCEDURE"}
}

// triggerRow is a trigger on child, with its body or without.
func triggerRow(name string, body any) []driver.Value {
	return []driver.Value{name, "child", "BEFORE", "INSERT", "ROW", body}
}

// fullCatalog holds one object of each kind, each readable.
func fullCatalog() visibleCatalog {
	return visibleCatalog{
		selected: "fx",
		tables:   []string{"child", "parent"},
		views:    [][]driver.Value{{"v_child", "select 1", "NONE"}},
		triggers: [][]driver.Value{triggerRow("trg_child", "SET NEW.n = 1")},
		routines: [][]driver.Value{procedureRow("p_noop", "SELECT 1")},
	}
}

// refusal is the whole message for a read of database fx by account app@%.
func refusal(lines ...string) string {
	return regexp.QuoteMeta(`the account app@% cannot see everything in database "fx": information_schema lists only what ` +
		`an account holds a privilege on, and a comparison reads a hidden object as absent. The account needs:` +
		"\n  - " + strings.Join(lines, "\n  - "))
}

// The lines of a refusal.
const (
	lineTables       = "tables and views: SELECT on `fx`.*"
	lineViews        = "the definitions of views: SELECT and SHOW VIEW on `fx`.*"
	lineEmptyView    = "the definitions of views v_child: SELECT and SHOW VIEW on `fx`.*"
	lineConstraints  = "primary keys, unique keys, foreign keys and CHECK constraints: a privilege beyond SELECT, such as SHOW VIEW, on `fx`.*"
	lineTriggers     = "triggers: TRIGGER on `fx`.*"
	lineRoutinesMy   = "procedures and functions: SHOW_ROUTINE on *.*"
	lineRoutinesMaDB = "procedures and functions: SHOW CREATE ROUTINE on `fx`.*"
	lineHiddenMy     = "the bodies of routines p_noop, unless the account is their definer: SHOW_ROUTINE on *.*"
	lineHiddenMaDB   = "the bodies of routines p_noop, unless the account is their definer: SHOW CREATE ROUTINE on `fx`.*"
)

// TestReader_RefusesWhatTheAccountCannotSee_FailurePath holds each account of
// stokaro/ptah#5497 to the privilege it has to be told about. The catalog each
// row scripts is what the server listed to an account with those grants.
func TestReader_RefusesWhatTheAccountCannotSee_FailurePath(t *testing.T) {
	selectOnly := func(c visibleCatalog) visibleCatalog {
		c.views = [][]driver.Value{{"v_child", "", "NONE"}}
		c.triggers = nil
		c.routines = nil
		return c
	}
	tests := []struct {
		name    string
		version string
		grants  []string
		catalog visibleCatalog
		wantErr string
	}{
		{
			name:    "MySQL, SELECT on one table",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT USAGE ON *.* TO `app`@`%`", "GRANT SELECT ON `fx`.`child` TO `app`@`%`"},
			catalog: visibleCatalog{selected: "fx", tables: []string{"child"}},
			wantErr: refusal(lineTables, lineViews, lineTriggers, lineRoutinesMy),
		},
		{
			name:    "MariaDB, SELECT on one table",
			version: dbtest.MariaDBVersion,
			grants:  []string{"GRANT USAGE ON *.* TO `app`@`%`", "GRANT SELECT ON `fx`.`child` TO `app`@`%`"},
			catalog: visibleCatalog{selected: "fx", tables: []string{"child"}},
			wantErr: refusal(lineTables, lineViews, lineConstraints, lineTriggers, lineRoutinesMaDB),
		},
		{
			name:    "MySQL, SELECT on the database",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT SELECT ON `fx`.* TO `app`@`%`"},
			catalog: selectOnly(fullCatalog()),
			wantErr: refusal(lineEmptyView, lineTriggers, lineRoutinesMy),
		},
		{
			name:    "MariaDB, SELECT on the database",
			version: dbtest.MariaDBVersion,
			grants:  []string{"GRANT SELECT ON `fx`.* TO `app`@`%`"},
			catalog: selectOnly(fullCatalog()),
			wantErr: refusal(lineEmptyView, lineConstraints, lineTriggers, lineRoutinesMaDB),
		},
		{
			name:    "MySQL, SELECT, SHOW VIEW, TRIGGER on the database",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT SELECT, SHOW VIEW, TRIGGER ON `fx`.* TO `app`@`%`"},
			catalog: func() visibleCatalog { c := fullCatalog(); c.routines = nil; return c }(),
			wantErr: refusal(lineRoutinesMy),
		},
		{
			name:    "MariaDB, SELECT, SHOW VIEW, TRIGGER on the database",
			version: dbtest.MariaDBVersion,
			grants:  []string{"GRANT SELECT, SHOW VIEW, TRIGGER ON `fx`.* TO `app`@`%`"},
			catalog: func() visibleCatalog { c := fullCatalog(); c.routines = nil; return c }(),
			wantErr: refusal(lineRoutinesMaDB),
		},
		{
			name:    "MySQL, a table-level TRIGGER that leaves a table out",
			version: dbtest.MySQLVersion,
			grants: []string{
				"GRANT SELECT, SHOW VIEW, EXECUTE ON `fx`.* TO `app`@`%`",
				"GRANT TRIGGER ON `fx`.`child` TO `app`@`%`",
				"GRANT SHOW_ROUTINE ON *.* TO `app`@`%`",
			},
			catalog: fullCatalog(),
			wantErr: refusal(lineTriggers),
		},
		{
			name:    "MariaDB, a trigger listed without its body",
			version: dbtest.MariaDBVersion,
			grants:  []string{"GRANT SELECT, INSERT, SHOW VIEW, SHOW CREATE ROUTINE ON `fx`.* TO `app`@`%`"},
			catalog: func() visibleCatalog {
				c := fullCatalog()
				c.triggers = [][]driver.Value{triggerRow("trg_child", nil)}
				return c
			}(),
			wantErr: refusal(lineTriggers),
		},
		{
			name:    "MySQL, a routine of another definer listed without its body",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT ALL PRIVILEGES ON `fx`.* TO `app`@`%`"},
			catalog: func() visibleCatalog {
				c := fullCatalog()
				c.routines = [][]driver.Value{procedureRow("p_noop", nil)}
				return c
			}(),
			wantErr: refusal(lineHiddenMy),
		},
		{
			name:    "MariaDB, a routine of another definer listed without its body",
			version: dbtest.MariaDBVersion,
			grants:  []string{"GRANT ALL PRIVILEGES ON `fx`.* TO `app`@`%`"},
			catalog: func() visibleCatalog {
				c := fullCatalog()
				c.routines = [][]driver.Value{procedureRow("p_noop", nil)}
				return c
			}(),
			wantErr: refusal(lineHiddenMaDB),
		},
		{
			name:    "MySQL, SELECT on mysql.proc is not a thing there",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT SELECT, SHOW VIEW, TRIGGER ON `fx`.* TO `app`@`%`", "GRANT SELECT ON `mysql`.* TO `app`@`%`"},
			catalog: func() visibleCatalog { c := fullCatalog(); c.routines = nil; return c }(),
			wantErr: refusal(lineRoutinesMy),
		},
		{
			name:    "a wildcard row a literal row hides",
			version: dbtest.MySQLVersion,
			grants: []string{
				"GRANT SELECT ON `fx`.* TO `app`@`%`",
				"GRANT SELECT, SHOW VIEW, TRIGGER, EXECUTE ON `f_`.* TO `app`@`%`",
			},
			catalog: selectOnly(fullCatalog()),
			wantErr: refusal(lineEmptyView, lineTriggers, lineRoutinesMy),
		},
		{
			name:    "SELECT held globally and revoked on this database",
			version: dbtest.MySQLVersion,
			grants: []string{
				"GRANT SELECT, SHOW VIEW, TRIGGER ON *.* TO `app`@`%`",
				"REVOKE SELECT ON `fx`.* FROM `app`@`%`",
			},
			catalog: visibleCatalog{selected: "fx"},
			wantErr: refusal(lineTables, lineRoutinesMy),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, dbtest.MySQLSession(test.version, test.grants, test.catalog.answer))

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// TestReader_NamesTheRoutinePrivilegeTheServerHas_FailurePath follows SHOW
// PRIVILEGES: neither engine's older lines have the privilege the newer ones
// are refused for.
func TestReader_NamesTheRoutinePrivilegeTheServerHas_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		version    string
		privileges [][]driver.Value
		wantLine   string
	}{
		{
			name:     "MySQL before SHOW_ROUTINE",
			version:  dbtest.MySQLVersion,
			wantLine: "procedures and functions: SELECT on *.*",
		},
		{
			name:     "MariaDB before SHOW CREATE ROUTINE",
			version:  dbtest.MariaDBVersion,
			wantLine: "procedures and functions: SELECT on mysql.proc",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			answer := dbtest.MySQLSession(test.version, []string{"GRANT ALL PRIVILEGES ON `fx`.`child` TO `app`@`%`"}, visibleCatalog{selected: "fx"}.answer)
			db := dbtest.Open(t, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
				asked := map[bool]func() (dbtest.QueryResult, error){
					true: func() (dbtest.QueryResult, error) {
						return dbtest.QueryResult{Columns: []string{"Privilege", "Context", "Comment"}, Rows: test.privileges}, nil
					},
					false: func() (dbtest.QueryResult, error) { return answer(query, args) },
				}
				return asked[query == "SHOW PRIVILEGES"]()
			})

			_, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

			c.Assert(err, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(test.wantLine)+`.*`)
		})
	}
}

// TestReader_AnswersWhenTheAccountSeesWhatTheRunNeeds_HappyPath is the other half
// of the refusal: an account that can see everything must get the description.
func TestReader_AnswersWhenTheAccountSeesWhatTheRunNeeds_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		version string
		grants  []string
		catalog visibleCatalog
		without []readscope.Kind
	}{
		{
			name:    "MySQL, every privilege globally",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT SELECT, INSERT, TRIGGER, SHOW VIEW ON *.* TO `app`@`%`", "GRANT SHOW_ROUTINE ON *.* TO `app`@`%`"},
			catalog: fullCatalog(),
		},
		{
			name:    "MariaDB, ALL PRIVILEGES globally",
			version: dbtest.MariaDBVersion,
			grants:  []string{"GRANT ALL PRIVILEGES ON *.* TO `app`@`%` IDENTIFIED BY PASSWORD '*7B' WITH GRANT OPTION"},
			catalog: fullCatalog(),
		},
		{
			name:    "MySQL, ALL PRIVILEGES on the database and routines of its own",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT ALL PRIVILEGES ON `fx`.* TO `app`@`%`"},
			catalog: fullCatalog(),
		},
		{
			name:    "the grant the container images write for their account",
			version: dbtest.MariaDBVersion,
			grants:  []string{"GRANT ALL PRIVILEGES ON `f\\x`.* TO `app`@`%`"},
			catalog: fullCatalog(),
		},
		{
			name:    "a wildcard on the database name",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT ALL PRIVILEGES ON `f%`.* TO `app`@`%`"},
			catalog: fullCatalog(),
		},
		{
			name:    "MySQL, the issue's account with SHOW_ROUTINE",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT SELECT, SHOW VIEW, TRIGGER ON `fx`.* TO `app`@`%`", "GRANT SHOW_ROUTINE ON *.* TO `app`@`%`"},
			catalog: fullCatalog(),
		},
		{
			name:    "MariaDB, the issue's account with SHOW CREATE ROUTINE on the database",
			version: dbtest.MariaDBVersion,
			grants:  []string{"GRANT SELECT, SHOW VIEW, TRIGGER, SHOW CREATE ROUTINE ON `fx`.* TO `app`@`%`"},
			catalog: fullCatalog(),
		},
		{
			name:    "MariaDB, SELECT on mysql.proc",
			version: dbtest.MariaDBVersion,
			grants:  []string{"GRANT SELECT, SHOW VIEW, TRIGGER ON `fx`.* TO `app`@`%`", "GRANT SELECT ON `mysql`.`proc` TO `app`@`%`"},
			catalog: fullCatalog(),
		},
		{
			name:    "MariaDB, SELECT on the mysql database",
			version: dbtest.MariaDBVersion,
			grants:  []string{"GRANT SELECT, SHOW VIEW, TRIGGER ON `fx`.* TO `app`@`%`", "GRANT SELECT ON `mysql`.* TO `app`@`%`"},
			catalog: fullCatalog(),
		},
		{
			name:    "a table-level grant on every table",
			version: dbtest.MySQLVersion,
			grants: []string{
				"GRANT SELECT, SHOW VIEW, EXECUTE ON `fx`.* TO `app`@`%`",
				"GRANT TRIGGER ON `fx`.`child` TO `app`@`%`",
				"GRANT TRIGGER ON `fx`.`parent` TO `app`@`%`",
			},
			catalog: fullCatalog(),
		},
		{
			name:    "MariaDB, a constraint privilege on every table",
			version: dbtest.MariaDBVersion,
			grants: []string{
				"GRANT SELECT, TRIGGER, EXECUTE ON `fx`.* TO `app`@`%`",
				"GRANT SHOW VIEW ON `fx`.`child` TO `app`@`%`",
				"GRANT REFERENCES ON `fx`.`parent` TO `app`@`%`",
			},
			catalog: func() visibleCatalog { c := fullCatalog(); c.views = nil; return c }(),
		},
		{
			name:    "a database that holds no view needs no SHOW VIEW",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT SELECT, TRIGGER, EXECUTE ON `fx`.* TO `app`@`%`"},
			catalog: func() visibleCatalog { c := fullCatalog(); c.views = nil; return c }(),
		},
		{
			name:    "a database that holds no table needs no TRIGGER",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT SELECT, SHOW VIEW, EXECUTE ON `fx`.* TO `app`@`%`"},
			catalog: visibleCatalog{selected: "fx"},
		},
		{
			name:    "a privilege other than SELECT lists the tables",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT INSERT, UPDATE, TRIGGER, EXECUTE ON `fx`.* TO `app`@`%`"},
			catalog: func() visibleCatalog { c := fullCatalog(); c.views = nil; return c }(),
		},
		{
			name:    "a run without triggers",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT SELECT, SHOW VIEW, EXECUTE ON `fx`.* TO `app`@`%`"},
			catalog: func() visibleCatalog { c := fullCatalog(); c.triggers = nil; return c }(),
			without: []readscope.Kind{readscope.Triggers},
		},
		{
			name:    "a run without view definitions",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT SELECT, TRIGGER, EXECUTE ON `fx`.* TO `app`@`%`"},
			catalog: func() visibleCatalog {
				c := fullCatalog()
				c.views = [][]driver.Value{{"v_child", "", "NONE"}}
				return c
			}(),
			without: []readscope.Kind{readscope.Views},
		},
		{
			name:    "a run without routines",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT SELECT, SHOW VIEW, TRIGGER ON `fx`.* TO `app`@`%`"},
			catalog: func() visibleCatalog { c := fullCatalog(); c.routines = nil; return c }(),
			without: []readscope.Kind{readscope.Routines},
		},
		{
			name:    "a run without routines lists one with no body",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT ALL PRIVILEGES ON `fx`.* TO `app`@`%`"},
			catalog: func() visibleCatalog {
				c := fullCatalog()
				c.routines = [][]driver.Value{procedureRow("p_noop", nil)}
				return c
			}(),
			without: []readscope.Kind{readscope.Routines},
		},
		{
			name:    "MariaDB, a run without constraints",
			version: dbtest.MariaDBVersion,
			grants:  []string{"GRANT SELECT, TRIGGER, EXECUTE ON `fx`.* TO `app`@`%`"},
			catalog: func() visibleCatalog { c := fullCatalog(); c.views = nil; return c }(),
			without: []readscope.Kind{readscope.Constraints},
		},
		{
			name:    "a grant on one table and a run that depends on nothing else",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT SELECT ON `fx`.`child` TO `app`@`%`"},
			catalog: visibleCatalog{selected: "fx", tables: []string{"child"}},
			without: []readscope.Kind{readscope.Tables, readscope.Views, readscope.Constraints, readscope.Triggers, readscope.Routines},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, dbtest.MySQLSession(test.version, test.grants, test.catalog.answer))
			ctx := readscope.Without(t.Context(), test.without...)

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(ctx)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.IsNotNil)
		})
	}
}

// TestReader_LeavingAKindOutLeavesTheOthers_FailurePath keeps a run's scope from
// waving through what it did not name.
func TestReader_LeavingAKindOutLeavesTheOthers_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		without []readscope.Kind
		wantErr string
	}{
		{
			name:    "triggers left out, routines still needed",
			without: []readscope.Kind{readscope.Triggers},
			wantErr: refusal(lineEmptyView, lineRoutinesMy),
		},
		{
			name:    "routines left out, triggers still needed",
			without: []readscope.Kind{readscope.Routines},
			wantErr: refusal(lineEmptyView, lineTriggers),
		},
		{
			name:    "views left out, tables still listed",
			without: []readscope.Kind{readscope.Views},
			wantErr: refusal(lineTriggers, lineRoutinesMy),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			catalog := fullCatalog()
			catalog.views = [][]driver.Value{{"v_child", "", "NONE"}}
			catalog.triggers, catalog.routines = nil, nil
			db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, []string{"GRANT SELECT ON `fx`.* TO `app`@`%`"}, catalog.answer))
			ctx := readscope.Without(context.Background(), test.without...)

			_, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(ctx)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestReader_WholeServerAnswersForEachDatabaseItSees_FailurePath covers a
// connection that selected no database. The read covers the databases the account
// can see, and each of them has to be complete.
func TestReader_WholeServerAnswersForEachDatabaseItSees_FailurePath(t *testing.T) {
	c := qt.New(t)
	catalog := visibleCatalog{selected: nil, databases: []string{"r1", "r2"}}
	grants := []string{"GRANT ALL PRIVILEGES ON `r1`.* TO `app`@`%`", "GRANT SELECT ON `r2`.`one` TO `app`@`%`"}
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, grants, catalog.answer))

	got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

	c.Assert(err, qt.ErrorMatches, `(?s)database r2: the account app@% cannot see everything in database "r2": .*`)
	c.Assert(got, qt.IsNil)
}

// TestReader_WholeServerAnswersForEachDatabaseItSees_HappyPath is the same read by
// accounts that can see all of what they can list.
func TestReader_WholeServerAnswersForEachDatabaseItSees_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		grants []string
		named  []string
		listed []string
	}{
		{
			name:   "ALL PRIVILEGES globally",
			grants: dbtest.MySQLAllGrants,
			listed: []string{"r1", "r2"},
		},
		{
			name:   "ALL PRIVILEGES on the databases it lists, and nothing that lists the others",
			grants: []string{"GRANT ALL PRIVILEGES ON `r1`.* TO `app`@`%`", "GRANT ALL PRIVILEGES ON `r2`.* TO `app`@`%`"},
			listed: []string{"r1", "r2"},
		},
		{
			name:   "the database it names and holds",
			grants: []string{"GRANT ALL PRIVILEGES ON `r1`.* TO `app`@`%`"},
			named:  []string{"r1"},
			listed: []string{"r1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			catalog := visibleCatalog{selected: nil, databases: test.listed}
			db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, test.grants, catalog.answer))
			reader := mysql.NewMySQLReader(db.SQL, "")
			reader.SetSchemas(test.named)

			got, err := reader.ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(got.Schemas, qt.HasLen, len(test.listed))
		})
	}
}

// TestReader_FailsWithoutTheAccountsGrants_FailurePath refuses a read when the
// question that proves it complete could not be asked.
func TestReader_FailsWithoutTheAccountsGrants_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		marker  string
		wantErr string
	}{
		{name: "who the session is", marker: "SELECT CURRENT_USER(), VERSION()", wantErr: `failed to read the connecting account: refused`},
		{name: "what it holds", marker: "SHOW GRANTS", wantErr: `failed to read the connecting account's grants: refused`},
		{name: "how grants apply", marker: "SHOW VARIABLES", wantErr: `failed to read the settings grants depend on: refused`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			answer := dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, fullCatalog().answer)
			db := dbtest.Open(t, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
				refused := map[bool]error{true: fmt.Errorf("refused"), false: nil}[strings.Contains(query, test.marker)]
				result, _ := answer(query, args)
				return result, refused
			})

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// TestReader_RefusesBeforeTheFirstCatalogQuery_FailurePath holds the refusal ahead
// of the read for an account that cannot list the tables. Some of what follows is
// refused to it outright, with an answer that names no privilege: on MariaDB, an
// account holding SELECT on one column gets Error 1142 from SHOW CREATE TABLE.
func TestReader_RefusesBeforeTheFirstCatalogQuery_FailurePath(t *testing.T) {
	c := qt.New(t)
	catalog := func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		failing := map[bool]error{true: errors.New("the read went on"), false: nil}[strings.Contains(query, "information_schema.")]
		result, _ := visibleCatalog{selected: "fx"}.answer(query, args)
		return result, failing
	}
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, []string{"GRANT SELECT (`id`) ON `fx`.`child` TO `app`@`%`"}, catalog))

	got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

	c.Assert(err, qt.ErrorMatches, refusal(lineTables, lineViews, lineTriggers, lineRoutinesMy))
	c.Assert(got, qt.IsNil)
}

// authoredGrants holds what creating tables takes, and no privilege that shows
// someone else's trigger or routine.
var authoredGrants = []string{"GRANT SELECT, CREATE, DROP, ALTER, INDEX ON *.* TO `app`@`%`"}

// TestReader_AnswersForWhatTheRunCreated_HappyPath reads a dev database: the
// account created what it finds there, so it needs no privilege that would show it
// another account's trigger or routine.
func TestReader_AnswersForWhatTheRunCreated_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		version string
	}{
		{name: "MySQL", version: dbtest.MySQLVersion},
		{name: "MariaDB", version: dbtest.MariaDBVersion},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, dbtest.MySQLSession(test.version, []string{"GRANT SELECT ON *.* TO `app`@`%`"}, fullCatalog().answer))

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(readscope.Authored(t.Context()))

			c.Assert(err, qt.IsNil)
			c.Assert(got.Triggers, qt.HasLen, 1)
		})
	}
}

// TestReader_AnswersForWhatTheRunCreated_FailurePath keeps what is still true of a
// dev read: a row that came back with nothing in it has another definer, and a
// creator is not shown columns without a privilege that lists them.
func TestReader_AnswersForWhatTheRunCreated_FailurePath(t *testing.T) {
	withEmptyView := fullCatalog()
	withEmptyView.views = [][]driver.Value{{"v_child", "", "NONE"}}
	withEmptyTrigger := fullCatalog()
	withEmptyTrigger.triggers = [][]driver.Value{triggerRow("trg_child", nil)}
	withHiddenRoutine := fullCatalog()
	withHiddenRoutine.routines = [][]driver.Value{procedureRow("p_noop", nil)}
	tests := []struct {
		name    string
		grants  []string
		catalog visibleCatalog
		wantErr string
	}{
		{
			name:    "a view with no definition",
			grants:  authoredGrants,
			catalog: withEmptyView,
			wantErr: refusal(lineEmptyView),
		},
		{
			name:    "a trigger with no body",
			grants:  authoredGrants,
			catalog: withEmptyTrigger,
			wantErr: refusal(lineTriggers),
		},
		{
			name:    "a routine with no body",
			grants:  authoredGrants,
			catalog: withHiddenRoutine,
			wantErr: refusal(lineHiddenMy),
		},
		{
			name:    "no privilege that lists columns",
			grants:  []string{"GRANT CREATE, DROP, ALTER, INDEX ON *.* TO `app`@`%`"},
			catalog: fullCatalog(),
			wantErr: refusal(lineTables),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, test.grants, test.catalog.answer))

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(readscope.Authored(t.Context()))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// serverSettings answers SHOW VARIABLES with settings, for a session whose other
// questions next answers.
func serverSettings(settings map[string]string, next dbtest.QueryHandler) dbtest.QueryHandler {
	rows := make([][]driver.Value, 0, len(settings))
	for name, value := range settings {
		rows = append(rows, []driver.Value{name, value})
	}
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		answers := map[bool]func() (dbtest.QueryResult, error){
			true: func() (dbtest.QueryResult, error) {
				return dbtest.QueryResult{Columns: []string{"Variable_name", "Value"}, Rows: rows}, nil
			},
			false: func() (dbtest.QueryResult, error) { return next(query, args) },
		}
		return answers[strings.HasPrefix(strings.TrimSpace(query), "SHOW VARIABLES")]()
	}
}

// TestReader_ReadsGrantsByTheServersSettings_HappyPath is a grant the server only
// applies under the setting the row names.
func TestReader_ReadsGrantsByTheServersSettings_HappyPath(t *testing.T) {
	c := qt.New(t)
	grants := []string{"GRANT ALL PRIVILEGES ON `FX`.* TO `app`@`%`"}
	session := dbtest.MySQLSession(dbtest.MySQLVersion, grants, fullCatalog().answer)
	db := dbtest.Open(t, serverSettings(map[string]string{"lower_case_table_names": "1"}, session))

	got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(got.Tables, qt.HasLen, 2)
}

// TestReader_ReadsGrantsByTheServersSettings_FailurePath is a grant the server does
// not apply under the setting the row names.
func TestReader_ReadsGrantsByTheServersSettings_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		grants   []string
		settings map[string]string
		wantErr  string
	}{
		{
			name:     "with partial_revokes on, a wildcard is an ordinary character",
			grants:   []string{"GRANT ALL PRIVILEGES ON `f%`.* TO `app`@`%`"},
			settings: map[string]string{"partial_revokes": "ON", "lower_case_table_names": "0"},
			wantErr:  refusal(lineTables, lineViews, lineTriggers, lineRoutinesMy),
		},
		{
			name:     "a name in another case, on a server that compares names exactly",
			grants:   []string{"GRANT ALL PRIVILEGES ON `FX`.* TO `app`@`%`"},
			settings: map[string]string{"lower_case_table_names": "0"},
			wantErr:  refusal(lineTables, lineViews, lineTriggers, lineRoutinesMy),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			session := dbtest.MySQLSession(dbtest.MySQLVersion, test.grants, visibleCatalog{selected: "fx"}.answer)
			db := dbtest.Open(t, serverSettings(test.settings, session))

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}
