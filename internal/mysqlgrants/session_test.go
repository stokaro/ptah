package mysqlgrants_test

import (
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/mysqlgrants"
)

// session answers SHOW GRANTS with lines, SHOW VARIABLES with settings and
// CURRENT_ROLE() with current, as a server does. Any other question is one
// [mysqlgrants.Read] has no reason to ask.
func session(lines []string, settings map[string]string, current string) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		answers := map[string]dbtest.QueryResult{
			"SHOW GRANTS":           {Columns: []string{"Grants"}, Rows: valueRows(lines)},
			"SELECT CURRENT_ROLE()": {Columns: []string{"CURRENT_ROLE()"}, Rows: valueRows([]string{current})},
		}
		answer, known := answers[strings.TrimSpace(query)]
		settingsAnswer := dbtest.QueryResult{Columns: []string{"Variable_name", "Value"}, Rows: settingRows(settings)}
		return map[bool]dbtest.QueryResult{true: answer, false: settingsAnswer}[known], nil
	}
}

func valueRows(lines []string) [][]driver.Value {
	rows := make([][]driver.Value, 0, len(lines))
	for _, line := range lines {
		rows = append(rows, []driver.Value{line})
	}
	return rows
}

func settingRows(settings map[string]string) [][]driver.Value {
	rows := make([][]driver.Value, 0, len(settings))
	for name, value := range settings {
		rows = append(rows, []driver.Value{name, value})
	}
	return rows
}

// TestRead_HappyPath holds each answer to the server's own settings: the same line
// grants on another database under another setting, and a server's role rows are
// read as that server prints them.
func TestRead_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		lines    []string
		settings map[string]string
		// mariaDB is the server that prints a role's rows under the role's name.
		mariaDB bool
		// current is what CURRENT_ROLE() answers.
		current  string
		database string
		want     bool
	}{
		{
			name:     "a wildcard row covers a database it matches",
			lines:    []string{"GRANT ALL PRIVILEGES ON `txa%`.* TO `a`@`%`"},
			settings: map[string]string{"partial_revokes": "OFF", "lower_case_table_names": "0"},
			database: "txa",
			want:     true,
		},
		{
			name:     "with partial_revokes on, the same row names no such database",
			lines:    []string{"GRANT ALL PRIVILEGES ON `txa%`.* TO `a`@`%`"},
			settings: map[string]string{"partial_revokes": "ON", "lower_case_table_names": "0"},
			database: "txa",
			want:     false,
		},
		{
			name:     "a server that has no partial_revokes reads the row as a pattern",
			lines:    []string{"GRANT ALL PRIVILEGES ON `txa%`.* TO `a`@`%`"},
			settings: map[string]string{"lower_case_table_names": "0"},
			database: "txa",
			want:     true,
		},
		{
			name:     "with lower_case_table_names set, a name folds",
			lines:    []string{"GRANT ALL PRIVILEGES ON `TXA`.* TO `a`@`%`"},
			settings: map[string]string{"lower_case_table_names": "2"},
			database: "txa",
			want:     true,
		},
		{
			name:     "without it, a name does not",
			lines:    []string{"GRANT ALL PRIVILEGES ON `TXA`.* TO `a`@`%`"},
			settings: map[string]string{"lower_case_table_names": "0"},
			database: "txa",
			want:     false,
		},
		{
			// Measured on MySQL 8.4: with a role active, the server used `f%`,
			// which holds INSERT alone, and listed no trigger.
			name: "MySQL does not rank wildcard rows while a role is active",
			lines: []string{
				"GRANT INSERT ON `f%`.* TO `a`@`%`",
				"GRANT TRIGGER ON `fx%`.* TO `a`@`%`",
				"GRANT `r`@`%` TO `a`@`%`",
			},
			current:  "`r`@`%`",
			database: "fx",
			want:     false,
		},
		{
			name: "MySQL ranks them while a granted role is not active",
			lines: []string{
				"GRANT INSERT ON `f%`.* TO `a`@`%`",
				"GRANT TRIGGER ON `fx%`.* TO `a`@`%`",
				"GRANT `r`@`%` TO `a`@`%`",
			},
			current:  "NONE",
			database: "fx",
			want:     true,
		},
		{
			name: "MariaDB ranks them",
			lines: []string{
				"GRANT INSERT ON `f%`.* TO `a`@`%`",
				"GRANT TRIGGER ON `fx%`.* TO `a`@`%`",
				"GRANT `r` TO `a`@`%`",
			},
			mariaDB:  true,
			current:  "r",
			database: "fx",
			want:     true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, session(test.lines, test.settings, test.current))

			grants, err := mysqlgrants.Read(t.Context(), db.SQL, test.mariaDB)

			c.Assert(err, qt.IsNil)
			c.Assert(grants.OnDatabase(test.database).Holds("TRIGGER"), qt.Equals, test.want)
		})
	}
}

// TestRead_FailurePath is a question the server would not answer: with no answer
// nothing proves what the session holds, so the read fails rather than answer as
// though the session held nothing.
func TestRead_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		marker  string
		wantErr string
	}{
		{name: "what it holds", marker: "SHOW GRANTS", wantErr: `failed to read the connecting account's grants: refused`},
		{name: "how grants apply", marker: "SHOW VARIABLES", wantErr: `failed to read the settings grants depend on: refused`},
		{name: "which roles are active", marker: "CURRENT_ROLE", wantErr: `failed to read the session's active roles: refused`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			answer := session([]string{"GRANT ALL PRIVILEGES ON *.* TO `a`@`%`"}, nil, "NONE")
			db := dbtest.Open(t, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
				refused := map[bool]error{true: errors.New("refused"), false: nil}[strings.Contains(query, test.marker)]
				result, _ := answer(query, args)
				return result, refused
			})

			_, err := mysqlgrants.Read(t.Context(), db.SQL, false)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
