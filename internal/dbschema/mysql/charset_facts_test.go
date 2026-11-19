package mysql_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
)

// sessionCharsetFacts answers the session's default_collation_for_utf8mb4 with
// utf8mb4, no row where utf8mb4 is empty as MariaDB answers, and what utf8
// names with utf8, and hands every other question to a scripted MySQL session
// over serverCatalog(selected).
func sessionCharsetFacts(selected any, utf8mb4, utf8 string) dbtest.QueryHandler {
	session := dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, serverCatalog(selected))
	var variable [][]driver.Value
	if utf8mb4 != "" {
		variable = [][]driver.Value{{"default_collation_for_utf8mb4", utf8mb4}}
	}
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		switch strings.TrimSpace(query) {
		case "SHOW SESSION VARIABLES LIKE 'default_collation_for_utf8mb4'":
			return dbtest.QueryResult{Columns: []string{"Variable_name", "Value"}, Rows: variable}, nil
		case "SELECT CHARSET(CONVERT('' USING utf8))":
			return dbtest.QueryResult{Columns: []string{"CHARSET"}, Rows: [][]driver.Value{{utf8}}}, nil
		}
		return session(query, args)
	}
}

// TestReader_RecordsTheCharacterSetFacts reads each character set's default
// collation from CHARACTER_SETS, with the utf8mb4 entry taken from the
// session's default_collation_for_utf8mb4 where the session has the variable,
// and what utf8 names in the session, on a selected database and on a whole
// server.
func TestReader_RecordsTheCharacterSetFacts(t *testing.T) {
	tests := []struct {
		name         string
		selected     any
		utf8mb4      string
		utf8         string
		wantDefaults map[string]string
		wantAliases  map[string]string
	}{
		{
			name: "a fresh MySQL session", selected: "r1", utf8mb4: "utf8mb4_0900_ai_ci", utf8: "utf8mb3",
			wantDefaults: map[string]string{"utf8mb4": "utf8mb4_0900_ai_ci", "latin1": "latin1_swedish_ci"},
			wantAliases:  map[string]string{"utf8": "utf8mb3"},
		},
		{
			name: "a MySQL session that sets the utf8mb4 default", selected: "r1", utf8mb4: "utf8mb4_general_ci", utf8: "utf8mb3",
			wantDefaults: map[string]string{"utf8mb4": "utf8mb4_general_ci", "latin1": "latin1_swedish_ci"},
			wantAliases:  map[string]string{"utf8": "utf8mb3"},
		},
		{
			name: "a server without the variable where utf8 names utf8mb4", selected: "r1", utf8: "UTF8MB4",
			wantDefaults: map[string]string{"utf8mb4": "utf8mb4_0900_ai_ci", "latin1": "latin1_swedish_ci"},
			wantAliases:  map[string]string{"utf8": "utf8mb4"},
		},
		{
			name: "a whole server", utf8mb4: "utf8mb4_general_ci", utf8: "utf8mb3",
			wantDefaults: map[string]string{"utf8mb4": "utf8mb4_general_ci", "latin1": "latin1_swedish_ci"},
			wantAliases:  map[string]string{"utf8": "utf8mb3"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, sessionCharsetFacts(test.selected, test.utf8mb4, test.utf8))

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(got.CharsetDefaultCollations, qt.DeepEquals, test.wantDefaults)
			c.Assert(got.CharsetAliases, qt.DeepEquals, test.wantAliases)
		})
	}
}
