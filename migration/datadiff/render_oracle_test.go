package datadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/datadiff"
)

// TestRender_OracleNamesTheTableItsSchemaRendererCreated pins the spelling a row
// statement shares with the Oracle DDL renderer.
//
// That renderer writes a plain name bare, and Oracle folds it: `ora_flags`
// becomes ORA_FLAGS. A row statement that quotes the name addresses
// "ora_flags", a table nobody created, and the server answers ORA-00942. A word
// Oracle refuses bare -- `comment` -- is quoted by both.
func TestRender_OracleNamesTheTableItsSchemaRendererCreated(t *testing.T) {
	c := qt.New(t)
	diff := &datadiff.DataDiff{
		Schema:  "app",
		Table:   "ora_flags",
		Keys:    []string{"code"},
		Inserts: []datadiff.Row{{"code": "one", "comment": "kept"}},
		Updates: []datadiff.RowUpdate{{
			Key:     map[string]any{"code": "two"},
			Desired: datadiff.Row{"code": "two", "comment": "new"},
			Live:    datadiff.Row{"code": "two", "comment": "old"},
		}},
		Deletes: []datadiff.Row{{"code": "three", "comment": "gone"}},
	}

	up, _, err := datadiff.Render(diff, "oracle")
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Equals, "INSERT INTO app.ora_flags (code, \"comment\") VALUES ('one', 'kept');\n"+
		"UPDATE app.ora_flags SET \"comment\" = 'new' WHERE code = 'two';\n"+
		"DELETE FROM app.ora_flags WHERE code = 'three';\n")
}

// TestRender_OracleDeclaredMoment_HappyPath renders the text a declaration
// writes for a moment into the column type it lands in.
//
// Oracle reads a plain string through NLS_TIMESTAMP_FORMAT, DD-MON-RR by
// default, and refuses '2024-03-01 12:30:45' with ORA-01843, so a column the
// type map created as DATE or TIMESTAMP takes a typed literal. A text column
// keeps the text, and so does every other dialect, which converts it itself.
func TestRender_OracleDeclaredMoment_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		columnType string
		value      any
		wantUp     string
	}{
		{
			name:       "text in a TIMESTAMP column",
			dialect:    "oracle",
			columnType: "TIMESTAMP",
			value:      "2024-03-01 12:30:45",
			wantUp:     "INSERT INTO t (id, v) VALUES ('r1', TIMESTAMP '2024-03-01 12:30:45+00:00');\n",
		},
		{
			name:       "a date in a DATE column",
			dialect:    "oracle",
			columnType: "DATE",
			value:      "2024-03-01",
			wantUp:     "INSERT INTO t (id, v) VALUES ('r1', TIMESTAMP '2024-03-01 00:00:00+00:00');\n",
		},
		{
			name:       "RFC 3339 in a TIMESTAMPTZ column keeps its offset",
			dialect:    "oracle",
			columnType: "TIMESTAMPTZ",
			value:      "2026-01-02T03:04:05+02:00",
			wantUp:     "INSERT INTO t (id, v) VALUES ('r1', TIMESTAMP '2026-01-02 03:04:05+02:00');\n",
		},
		{
			name:       "bytes in a DATETIME column",
			dialect:    "oracle",
			columnType: "DATETIME",
			value:      []byte("2024-03-01 12:30:45.5"),
			wantUp:     "INSERT INTO t (id, v) VALUES ('r1', TIMESTAMP '2024-03-01 12:30:45.5+00:00');\n",
		},
		{
			name:       "text in a VARCHAR2 column stays text",
			dialect:    "oracle",
			columnType: "VARCHAR(32)",
			value:      "2024-03-01 12:30:45",
			wantUp:     "INSERT INTO t (id, v) VALUES ('r1', '2024-03-01 12:30:45');\n",
		},
		{
			name:       "a column of unknown type renders its Go value",
			dialect:    "oracle",
			columnType: "",
			value:      "2024-03-01",
			wantUp:     "INSERT INTO t (id, v) VALUES ('r1', '2024-03-01');\n",
		},
		{
			name:       "postgres keeps the text the declaration wrote",
			dialect:    "postgres",
			columnType: "TIMESTAMP",
			value:      "2024-03-01 12:30:45",
			wantUp:     "INSERT INTO \"t\" (\"id\", \"v\") VALUES ('r1', '2024-03-01 12:30:45');\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &datadiff.DataDiff{
				Table:       "t",
				Keys:        []string{"id"},
				ColumnTypes: map[string]string{"v": tt.columnType},
				Inserts:     []datadiff.Row{{"id": "r1", "v": tt.value}},
			}
			up, _, err := datadiff.Render(diff, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(up, qt.Equals, tt.wantUp)
		})
	}
}

// TestRender_OracleDeclaredMomentReachesEveryStatement keys a table on a
// TIMESTAMP column, so the typed literal has to reach the SET list, the WHERE
// clause and the re-inserted row of the down script, not the INSERT alone.
func TestRender_OracleDeclaredMomentReachesEveryStatement(t *testing.T) {
	c := qt.New(t)
	diff := &datadiff.DataDiff{
		Table:       "t",
		Keys:        []string{"at"},
		ColumnTypes: map[string]string{"at": "TIMESTAMP", "until": "DATE"},
		Updates: []datadiff.RowUpdate{{
			Key:     map[string]any{"at": "2024-03-01"},
			Desired: datadiff.Row{"at": "2024-03-01", "until": "2024-04-01"},
			Live:    datadiff.Row{"at": "2024-03-01", "until": "2024-03-15"},
		}},
		Deletes: []datadiff.Row{{"at": "2024-03-02", "until": "2024-03-03"}},
	}

	up, down, err := datadiff.Render(diff, "oracle")
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Equals,
		"UPDATE t SET until = TIMESTAMP '2024-04-01 00:00:00+00:00' WHERE at = TIMESTAMP '2024-03-01 00:00:00+00:00';\n"+
			"DELETE FROM t WHERE at = TIMESTAMP '2024-03-02 00:00:00+00:00';\n")
	c.Assert(down, qt.Equals,
		"INSERT INTO t (at, until) VALUES (TIMESTAMP '2024-03-02 00:00:00+00:00', TIMESTAMP '2024-03-03 00:00:00+00:00');\n"+
			"UPDATE t SET until = TIMESTAMP '2024-03-15 00:00:00+00:00' WHERE at = TIMESTAMP '2024-03-01 00:00:00+00:00';\n")
}

// TestRender_OracleDeclaredMoment_FailurePath refuses text that names no moment
// for an Oracle datetime column, when the plan is built, rather than sending a
// statement the server refuses.
func TestRender_OracleDeclaredMoment_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := &datadiff.DataDiff{
		Table:       "t",
		Keys:        []string{"id"},
		ColumnTypes: map[string]string{"v": "TIMESTAMP"},
		Inserts:     []datadiff.Row{{"id": "r1", "v": "next tuesday"}},
	}

	up, down, err := datadiff.Render(diff, "oracle")
	c.Assert(err, qt.ErrorMatches,
		`datadiff: column "v": datadiff: "next tuesday" does not name a moment, and a TIMESTAMP column on oracle does not accept text; write it as 2006-01-02, 2006-01-02 15:04:05 or RFC 3339`)
	c.Assert(up, qt.Equals, "")
	c.Assert(down, qt.Equals, "")
}
