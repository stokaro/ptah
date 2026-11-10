package datadiff_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/dataext"
	"ptah.run/migration/datadiff"
)

func TestComputeWithOptionsMySQLDateTime(t *testing.T) {
	zone := time.FixedZone("Tokyo", 9*60*60)
	tests := []struct {
		name       string
		dialect    string
		columnType string
		desired    any
		live       any
		updates    int
	}{
		{"MySQL driver text", "mysql", "DATETIME", "2026-01-02T03:04:05Z", []byte("2026-01-02 03:04:05"), 0},
		{"MariaDB driver text", "mariadb", "DATETIME(6)", "2026-01-02T03:04:05Z", []byte("2026-01-02 03:04:05"), 0},
		{"driver location", "mysql", "DATETIME", "2026-01-02T03:04:05Z", time.Date(2026, 1, 2, 3, 4, 5, 0, zone), 0},
		{"another wall clock", "mysql", "DATETIME", "2026-01-02T03:04:05Z", []byte("2026-01-02 04:04:05"), 1},
		{"text remains text", "mysql", "TEXT", "2026-01-02T03:04:05Z", []byte("2026-01-02 03:04:05"), 1},
		{"other dialect remains unchanged", "postgres", "TIMESTAMP", "2026-01-02T03:04:05Z", []byte("2026-01-02 03:04:05"), 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			diff, err := datadiff.ComputeWithOptions("", "events", []string{"id"},
				[]datadiff.Row{{"id": 1, "at": tt.desired}},
				[]datadiff.Row{{"id": 1, "at": tt.live}},
				datadiff.ComputeOptions{Dialect: tt.dialect, ColumnTypes: map[string]string{"at": tt.columnType}})
			c.Assert(err, qt.IsNil)
			c.Assert(diff.Updates, qt.HasLen, tt.updates)
			c.Assert(diff.ColumnTypes["at"], qt.Equals, tt.columnType)
		})
	}
}

func TestComputeWithOptionsMySQLDateTimeKey(t *testing.T) {
	c := qt.New(t)
	diff, err := datadiff.ComputeWithOptions("", "events", []string{"at"},
		[]datadiff.Row{{"at": "2026-01-02T03:04:05Z", "label": "same"}},
		[]datadiff.Row{{"at": []byte("2026-01-02 03:04:05"), "label": "same"}},
		datadiff.ComputeOptions{Dialect: "mysql", ColumnTypes: map[string]string{"at": "DATETIME", "label": "TEXT"}})
	c.Assert(err, qt.IsNil)
	c.Assert(diff.Inserts, qt.HasLen, 0)
	c.Assert(diff.Updates, qt.HasLen, 0)
	c.Assert(diff.Deletes, qt.HasLen, 0)
}

func TestRenderMySQLDateTimeText(t *testing.T) {
	c := qt.New(t)
	diff := &datadiff.DataDiff{
		Table: "events", Keys: []string{"id"}, ColumnTypes: map[string]string{"at": "DATETIME"},
		Updates: []datadiff.RowUpdate{{
			Key:     map[string]any{"id": 1},
			Desired: datadiff.Row{"id": 1, "at": "2026-01-02T03:04:05Z"},
			Live:    datadiff.Row{"id": 1, "at": []byte("2026-01-02 04:04:05")},
		}},
	}
	up, down, err := datadiff.Render(diff, "mysql", dataext.Services{})
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Equals, "UPDATE `events` SET `at` = '2026-01-02 03:04:05' WHERE `id` = 1;\n")
	c.Assert(down, qt.Equals, "UPDATE `events` SET `at` = '2026-01-02 04:04:05' WHERE `id` = 1;\n")

	diff.Updates[0].Desired["at"] = "not a date"
	_, _, err = datadiff.Render(diff, "mysql", dataext.Services{})
	c.Assert(err, qt.ErrorMatches, `datadiff: column "at": "not a date" is not a valid DATETIME value`)
}
