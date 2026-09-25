package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// PG108 reads the table an index is built on as partitioned from the same
// file's CREATE TABLE ... PARTITION BY, or, for a table an earlier migration
// created, from the starting state (stokaro/ptah#3588). Where it fires it
// replaces PG101, whose CONCURRENTLY remedy PostgreSQL refuses on a
// partitioned table.

const pg101Advice = "CREATE INDEX without CONCURRENTLY blocks writes to the table for the whole build; " +
	"on a populated table use CREATE INDEX CONCURRENTLY outside a transaction"

// pg108LockMessage is PG108's message for a plain build on the table spelled
// name.
func pg108LockMessage(name string) string {
	return "building this index takes a SHARE lock on " + name + " and on every one of its partitions at once, " +
		"so writes stop across the whole set rather than one partition at a time; CONCURRENTLY is refused on a " +
		"partitioned table, so the way to keep writes going is to build the index on each partition concurrently " +
		"and attach it to a parent index created with ON ONLY"
}

// measurementsColumns is the starting state of version 2 for measurements in
// each schema named, partitioned or not.
func measurementsColumns(partitioned bool, schemas ...string) []lint.BaselineColumn {
	var columns []lint.BaselineColumn
	for _, schema := range schemas {
		for _, name := range []string{"id", "taken_on", "value"} {
			columns = append(columns, lint.BaselineColumn{
				Version: 2, Schema: schema, Table: "measurements", Name: name, TablePartitioned: partitioned,
			})
		}
	}
	return columns
}

// analyzeMeasurements lints a directory whose first version creates
// measurements and whose second is the statement under test, in a
// no_transaction file so PG103 stays out of the way of a CONCURRENTLY row.
func analyzeMeasurements(c *qt.C, migration string, columns []lint.BaselineColumn, disabled ...string) lint.Analysis {
	c.Helper()
	analysis, err := lint.AnalyzeFS(fixture(map[string]string{
		"1_measurements.sql": "CREATE TABLE measurements (id bigint, taken_on date, value int) PARTITION BY RANGE (taken_on);",
		"2_change.sql":       "-- atlas:txmode none\n\n" + migration,
	}), lint.Options{
		Dialect:   "postgres",
		DirFormat: migrationfile.DirFormatAtlas,
		Selection: lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Baseline:  columns,
		Disabled:  disabled,
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

// indexBuildFindings keeps PG101 and PG108, spelled code: message.
func indexBuildFindings(findings []lint.Finding) []string {
	var kept []string
	for _, finding := range findings {
		if finding.Rule == "PG101" || finding.Rule == "PG108" {
			kept = append(kept, finding.Rule+": "+finding.Message)
		}
	}
	return kept
}

func TestPG108_ReadsTheStartingState(t *testing.T) {
	tests := []struct {
		name      string
		migration string
		columns   []lint.BaselineColumn
		want      []string
	}{
		{
			name:      "an index on a parent an earlier migration created",
			migration: "CREATE INDEX m_value ON measurements (value);",
			columns:   measurementsColumns(true, "public"),
			want:      []string{"PG108: " + pg108LockMessage("measurements")},
		},
		{
			name:      "the parent named with its schema",
			migration: "CREATE INDEX m_value ON public.measurements (value);",
			columns:   measurementsColumns(true, "public"),
			want:      []string{"PG108: " + pg108LockMessage("public.measurements")},
		},
		{
			name:      "the parent named with its schema, beside an ordinary table of that name in another",
			migration: "CREATE INDEX m_value ON public.measurements (value);",
			columns:   append(measurementsColumns(true, "public"), measurementsColumns(false, "archive")...),
			want:      []string{"PG108: " + pg108LockMessage("public.measurements")},
		},
		{
			name:      "CONCURRENTLY on that parent, which PostgreSQL refuses",
			migration: "CREATE INDEX CONCURRENTLY m_value ON measurements (value);",
			columns:   measurementsColumns(true, "public"),
			want: []string{"PG108: PostgreSQL refuses CREATE INDEX CONCURRENTLY on the partitioned table measurements; " +
				"build the index on each partition concurrently and attach it to a parent index created with ON ONLY"},
		},
		{
			name:      "ON ONLY on that parent, the first step of the remedy",
			migration: "CREATE INDEX m_value ON ONLY measurements (value);",
			columns:   measurementsColumns(true, "public"),
		},
		{
			name:      "an ordinary table",
			migration: "CREATE INDEX m_value ON measurements (value);",
			columns:   measurementsColumns(false, "public"),
			want:      []string{"PG101: " + pg101Advice},
		},
		{
			name:      "ON ONLY on an ordinary table, which builds the whole index",
			migration: "CREATE INDEX m_value ON ONLY measurements (value);",
			columns:   measurementsColumns(false, "public"),
			want:      []string{"PG101: " + pg101Advice},
		},
		{
			name:      "no state",
			migration: "CREATE INDEX m_value ON measurements (value);",
			want:      []string{"PG101: " + pg101Advice},
		},
		{
			name:      "ON ONLY with no state",
			migration: "CREATE INDEX m_value ON ONLY measurements (value);",
			want:      []string{"PG101: " + pg101Advice},
		},
		{
			name:      "a bare name two schemas carry is placed in neither",
			migration: "CREATE INDEX m_value ON measurements (value);",
			columns:   append(measurementsColumns(true, "public"), measurementsColumns(false, "archive")...),
			want:      []string{"PG101: " + pg101Advice},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeMeasurements(c, test.migration, test.columns)
			c.Assert(indexBuildFindings(analysis.Findings()), qt.DeepEquals, test.want)
		})
	}
}

// TestPG108_ReadsTheSameFile pins the text half, where the parent is created in
// the migration that indexes it, and the spellings the word after ON gets
// wrong: a schema-qualified parent, and ON ONLY on a table the file creates.
func TestPG108_ReadsTheSameFile(t *testing.T) {
	tests := []struct {
		name      string
		migration string
		want      []string
	}{
		{
			name:      "the parent created in the same file",
			migration: "CREATE TABLE m (id bigint, taken_on date) PARTITION BY RANGE (taken_on);\nCREATE INDEX m_id ON m (id);",
			want:      []string{"PG108: " + pg108LockMessage("m")},
		},
		{
			name:      "the same, named with its schema",
			migration: "CREATE TABLE public.m (id bigint, taken_on date) PARTITION BY RANGE (taken_on);\nCREATE INDEX m_id ON public.m (id);",
			want:      []string{"PG108: " + pg108LockMessage("public.m")},
		},
		{
			name:      "ON ONLY on a parent the file creates",
			migration: "CREATE TABLE m (id bigint, taken_on date) PARTITION BY RANGE (taken_on);\nCREATE INDEX m_id ON ONLY m (id);",
		},
		{
			name:      "an ordinary table the file creates is empty",
			migration: "CREATE TABLE m (id bigint);\nCREATE INDEX m_id ON m (id);",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeMeasurements(c, test.migration, nil)
			c.Assert(indexBuildFindings(analysis.Findings()), qt.DeepEquals, test.want)
		})
	}
}

// TestPG108_AsksForTheStateOfAnEarlierTable: PG108 asks for the starting state
// of every index build on a table an earlier migration created, and of nothing
// else, so a run without a dev database names it and a file the text settles
// costs no catalog read. PG101 is disabled because it asks for the same
// statements on its own account.
func TestPG108_AsksForTheStateOfAnEarlierTable(t *testing.T) {
	tests := []struct {
		name         string
		migration    string
		wantVersions []int64
		wantUnmet    []string
	}{
		{
			name:         "a build on an earlier table",
			migration:    "CREATE INDEX m_value ON measurements (value);",
			wantVersions: []int64{2},
			wantUnmet:    []string{"PG108"},
		},
		{
			name:         "a concurrent build on an earlier table",
			migration:    "CREATE INDEX CONCURRENTLY m_value ON measurements (value);",
			wantVersions: []int64{2},
			wantUnmet:    []string{"PG108"},
		},
		{
			name:      "ON ONLY",
			migration: "CREATE INDEX m_value ON ONLY measurements (value);",
		},
		{
			name:      "a TimescaleDB per-chunk build, which a partitioned table refuses",
			migration: "CREATE INDEX m_value ON measurements (value) WITH (timescaledb.transaction_per_chunk);",
		},
		{
			name:      "a parent the file creates",
			migration: "CREATE TABLE m (id bigint, taken_on date) PARTITION BY RANGE (taken_on);\nCREATE INDEX m_id ON m (id);",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeMeasurements(c, test.migration, nil, "PG101")
			var unmet []string
			for _, entry := range analysis.UnmetInputs() {
				unmet = append(unmet, entry.Rule)
			}
			c.Assert(analysis.BaselineVersions(), qt.DeepEquals, test.wantVersions)
			c.Assert(unmet, qt.DeepEquals, test.wantUnmet)
		})
	}
}
