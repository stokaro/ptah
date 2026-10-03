package migrator

// White-box testing required: the YDB spellings of the migrator's tables are
// chosen by package-local builders, and a statement YDB refuses surfaces only
// as a driver error from inside Initialize. The live tests under
// integration/dbschema/ydb prove the server takes each statement; these name
// the clauses the builders owe and the ones another dialect's branch would
// have given YDB.

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// Each table carries its key as a table-level clause, which is the only one
// YQL has, and every integer column is Int64, the width the YDB connection
// binds a Go int at: YDB refuses an Int64 value for an Int32 column. The
// generic branch would have given YDB an inline PRIMARY KEY, BIGINT, TEXT and
// expression-free DEFAULT clauses it does not need.
func TestYDBMetadataTables_SpellTheTablesYDBTakes(t *testing.T) {
	tests := []struct {
		name   string
		ddl    string
		want   []string
		absent []string
	}{
		{
			name: "native revision table",
			ddl:  ptahRevisionsTableDDL(platform.YDB, "`schema_migrations`", "", ""),
			want: []string{
				"CREATE TABLE IF NOT EXISTS `schema_migrations` (",
				"version Int64 NOT NULL,",
				"applied_at Timestamp NOT NULL,",
				"applied Int64 NOT NULL,",
				"error Utf8,",
				"PRIMARY KEY (version)",
			},
			absent: []string{"BIGINT", "TEXT", "INTEGER", "VARCHAR", "DEFAULT", "ENGINE", "NOT NULL PRIMARY KEY"},
		},
		{
			name: "migration log",
			ddl:  ptahMigrationLogDDL(platform.YDB, "`schema_migrations_log`", "", ""),
			want: []string{
				"run_id Utf8 NOT NULL,",
				"seq Int64 NOT NULL,",
				"logged_at Timestamp NOT NULL,",
				"PRIMARY KEY (run_id, seq)",
			},
			absent: []string{"BIGINT", "TEXT", "VARCHAR", "ENGINE"},
		},
		{
			name:   "tags",
			ddl:    ydbMigrationTagsTableDDL("`ptah_migration_tags`"),
			want:   []string{"tag Utf8 NOT NULL,", "version Int64 NOT NULL,", "recorded_at Timestamp NOT NULL,", "PRIMARY KEY (tag)"},
			absent: []string{"BIGINT", "VARCHAR", "TIMESTAMPTZ", "ENGINE"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			for _, want := range test.want {
				c.Assert(test.ddl, qt.Contains, want)
			}
			for _, absent := range test.absent {
				c.Assert(test.ddl, qt.Not(qt.Contains), absent)
			}
		})
	}
}

// The native layout's column list is what the refusal of a foreign table
// compares against, so it has to name every column the DDL declares, in its
// order, and nothing else.
func TestYDBNativeRevisionColumns_MatchTheDDL(t *testing.T) {
	c := qt.New(t)
	ddl := ydbRevisionsTableDDL("`t`")

	var positions []int
	for _, column := range ydbNativeRevisionColumns {
		positions = append(positions, strings.Index(ddl, "\n    "+column+" "))
	}
	c.Assert(slices.Contains(positions, -1), qt.IsFalse, qt.Commentf("positions %v", positions))
	c.Assert(slices.IsSorted(positions), qt.IsTrue, qt.Commentf("positions %v", positions))
	c.Assert(strings.Count(ddl, "\n    "), qt.Equals, len(ydbNativeRevisionColumns)+1)
}
