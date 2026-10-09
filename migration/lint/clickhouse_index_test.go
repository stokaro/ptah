package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

func TestClickHouseIndexAdditionReportsTheIndex(t *testing.T) {
	c := qt.New(t)
	analysis, err := lint.AnalyzeFS(fixture(map[string]string{
		"1_index.sql": "ALTER TABLE events ADD INDEX idx_source source TYPE minmax GRANULARITY 1;",
	}), lint.Options{DirFormat: migrationfile.DirFormatAtlas, Dialect: "clickhouse"})
	c.Assert(err, qt.IsNil)
	c.Assert(projectChanges(fileByName(c, analysis, "1_index.sql").Changes), qt.DeepEquals,
		[]changeProjection{{Kind: lint.SchemaChangeAdd, Object: "idx_source"}})
}
