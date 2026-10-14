package replayguards_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replayguards"
)

func TestAll_ServesEachDialectOnceUnderItsCanonicalName(t *testing.T) {
	c := qt.New(t)
	var dialects []string
	for _, service := range replayguards.All() {
		c.Assert(platform.NormalizeDialect(service.Dialect), qt.Equals, service.Dialect)
		c.Assert(service.Guard, qt.IsNotNil)
		dialects = append(dialects, service.Dialect)
	}
	slices.Sort(dialects)
	c.Assert(dialects, qt.DeepEquals, []string{
		"clickhouse", "cockroachdb", "mariadb", "mysql", "postgres",
		"spanner", "sqlite", "sqlserver", "ydb", "yugabytedb",
	})
}
