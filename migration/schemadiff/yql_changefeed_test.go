package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
)

func TestCompare_YQLChangefeedsDeclaredAndOmitted(t *testing.T) {
	const table = "CREATE TABLE events (id Int64 NOT NULL, PRIMARY KEY (id));"
	const feed = "ALTER TABLE events ADD CHANGEFEED updates WITH (mode='UPDATES', format='JSON');"
	const consumer = "ALTER TOPIC `events/updates` ADD CONSUMER worker;"
	for _, test := range []struct {
		name, source string
		changes      int
	}{
		{name: "declared", source: table + feed + consumer, changes: 0},
		{name: "consumer omitted", source: table + feed, changes: 1},
		{name: "feed omitted", source: table, changes: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, _, err := sqlschema.Read([]byte(test.source), "ydb")
			c.Assert(err, qt.IsNil)
			held := changefeedCatalog(ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", Consumers: []ydbtopic.ConsumerSpec{{Name: "worker"}}})
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, held, "ydb", must.Must(builtin.New())))
			c.Assert(diff.TablesModified, qt.HasLen, test.changes)
		})
	}
}
