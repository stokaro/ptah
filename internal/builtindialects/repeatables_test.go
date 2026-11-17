package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/builtindialects"
)

// TestDescriptors_StateTheRepeatableRecordWhereItIsMeasured pins the engines
// that keep a repeatable migration record. The migrator runs repeatable
// migrations only where a descriptor states the table and refuses a directory
// holding one everywhere else, so a descriptor that gains the statement is an
// engine the repeatable tests have to reach: SQLite in process, PostgreSQL in
// the integration contour.
func TestDescriptors_StateTheRepeatableRecordWhereItIsMeasured(t *testing.T) {
	c := qt.New(t)
	c.Assert(enginesWithRepeatableRecord(), qt.DeepEquals, []string{platform.Postgres, platform.SQLite})
}

// enginesWithRepeatableRecord names the descriptors that state the record, in
// seed order.
func enginesWithRepeatableRecord() []string {
	var stated []string
	for _, described := range builtindialects.Descriptors() {
		if described.RevisionTable.CreateRepeatables != "" {
			stated = append(stated, described.Name)
		}
	}
	return stated
}
