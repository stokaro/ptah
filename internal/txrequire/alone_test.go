package txrequire_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/txrequire"
)

// A file runs alone outside a transaction when its one statement is one the
// engine refuses inside a transaction. A statement the engine runs inside one,
// and a second statement beside the build, leave the file to the migrator's
// transaction.
func TestAloneOutsideATransaction(t *testing.T) {
	for _, test := range []struct {
		name       string
		statements []string
		want       bool
	}{
		{"a concurrent index alone", []string{"CREATE INDEX CONCURRENTLY i ON t (id)"}, true},
		{"a concurrent index after a comment", []string{"-- build it\nCREATE INDEX CONCURRENTLY i ON t (id)"}, true},
		{"a concurrent drop alone", []string{"DROP INDEX CONCURRENTLY i"}, true},
		{"a concurrent index beside a table", []string{"CREATE TABLE t (id int)", "CREATE INDEX CONCURRENTLY i ON t (id)"}, false},
		{"a plain index alone", []string{"CREATE INDEX i ON t (id)"}, false},
		{"a vacuum alone", []string{"VACUUM base"}, true},
		{"a concurrent reindex alone", []string{"REINDEX INDEX CONCURRENTLY base_i"}, true},
		{"a plain reindex alone", []string{"REINDEX INDEX base_i"}, false},
		{"nothing", nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(txrequire.AloneOutsideATransaction(platform.Postgres, capability.Postgres16(), test.statements), qt.Equals, test.want)
		})
	}
}
