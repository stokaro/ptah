package atlasschema_test

// Coverage for the schema apply lock NAME, which carries Atlas's
// `schema apply --lock-name`. The acquired lock records the name it was taken
// under, including on dialects without advisory-lock semantics, so a caller can
// report which lock a run coordinates on without a live database.

import (
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/dblock"
)

func TestEffectiveApplyLockName(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "empty selects the default", value: "", want: atlasschema.ApplyLockName},
		{name: "whitespace selects the default", value: "   ", want: atlasschema.ApplyLockName},
		{name: "named", value: "atlas_migrate_execute", want: "atlas_migrate_execute"},
		{name: "trimmed", value: "  atlas_migrate_execute  ", want: "atlas_migrate_execute"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlasschema.EffectiveApplyLockName(test.value), qt.Equals, test.want)
		})
	}
}

func TestAcquireApplyLock_RecordsRequestedName(t *testing.T) {
	tests := []struct {
		name      string
		requested string
		want      string
	}{
		{name: "default", requested: "", want: atlasschema.ApplyLockName},
		{name: "named", requested: "atlas_migrate_execute", want: "atlas_migrate_execute"},
		{name: "trimmed", requested: "  custom-lock ", want: "custom-lock"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connectSQLite(c, filepath.Join(c.TB.TempDir(), "lock-name.db"))
			defer dbschema.CloseAndWarn(conn)

			lock, err := atlasschema.AcquireApplyLock(c.Context(), conn, test.requested, time.Second)

			c.Assert(err, qt.IsNil)
			c.Assert(lock.Name(), qt.Equals, test.want)
			// The recorded name is the one the dialect-specific acquisition
			// would use, so the PostgreSQL-family key follows from it.
			c.Assert(dblock.PostgresKey(lock.Name()), qt.Equals, dblock.PostgresKey(test.want))
			c.Assert(lock.Release(), qt.IsNil)
		})
	}
}

func TestApplyLock_NilReportsNoName(t *testing.T) {
	c := qt.New(t)

	// A skipped acquisition leaves a nil lock: no name, no capability claim,
	// and releasing it touches nothing.
	var lock *atlasschema.ApplyLock
	c.Assert(lock.Name(), qt.Equals, "")
	c.Assert(lock.Supported(), qt.IsFalse)
	c.Assert(lock.Release(), qt.IsNil)
}

func TestAcquireApplyLock_DistinctNamesGetDistinctKeys(t *testing.T) {
	c := qt.New(t)

	// Two runs that name different locks do not serialize against each other.
	// That is the whole function of --lock-name, so pin that the names map to
	// different advisory-lock keys rather than collapsing.
	c.Assert(
		dblock.PostgresKey(atlasschema.EffectiveApplyLockName("atlas_migrate_execute")),
		qt.Not(qt.Equals),
		dblock.PostgresKey(atlasschema.EffectiveApplyLockName("")),
	)
}
