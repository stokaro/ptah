package migrator_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// An executable provider's log store has no field for a run reference or an
// actor origin in the current contract. The entries are written without them,
// a person is told once, and the report names no reference, because none was
// stored.
func TestProviderMetadataLog_StoresNoRunReference_HappyPath(t *testing.T) {
	c := qt.New(t)
	owner := &metadataSession{}
	_, runner := metadataMigrator(c, owner)
	var warnings bytes.Buffer
	runner = runner.WithActorOrigin("admission").WithRunReference(operationID).
		WithLogger(slog.New(slog.NewTextHandler(&warnings, nil)))

	err := runner.MigrateUp(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(owner.log.records, qt.HasLen, 2)
	c.Assert(runner.MigrationLogReport(), qt.DeepEquals, &migrator.MigrationLogReport{Written: 2})
	c.Assert(strings.Count(warnings.String(), "stores no run reference"), qt.Equals, 1)
	attempts, err := runner.MigrationLog(c.Context(), 0)
	c.Assert(err, qt.IsNil)
	c.Assert(attempts, qt.HasLen, 1)
	c.Assert(attempts[0].Start.RunReference, qt.Equals, "")
	c.Assert(attempts[0].Start.ActorOrigin, qt.Equals, "")
}

// The control: without a reference to drop, nobody is warned about one.
func TestProviderMetadataLog_WarnsOfNoReferenceNobodyGave_HappyPath(t *testing.T) {
	c := qt.New(t)
	owner := &metadataSession{}
	_, runner := metadataMigrator(c, owner)
	var warnings bytes.Buffer
	runner = runner.WithLogger(slog.New(slog.NewTextHandler(&warnings, nil)))

	err := runner.MigrateUp(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(warnings.String(), qt.Not(qt.Contains), "stores no run reference")
}

// A provider-backed target keeps no schema apply log: its protocol has no
// store for one, and SQL is not its path.
func TestNewSchemaApplyLog_KeepsNoneForAProvider_HappyPath(t *testing.T) {
	c := qt.New(t)
	conn, _ := metadataMigrator(c, &metadataSession{})

	log := migrator.NewSchemaApplyLog(conn, migrator.SchemaApplyLogOptions{RunReference: operationID})

	c.Assert(log, qt.IsNil)
}
