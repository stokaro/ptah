package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemafile"
)

func TestYQLReplicationChangesAcrossFiles(t *testing.T) {
	c := qt.New(t)
	directory := c.TempDir()
	first := "CREATE ASYNC REPLICATION `archive/mirror` FOR src AS `archive/rep` WITH (CONNECTION_STRING='grpc://source:2136/?database=/remote'); CREATE TRANSFER `archive/ingest` FROM `archive/topic` TO `archive/rows` USING ($msg) -> { RETURN [<|id:$msg._offset|>]; };"
	second := "ALTER ASYNC REPLICATION `archive/mirror` SET (ENDPOINT='moved:2136'); ALTER TRANSFER `archive/ingest` SET (BATCH_SIZE_BYTES=4096);"
	c.Assert(os.WriteFile(filepath.Join(directory, "01.sql"), []byte(first), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(directory, "02.sql"), []byte(second), 0o600), qt.IsNil)
	database, err := schemafile.LoadPath(directory, schemafile.Options{YAML: builtintest.Runtime().YAML(), Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
	replication, found, err := database.FeatureObjects.Get(ydbreplication.ReplicationRef("archive", "mirror"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(replication.Value.(*ydbreplication.DesiredReplication).Spec.Connection, qt.DeepEquals,
		ydbreplication.Connection{ConnectionString: "grpc://moved:2136/?database=/remote"})
	transfer, found, err := database.FeatureObjects.Get(ydbreplication.TransferRef("archive", "ingest"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(transfer.Value.(*ydbreplication.DesiredTransfer).Spec.BatchSizeBytes, qt.Equals, uint64(4096))
}
