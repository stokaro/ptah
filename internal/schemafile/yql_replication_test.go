package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/internal/schemafile"
)

func TestYQLReplicationChangesAcrossFiles(t *testing.T) {
	c := qt.New(t)
	directory := c.TempDir()
	first := "CREATE ASYNC REPLICATION `archive/mirror` FOR src AS `archive/rep` WITH (CONNECTION_STRING='grpc://source:2136/?database=/remote'); CREATE TRANSFER `archive/ingest` FROM `archive/topic` TO `archive/rows` USING ($msg) -> { RETURN [<|id:$msg._offset|>]; };"
	second := "ALTER ASYNC REPLICATION `archive/mirror` SET (ENDPOINT='moved:2136'); ALTER TRANSFER `archive/ingest` SET (BATCH_SIZE_BYTES=4096);"
	c.Assert(os.WriteFile(filepath.Join(directory, "01.sql"), []byte(first), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(directory, "02.sql"), []byte(second), 0o600), qt.IsNil)
	database, err := schemafile.LoadPath(directory, schemafile.Options{Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
	c.Assert(database.AsyncReplications, qt.HasLen, 1)
	c.Assert(database.AsyncReplications[0].Spec.Connection, qt.DeepEquals, ast.ReplicationConnectionSpec{ConnectionString: "grpc://moved:2136/?database=/remote"})
	c.Assert(database.Transfers, qt.HasLen, 1)
	c.Assert(database.Transfers[0].Spec.BatchSizeBytes, qt.Equals, uint64(4096))
	c.Assert(database.NotDescribed.Describes(coverage.Replication), qt.IsTrue)
	c.Assert(database.NotDescribed.Describes(coverage.Transfer), qt.IsTrue)
}
