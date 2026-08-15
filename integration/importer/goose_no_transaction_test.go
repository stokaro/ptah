//go:build integration

package importer_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/importer"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestGooseNoTransactionArtifactImportsAppliesAndRollsBack(t *testing.T) {
	c := qt.New(t)
	workspace := c.TempDir()
	sourceDir := filepath.Join(workspace, "source")
	c.Assert(os.MkdirAll(sourceDir, 0o755), qt.IsNil)
	desiredPath := filepath.Join(workspace, "desired.sql")
	c.Assert(os.WriteFile(
		desiredPath,
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	desired, err := atlassource.ClassifySet(
		"--to",
		[]string{"file://" + desiredPath},
		atlassource.ProjectEnv{},
	)
	c.Assert(err, qt.IsNil)
	dev, err := dbschema.ConnectToDatabase(
		context.Background(),
		"sqlite://"+filepath.Join(workspace, "dev.db"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(dev) })

	generated, err := atlasmigrate.GenerateDiff(context.Background(), dev, atlasmigrate.DiffOptions{
		Dir:               sourceDir,
		Desired:           desired,
		Name:              "widgets",
		DirFormat:         atlasmigrateimport.FormatGoose,
		LockTimeout:       time.Second,
		PlanBidirectional: gooseWholeFileNoTransactionPlan,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(generated.MigrationPaths, qt.HasLen, 1)
	artifact, err := os.ReadFile(generated.MigrationPaths[0])
	c.Assert(err, qt.IsNil)
	c.Assert(strings.HasPrefix(
		string(artifact),
		"-- +goose NO TRANSACTION\n-- +goose Up\n",
	), qt.IsTrue)
	c.Assert(string(artifact), qt.Contains, "-- +goose Down\n")
	c.Assert(string(artifact), qt.Contains, "DROP TABLE")

	out := c.TempDir()
	parser, err := importer.ParserByName("goose")
	c.Assert(err, qt.IsNil)

	result, err := importer.Import(os.DirFS(sourceDir), parser, out, importer.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Files, qt.HasLen, 2)
	c.Assert(result.Remapped, qt.IsTrue)
	c.Assert(result.Files[0], qt.Matches, `0000000001_v[0-9]+_widgets\.up\.sql`)
	c.Assert(result.Files[1], qt.Matches, `0000000001_v[0-9]+_widgets\.down\.sql`)
	up, readErr := os.ReadFile(filepath.Join(out, result.Files[0]))
	c.Assert(readErr, qt.IsNil)
	c.Assert(strings.HasPrefix(string(up), "-- +ptah no_transaction\n"), qt.IsTrue)
	c.Assert(string(up), qt.Contains, "CREATE TABLE")
	down, readErr := os.ReadFile(filepath.Join(out, result.Files[1]))
	c.Assert(readErr, qt.IsNil)
	c.Assert(strings.HasPrefix(string(down), "-- +ptah no_transaction\n"), qt.IsTrue)
	c.Assert(string(down), qt.Contains, "DROP TABLE")

	conn, err := dbschema.ConnectToDatabase(
		t.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "target.db"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	migrations, err := migrator.NewFSMigrator(conn, os.DirFS(out))
	c.Assert(err, qt.IsNil)

	c.Assert(migrations.MigrateUp(t.Context()), qt.IsNil)
	c.Assert(gooseImportedTableCount(c.TB, conn, "widgets"), qt.Equals, 1)
	c.Assert(migrations.MigrateDownTo(t.Context(), 0), qt.IsNil)
	c.Assert(gooseImportedTableCount(c.TB, conn, "widgets"), qt.Equals, 0)
}

func gooseWholeFileNoTransactionPlan(
	input atlasmigrate.BidirectionalPlanInput,
) (atlasmigrate.BidirectionalPlan, error) {
	planned, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          input.Diff,
		DesiredSchema: input.DesiredSchema,
		CurrentSchema: input.CurrentSchema,
		Dialect:       input.Dialect,
		Capabilities:  input.Capabilities,
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})
	if err != nil {
		return atlasmigrate.BidirectionalPlan{}, err
	}
	return atlasmigrate.BidirectionalPlan{
		ForwardNodes:                 planned.Forward.Nodes,
		ReverseNodes:                 planned.Reverse.Nodes,
		ReverseRequiresNoTransaction: true,
	}, nil
}

func gooseImportedTableCount(
	tb testing.TB,
	conn *dbschema.DatabaseConnection,
	table string,
) int {
	c := qt.New(tb)
	c.Helper()
	var count int
	err := conn.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
		table,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}
