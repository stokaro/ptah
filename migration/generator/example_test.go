package generator_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing/fstest"

	"github.com/go-extras/go-kit/must"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/migrationfile"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// exampleEntities is the desired schema every example plans toward: one
// annotated Go entity, the same source `ptah schema render` reads.
const exampleEntities = `package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//ptah:schema:field name="email" type="TEXT" not_null="true"
	Email string
}
`

// stripVersion drops the leading Unix-seconds version from a generated file
// name, leaving the stable part an example can print.
func stripVersion(path string) string {
	return strings.SplitN(filepath.Base(path), "_", 2)[1]
}

// stripHeader drops a generated migration body's comment header, whose
// Generated-on line carries the wall clock, leaving the SQL statements.
func stripHeader(sql string) string {
	_, body, _ := strings.Cut(sql, "\n\n")
	return body
}

// ExampleGenerateMigration is the one-call path: compare annotated Go entities
// with a live database and publish the up/down pair in one step. The entities
// come from a testing/fstest.MapFS and the database is a SQLite file, so the
// whole flow runs without a server; any URL dbschema.ConnectToDatabase accepts
// works the same way.
func ExampleGenerateMigration() {
	dir := must.Must(os.MkdirTemp("", "generator-example"))
	defer os.RemoveAll(dir)

	files := must.Must(generator.GenerateMigration(context.Background(), generator.GenerateMigrationOptions{
		GoEntitiesFS:  fstest.MapFS{"models/user.go": &fstest.MapFile{Data: []byte(exampleEntities)}},
		GoEntitiesDir: "models",
		DatabaseURL:   "sqlite://" + filepath.Join(dir, "app.db"),
		MigrationName: "create_users",
		OutputDir:     filepath.Join(dir, "migrations"),
	}))

	for _, pair := range files.Files {
		fmt.Println("up:  ", stripVersion(pair.UpFile))
		fmt.Println("down:", stripVersion(pair.DownFile))
	}
	fmt.Print(stripHeader(string(must.Must(os.ReadFile(files.Files[0].UpFile)))))

	// Output:
	// up:   create_users.up.sql
	// down: create_users.down.sql
	// CREATE TABLE "users" (
	//   "id" INTEGER PRIMARY KEY,
	//   "email" TEXT NOT NULL
	// );
}

// ExamplePlanMigration is the two-phase embedder contract: plan first, publish
// once, and defer Close so an abandoned plan releases the migration directory
// it holds (on Windows an unreleased handle blocks removing or renaming the
// directory). Close after publication is a no-op, so the defer is always
// correct, and a second publication through the same plan is refused: the
// honest retry is a fresh PlanMigration.
func ExamplePlanMigration() {
	dir := must.Must(os.MkdirTemp("", "generator-example"))
	defer os.RemoveAll(dir)
	ctx := context.Background()

	plan := must.Must(generator.PlanMigration(ctx, generator.GenerateMigrationOptions{
		GoEntitiesFS:  fstest.MapFS{"models/user.go": &fstest.MapFile{Data: []byte(exampleEntities)}},
		GoEntitiesDir: "models",
		DatabaseURL:   "sqlite://" + filepath.Join(dir, "app.db"),
		MigrationName: "create_users",
		OutputDir:     filepath.Join(dir, "migrations"),
	}))
	if plan == nil {
		fmt.Println("schemas already match") // not reached: the database is empty
		return
	}
	defer plan.Close()

	files := must.Must(plan.WriteFilesContext(ctx))
	fmt.Println("published pairs:", len(files.Files))

	_, err := plan.WriteFilesContext(ctx)
	fmt.Println("second publication:", err)

	// Output:
	// published pairs: 1
	// second publication: migration plan has already been written
}

// ExamplePlanMigration_directoryChanged shows the publication precondition: a
// plan records the migration directory's contents while it is built, and a
// file that appears between planning and publication refuses the batch with
// ErrMigrationDirectoryChanged instead of publishing on top of history the
// plan never saw.
func ExamplePlanMigration_directoryChanged() {
	dir := must.Must(os.MkdirTemp("", "generator-example"))
	defer os.RemoveAll(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	must.Assert(os.MkdirAll(migrationsDir, 0o755))
	ctx := context.Background()

	plan := must.Must(generator.PlanMigration(ctx, generator.GenerateMigrationOptions{
		GoEntitiesFS:  fstest.MapFS{"models/user.go": &fstest.MapFile{Data: []byte(exampleEntities)}},
		GoEntitiesDir: "models",
		DatabaseURL:   "sqlite://" + filepath.Join(dir, "app.db"),
		MigrationName: "create_users",
		OutputDir:     migrationsDir,
	}))
	defer plan.Close()

	// A concurrent writer adds a migration after planning.
	must.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "1000000000_concurrent.up.sql"),
		[]byte("CREATE TABLE other (id INTEGER);\n"),
		0o600,
	))

	_, err := plan.WriteFilesContext(ctx)
	fmt.Println(errors.Is(err, generator.ErrMigrationDirectoryChanged))
	fmt.Println(err)

	// Output:
	// true
	// migration directory changed before publication
}

// ExampleGenerateEmptyMigration creates skeletons for manual SQL with no
// database at all. The default layout writes a reversible Ptah pair; the
// Atlas layout writes a single up-only file and commits atlas.sum over it,
// which is why its pair carries no DownFile.
func ExampleGenerateEmptyMigration() {
	dir := must.Must(os.MkdirTemp("", "generator-example"))
	defer os.RemoveAll(dir)

	pairDir := filepath.Join(dir, "ptah")
	files := must.Must(generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: "backfill_orders",
		OutputDir:     pairDir,
	}))
	fmt.Println("up:  ", stripVersion(files.Files[0].UpFile))
	fmt.Println("down:", stripVersion(files.Files[0].DownFile))

	atlasDir := filepath.Join(dir, "atlas")
	atlasFiles := must.Must(generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: "backfill_orders",
		OutputDir:     atlasDir,
		DirFormat:     migrationfile.DirFormatAtlas,
	}))
	fmt.Println("atlas file:", stripVersion(atlasFiles.Files[0].UpFile))
	fmt.Printf("atlas down file: %q\n", atlasFiles.Files[0].DownFile)
	_, sumErr := os.Stat(filepath.Join(atlasDir, "atlas.sum"))
	fmt.Println("atlas.sum written:", sumErr == nil)

	// Output:
	// up:   backfill_orders.up.sql
	// down: backfill_orders.down.sql
	// atlas file: backfill_orders.sql
	// atlas down file: ""
	// atlas.sum written: true
}

// ExampleGenerateCheckpointFromShadow squashes a migration directory into one
// cumulative checkpoint body pair. The directory is replayed into a disposable
// SQLite shadow database and the resulting schema is introspected and
// re-rendered, so the checkpoint reflects what the history actually builds --
// here, one CREATE TABLE carrying the column a later migration added.
func ExampleGenerateCheckpointFromShadow() {
	dir := must.Must(os.MkdirTemp("", "generator-example"))
	defer os.RemoveAll(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	must.Assert(os.MkdirAll(migrationsDir, 0o755))
	write := func(name, sql string) {
		must.Assert(os.WriteFile(filepath.Join(migrationsDir, name), []byte(sql), 0o600))
	}
	write("0000000001_init.up.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	write("0000000001_init.down.sql", "DROP TABLE users;\n")
	write("0000000002_add_email.up.sql", "ALTER TABLE users ADD COLUMN email TEXT;\n")
	write("0000000002_add_email.down.sql", "ALTER TABLE users DROP COLUMN email;\n")

	up, down, err := generator.GenerateCheckpointFromShadow(context.Background(), generator.CheckpointFromShadowOptions{
		ShadowDatabaseURL: "sqlite://" + filepath.Join(dir, "shadow.db"),
		MigrationsDir:     migrationsDir,
	})
	if err != nil {
		fmt.Println("checkpoint:", err)
		return
	}
	fmt.Println(stripHeader(up))
	fmt.Println(stripHeader(down))

	// Output:
	// CREATE TABLE "users" (
	//   "id" INTEGER PRIMARY KEY,
	//   "email" TEXT
	// );
	// -- WARNING: This will delete all data!
	// DROP TABLE IF EXISTS "users";
}

// ExamplePlanBidirectionalSchemaDiff is the lower-level, fully offline
// planning boundary for a caller that already holds a schema diff: no
// connection, no filesystem. It returns the forward plan and the exact
// rollback in one result, each with its own RequiresNoTransaction
// classification.
func ExamplePlanBidirectionalSchemaDiff() {
	desired := must.Must(goschema.ParseFS(
		fstest.MapFS{"models/user.go": &fstest.MapFile{Data: []byte(exampleEntities)}},
		"models",
	))
	current := &catalog.Database{} // an empty database

	plan := must.Must(generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          schemadiff.CompareWithDialect(desired, current, "postgres"),
		DesiredSchema: desired,
		CurrentSchema: current,
		Dialect:       "postgres",
	}))

	r := must.Must(renderer.NewRenderer("postgres"))
	fmt.Println("-- forward, no-transaction:", plan.Forward.RequiresNoTransaction)
	for _, node := range plan.Forward.Nodes {
		fmt.Print(must.Must(r.Render(node)))
	}
	fmt.Println("-- reverse, no-transaction:", plan.Reverse.RequiresNoTransaction)
	for _, node := range plan.Reverse.Nodes {
		fmt.Print(must.Must(r.Render(node)))
	}

	// Output:
	// -- forward, no-transaction: false
	// -- POSTGRES TABLE: users --
	// CREATE TABLE "users" (
	//   "id" INTEGER PRIMARY KEY NOT NULL,
	//   "email" TEXT NOT NULL
	// );
	//
	// -- reverse, no-transaction: false
	// -- WARNING: This will delete all data!
	// DROP TABLE IF EXISTS "users" CASCADE;
}
