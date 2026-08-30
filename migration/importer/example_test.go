package importer_test

import (
	"errors"
	"fmt"
	"os"
	"testing/fstest"

	"github.com/go-extras/go-kit/must"

	"go.5x5.cz/ptah/migration/importer"
)

// ExampleImport runs the whole pipeline offline: with a nil parser the source
// tool is auto-detected from the directory shape, and Options.DryRun reports
// the Ptah files the import would write without touching the output directory.
// The golang-migrate versions fit Ptah's 10-digit file-name format, so they
// are preserved in the generated names.
func ExampleImport() {
	source := fstest.MapFS{
		"000001_create_users.up.sql":   {Data: []byte("CREATE TABLE users (id BIGINT PRIMARY KEY);\n")},
		"000001_create_users.down.sql": {Data: []byte("DROP TABLE users;\n")},
		"000002_add_email.up.sql":      {Data: []byte("ALTER TABLE users ADD COLUMN email TEXT;\n")},
		"000002_add_email.down.sql":    {Data: []byte("ALTER TABLE users DROP COLUMN email;\n")},
	}

	result := must.Must(importer.Import(source, nil, "migrations", importer.Options{DryRun: true}))
	for _, name := range result.Files {
		fmt.Println(name)
	}

	// Output:
	// 0000000001_create_users.up.sql
	// 0000000001_create_users.down.sql
	// 0000000002_add_email.up.sql
	// 0000000002_add_email.down.sql
}

// ExampleImport_partialRefused shows the partial-import guard: a file that
// carries SQL but matches no golang-migrate file name is declined, and a
// non-dry-run import refuses to write ptah.sum over the subset that survived.
// The error is a *importer.PartialImportError carrying the declined files, so
// a caller can present them and decide whether Options.AllowPartial — the
// deliberate override — is warranted.
func ExampleImport_partialRefused() {
	source := fstest.MapFS{
		"000001_create_users.up.sql":   {Data: []byte("CREATE TABLE users (id BIGINT PRIMARY KEY);\n")},
		"000001_create_users.down.sql": {Data: []byte("DROP TABLE users;\n")},
		"000002_add_email.sql":         {Data: []byte("ALTER TABLE users ADD COLUMN email TEXT;\n")},
	}
	outDir := must.Must(os.MkdirTemp("", "importer-example"))
	defer os.RemoveAll(outDir)

	_, err := importer.Import(source, nil, outDir, importer.Options{})

	if partial, ok := errors.AsType[*importer.PartialImportError](err); ok {
		for _, declined := range partial.Declined {
			fmt.Printf("%s: %s\n", declined.Path, declined.Reason)
		}
	}

	// Output:
	// 000002_add_email.sql: its name is not a golang-migrate migration file name (<version>_<name>.up.sql / .down.sql)
}

// ExampleDetectParser identifies the source tool from the directory shape
// alone, which is what Import does when handed a nil parser. A Goose migration
// is a single .sql file whose sections are separated by -- +goose Up and
// -- +goose Down markers, and the marker is what detection reads.
func ExampleDetectParser() {
	source := fstest.MapFS{
		"00001_create_users.sql": {Data: []byte(
			"-- +goose Up\nCREATE TABLE users (id BIGINT PRIMARY KEY);\n-- +goose Down\nDROP TABLE users;\n")},
	}

	parser := must.Must(importer.DetectParser(source))
	fmt.Println(parser.Name())

	// Output:
	// goose
}

// ExampleNormalize orders migrations by source version and validates them
// before emission: versioned migrations sort ascending, repeatable ones sort
// after every versioned one in parse order, and a duplicate or non-positive
// version is an error. The input slice is not mutated; the result is a sorted
// copy.
func ExampleNormalize() {
	migrations := []importer.SourceMigration{
		{Version: 20, Name: "add_index", UpSQL: "CREATE INDEX idx_users_email ON users (email);"},
		{Name: "refresh_view", Repeatable: true, UpSQL: "CREATE VIEW active_users AS SELECT id FROM users;"},
		{Version: 3, Name: "create_users", UpSQL: "CREATE TABLE users (id BIGINT PRIMARY KEY);"},
	}

	normalized := must.Must(importer.Normalize(migrations))
	for _, migration := range normalized {
		fmt.Printf("version=%d name=%s repeatable=%t\n", migration.Version, migration.Name, migration.Repeatable)
	}

	// Output:
	// version=3 name=create_users repeatable=false
	// version=20 name=add_index repeatable=false
	// version=0 name=refresh_view repeatable=true
}

// ExampleEmit shows version remapping: a 14-digit golang-migrate timestamp does
// not fit Ptah's 10-digit file-name format, so every migration is reassigned a
// sequential Ptah version and the source version is folded into the description,
// keeping history traceable. EmitResult.Remapped reports that this happened.
// Options.DryRun plans without writing, so no output directory is needed.
func ExampleEmit() {
	migrations := []importer.SourceMigration{
		{Version: 20240102150405, Name: "create_users",
			UpSQL: "CREATE TABLE users (id BIGINT PRIMARY KEY);", DownSQL: "DROP TABLE users;"},
		{Version: 20240301090000, Name: "add_email",
			UpSQL: "ALTER TABLE users ADD COLUMN email TEXT;", DownSQL: "ALTER TABLE users DROP COLUMN email;"},
	}

	result := must.Must(importer.Emit("migrations", migrations, nil, importer.Options{DryRun: true}))
	fmt.Println("remapped:", result.Remapped)
	for _, name := range result.Files {
		fmt.Println(name)
	}

	// Output:
	// remapped: true
	// 0000000001_v20240102150405_create_users.up.sql
	// 0000000001_v20240102150405_create_users.down.sql
	// 0000000002_v20240301090000_add_email.up.sql
	// 0000000002_v20240301090000_add_email.down.sql
}
