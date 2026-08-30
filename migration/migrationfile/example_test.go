package migrationfile_test

import (
	"fmt"
	"maps"
	"slices"
	"testing/fstest"

	"github.com/go-extras/go-kit/must"

	"go.5x5.cz/ptah/migration/migrationfile"
)

// ExampleDiscover walks a migration directory in auto format. Results come
// back ascending by Version, in an order two runs over the same filesystem
// reproduce. Ptah files win in auto mode, so the Atlas-shaped
// 20240101_seed.sql is not selected, and the stray notes.sql is simply left
// out rather than failing the walk. A directory whose files match no name
// grammar at all is refused, which is why the error is worth branching on.
func ExampleDiscover() {
	fsys := fstest.MapFS{
		"0000000001_create_users.up.sql":   {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n")},
		"0000000001_create_users.down.sql": {Data: []byte("DROP TABLE users;\n")},
		"0000000002_add_email.up.sql":      {Data: []byte("ALTER TABLE users ADD COLUMN email TEXT;\n")},
		"0000000002_add_email.down.sql":    {Data: []byte("ALTER TABLE users DROP COLUMN email;\n")},
		"20240101_seed.sql":                {Data: []byte("INSERT INTO users DEFAULT VALUES;\n")},
		"notes.sql":                        {Data: []byte("-- scratch pad, not a migration\n")},
	}

	files, err := migrationfile.Discover(fsys, migrationfile.DirFormatAuto)
	if err != nil {
		fmt.Println("discover:", err)
		return
	}
	for _, file := range files {
		fmt.Printf("%d %s %s (%s)\n", file.Version, file.Direction, file.Path, file.Name)
	}

	// Output:
	// 1 down 0000000001_create_users.down.sql (Create Users)
	// 1 up 0000000001_create_users.up.sql (Create Users)
	// 2 down 0000000002_add_email.down.sql (Add Email)
	// 2 up 0000000002_add_email.up.sql (Add Email)
}

// ExampleParseFileName reads one Ptah migration file name into its
// components. The description token is humanized on the way in — underscores
// become spaces and words are Title-Cased — and a ".checkpoint" marker before
// the direction is recognized at name-parse time.
func ExampleParseFileName() {
	file := must.Must(migrationfile.ParseFileName("0000000001_create_users_table.up.sql"))
	fmt.Println(file.Version, file.Direction, file.Name, file.Format)

	checkpoint := must.Must(migrationfile.ParseFileName("0000000007_baseline.checkpoint.up.sql"))
	fmt.Println(checkpoint.Version, checkpoint.IsCheckpoint)

	_, err := migrationfile.ParseFileName("cleanup.sql")
	fmt.Println(err)

	// Output:
	// 1 up Create Users Table ptah
	// 7 true
	// invalid migration file name format
}

// ExampleFileName produces the canonical name for one direction of a Ptah
// migration pair. Any human description becomes a parseable stem — it is
// lowercased, spaces become underscores, and everything else outside
// [a-z0-9_] is stripped — so the result round-trips through ParseFileName.
func ExampleFileName() {
	version := int64(1712000000)
	fmt.Println(migrationfile.FileName(version, "Add User Preferences!", "up"))
	fmt.Println(migrationfile.FileName(version, "Add User Preferences!", "down"))

	// Output:
	// 1712000000_add_user_preferences.up.sql
	// 1712000000_add_user_preferences.down.sql
}

// ExampleParseUp resolves the executable up-direction view of two file
// shapes: a plain SQL file and an Atlas txtar archive. Both converge on one
// ParsedUp — the SQL to run, the explicit transaction mode (empty means the
// caller's global mode applies), and the offset that maps line numbers in the
// extracted SQL back to the source file.
func ExampleParseUp() {
	plain := "-- +ptah no_transaction\nCREATE INDEX CONCURRENTLY idx_users_email ON users (email);\n"
	up, err := migrationfile.ParseUp("0000000001_add_email_index.up.sql", plain)
	if err != nil {
		fmt.Println("parse plain:", err)
		return
	}
	fmt.Printf("plain: mode=%q offset=%d\n", up.TxMode, up.SourceLineOffset)

	archive := `-- atlas:txtar

-- checks.sql --
SELECT count(*) = 0 FROM users;

-- migration.sql --
CREATE TABLE users (id INTEGER PRIMARY KEY);
`
	up, err = migrationfile.ParseUp("20240101000000_users.sql", archive)
	if err != nil {
		fmt.Println("parse txtar:", err)
		return
	}
	fmt.Printf("txtar: mode=%q offset=%d\n", up.TxMode, up.SourceLineOffset)
	fmt.Print(up.SQL)

	// Output:
	// plain: mode="none" offset=0
	// txtar: mode="" offset=6
	// CREATE TABLE users (id INTEGER PRIMARY KEY);
}

// ExampleParseDirectives reads the merged directive map from a migration
// file's directive header — the run of blank lines and line comments before
// the first executable statement. The statement_timeout line below the
// statement contributes nothing here; MisplacedDirectives is what reports it.
func ExampleParseDirectives() {
	sql := `-- +ptah no_transaction
-- +ptah lock_timeout=5s

CREATE INDEX CONCURRENTLY idx_users_email ON users (email);

-- +ptah statement_timeout=30s
`
	directives := migrationfile.ParseDirectives(sql)
	for _, key := range slices.Sorted(maps.Keys(directives)) {
		fmt.Printf("%s=%s\n", key, directives[key])
	}

	// Output:
	// lock_timeout=5s
	// no_transaction=true
}

// ExampleMisplacedDirectives diagnoses directive lines a reader would take
// for directives but the migrator does not honor, because they sit outside
// the region where their family is significant. Each finding carries the
// line, the text as written, and a remedy naming that family's region.
func ExampleMisplacedDirectives() {
	sql := `CREATE INDEX CONCURRENTLY idx_users_email ON users (email);
-- +ptah no_transaction
-- atlas:txmode none
`
	for _, misplaced := range migrationfile.MisplacedDirectives(sql, "") {
		fmt.Printf("line %d: %s\n  remedy: %s\n", misplaced.Line, misplaced.Text, misplaced.Remedy)
	}

	// Output:
	// line 2: -- +ptah no_transaction
	//   remedy: move it above the first SQL statement
	// line 3: -- atlas:txmode none
	//   remedy: move it into the unbroken comment block that starts on line 1
}

// ExampleParseFileTxMode resolves a file's transaction mode across both
// directive families. Source names which family set the mode, so a caller
// can keep the Atlas-specific error contract (AtlasTxModeDirectiveError
// through errors.As) apart from the Ptah spelling.
func ExampleParseFileTxMode() {
	sources := map[migrationfile.FileTxModeSource]string{
		migrationfile.FileTxModeSourcePtah:  "ptah",
		migrationfile.FileTxModeSourceAtlas: "atlas",
	}

	atlas := migrationfile.ParseFileTxMode("concurrent_index.sql",
		"-- atlas:txmode none\nCREATE INDEX CONCURRENTLY idx_users_email ON users (email);\n")
	if atlas.Err != nil {
		fmt.Println(atlas.Err)
		return
	}
	fmt.Printf("mode=%q source=%s\n", atlas.Mode, sources[atlas.Source])

	ptah := migrationfile.ParseFileTxMode("enum_value.sql",
		"-- +ptah no_transaction\nALTER TYPE status ADD VALUE 'archived';\n")
	if ptah.Err != nil {
		fmt.Println(ptah.Err)
		return
	}
	fmt.Printf("mode=%q source=%s\n", ptah.Mode, sources[ptah.Source])

	// Output:
	// mode="none" source=atlas
	// mode="none" source=ptah
}

// ExampleRenderAtlasTemplateSQL renders an Atlas SQL template migration over
// a filesystem holding a shared template in a subdirectory. The shared file
// is referenced by its path with the .sql extension stripped. A file with no
// template actions comes back unchanged with rendered=false — and nil data
// is accepted, rendering with a zero AtlasTemplateData.
func ExampleRenderAtlasTemplateSQL() {
	fsys := fstest.MapFS{
		"20240101000000_seed.sql": {Data: []byte(
			"INSERT INTO settings (env) VALUES ('{{ .Env }}');\n{{ template \"shared/users\" . }}\n")},
		"shared/users.sql": {Data: []byte(
			"INSERT INTO users (name) VALUES ('{{ .Env }}-admin');")},
		"20240102000000_plain.sql": {Data: []byte(
			"CREATE TABLE plain (id INTEGER PRIMARY KEY);\n")},
	}

	sql, rendered, err := migrationfile.RenderAtlasTemplateSQL(fsys, "20240101000000_seed.sql",
		migrationfile.AtlasTemplateData{Env: "prod"})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("rendered=%t\n%s", rendered, sql)

	sql, rendered, err = migrationfile.RenderAtlasTemplateSQL(fsys, "20240102000000_plain.sql", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("rendered=%t\n%s", rendered, sql)

	// Output:
	// rendered=true
	// INSERT INTO settings (env) VALUES ('prod');
	// INSERT INTO users (name) VALUES ('prod-admin');
	// rendered=false
	// CREATE TABLE plain (id INTEGER PRIMARY KEY);
}
