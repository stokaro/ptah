package atlascompat_test

import (
	"fmt"
	"testing/fstest"

	"github.com/go-extras/go-kit/must"

	"ptah.run/atlascompat"
	"ptah.run/migration/migrationfile"
)

// ExampleParseAtlasHCL parses an Atlas schema HCL document into the schema
// model IR — the same *schemamodel.Database that Go annotations, YAML, and SQL
// produce, so everything downstream of the parse is format-blind. It is the
// front door for tooling that holds a schema authored for the Atlas CLI.
func ExampleParseAtlasHCL() {
	db := must.Must(atlascompat.ParseAtlasHCL([]byte(`
schema "main" {}

table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
  column "email" {
    null = false
    type = varchar(255)
  }
  primary_key {
    columns = [column.id]
  }
}
`), "schema.hcl"))

	for _, table := range db.Tables {
		fmt.Println("table:", table.Name)
	}
	for _, field := range db.Fields {
		fmt.Printf("column %s %s primary=%t\n", field.Name, field.Type, field.Primary)
	}

	// Output:
	// table: users
	// column id int primary=true
	// column email varchar(255) primary=false
}

// ExampleParseAtlasHCL_projectFile shows the refusal that keeps a project file
// from reading as an empty schema. An atlas.hcl carrying an env block is
// configuration, not schema; parsing it as one would yield an empty IR, which
// a caller diffing against a live database cannot tell apart from a request to
// drop everything. The refusal is the contract here — the wording of the
// message is not, so branch on the error rather than on its text.
func ExampleParseAtlasHCL_projectFile() {
	db, err := atlascompat.ParseAtlasHCL([]byte(`env "local" {
  url = "sqlite://local.db"
}
`), "atlas.hcl")

	fmt.Println("refused:", err != nil)
	fmt.Println("no schema returned:", db == nil)

	// Output:
	// refused: true
	// no schema returned: true
}

// ExampleParseSQL parses SQL DDL text into Ptah AST statements. The dialect
// selects dialect-specific grammar; the resulting nodes are what the renderer,
// diff, and planning layers consume.
func ExampleParseSQL() {
	list := must.Must(atlascompat.ParseSQL(`
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  email VARCHAR(255) NOT NULL
);
CREATE INDEX idx_users_email ON users (email);
`, atlascompat.ParseSQLOptions{Dialect: "postgres"}))

	for _, statement := range list.Statements {
		fmt.Printf("%T\n", statement)
	}

	// Output:
	// *ast.CreateTableNode
	// *ast.IndexNode
}

// ExampleComputeSum hashes a migration directory into an integrity file and
// round-trips it through ParseSum. Under DirFormatAtlas the chained hashing
// scheme matches the atlas.sum format, so the rendered bytes interoperate with
// Atlas migration-directory validation. The result is deterministic: entries
// are sorted by name and depend only on file contents.
func ExampleComputeSum() {
	migrations := fstest.MapFS{
		"20260721150000_init.sql": {
			Data: []byte("CREATE TABLE users (id int);\n"),
		},
		"20260721150500_add_email.sql": {
			Data: []byte("ALTER TABLE users ADD COLUMN email varchar(255);\n"),
		},
	}

	sum := must.Must(atlascompat.ComputeSum(migrations, migrationfile.DirFormatAtlas))
	for _, entry := range sum.Entries {
		fmt.Println(entry.Name)
	}

	parsed := must.Must(atlascompat.ParseSum(sum.Bytes()))
	fmt.Println("round trip preserves the directory hash:", parsed.DirHash == sum.DirHash)

	// Output:
	// 20260721150000_init.sql
	// 20260721150500_add_email.sql
	// round trip preserves the directory hash: true
}

// ExampleVerifySum detects drift: a migration edited after it was hashed and a
// new file the integrity file does not cover. The drift lands in the SumResult
// rather than in the error, so the caller chooses the exit code; Describe
// renders the changed/added/removed vocabulary the CLI prints.
func ExampleVerifySum() {
	sum := must.Must(atlascompat.ComputeSum(fstest.MapFS{
		"20260721150000_init.sql": {
			Data: []byte("CREATE TABLE users (id int);\n"),
		},
	}, migrationfile.DirFormatAtlas))

	drifted := fstest.MapFS{
		"20260721150000_init.sql": {
			Data: []byte("CREATE TABLE users (id bigint);\n"),
		},
		"20260721150500_add_email.sql": {
			Data: []byte("ALTER TABLE users ADD COLUMN email varchar(255);\n"),
		},
		atlascompat.AtlasSumFileName: {
			Data: sum.Bytes(),
		},
	}

	// The two channels are separate on purpose: an error means the directory
	// could not be verified at all — no integrity file, or an unparsable one —
	// while drift arrives in the result.
	result, err := atlascompat.VerifySum(drifted, migrationfile.DirFormatAtlas)
	if err != nil {
		fmt.Println("cannot verify:", err)
		return
	}

	fmt.Println("ok:", result.OK())
	fmt.Println("changed:", result.Changed)
	fmt.Println("added:", result.Added)
	fmt.Println(result.Describe())

	// Output:
	// ok: false
	// changed: [20260721150000_init.sql]
	// added: [20260721150500_add_email.sql]
	// migration directory does not match atlas.sum:
	//   changed: 20260721150000_init.sql
	//   added (not in atlas.sum): 20260721150500_add_email.sql
}
