package atlascompat_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stokaro/ptah/atlascompat"
	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestParseAtlasHCL(t *testing.T) {
	db, err := atlascompat.ParseAtlasHCL([]byte(`
schema "main" {}

table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`), "schema.hcl")
	if err != nil {
		t.Fatal(err)
	}
	if got := len(db.Tables); got != 1 {
		t.Fatalf("tables = %d", got)
	}
	if got := len(db.Fields); got != 1 {
		t.Fatalf("fields = %d", got)
	}
}

func TestParseSQL(t *testing.T) {
	list, err := atlascompat.ParseSQL("CREATE TABLE users (id int PRIMARY KEY);", atlascompat.ParseSQLOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(list.Statements); got != 1 {
		t.Fatalf("statements = %d", got)
	}
	if _, ok := list.Statements[0].(*ast.CreateTableNode); !ok {
		t.Fatalf("statement = %T, want *ast.CreateTableNode", list.Statements[0])
	}
}

func TestSchemaToAST(t *testing.T) {
	list := atlascompat.SchemaToAST(goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{{
			StructName: "User",
			FieldName:  "ID",
			Name:       "id",
			Type:       "INTEGER",
			Primary:    true,
		}},
	}, "sqlite")

	if got := len(list.Statements); got != 1 {
		t.Fatalf("statements = %d", got)
	}
	if _, ok := list.Statements[0].(*ast.CreateTableNode); !ok {
		t.Fatalf("statement = %T, want *ast.CreateTableNode", list.Statements[0])
	}
}

func TestDBSchemaToGoSchema(t *testing.T) {
	db := atlascompat.DBSchemaToGoSchema(&dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{{
			Name: "users",
			Columns: []dbschematypes.DBColumn{{
				Name:         "id",
				DataType:     "integer",
				IsNullable:   "NO",
				IsPrimaryKey: true,
			}},
		}},
	})

	if got := len(db.Tables); got != 1 {
		t.Fatalf("tables = %d", got)
	}
	if got := len(db.Fields); got != 1 {
		t.Fatalf("fields = %d", got)
	}
	if !db.Fields[0].Primary {
		t.Fatalf("field primary = false")
	}
}

func TestSumHelpers(t *testing.T) {
	fsys := fstest.MapFS{
		"20260721150000_init.sql": {
			Data: []byte("CREATE TABLE users (id int);\n"),
		},
	}
	sum, err := atlascompat.ComputeSum(fsys, migrator.MigrationDirFormatAtlas)
	if err != nil {
		t.Fatal(err)
	}
	if sum.DirHash == "" {
		t.Fatal("missing directory hash")
	}
	if got := len(sum.Entries); got != 1 {
		t.Fatalf("entries = %d", got)
	}

	parsed, err := atlascompat.ParseSum(sum.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.DirHash != sum.DirHash {
		t.Fatalf("parsed dir hash = %q, want %q", parsed.DirHash, sum.DirHash)
	}

	withSum := fstest.MapFS{
		"20260721150000_init.sql": {
			Data: []byte("CREATE TABLE users (id int);\n"),
		},
		atlascompat.AtlasSumFileName: {
			Data: sum.Bytes(),
		},
	}
	result, err := atlascompat.VerifySum(withSum, migrator.MigrationDirFormatAtlas)
	if err != nil {
		t.Fatal(err)
	}
	if !result.OK() {
		t.Fatalf("verify result = %s", result.Describe())
	}
}

func TestSumFileNameConstantsMatchFormats(t *testing.T) {
	ptahName, err := atlascompat.SumFileNameForFormat(migrator.MigrationDirFormatPtah)
	if err != nil {
		t.Fatal(err)
	}
	if ptahName != atlascompat.PtahSumFileName {
		t.Fatalf("ptah sum file name = %q, want %q", ptahName, atlascompat.PtahSumFileName)
	}

	atlasName, err := atlascompat.SumFileNameForFormat(migrator.MigrationDirFormatAtlas)
	if err != nil {
		t.Fatal(err)
	}
	if atlasName != atlascompat.AtlasSumFileName {
		t.Fatalf("atlas sum file name = %q, want %q", atlasName, atlascompat.AtlasSumFileName)
	}
}

func TestWriteSumBytes(t *testing.T) {
	dir := t.TempDir()
	sum := &atlascompat.SumFile{
		DirHash: "h1:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=",
	}
	if err := atlascompat.WriteSumBytes(dir, atlascompat.AtlasSumFileName, sum); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, atlascompat.AtlasSumFileName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(data), sum.DirHash+"\n") {
		t.Fatalf("written sum = %q", data)
	}
}
