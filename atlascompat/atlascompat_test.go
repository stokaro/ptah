package atlascompat_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/atlascompat"
	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestParseAtlasHCL(t *testing.T) {
	c := qt.New(t)
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

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Fields, qt.HasLen, 1)
}

func TestParseSQL(t *testing.T) {
	c := qt.New(t)
	list, err := atlascompat.ParseSQL("CREATE TABLE users (id int PRIMARY KEY);", atlascompat.ParseSQLOptions{})

	c.Assert(err, qt.IsNil)
	c.Assert(list.Statements, qt.HasLen, 1)
	_, ok := list.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
}

func TestSchemaToAST(t *testing.T) {
	c := qt.New(t)
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

	c.Assert(list.Statements, qt.HasLen, 1)
	_, ok := list.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
}

func TestDBSchemaToGoSchema(t *testing.T) {
	c := qt.New(t)
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

	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].Primary, qt.IsTrue)
}

func TestSumHelpers(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"20260721150000_init.sql": {
			Data: []byte("CREATE TABLE users (id int);\n"),
		},
	}
	sum, err := atlascompat.ComputeSum(fsys, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(sum.DirHash, qt.Not(qt.Equals), "")
	c.Assert(sum.Entries, qt.HasLen, 1)

	parsed, err := atlascompat.ParseSum(sum.Bytes())
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.DirHash, qt.Equals, sum.DirHash)

	withSum := fstest.MapFS{
		"20260721150000_init.sql": {
			Data: []byte("CREATE TABLE users (id int);\n"),
		},
		atlascompat.AtlasSumFileName: {
			Data: sum.Bytes(),
		},
	}
	result, err := atlascompat.VerifySum(withSum, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(result.OK(), qt.IsTrue, qt.Commentf("verify result = %s", result.Describe()))
}

func TestSumFileNameConstantsMatchFormats(t *testing.T) {
	c := qt.New(t)
	ptahName, err := atlascompat.SumFileNameForFormat(migrator.MigrationDirFormatPtah)
	c.Assert(err, qt.IsNil)
	c.Assert(ptahName, qt.Equals, atlascompat.PtahSumFileName)

	atlasName, err := atlascompat.SumFileNameForFormat(migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(atlasName, qt.Equals, atlascompat.AtlasSumFileName)
}

func TestWriteSumBytes(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	sum := &atlascompat.SumFile{
		DirHash: "h1:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=",
	}
	err := atlascompat.WriteSumBytes(dir, atlascompat.AtlasSumFileName, sum)
	c.Assert(err, qt.IsNil)
	data, err := os.ReadFile(filepath.Join(dir, atlascompat.AtlasSumFileName))
	c.Assert(err, qt.IsNil)
	c.Assert(strings.HasPrefix(string(data), sum.DirHash+"\n"), qt.IsTrue)
}
