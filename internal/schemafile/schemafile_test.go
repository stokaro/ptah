package schemafile_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/convert/goschematodb"
	"go.5x5.cz/ptah/internal/schemafile"
)

func TestLoad_SQLFile(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);
CREATE INDEX idx_users_name ON users (name);
`), 0o600), qt.IsNil)

	db, err := schemafile.Load("file://"+path, schemafile.Options{Dialect: platform.SQLite})

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "users")
	c.Assert(db.Fields, qt.HasLen, 2)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Name, qt.Equals, "idx_users_name")
}

func TestLoadAll_HCLPreservesExtendedSchemaObjects(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "schema.hcl")
	db := &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "app"}},
		Tables:  []schemamodel.Table{{StructName: "User", Name: "users", Schema: "app"}},
		Fields:  []schemamodel.Field{{StructName: "User", Name: "id", Type: "bigint"}},
		Extensions: []schemamodel.Extension{{
			Name:   "pgcrypto",
			Schema: "app",
		}},
		Sequences: []schemamodel.Sequence{{
			Name:   "order_seq",
			Schema: "app",
		}},
		Domains: []schemamodel.Domain{{
			Name:     "email",
			Schema:   "app",
			BaseType: "text",
		}},
		CompositeTypes: []schemamodel.CompositeType{{
			Name:   "address",
			Schema: "app",
			Fields: []schemamodel.CompositeField{{Name: "city", Type: "text"}},
		}},
		Ranges: []schemamodel.Range{{
			Name:    "price_range",
			Schema:  "app",
			Subtype: "numeric",
		}},
		ManagedData: []schemamodel.ManagedData{{
			Table:  "users",
			Schema: "app",
			Keys:   []string{"id"},
			File:   "users.yaml",
		}},
	}
	rendered, err := atlashclrender.Render(db)
	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Diagnostics, qt.HasLen, 0)
	c.Assert(os.WriteFile(path, rendered.Data, 0o600), qt.IsNil)

	got, err := schemafile.LoadAll([]string{path}, schemafile.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(got.Schemas, qt.HasLen, 1)
	c.Assert(got.Sequences, qt.HasLen, 1)
	c.Assert(got.Domains, qt.HasLen, 1)
	c.Assert(got.CompositeTypes, qt.HasLen, 1)
	c.Assert(got.Ranges, qt.HasLen, 1)
	c.Assert(got.ManagedData, qt.HasLen, 1)
	resolvedDir, err := filepath.EvalSymlinks(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(got.ManagedData[0].SourceDir, qt.Equals, resolvedDir)
}

func TestToDBSchema_PreservesTableAndColumnMetadata(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`
table "users" {
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
  index "idx_users_email" {
    unique = true
    columns = [column.email]
  }
}
`), 0o600), qt.IsNil)

	db, err := schemafile.Load(path, schemafile.Options{})
	c.Assert(err, qt.IsNil)

	got := goschematodb.ToDBSchema(db, platform.Postgres)

	c.Assert(got.Tables, qt.HasLen, 1)
	c.Assert(got.Tables[0].Name, qt.Equals, "users")
	c.Assert(got.Tables[0].Columns, qt.HasLen, 2)
	c.Assert(got.Tables[0].Columns[0].Name, qt.Equals, "id")
	c.Assert(got.Tables[0].Columns[0].IsPrimaryKey, qt.IsTrue)
	c.Assert(got.Tables[0].Columns[1].Name, qt.Equals, "email")
	c.Assert(got.Tables[0].Columns[1].IsNullable, qt.Equals, "NO")
	c.Assert(got.Indexes, qt.HasLen, 1)
	c.Assert(got.Indexes[0].Name, qt.Equals, "idx_users_email")
	c.Assert(got.Indexes[0].IsUnique, qt.IsTrue)
}

func TestLocalFilePath_RejectsRemoteURL(t *testing.T) {
	c := qt.New(t)

	_, err := schemafile.LocalFilePath("postgres://localhost/db")

	c.Assert(err, qt.ErrorMatches, `only local file:// schema files are supported`)
}

// ignoredBlocksSchema is a schema file carrying the three top-level names the
// compat surface accepts and drops: two Atlas project-shaped blocks and one
// unmodeled object kind whose body resolves a declared schema.
const ignoredBlocksSchema = `schema "main" {
}

lock {
}

atlas {
}

wibble "do_thing" {
  schema = schema.main
}

table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`

// TestLoad_ReportsIgnoredTopLevelBlocks covers stokaro/ptah#1709.
//
// The compat surface accepts these three names and contributes nothing for
// them, matching a documented tolerance. Matching it is defensible; matching it
// in SILENCE is not -- a dropped top-level block is something the author
// believes is managed, and the same product already warns about an ignored
// atlas.hcl block through dbcli.ReportIgnoredAtlasConstructs.
//
// The third name was `procedure` when this test was written, and the example
// was chosen because a dropped procedure is a stored routine that silently
// stops being managed. That one is no longer dropped: a procedure is a modeled
// top-level block since stokaro/ptah#2209, so the row moved to `wibble`, which
// nothing models and nothing will.
//
// The load still succeeds and the modeled objects still arrive, which is the
// half a reporting change could break by turning a warning into a refusal.
func TestLoad_ReportsIgnoredTopLevelBlocks(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(ignoredBlocksSchema), 0o600), qt.IsNil)
	var reported bytes.Buffer

	db, err := schemafile.Load("file://"+path, schemafile.Options{
		Dialect:               platform.SQLite,
		IgnoreUnknownHCLNames: true,
		ReportIgnored:         &reported,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)

	lines := strings.Split(strings.TrimSpace(reported.String()), "\n")
	c.Assert(lines, qt.HasLen, 3)
	// The line number is what makes the warning actionable in a file that
	// declares dozens of blocks, so it is asserted rather than the name alone.
	c.Assert(lines[0], qt.Matches, `warning: schema file block "lock" at .*schema\.hcl:4 is ignored for Atlas compatibility and has no effect`)
	c.Assert(lines[1], qt.Matches, `warning: schema file block "atlas" at .*schema\.hcl:7 is ignored for Atlas compatibility and has no effect`)
	c.Assert(lines[2], qt.Matches, `warning: schema file block "wibble" at .*schema\.hcl:10 is ignored for Atlas compatibility and has no effect`)
}

// TestLoad_RefusesIgnoredTopLevelBlocksWithoutTolerance is the other half of
// the pair, and the one that keeps the two surfaces apart on purpose: native
// `ptah` reads the same file with the tolerance off, where an unmodeled name is
// a user error worth naming rather than a warning worth printing.
//
// Nothing is written to the reporter, because there is nothing ignored -- the
// first unmodeled name ends the load.
func TestLoad_RefusesIgnoredTopLevelBlocksWithoutTolerance(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(ignoredBlocksSchema), 0o600), qt.IsNil)
	var reported bytes.Buffer

	_, err := schemafile.Load("file://"+path, schemafile.Options{
		Dialect:       platform.SQLite,
		ReportIgnored: &reported,
	})

	c.Assert(err, qt.ErrorMatches, `(?s).*unsupported top-level block "lock".*`)
	c.Assert(reported.String(), qt.Equals, "")
}

// TestLoad_IgnoredBlocksStaySilentWithoutAReporter pins that the reporting is
// the caller's choice and not a behavior change forced on everyone: with no
// writer the load is exactly what it was before, which is what the community
// binary does.
func TestLoad_IgnoredBlocksStaySilentWithoutAReporter(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(ignoredBlocksSchema), 0o600), qt.IsNil)

	db, err := schemafile.Load("file://"+path, schemafile.Options{
		Dialect:               platform.SQLite,
		IgnoreUnknownHCLNames: true,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
}
