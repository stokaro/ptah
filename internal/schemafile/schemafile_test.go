package schemafile_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/schemafile"
	"go.5x5.cz/ptah/migration/schemadiff"
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
	db := &goschema.Database{
		Schemas: []goschema.Schema{{Name: "app"}},
		Tables:  []goschema.Table{{StructName: "User", Name: "users", Schema: "app"}},
		Fields:  []goschema.Field{{StructName: "User", Name: "id", Type: "bigint"}},
		Extensions: []goschema.Extension{{
			Name:   "pgcrypto",
			Schema: "app",
		}},
		Sequences: []goschema.Sequence{{
			Name:   "order_seq",
			Schema: "app",
		}},
		Domains: []goschema.Domain{{
			Name:     "email",
			Schema:   "app",
			BaseType: "text",
		}},
		CompositeTypes: []goschema.CompositeType{{
			Name:   "address",
			Schema: "app",
			Fields: []goschema.CompositeTypeField{{Name: "city", Type: "text"}},
		}},
		Ranges: []goschema.Range{{
			Name:    "price_range",
			Schema:  "app",
			Subtype: "numeric",
		}},
		ManagedData: []goschema.ManagedData{{
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

	got := schemafile.ToDBSchema(db, platform.Postgres)

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

func TestToDBSchema_PreservesExtendedSchemaObjects(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Schemas: []goschema.Schema{{Name: "app", Comment: "Application"}},
		Tables:  []goschema.Table{{StructName: "User", Name: "users", Schema: "app"}},
		Fields:  []goschema.Field{{StructName: "User", Name: "id", Type: "bigint"}},
		Extensions: []goschema.Extension{{
			Name:   "pgcrypto",
			Schema: "app",
		}},
		Sequences: []goschema.Sequence{{
			Name:      "order_seq",
			Schema:    "app",
			AsType:    "bigint",
			Increment: new(int64(2)),
		}},
		Domains: []goschema.Domain{{
			Name:     "email",
			Schema:   "app",
			BaseType: "text",
			NotNull:  true,
		}},
		CompositeTypes: []goschema.CompositeType{{
			Name:   "address",
			Schema: "app",
			Fields: []goschema.CompositeTypeField{{Name: "city", Type: "text"}},
		}},
		Ranges: []goschema.Range{{
			Name:    "price_range",
			Schema:  "app",
			Subtype: "numeric",
		}},
		RLSEnabledTables: []goschema.RLSEnabledTable{{
			StructName: "User",
			Table:      "app.users",
		}},
		Grants: []goschema.Grant{{
			Role:       "app_user",
			Privileges: []string{"USAGE"},
			OnSequence: "app.order_seq",
		}},
	}
	goschema.Finalize(db)

	got := schemafile.ToDBSchema(db, platform.Postgres)

	c.Assert(got.Schemas, qt.HasLen, 1)
	c.Assert(got.Schemas[0].Comment, qt.Equals, "Application")
	c.Assert(got.Tables[0].RLSEnabled, qt.IsTrue)
	c.Assert(got.Extensions, qt.DeepEquals, []dbschematypes.DBExtension{{Name: "pgcrypto", Schema: "app"}})
	c.Assert(got.Sequences, qt.HasLen, 1)
	c.Assert(got.Sequences[0].Increment, qt.DeepEquals, new(int64(2)))
	c.Assert(got.Domains, qt.HasLen, 1)
	c.Assert(got.Domains[0].NotNull, qt.IsTrue)
	c.Assert(got.Composites, qt.HasLen, 1)
	c.Assert(got.Composites[0].Fields, qt.DeepEquals, []dbschematypes.DBCompositeField{{
		Name: "city",
		Type: "text",
	}})
	c.Assert(got.Ranges, qt.HasLen, 1)
	c.Assert(got.Grants, qt.HasLen, 1)
	c.Assert(got.Grants[0].ObjectType, qt.Equals, "SEQUENCE")
	c.Assert(got.Grants[0].Schema, qt.Equals, "app")
	c.Assert(got.Grants[0].ObjectName, qt.Equals, "order_seq")
	diff := schemadiff.CompareWithDialect(db, got, platform.Postgres)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("extended HCL objects should not churn: %#v", diff))
}

func TestToDBSchema_FieldLevelConstraintsStayIdempotent(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Post", Name: "posts"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName: "User",
				Name:       "status",
				Type:       "TEXT",
				Check:      "status IN ('active', 'disabled')",
				CheckName:  "users_status_check",
			},
			{StructName: "Post", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName:     "Post",
				Name:           "user_id",
				Type:           "INTEGER",
				Foreign:        "users(id)",
				ForeignKeyName: "posts_user_id_fkey",
				OnDelete:       "CASCADE",
			},
		},
	}

	current := schemafile.ToDBSchema(db, platform.Postgres)
	diff := schemadiff.CompareWithDialect(db, current, platform.Postgres)

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("field-level CHECK/FK should not produce a file-to-file churn diff: %#v", diff))
}

func TestToDBSchema_ExplicitConstraintOverridesFieldLevelConstraintWithSameName(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{{
			StructName: "User",
			Name:       "status",
			Type:       "TEXT",
			Check:      "status <> ''",
			CheckName:  "users_status_check",
		}},
		Constraints: []goschema.Constraint{{
			StructName:      "User",
			Name:            "users_status_check",
			Type:            "CHECK",
			Table:           "users",
			CheckExpression: "status IN ('active', 'disabled')",
		}},
	}

	got := schemafile.ToDBSchema(db, platform.Postgres)

	c.Assert(got.Constraints, qt.HasLen, 1)
	c.Assert(*got.Constraints[0].CheckClause, qt.Equals, "status IN ('active', 'disabled')")
}

func TestToDBSchema_PreservesStructuralObjectIdentities(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Qualified", Schema: "tenant", Name: "data", PrimaryKey: []string{"qualified_id"}},
			{StructName: "Literal", Name: "tenant.data", PrimaryKey: []string{"literal_id"}},
		},
		Fields: []goschema.Field{
			{StructName: "Qualified", Name: "qualified_id", Type: "INTEGER"},
			{StructName: "Qualified", Name: "id", Type: "INTEGER"},
			{StructName: "Literal", Name: "literal_id", Type: "INTEGER"},
			{StructName: "Literal", Name: "id", Type: "INTEGER"},
		},
		Indexes: []goschema.Index{
			{StructName: "Literal", Name: "literal_lookup", TableName: `"tenant.data"`, Fields: []string{"id"}},
			{StructName: "Qualified", Name: "qualified_lookup", TableName: "tenant.data", Fields: []string{"id"}},
		},
		Constraints: []goschema.Constraint{{
			StructName:    "Literal",
			Name:          "literal_to_qualified_fk",
			Type:          "FOREIGN KEY",
			Table:         `"tenant.data"`,
			Columns:       []string{"id"},
			ForeignTable:  "tenant.data",
			ForeignColumn: "id",
		}},
		Views: []goschema.View{
			{Name: `"tenant.data"`, Body: "SELECT 'literal'"},
			{Name: "tenant.data", Body: "SELECT 'qualified'"},
		},
		MaterializedViews: []goschema.MaterializedView{
			{Name: `"tenant.data"`, Body: "SELECT 'literal'"},
			{Name: "tenant.data", Body: "SELECT 'qualified'"},
		},
		Triggers: []goschema.Trigger{
			{Name: "literal_trigger", Table: `"tenant.data"`, Timing: "AFTER", Event: "INSERT", Body: "SELECT 1"},
			{Name: "qualified_trigger", Table: "tenant.data", Timing: "AFTER", Event: "INSERT", Body: "SELECT 1"},
		},
		Grants: []goschema.Grant{
			{Role: "app", Privileges: []string{"SELECT"}, OnTable: `"tenant.data"`},
			{Role: "app", Privileges: []string{"SELECT"}, OnTable: "tenant.data"},
		},
	}
	goschema.Finalize(db)

	got := schemafile.ToDBSchema(db, platform.Postgres)

	c.Assert(got.Tables[0].Columns[0].IsPrimaryKey, qt.IsTrue)
	c.Assert(got.Tables[0].Columns[1].IsPrimaryKey, qt.IsFalse)
	c.Assert(got.Tables[1].Columns[0].IsPrimaryKey, qt.IsTrue)
	c.Assert(got.Tables[1].Columns[1].IsPrimaryKey, qt.IsFalse)
	c.Assert(got.Indexes[0].QualifiedTableName(), qt.Equals, `"tenant.data"`)
	c.Assert(got.Indexes[1].QualifiedTableName(), qt.Equals, "tenant.data")
	c.Assert(got.Constraints, qt.HasLen, 3)
	c.Assert(got.Constraints[2].QualifiedTableName(), qt.Equals, `"tenant.data"`)
	c.Assert(got.Constraints[2].ForeignSchema, qt.Equals, "tenant")
	c.Assert(*got.Constraints[2].ForeignTable, qt.Equals, "data")
	c.Assert(got.Views[0].QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(got.Views[1].QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(got.MatViews[0].QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(got.MatViews[1].QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(got.Triggers[0].QualifiedTable(), qt.Equals, `"tenant.data"`)
	c.Assert(got.Triggers[1].QualifiedTable(), qt.Equals, "tenant.data")
	c.Assert(got.Grants[0].QualifiedTarget(), qt.Equals, `"tenant.data"`)
	c.Assert(got.Grants[1].QualifiedTarget(), qt.Equals, "tenant.data")

	diff := schemadiff.CompareWithDialect(db, got, platform.Postgres)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

// TestToDBSchema_PreservesFunctionIdentities pins the local-schema conversion
// used as the current side of schema diff. A qualified function must keep its
// schema separate from its name, while a quoted literal dot remains part of
// the name rather than becoming a qualification boundary.
func TestToDBSchema_PreservesFunctionIdentities(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{Functions: []goschema.Function{
		{Name: `"tenant.data"`, Parameters: "literal INTEGER", Returns: "integer", Language: "sql", Body: "SELECT 1"},
		{Name: "tenant.data", Parameters: "value TEXT", Returns: "integer", Language: "sql", Body: "SELECT 2"},
	}}
	goschema.Finalize(db)

	got := schemafile.ToDBSchema(db, platform.Postgres)

	c.Assert(got.Functions, qt.HasLen, 2)
	c.Assert(got.Functions[0].Name, qt.Equals, "tenant.data")
	c.Assert(got.Functions[0].Schema, qt.Equals, "")
	c.Assert(got.Functions[0].QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(got.Functions[0].Parameters, qt.Equals, "literal integer")
	c.Assert(got.Functions[1].Name, qt.Equals, "data")
	c.Assert(got.Functions[1].Schema, qt.Equals, "tenant")
	c.Assert(got.Functions[1].QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(got.Functions[1].Parameters, qt.Equals, "value text")

	diff := schemadiff.CompareWithDialect(db, got, platform.Postgres)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
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

procedure "do_thing" {
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
// in SILENCE is not -- a `procedure` block dropped from a schema file is a
// stored routine the author believes is managed, and the same product already
// warns about an ignored atlas.hcl block through
// dbcli.ReportIgnoredAtlasConstructs.
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
	c.Assert(lines[2], qt.Matches, `warning: schema file block "procedure" at .*schema\.hcl:10 is ignored for Atlas compatibility and has no effect`)
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
