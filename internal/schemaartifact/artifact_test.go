package schemaartifact_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"oras.land/oras-go/v2/content/memory"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemaartifact"
)

func TestPushToPullFrom_RoundTrip(t *testing.T) {
	c := qt.New(t)
	store := memory.New()
	db := usersDatabase()
	db.Sequences = []goschema.Sequence{{Name: "users_id_seq"}}

	pushed, err := schemaartifact.PushTo(context.Background(), store, db, schemaartifact.PushOptions{
		Tags: []string{"stable"},
		Now: func() time.Time {
			return time.Date(2026, time.July, 28, 9, 10, 11, 0, time.UTC)
		},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(pushed.Version, qt.Matches, `v20260728091011-[A-Z2-7]+`)
	c.Assert(pushed.Tags, qt.DeepEquals, []string{pushed.Version, "stable", "latest"})

	pulled, err := schemaartifact.PullFrom(context.Background(), store, "latest")
	c.Assert(err, qt.IsNil)
	c.Assert(pulled.Database.Tables, qt.HasLen, 1)
	c.Assert(pulled.Database.Tables[0].Name, qt.Equals, "users")
	c.Assert(pulled.Database.Sequences, qt.HasLen, 1)
	c.Assert(pulled.Database.Sequences[0].Name, qt.Equals, "users_id_seq")
	c.Assert(pulled.Database.Fields, qt.HasLen, 2)
	c.Assert(
		[]string{pulled.Database.Fields[0].Name, pulled.Database.Fields[1].Name},
		qt.DeepEquals,
		[]string{"email", "id"},
	)
	data, err := fs.ReadFile(pulled.FileSystem, schemaartifact.FileName)
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Contains, `table "users"`)
}

func TestCapture_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("nil database", func(c *qt.C) {
		snapshot, err := schemaartifact.Capture(nil)
		c.Assert(err, qt.ErrorMatches, "schema database is required")
		c.Assert(snapshot, qt.IsNil)
	})

	c.Run("managed data", func(c *qt.C) {
		db := usersDatabase()
		db.ManagedData = []goschema.ManagedData{{Table: "users"}}
		snapshot, err := schemaartifact.Capture(db)
		c.Assert(err, qt.ErrorMatches, "schema artifact cannot represent managed data without loss")
		c.Assert(snapshot, qt.IsNil)
	})

	c.Run("role password", func(c *qt.C) {
		db := usersDatabase()
		db.Roles = []goschema.Role{{Name: "app_user", Password: "secret"}}
		snapshot, err := schemaartifact.Capture(db)
		c.Assert(err, qt.ErrorMatches, `schema artifact cannot contain password for role "app_user"`)
		c.Assert(snapshot, qt.IsNil)
	})

	c.Run("lossy HCL export", func(c *qt.C) {
		db := usersDatabase()
		db.Indexes = []goschema.Index{{Name: "missing_idx", TableName: "missing"}}
		snapshot, err := schemaartifact.Capture(db)
		c.Assert(err, qt.ErrorMatches, "(?s).*schema artifact cannot be rendered without loss:.*index missing_idx.*")
		c.Assert(snapshot, qt.IsNil)
	})
}

func TestCapturePreservesSystemExtensionPlacementWithoutDeclaringIt(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{Extensions: []goschema.Extension{{
		Name: "plpgsql", Schema: "pg_catalog", Version: "1.0", IfNotExists: true,
	}}}

	snapshot, err := schemaartifact.Capture(db)
	c.Assert(err, qt.IsNil)
	data, err := fs.ReadFile(snapshot, schemaartifact.FileName)
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Not(qt.Contains), `schema "pg_catalog"`)
	c.Assert(string(data), qt.Not(qt.Contains), `schema = schema.pg_catalog`)
	c.Assert(string(data), qt.Contains, `schema = "pg_catalog"`)
}

func TestPullToFile_RejectsExistingOutputBeforeNetwork(t *testing.T) {
	c := qt.New(t)
	output := filepath.Join(t.TempDir(), "schema.hcl")
	err := os.WriteFile(output, []byte("existing"), 0o600)
	c.Assert(err, qt.IsNil)

	_, _, err = schemaartifact.PullToFile(
		context.Background(),
		"oci://registry.invalid/acme/schema:latest",
		output,
		false,
	)

	c.Assert(err, qt.ErrorMatches, "schema artifact output already exists: .*")
}

func usersDatabase() *goschema.Database {
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", FieldName: "ID", Name: "id", Type: "bigint", Primary: true},
			{StructName: "User", FieldName: "Email", Name: "email", Type: "text", Unique: true},
		},
	}
	goschema.Finalize(db)
	return db
}
