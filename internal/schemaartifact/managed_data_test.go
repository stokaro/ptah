package schemaartifact_test

import (
	"context"
	"io/fs"
	"testing"

	qt "github.com/frankban/quicktest"
	"oras.land/oras-go/v2/content/memory"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/ociartifact"
	"ptah.run/internal/schemaartifact"
)

// TestPushToPullFrom_CarriesDeclaredRows is the property the managed-data layer
// exists for: what the author declared is what the consumer reads, including
// the scalars a YAML resolver would have rewritten on the way.
func TestPushToPullFrom_CarriesDeclaredRows(t *testing.T) {
	c := qt.New(t)
	store := memory.New()
	db := usersDatabase()
	db.ManagedData = []schemamodel.ManagedData{{
		Table: "users",
		Keys:  []string{"id"},
		File:  "users.yaml",
		Rows: []schemamodel.ManagedRow{
			{
				"id":         {Tag: "int", Text: "007"},
				"email":      {Tag: "str", Text: "first@example.com"},
				"rate":       {Tag: "float", Text: "1.0"},
				"created_at": {Tag: "timestamp", Text: "2020-01-01"},
				"active":     {Tag: "bool", Text: "true"},
				"retired_at": {Tag: "null", Null: true},
			},
			{
				"id":    {Tag: "str", Text: "007"},
				"email": {Tag: "str", Text: "second@example.com"},
			},
		},
	}}

	_, err := schemaartifact.PushTo(context.Background(), store, db, schemaartifact.PushOptions{Tags: []string{"stable"}})
	c.Assert(err, qt.IsNil)

	pulled, err := schemaartifact.PullFrom(context.Background(), store, "stable")
	c.Assert(err, qt.IsNil)
	c.Assert(pulled.Database.ManagedData, qt.HasLen, 1)
	declaration := pulled.Database.ManagedData[0]
	c.Assert(declaration.Table, qt.Equals, "users")
	c.Assert(declaration.Keys, qt.DeepEquals, []string{"id"})
	c.Assert(declaration.Rows, qt.HasLen, 2)
	// The two rows declare the same key text under different tags, which is
	// what separates the integer 7 from the string "007" in the same column.
	c.Assert(declaration.Rows[0]["id"], qt.Equals, schemamodel.ManagedValue{Tag: "int", Text: "007"})
	c.Assert(declaration.Rows[1]["id"], qt.Equals, schemamodel.ManagedValue{Tag: "str", Text: "007"})
	c.Assert(declaration.Rows[0]["rate"], qt.Equals, schemamodel.ManagedValue{Tag: "float", Text: "1.0"})
	c.Assert(declaration.Rows[0]["created_at"], qt.Equals, schemamodel.ManagedValue{Tag: "timestamp", Text: "2020-01-01"})
	c.Assert(declaration.Rows[0]["retired_at"], qt.Equals, schemamodel.ManagedValue{Tag: "null", Null: true})
	_, declared := declaration.Rows[1]["retired_at"]
	c.Assert(declared, qt.IsFalse, qt.Commentf("a column the row never names is absent, not null"))
}

// TestCaptureRendersTheSameLayerTwice pins the canonical form. A layer whose
// bytes depend on map iteration would give one declaration two digests.
func TestCaptureRendersTheSameLayerTwice(t *testing.T) {
	c := qt.New(t)

	first, err := schemaartifact.Capture(managedUsersDatabase())
	c.Assert(err, qt.IsNil)
	second, err := schemaartifact.Capture(managedUsersDatabase())
	c.Assert(err, qt.IsNil)

	firstLayer, err := fs.ReadFile(first, schemaartifact.ManagedDataFileName)
	c.Assert(err, qt.IsNil)
	secondLayer, err := fs.ReadFile(second, schemaartifact.ManagedDataFileName)
	c.Assert(err, qt.IsNil)
	c.Assert(string(firstLayer), qt.Equals, string(secondLayer))
	c.Assert(string(firstLayer), qt.Equals,
		`{"sets":[{"table":"users","keys":["id"],"columns":["email","id"],`+
			`"rows":[{"email":{"tag":"str","text":"first@example.com"},"id":{"tag":"int","text":"1"}}]}]}`+"\n")
}

// TestCaptureRefusesUnusableRows covers the refusals that belong before a
// database is touched rather than after: a key that does not identify, and two
// rows that claim one identity.
func TestCaptureRefusesUnusableRows(t *testing.T) {
	tests := []struct {
		name    string
		rows    []schemamodel.ManagedRow
		keys    []string
		message string
	}{
		{
			name:    "duplicate key",
			keys:    []string{"id"},
			rows:    []schemamodel.ManagedRow{{"id": {Tag: "int", Text: "1"}}, {"id": {Tag: "int", Text: "1"}}},
			message: `managed data for table users declares rows 1 and 2 with the same key`,
		},
		{
			name:    "missing key column",
			keys:    []string{"id"},
			rows:    []schemamodel.ManagedRow{{"email": {Tag: "str", Text: "first@example.com"}}},
			message: `managed data for table users, row 1 does not declare key column "id"`,
		},
		{
			name:    "null key column",
			keys:    []string{"id"},
			rows:    []schemamodel.ManagedRow{{"id": {Tag: "null", Null: true}}},
			message: `managed data for table users, row 1 declares key column "id" as null`,
		},
		{
			name:    "no key column",
			keys:    nil,
			rows:    []schemamodel.ManagedRow{{"id": {Tag: "int", Text: "1"}}},
			message: `managed data for table users declares no key column`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := usersDatabase()
			db.ManagedData = []schemamodel.ManagedData{{
				Table: "users", Keys: test.keys, File: "users.yaml", Rows: test.rows,
			}}

			snapshot, err := schemaartifact.Capture(db)

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(snapshot, qt.IsNil)
		})
	}
}

// TestPullFromRefusesAReaderThatCannotSeeTheRows is the compatibility boundary.
// A reader that admits only the schema layer must refuse the whole artifact,
// because reading the schema and ignoring the rows deploys a database the
// author did not declare and reports success.
func TestPullFromRefusesAReaderThatCannotSeeTheRows(t *testing.T) {
	c := qt.New(t)
	store := memory.New()
	_, err := schemaartifact.PushTo(context.Background(), store, managedUsersDatabase(),
		schemaartifact.PushOptions{Tags: []string{"stable"}})
	c.Assert(err, qt.IsNil)

	_, err = ociartifact.PullFrom(context.Background(), store, "stable", ociartifact.PullOptions{
		ExpectedArtifactTypes: []string{ociartifact.SchemaArtifactType},
		LayerMediaType:        schemaartifact.LayerMediaType,
	})

	c.Assert(err, qt.ErrorIs, ociartifact.ErrUnexpectedArtifactType)
	c.Assert(err, qt.ErrorMatches, `.*managed-data\.json.*`)
}

func managedUsersDatabase() *schemamodel.Database {
	db := usersDatabase()
	db.ManagedData = []schemamodel.ManagedData{{
		Table: "users",
		Keys:  []string{"id"},
		File:  "users.yaml",
		Rows: []schemamodel.ManagedRow{{
			"id":    {Tag: "int", Text: "1"},
			"email": {Tag: "str", Text: "first@example.com"},
		}},
	}}
	return db
}
