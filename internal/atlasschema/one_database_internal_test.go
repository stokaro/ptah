package atlasschema

// White-box testing required: refuseOutsideComparedDatabase decides from the two
// sides of a diff. The exported path that reaches it needs a live one-database
// connection before it gets there; the e2e tests drive that path, and these rows
// pin which side's URL the refusal names.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlassource"
)

// TestRefuseOutsideComparedDatabase names the side of a diff whose URL is
// limited, and leaves a comparison of two databases alone.
func TestRefuseOutsideComparedDatabase(t *testing.T) {
	database := atlassource.State{Kind: atlassource.KindDatabase, DefaultSchema: "app"}
	document := atlassource.State{Kind: atlassource.KindLocalFile, Schema: &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "u", Schema: "other"}},
	}}
	rows := []struct {
		name     string
		from, to atlassource.State
		wantErr  string
	}{
		{name: "one database to a document", from: database, to: document,
			wantErr: `table "other.u" is in database "other", but --from is limited to database "app"; .*`},
		{name: "a document to one database", from: document, to: database,
			wantErr: `table "other.u" is in database "other", but --to is limited to database "app"; .*`},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(refuseOutsideComparedDatabase(platform.MySQL, row.from, row.to), qt.ErrorMatches, row.wantErr)
		})
	}
	t.Run("two databases", func(t *testing.T) {
		c := qt.New(t)
		other := atlassource.State{Kind: atlassource.KindDatabase, DefaultSchema: "app2", Schema: document.Schema}

		c.Assert(refuseOutsideComparedDatabase(platform.MySQL, database, other), qt.IsNil)
	})
}
