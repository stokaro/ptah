package postgres_test

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
)

// supabaseArtifacts are database-scoped objects the Supabase image ships: an
// event trigger no extension owns, and an empty publication.
var supabaseArtifacts = []dbreset.Object{
	{Kind: "event trigger", Name: "pgrst_ddl_watch"},
	{Kind: "publication", Name: "supabase_realtime"},
}

// heldArtifacts answers the read of the database-scoped artifacts with each
// object, as the catalog query selects them.
func heldArtifacts(objects ...dbreset.Object) [][]driver.Value {
	rows := make([][]driver.Value, 0, len(objects))
	for _, object := range objects {
		rows = append(rows, []driver.Value{object.Kind, object.Name})
	}
	return rows
}

// TestDropDatabaseRealmKeeping_HappyPath cleans a realm that holds the
// database-scoped objects the claim recorded. They are the dev database's
// environment, so the cleanup leaves them, checks they are still there, and
// commits.
func TestDropDatabaseRealmKeeping_HappyPath(t *testing.T) {
	t.Run("the artifacts the claim recorded", func(t *testing.T) {
		c := qt.New(t)
		queryHandler := newPostgresRealmMetadataQuery()
		queryHandler.databaseArtifacts = heldArtifacts(supabaseArtifacts...)
		db := dbtest.OpenWithExec(t, queryHandler.query, nil)

		err := postgres.NewPostgreSQLWriter(db.SQL, "public").DropDatabaseRealmKeeping(t.Context(),
			dbreset.Kept{Artifacts: supabaseArtifacts})

		c.Assert(err, qt.IsNil)
		c.Assert(db.CommitCount(), qt.Equals, 1)
		c.Assert(db.RollbackCount(), qt.Equals, 0)
	})
}

// TestDropDatabaseRealmKeeping_FailurePath is what a kept artifact does not
// cover. An artifact the claim did not record is the run's, and the cleanup
// refuses it before it changes anything. A recorded artifact that is gone was
// dropped by the run, and the cleanup refuses to commit without it.
func TestDropDatabaseRealmKeeping_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		held []dbreset.Object
		kept []dbreset.Object
		want string
		// exec is how many statements ran before the refusal, all inside the
		// transaction the refusal rolls back: none for an artifact refused
		// before the cleanup starts, every drop and restore for one found gone
		// after it.
		exec int
	}{
		{
			name: "an event trigger the run created",
			held: append([]dbreset.Object{{Kind: "event trigger", Name: "audit_ddl"}}, supabaseArtifacts...),
			kept: supabaseArtifacts,
			want: `refusing to clean PostgreSQL database realm with unsupported database-scoped event trigger "audit_ddl"`,
		},
		{
			name: "a recorded publication the run dropped",
			held: supabaseArtifacts[:1],
			kept: supabaseArtifacts,
			want: `PostgreSQL database realm cleanup found the dev database's publication "supabase_realtime" gone; ` +
				`it was there when the run took the database, and the cleanup cannot create it again`,
			exec: 15,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			queryHandler := newPostgresRealmMetadataQuery()
			queryHandler.databaseArtifacts = heldArtifacts(test.held...)
			db := dbtest.OpenWithExec(t, queryHandler.query, nil)

			err := postgres.NewPostgreSQLWriter(db.SQL, "public").DropDatabaseRealmKeeping(t.Context(),
				dbreset.Kept{Artifacts: test.kept})

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(db.ExecCount(), qt.Equals, test.exec)
			c.Assert(db.CommitCount(), qt.Equals, 0)
			c.Assert(db.RollbackCount(), qt.Equals, 1)
		})
	}
}
