package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/feature/pgtype"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
)

// userTypeStatements declare a domain and a composite type in the syntax
// PostgreSQL and Oracle share.
const userTypeStatements = `CREATE DOMAIN email AS VARCHAR(320) NOT NULL;
CREATE TYPE address AS (street VARCHAR(80));
`

// TestReadWith_UserTypesReachTheTypeOwnerWithoutAnotherTarget pins that a
// document of a PostgreSQL-family target, or of no target, hands its domains
// and composite types to the PostgreSQL type owner, and keeps none on the
// common model.
func TestReadWith_UserTypesReachTheTypeOwnerWithoutAnotherTarget(t *testing.T) {
	for _, dialect := range []string{"", "postgres", "yugabytedb"} {
		t.Run("dialect "+dialect, func(t *testing.T) {
			c := qt.New(t)

			db, _, err := sqlschema.ReadWith(c.Context(), []byte(userTypeStatements), dialect, nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})

			c.Assert(err, qt.IsNil)
			c.Assert(db.Domains, qt.HasLen, 0)
			c.Assert(db.CompositeTypes, qt.HasLen, 0)
			objects := must.Must(db.FeatureObjects.All())
			c.Assert(objects, qt.HasLen, 2)
			c.Assert(objects[0].Ref, qt.DeepEquals, pgtype.Ref(pgtype.CompositeKind, "", "address"))
			c.Assert(objects[1].Ref, qt.DeepEquals, pgtype.Ref(pgtype.DomainKind, "", "email"))
		})
	}
}

// TestReadWith_AnOracleDocumentKeepsItsUserTypesOnTheCommonModel is the other
// half: an Oracle document's domains and composite types are Oracle's, which
// the common model holds, and the PostgreSQL type owner takes neither.
func TestReadWith_AnOracleDocumentKeepsItsUserTypesOnTheCommonModel(t *testing.T) {
	c := qt.New(t)

	db, _, err := sqlschema.ReadWith(c.Context(), []byte(userTypeStatements), "oracle", nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})

	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(db.FeatureObjects.All()), qt.HasLen, 0)
	c.Assert(db.Domains, qt.HasLen, 1)
	c.Assert(db.Domains[0].Name, qt.Equals, "email")
	c.Assert(db.Domains[0].NotNull, qt.IsTrue)
	c.Assert(db.CompositeTypes, qt.HasLen, 1)
	c.Assert(db.CompositeTypes[0].Name, qt.Equals, "address")
}
