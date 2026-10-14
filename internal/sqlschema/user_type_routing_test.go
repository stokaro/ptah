package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/feature/oratype"
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

// TestReadWith_AnOracleDocumentHandsItsUserTypesToTheOracleOwner is the other
// half: an Oracle document's domains and composite types are Oracle's, which
// the Oracle type owner reads, and the common model holds none. The object
// type reads the same in the shared spelling and in the one Oracle requires.
func TestReadWith_AnOracleDocumentHandsItsUserTypesToTheOracleOwner(t *testing.T) {
	for _, test := range []struct{ name, statements string }{
		{name: "shared spelling", statements: userTypeStatements},
		{name: "Oracle spelling", statements: `CREATE DOMAIN email AS VARCHAR(320) NOT NULL;
CREATE OR REPLACE TYPE address AS OBJECT (street VARCHAR(80));
`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, _, err := sqlschema.ReadWith(c.Context(), []byte(test.statements), "oracle", nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})

			c.Assert(err, qt.IsNil)
			c.Assert(db.Domains, qt.HasLen, 0)
			c.Assert(db.CompositeTypes, qt.HasLen, 0)
			objects := must.Must(db.FeatureObjects.All())
			c.Assert(objects, qt.HasLen, 2)
			c.Assert(objects[0].Ref, qt.DeepEquals, oratype.Ref(oratype.DomainKind, "", "email"))
			c.Assert(objects[0].Value, qt.DeepEquals, &oratype.DesiredDomain{BaseType: "VARCHAR(320)", NotNull: true})
			c.Assert(objects[1].Ref, qt.DeepEquals, oratype.Ref(oratype.ObjectTypeKind, "", "address"))
			c.Assert(objects[1].Value, qt.DeepEquals, &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "street", Type: "VARCHAR(80)"}}})
		})
	}
}
