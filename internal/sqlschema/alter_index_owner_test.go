package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/definedtypetest"
	"ptah.run/internal/sqlschema"
)

// indexedWithType is a document declaring table c with index ix, beside an
// owner's object that defines the type pair of kind.
func indexedWithType(c *qt.C, kind objectidentity.Kind) schemamodel.Database {
	c.Helper()
	earlier, _, err := sqlschema.Read([]byte("CREATE TABLE c (id int PRIMARY KEY, b int);\nCREATE INDEX ix ON c (b);"), "postgres")
	c.Assert(err, qt.IsNil)
	earlier.FeatureObjects = must.Must(schemaext.NewObjects(definedtypetest.Object(kind, "", "pair", "int")))
	return earlier
}

// A composite type a feature owner's object defines holds its name among the
// relations, as a common composite type does (see "onto a composite type" in
// TestRead_AlterIndex_FailurePath): an index renamed onto it is refused.
func TestReadOnto_AlterIndexOntoAnOwnersType_FailurePath(t *testing.T) {
	c := qt.New(t)
	earlier := indexedWithType(c, objectidentity.KindComposite)

	_, _, err := sqlschema.ReadOnto([]byte("ALTER INDEX ix RENAME TO pair;"), "postgres", sqlschema.NewDocument(&earlier))

	c.Assert(err, qt.ErrorMatches, `ALTER INDEX ix RENAME TO pair: the schema already holds a relation named pair`)
}

// The control: a domain of the same name is a type and not a relation, so the
// rename onto it goes through.
func TestReadOnto_AlterIndexOntoAnOwnersType_HappyPath(t *testing.T) {
	c := qt.New(t)
	earlier := indexedWithType(c, objectidentity.KindDomain)

	_, _, err := sqlschema.ReadOnto([]byte("ALTER INDEX ix RENAME TO pair;"), "postgres", sqlschema.NewDocument(&earlier))

	c.Assert(err, qt.IsNil)
	c.Assert(indexNamesOf(earlier), qt.DeepEquals, []string{"pair"})
}
