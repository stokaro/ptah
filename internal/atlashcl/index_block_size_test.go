package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// TestParse_ClaimsTheBlockSizeOfEveryIndex is the MySQL owner's claim over
// an HCL document: the format has no spelling for an index's block size, so
// an index the document declares has none, and applying the document removes
// a hint the table holds.
func TestParse_ClaimsTheBlockSizeOfEveryIndex(t *testing.T) {
	c := qt.New(t)
	source := "table \"t\" {\n  column \"id\" {\n    type = int\n  }\n  index \"k\" {\n    columns = [column.id]\n  }\n}\n"

	db, err := parseWithOwners([]byte(source), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureCoverage.Lookup(mysqlschema.IndexBlockSizeKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
}
