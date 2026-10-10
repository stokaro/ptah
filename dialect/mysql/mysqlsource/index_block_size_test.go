package mysqlsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/mysql/mysqlsource"
)

// parseIndexSource parses one struct declaring index k with the given
// attributes, with the MySQL owner selected.
func parseIndexSource(c *qt.C, attributes string) (schemamodel.Database, error) {
	c.Helper()
	owner := must.Must(annotation.NewSet(mysqlsource.Annotations()))
	source := "package entities\ntype T struct {\n//ptah:schema:index name=\"k\" fields=\"a\"" + attributes + "\nA int\n}"
	return goschema.ParseSource(owner, "entities.go", source)
}

// TestParseSource_IndexBlockSize_HappyPath reads the index directive's
// key_block_size as the owner's declaration, bound to no target, and claims
// the hint for every index of the source, so one written without it declares
// none.
func TestParseSource_IndexBlockSize_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, attributes string
		want             []schemaext.Kind
		size             uint64
	}{
		{"declared", ` key_block_size="8"`, []schemaext.Kind{mysqlschema.IndexBlockSizeKind}, 8},
		{"zero declares none", ` key_block_size="0"`, nil, 0},
		{"left out", ``, nil, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := parseIndexSource(c, test.attributes)
			c.Assert(err, qt.IsNil)
			c.Assert(db.Indexes, qt.HasLen, 1)
			c.Assert(db.Indexes[0].Facets.Kinds(), qt.DeepEquals, test.want)
			c.Assert(db.Indexes[0].Facets.TargetScope(mysqlschema.IndexBlockSizeKind), qt.HasLen, 0)
			size, _, err := mysqlschema.IndexBlockSize(db.Indexes[0].Facets)
			c.Assert(err, qt.IsNil)
			c.Assert(size, qt.Equals, test.size)
			c.Assert(db.FeatureCoverage.Lookup(mysqlschema.IndexBlockSizeKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
		})
	}
}

// TestParseSource_IndexBlockSize_FailurePath refuses a value that is not a
// whole number of kilobytes MySQL can store, as an invalid value of the
// attribute.
func TestParseSource_IndexBlockSize_FailurePath(t *testing.T) {
	for _, value := range []string{"", "-1", "1.5", "wrong", "18446744073709551616", "4294967296"} {
		t.Run(value, func(t *testing.T) {
			c := qt.New(t)
			db, err := parseIndexSource(c, ` key_block_size="`+value+`"`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(db.Indexes, qt.HasLen, 0)
		})
	}
}
