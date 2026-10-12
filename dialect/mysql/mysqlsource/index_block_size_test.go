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

// parseKeySource parses one struct whose table declares the given
// attributes, with the MySQL owner selected.
func parseKeySource(c *qt.C, table, constraint string) (schemamodel.Database, error) {
	c.Helper()
	owner := must.Must(annotation.NewSet(mysqlsource.Annotations()))
	source := "package entities\n//ptah:schema:table name=\"t\"" + table + "\ntype T struct {\n" + constraint + "\nA int\n}"
	return goschema.ParseSource(owner, "entities.go", source)
}

// TestParseSource_PrimaryKeyBlockSize_HappyPath reads the table directive's
// primary_key_block_size as the hint of the table's primary key, and a
// PRIMARY KEY constraint's key_block_size as the constraint's. Neither lands
// on the table itself.
func TestParseSource_PrimaryKeyBlockSize_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, table, constraint string
		key, constraintSize     uint64
	}{
		{"on the table", ` primary_key="a" primary_key_block_size="8"`, ``, 8, 0},
		{"on a constraint", ``, `//ptah:schema:constraint name="pk" type="PRIMARY KEY" columns="a" key_block_size="4"`, 0, 4},
		{"zero declares none", ` primary_key="a" primary_key_block_size="0"`, ``, 0, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := parseKeySource(c, test.table, test.constraint)
			c.Assert(err, qt.IsNil)
			c.Assert(db.Tables, qt.HasLen, 1)
			c.Assert(db.Tables[0].Facets.IsZero(), qt.IsTrue)
			key, _, err := mysqlschema.IndexBlockSize(db.Tables[0].PrimaryKeyFacets)
			c.Assert(err, qt.IsNil)
			c.Assert(key, qt.Equals, test.key)
			sizes := make([]uint64, 0, len(db.Constraints))
			for _, constraint := range db.Constraints {
				sizes = append(sizes, must.Must(blockSizeOf(constraint.Facets)))
			}
			c.Assert(sizes, qt.DeepEquals, map[bool][]uint64{true: {test.constraintSize}, false: {}}[test.constraint != ""])
		})
	}
}

// TestParseSource_PrimaryKeyBlockSize_FailurePath refuses a value that is not
// a whole number of kilobytes MySQL can store, and a hint of a key the table
// does not declare: a field's own primary key is written as a column's
// PRIMARY KEY, which has no place for one.
func TestParseSource_PrimaryKeyBlockSize_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, table, constraint, wantErr string
	}{
		{"not a number", ` primary_key="a" primary_key_block_size="wrong"`, ``,
			`invalid primary_key_block_size "wrong" \(must be a non-negative integer\) on //ptah:schema:table at T`},
		{"over the limit", ` primary_key="a" primary_key_block_size="4294967296"`, ``, `.*KEY_BLOCK_SIZE.*`},
		{"a constraint's, not a number", ``, `//ptah:schema:constraint name="pk" type="PRIMARY KEY" columns="a" key_block_size="-1"`,
			`invalid key_block_size "-1" \(must be a non-negative integer\) on //ptah:schema:constraint at T`},
		{"no key on the table", ` primary_key_block_size="8"`, ``,
			`a setting of the primary key needs the key declared with primary_key on //ptah:schema:table at T`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := parseKeySource(c, test.table, test.constraint)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db.Tables, qt.HasLen, 0)
		})
	}
}

// TestParseSource_ConstraintBlockSize_HappyPath reads a PRIMARY KEY
// constraint's key_block_size in any spelling of the type, and a constraint
// of another type that states no hint as one without it: the owner reads the
// type of every constraint and adds nothing to one that has no hint.
func TestParseSource_ConstraintBlockSize_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, constraint string
		size             uint64
	}{
		{"a primary key in lower case", `//ptah:schema:constraint name="pk" type="primary key" columns="a" key_block_size="4"`, 4},
		{"a primary key with spaces", `//ptah:schema:constraint name="pk" type=" PRIMARY KEY " columns="a" key_block_size="2"`, 2},
		{"a UNIQUE constraint without a hint", `//ptah:schema:constraint name="u" type="UNIQUE" columns="a"`, 0},
		{"a UNIQUE constraint with a hint of zero", `//ptah:schema:constraint name="u" type="UNIQUE" columns="a" key_block_size="0"`, 0},
		{"a foreign key", `//ptah:schema:constraint name="fk" type="FOREIGN KEY" columns="a" foreign_table="t" foreign_column="a"`, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := parseKeySource(c, ``, test.constraint)
			c.Assert(err, qt.IsNil)
			c.Assert(db.Constraints, qt.HasLen, 1)
			size, err := blockSizeOf(db.Constraints[0].Facets)
			c.Assert(err, qt.IsNil)
			c.Assert(size, qt.Equals, test.size)
			c.Assert(db.Constraints[0].Facets.Len(), qt.Equals, map[bool]int{true: 1, false: 0}[test.size != 0])
		})
	}
}

// TestParseSource_ConstraintBlockSize_FailurePath refuses a key_block_size on
// a constraint other than a PRIMARY KEY with the text the renderer gives:
// MySQL keeps a UNIQUE key's hint on the unique index it is, so a unique index
// declares it.
func TestParseSource_ConstraintBlockSize_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, constraint, wantErr string
	}{
		{"a UNIQUE constraint", `//ptah:schema:constraint name="u" type="UNIQUE" columns="a" key_block_size="8"`,
			`unsupported feature: KEY_BLOCK_SIZE on UNIQUE constraint "u" is not supported; declare a unique index for a UNIQUE key on //ptah:schema:constraint at T`},
		{"a type in lower case", `//ptah:schema:constraint name="u" type="unique" columns="a" key_block_size="8"`,
			`unsupported feature: KEY_BLOCK_SIZE on UNIQUE constraint "u" is not supported; declare a unique index for a UNIQUE key on //ptah:schema:constraint at T`},
		{"a foreign key", `//ptah:schema:constraint name="fk" type="FOREIGN KEY" columns="a" foreign_table="t" foreign_column="a" key_block_size="2"`,
			`unsupported feature: KEY_BLOCK_SIZE on FOREIGN KEY constraint "fk" is not supported; declare a unique index for a UNIQUE key on //ptah:schema:constraint at T`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := parseKeySource(c, ``, test.constraint)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db.Constraints, qt.HasLen, 0)
		})
	}
}

// blockSizeOf is the hint facets hold, zero for none.
func blockSizeOf(facets schemaext.Facets) (uint64, error) {
	size, _, err := mysqlschema.IndexBlockSize(facets)
	return size, err
}
