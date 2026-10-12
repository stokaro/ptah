package schemadiff_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqldiff"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/mysql/mysqlsource"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestCompare_IndexBlockSize compares the hint the MySQL owner holds. A
// change is reported only where the server retains a hint, which the reader
// records on each index's observation, and a declaration without a hint
// removes one only when its source claims to describe the hint. The change
// is the owner's, attached to the table, never a common index replacement.
func TestCompare_IndexBlockSize(t *testing.T) {
	for _, test := range []struct {
		name, dialect string
		retained      bool
		desired, live uint64
		covered       bool
		want          []schemaext.Kind
	}{
		{"MariaDB retains", "mariadb", true, 8, 4, true, []schemaext.Kind{mysqldiff.IndexBlockSizeKind}},
		{"MariaDB equal", "mariadb", true, 8, 8, true, nil},
		{"MariaDB removes", "mariadb", true, 0, 8, true, []schemaext.Kind{mysqldiff.IndexBlockSizeKind}},
		{"MySQL compressed retains", "mysql", true, 8, 4, true, []schemaext.Kind{mysqldiff.IndexBlockSizeKind}},
		{"MySQL compressed equal", "mysql", true, 8, 8, true, nil},
		{"MySQL compressed removes", "mysql", true, 0, 8, true, []schemaext.Kind{mysqldiff.IndexBlockSizeKind}},
		{"MySQL dynamic drops", "mysql", false, 8, 0, true, nil},
		{"a source that does not describe the hint keeps it", "mariadb", true, 0, 8, false, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := optionedIndexTable("", false)
			live := liveOptionedIndexTable("", false)
			desired.Indexes[0].Facets = must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, test.desired))
			desired.FeatureCoverage = coverageOf(test.covered)
			live.Indexes[0].Facets = must.Must(mysqlschema.WithObservedIndexBlockSize(schemaext.Facets{},
				mysqlschema.ObservedIndexBlockSize{KeyBlockSize: test.live, Retained: test.retained}))
			diff := compareForDialect(test.dialect, desired, live)
			c.Assert(diff.HasChanges(), qt.Equals, test.want != nil)
			c.Assert(featureChangeKinds(diff), qt.DeepEquals, test.want)
			c.Assert(diff.IndexesAdded, qt.HasLen, 0)
			c.Assert(diff.IndexesRemoved, qt.HasLen, 0)
		})
	}
}

// coverageOf is the claim a source that describes the hint makes, or none.
func coverageOf(covered bool) schemaext.Coverage {
	return map[bool]schemaext.Coverage{true: must.Must(mysqlsource.BlockSizeCoverage())}[covered]
}

// featureChangeKinds lists the kinds of the feature changes attached to the
// diff's tables, in order, and nil when there are none.
func featureChangeKinds(diff *difftypes.SchemaDiff) []schemaext.Kind {
	var kinds []schemaext.Kind
	for _, table := range diff.TablesModified {
		for _, change := range table.FeatureChanges {
			kinds = append(kinds, change.Value.Kind())
		}
	}
	return kinds
}

// TestCompare_PrimaryKeyOptions compares a primary key's comment and its
// block-size hint. The comment is the common key's, and a different one
// replaces the key. The hint is the MySQL owner's, compared as an index's is
// under the table's primary key, whatever the server named the key: a
// different hint is the owner's change, attached to the table, and never a
// common replacement. Where the server discards the hint, nothing is
// compared.
func TestCompare_PrimaryKeyOptions(t *testing.T) {
	for _, test := range []struct {
		name, dialect, comment string
		retained               bool
		size                   uint64
		replaced               bool
		want                   []schemaext.Kind
	}{
		{"MariaDB size", "mariadb", "old", true, 8, false, []schemaext.Kind{mysqldiff.IndexBlockSizeKind}},
		{"MariaDB comment", "mariadb", "new", true, 4, true, nil},
		{"MariaDB synced", "mariadb", "old", true, 4, false, nil},
		{"MySQL compressed size", "mysql", "old", true, 8, false, []schemaext.Kind{mysqldiff.IndexBlockSizeKind}},
		{"MySQL compressed size removed", "mysql", "old", true, 0, false, []schemaext.Kind{mysqldiff.IndexBlockSizeKind}},
		{"MySQL drops size", "mysql", "old", false, 8, false, nil},
		{"MySQL comment", "mysql", "new", false, 8, true, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := methodDesired("")
			live := methodCurrent(nil)
			desired.Tables[0].PrimaryKeyFacets = must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, test.size))
			desired.Tables[0].PrimaryKeyComment = test.comment
			desired.FeatureCoverage = coverageOf(true)
			live.Constraints[0].Facets = must.Must(mysqlschema.WithObservedIndexBlockSize(schemaext.Facets{},
				mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 4, Retained: test.retained}))
			live.Constraints[0].Comment = "old"
			diff := compareForDialect(test.dialect, desired, live)
			c.Assert(diff.HasChanges(), qt.Equals, test.replaced || test.want != nil)
			c.Assert(len(diff.ConstraintsAdded) > 0, qt.Equals, test.replaced)
			c.Assert(len(diff.ConstraintsRemoved) > 0, qt.Equals, test.replaced)
			c.Assert(featureChangeKinds(diff), qt.DeepEquals, test.want)
		})
	}
}

// TestCompare_PrimaryKeyBlockSizeNamesNoKey pins that the hint of a key pairs
// by its table, not by a name: the key a declaration names pk_t is the one
// MySQL names PRIMARY, and the key a PRIMARY KEY constraint declares is the
// table's key as much as one declared on the table.
func TestCompare_PrimaryKeyBlockSizeNamesNoKey(t *testing.T) {
	for _, test := range []struct {
		name    string
		desired *schemamodel.Database
	}{
		{"a key named on the table", namedKeyDesired(false)},
		{"a PRIMARY KEY constraint", namedKeyDesired(true)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			live := methodCurrent(nil)
			live.Constraints[0].Facets = must.Must(mysqlschema.WithObservedIndexBlockSize(schemaext.Facets{},
				mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 4, Retained: true}))
			diff := compareForDialect("mariadb", test.desired, live)
			c.Assert(featureChangeKinds(diff), qt.DeepEquals, []schemaext.Kind{mysqldiff.IndexBlockSizeKind})
			c.Assert(diff.TablesModified[0].FeatureChanges[0].Subject.Kind, qt.Equals, objectidentity.KindPrimaryKey)
		})
	}
}

// namedKeyDesired declares table t's primary key pk_t with a hint of 8, as a
// PRIMARY KEY constraint or on the table.
func namedKeyDesired(constraint bool) *schemamodel.Database {
	desired := methodDesired("")
	desired.FeatureCoverage = coverageOf(true)
	hint := must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 8))
	desired.Tables[0].PrimaryKeyName = "pk_t"
	desired.Tables[0].PrimaryKeyFacets = map[bool]schemaext.Facets{false: hint}[constraint]
	desired.Constraints = map[bool][]schemamodel.Constraint{true: {{StructName: "T", Name: "pk_t", Type: "PRIMARY KEY",
		Columns: []string{"id"}, Facets: hint}}}[constraint]
	desired.Tables[0].PrimaryKey = map[bool][]string{false: {"id"}}[constraint]
	return desired
}

// TestCompare_PrimaryKeyBlockSizeInTwoPlaces_FailurePath refuses a hint
// declared on the table beside a PRIMARY KEY constraint, which is where the
// key's settings are read from: the table's own would disappear.
func TestCompare_PrimaryKeyBlockSizeInTwoPlaces_FailurePath(t *testing.T) {
	c := qt.New(t)
	desired := methodDesired("")
	desired.Tables[0].PrimaryKey = nil
	desired.Tables[0].PrimaryKeyFacets = must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 8))
	desired.Constraints = []schemamodel.Constraint{{StructName: "T", Name: "pk_t", Type: "PRIMARY KEY", Columns: []string{"id"}}}
	opts := config.DefaultCompareOptions()
	opts.Dialect = "mariadb"
	diff, err := schemadiff.CompareWithOptions(c.Context(), desired, methodCurrent(nil), opts, builtintest.Runtime())
	c.Assert(err, qt.ErrorMatches, `.*no comparison identity capture for attached facets \[ptah.run/mysql/index-block-size\]`)
	c.Assert(diff, qt.IsNil)
}

// TestCompareSchemas_IndexBlockSize compares two declarations, the second
// standing for the database, as a file-to-file comparison does. Whether the
// table keeps a hint follows the row format the second declares, as the
// owner's CREATE prediction states it: a declaration's values alone cannot
// say, since the row format belongs to the table.
func TestCompareSchemas_IndexBlockSize(t *testing.T) {
	const table = "CREATE TABLE t (id int NOT NULL PRIMARY KEY, a int, KEY k(a)%s)%s;"
	for _, test := range []struct {
		name, dialect, options string
		current, desired       string
		want                   []schemaext.Kind
	}{
		{"MySQL, compressed", "mysql", " ROW_FORMAT=COMPRESSED", " KEY_BLOCK_SIZE=4", " KEY_BLOCK_SIZE=8", []schemaext.Kind{mysqldiff.IndexBlockSizeKind}},
		{"MySQL, compressed, a hint added", "mysql", " ROW_FORMAT=COMPRESSED", "", " KEY_BLOCK_SIZE=8", []schemaext.Kind{mysqldiff.IndexBlockSizeKind}},
		{"MySQL, compressed, the same hint", "mysql", " ROW_FORMAT=COMPRESSED", " KEY_BLOCK_SIZE=8", " KEY_BLOCK_SIZE=8", nil},
		{"MySQL, the default row format", "mysql", "", " KEY_BLOCK_SIZE=4", " KEY_BLOCK_SIZE=8", nil},
		{"MariaDB", "mariadb", "", " KEY_BLOCK_SIZE=4", " KEY_BLOCK_SIZE=8", []schemaext.Kind{mysqldiff.IndexBlockSizeKind}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current := readDeclaration(c, fmt.Sprintf(table, test.current, test.options), test.dialect)
			desired := readDeclaration(c, fmt.Sprintf(table, test.desired, test.options), test.dialect)

			diff, err := schemadiff.CompareSchemas(c.Context(), &desired, &current, test.dialect, builtintest.Runtime())

			c.Assert(err, qt.IsNil)
			c.Assert(featureChangeKinds(diff), qt.DeepEquals, test.want)
			c.Assert(diff.IndexesAdded, qt.HasLen, 0)
		})
	}
}

// readDeclaration reads one SQL declaration for dialect with the bundled
// runtime's SQL owners, whose syntax and claims a document carries.
func readDeclaration(c *qt.C, sql, dialect string) schemamodel.Database {
	c.Helper()
	runtime := builtintest.Runtime()
	db, _, err := sqlschema.ReadWith(c.Context(), []byte(sql), dialect, nil,
		sqlschema.Options{Owners: runtime.SQL(), Vocabulary: runtime.CoverageVocabulary()})
	c.Assert(err, qt.IsNil)
	return db
}

// TestCompare_KeysAConstraintOfAnUndeclaredTableByItsTable pins that a
// PRIMARY KEY constraint naming a table the declaration does not hold is
// keyed by the table it names, as an index naming one is, rather than refused
// as settings with no owner.
func TestCompare_KeysAConstraintOfAnUndeclaredTableByItsTable(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Constraints: []schemamodel.Constraint{{StructName: "T", Table: "t", Name: "pk_t", Type: "PRIMARY KEY",
		Columns: []string{"id"}, Facets: must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 8))}}}
	opts := config.DefaultCompareOptions()
	opts.Dialect = "mysql"

	diff, err := schemadiff.CompareWithOptions(c.Context(), desired, &catalog.Database{}, opts, builtintest.Runtime())

	c.Assert(err, qt.IsNil)
	c.Assert(featureChangeKinds(diff), qt.HasLen, 0)
}
