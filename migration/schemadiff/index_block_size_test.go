package schemadiff_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

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

func TestCompare_PrimaryKeyOptions(t *testing.T) {
	for _, test := range []struct {
		name, dialect, format, comment string
		size                           uint64
		changes                        bool
	}{
		{"MariaDB size", "mariadb", "Dynamic", "old", 8, true},
		{"MariaDB comment", "mariadb", "Dynamic", "new", 4, true},
		{"MariaDB synced", "mariadb", "Dynamic", "old", 4, false},
		{"MySQL compressed size", "mysql", "Compressed", "old", 8, true},
		{"MySQL drops size", "mysql", "Dynamic", "old", 8, false},
		{"MySQL comment", "mysql", "Dynamic", "new", 8, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := methodDesired("")
			live := methodCurrent(nil)
			desired.Tables[0].PrimaryKeyBlockSize = test.size
			desired.Tables[0].PrimaryKeyComment = test.comment
			live.Tables[0].RowFormat = test.format
			live.Constraints[0].KeyBlockSize = 4
			live.Constraints[0].Comment = "old"
			diff := compareForDialect(test.dialect, desired, live)
			c.Assert(diff.HasChanges(), qt.Equals, test.changes)
			c.Assert(len(diff.ConstraintsAdded) > 0, qt.Equals, test.changes)
			c.Assert(len(diff.ConstraintsRemoved) > 0, qt.Equals, test.changes)
		})
	}
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

// readDeclaration reads one SQL declaration for dialect.
func readDeclaration(c *qt.C, sql, dialect string) schemamodel.Database {
	c.Helper()
	db, _, err := sqlschema.Read([]byte(sql), dialect)
	c.Assert(err, qt.IsNil)
	return db
}
