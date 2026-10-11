package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
)

// A declaration standing for a MySQL-family database observes the block
// sizes a read would: whether the table keeps a hint follows the row format it
// declares. An index gets the observation where it says more than its
// absence, a hint or a MySQL table that keeps one, and another target gets
// none.
func TestToDBSchema_ObservesIndexBlockSizes(t *testing.T) {
	declared := must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 8))
	for _, test := range []struct {
		name, dialect, rowFormat string
		facets                   schemaext.Facets
		want                     *mysqlschema.ObservedIndexBlockSize
	}{
		{name: "MySQL, compressed", dialect: "mysql", rowFormat: "COMPRESSED", facets: declared,
			want: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 8, Retained: true}},
		{name: "MySQL, default row format", dialect: "mysql", facets: declared,
			want: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 8}},
		{name: "MySQL, compressed, no hint declared", dialect: "mysql", rowFormat: "COMPRESSED",
			want: &mysqlschema.ObservedIndexBlockSize{Retained: true}},
		{name: "MariaDB, no hint declared", dialect: "mariadb"},
		{name: "PostgreSQL", dialect: "postgres"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables:  []schemamodel.Table{{StructName: "T", Name: "t", Overrides: map[string]map[string]string{test.dialect: {"row_format": test.rowFormat}}}},
				Fields:  []schemamodel.Field{{StructName: "T", Name: "a", Type: "INT"}},
				Indexes: []schemamodel.Index{{StructName: "T", Name: "k", Fields: []string{"a"}, Facets: test.facets}},
			}

			converted, err := goschematodb.ToDBSchema(c.Context(), db, test.dialect, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(converted.Indexes, qt.HasLen, 1)
			observed, _, err := schemaext.FacetAs[*mysqlschema.ObservedIndexBlockSize](converted.Indexes[0].Facets, mysqlschema.IndexBlockSizeKind)
			c.Assert(err, qt.IsNil)
			c.Assert(observed, qt.DeepEquals, test.want)
		})
	}
}

// A declaration standing for a MySQL-family database observes its primary
// key's block size as a read would, on the PRIMARY KEY constraint the key
// converts to: the hint declared for the key, and whether the table keeps
// one.
func TestToDBSchema_ObservesPrimaryKeyBlockSizes(t *testing.T) {
	declared := must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 8))
	for _, test := range []struct {
		name, dialect, rowFormat string
		facets                   schemaext.Facets
		want                     *mysqlschema.ObservedIndexBlockSize
	}{
		{name: "MySQL, compressed", dialect: "mysql", rowFormat: "COMPRESSED", facets: declared,
			want: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 8, Retained: true}},
		{name: "MySQL, compressed, no hint declared", dialect: "mysql", rowFormat: "COMPRESSED",
			want: &mysqlschema.ObservedIndexBlockSize{Retained: true}},
		{name: "MySQL, default row format", dialect: "mysql", facets: declared,
			want: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 8}},
		{name: "MariaDB, no hint declared", dialect: "mariadb"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"a"}, PrimaryKeyFacets: test.facets,
					Overrides: map[string]map[string]string{test.dialect: {"row_format": test.rowFormat}}}},
				Fields: []schemamodel.Field{{StructName: "T", Name: "a", Type: "INT"}},
			}

			converted, err := goschematodb.ToDBSchema(c.Context(), db, test.dialect, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(converted.Constraints, qt.HasLen, 1)
			c.Assert(converted.Constraints[0].Type, qt.Equals, "PRIMARY KEY")
			observed, _, err := schemaext.FacetAs[*mysqlschema.ObservedIndexBlockSize](converted.Constraints[0].Facets, mysqlschema.IndexBlockSizeKind)
			c.Assert(err, qt.IsNil)
			c.Assert(observed, qt.DeepEquals, test.want)
		})
	}
}
