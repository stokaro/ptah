package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

var knownFamilies = schemaext.Knowledge{State: schemaext.Complete}

// ydbFamilyDeclaration declares a table with columns id, body and blob in the
// given column families, as the YDB owner's facet, with complete knowledge of
// them, as a Go annotation, YAML or YQL source records it.
func ydbFamilyDeclaration(families ...ydbschema.ColumnFamily) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
		Fields: []schemamodel.Field{
			{StructName: "Doc", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Doc", Name: "body", Type: "TEXT", Nullable: true},
			{StructName: "Doc", Name: "blob", Type: "BYTEA", Nullable: true},
		},
		FeatureCoverage: must.Must(ydbschema.ColumnFamiliesCoverage(schemaext.Desired, knownFamilies, nil)),
	}
	if len(families) > 0 {
		db.Tables[0].Facets = must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{Families: families}))
	}
	return db
}

// ydbFamilyCatalog is the table as the YDB reader reports it, with its column
// families read back as the owner's facet and recorded as known.
func ydbFamilyCatalog(families ...ydbschema.ColumnFamily) *catalog.Database {
	db := &catalog.Database{
		Tables: []catalog.Table{{Name: "docs", Type: "TABLE", Columns: []catalog.Column{
			{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			{Name: "body", DataType: "Utf8", ColumnType: "Utf8", IsNullable: "YES", OrdinalPosition: 2},
			{Name: "blob", DataType: "String", ColumnType: "String", IsNullable: "YES", OrdinalPosition: 3},
		}}},
		Constraints: []catalog.Constraint{{Name: "docs_pkey", TableName: "docs", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
		FeatureCoverage: must.Must(ydbschema.ColumnFamiliesCoverage(schemaext.Observed, knownFamilies, nil)),
	}
	if len(families) > 0 {
		db.Tables[0].Facets = must.Must(must.Must(schemaext.NewFacets(&ydbschema.ObservedColumnFamilies{Families: families})).
			WithTargetScope(ydbschema.ColumnFamiliesKind, platform.YDB))
	}
	return db
}

// familyChange is the one column family change a comparison reports for docs.
func familyChange(c *qt.C, tables []schemaext.ChangeRecord) *ydbdiff.ColumnFamilies {
	c.Helper()
	c.Assert(tables, qt.HasLen, 1)
	change, ok := tables[0].Value.(*ydbdiff.ColumnFamilies)
	c.Assert(ok, qt.IsTrue)
	return change
}

// TestCompare_YDBColumnFamilies_HappyPath plans nothing for a table that
// holds what the declaration states, against the database and against the
// same document alike: the order and the case of a setting do not matter, and
// neither does a setting, a family or a keep_in_memory the table holds and
// the declaration does not state, which a cluster's table profile can give
// every new table. The rows read the table as the YDB reader does, each
// setting at the value the table holds.
func TestCompare_YDBColumnFamilies_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		declared []ydbschema.ColumnFamily
		read     []ydbschema.ColumnFamily
	}{
		{
			name: "families declared in another order",
			declared: []ydbschema.ColumnFamily{
				{Name: "warm", Columns: []string{"blob"}},
				{Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"body"}},
			},
			read: []ydbschema.ColumnFamily{
				{Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"body"}},
				{Name: "default", Compression: "off"},
				{Name: "warm", Compression: "off", Columns: []string{"blob"}},
			},
		},
		{
			name:     "settings and families the declaration does not state",
			declared: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body"}}},
			read: []ydbschema.ColumnFamily{
				{Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"body"}},
				{Name: "default", Compression: "lz4", CacheMode: "in_memory", KeepInMemory: true},
				{Name: "extra", Compression: "lz4"},
			},
		},
		{
			name:     "the default family stating the compression the table holds",
			declared: []ydbschema.ColumnFamily{{Name: "default", Compression: "off"}},
			read:     []ydbschema.ColumnFamily{{Name: "default", Compression: "off"}},
		},
		{name: "no family on either side"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			against := must.Must(schemadiff.CompareWithDialect(t.Context(), ydbFamilyDeclaration(test.declared...), ydbFamilyCatalog(test.read...), platform.YDB, must.Must(builtin.New())))
			c.Assert(against.TablesModified, qt.HasLen, 0)
			itself := must.Must(schemadiff.CompareSchemas(t.Context(), ydbFamilyDeclaration(test.declared...), ydbFamilyDeclaration(test.declared...), platform.YDB, must.Must(builtin.New())))
			c.Assert(itself.TablesModified, qt.HasLen, 0)
		})
	}
}

// TestCompare_YDBColumnFamilies_Change reports a table that does not hold
// what the declaration states. The change carries what the table holds once
// the declaration is applied: every family it holds stays, and each setting
// the declaration leaves out keeps the value the table holds, so a rollback
// sees the whole table.
func TestCompare_YDBColumnFamilies_Change(t *testing.T) {
	tests := []struct {
		name     string
		declared []ydbschema.ColumnFamily
		read     []ydbschema.ColumnFamily
		want     []ydbschema.ColumnFamily
	}{
		{
			name:     "a column in another family",
			declared: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body", "blob"}}},
			read:     []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Compression: "off", Columns: []string{"body"}}},
			want:     []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Compression: "off", Columns: []string{"blob", "body"}}},
		},
		{
			name:     "a setting",
			declared: []ydbschema.ColumnFamily{{Name: "cold", Compression: "lz4", Columns: []string{"body"}}},
			read:     []ydbschema.ColumnFamily{{Name: "cold", Compression: "off", Columns: []string{"body"}}},
			want:     []ydbschema.ColumnFamily{{Name: "cold", Compression: "lz4", Columns: []string{"body"}}},
		},
		{
			name:     "the default family's compression over a profile's",
			declared: []ydbschema.ColumnFamily{{Name: "default", Compression: "off"}},
			read:     []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4", KeepInMemory: true}},
			want:     []ydbschema.ColumnFamily{{Name: "default", Compression: "off", KeepInMemory: true}},
		},
		{
			name:     "a column out of a family the declaration leaves out",
			declared: nil,
			read:     []ydbschema.ColumnFamily{{Name: "cold", Compression: "lz4", Columns: []string{"body"}}},
			want:     []ydbschema.ColumnFamily{{Name: "cold", Compression: "lz4"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), ydbFamilyDeclaration(test.declared...), ydbFamilyCatalog(test.read...), platform.YDB, must.Must(builtin.New())))
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			change := familyChange(c, diff.TablesModified[0].FeatureChanges)
			c.Assert(change.Before.Families, qt.DeepEquals, test.read)
			c.Assert(change.After.Families, qt.DeepEquals, test.want)
		})
	}
}

// TestCompare_YDBColumnFamilies_Coverage reads each side's silence through
// coverage. A document that cannot spell a family -- HCL or DBML, which
// enroll no family coverage -- keeps the database's families, columns
// included, and plans nothing for them. A read that could not describe a
// table's families withholds the declared ones, and says so, rather than
// planning against families it did not see.
func TestCompare_YDBColumnFamilies_Coverage(t *testing.T) {
	c := qt.New(t)
	held := []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Columns: []string{"body", "gone"}}}
	silent := ydbFamilyDeclaration()
	silent.FeatureCoverage = schemaext.Coverage{}
	read := ydbFamilyCatalog(held...)
	read.Tables[0].Columns = append(read.Tables[0].Columns, catalog.Column{Name: "gone", DataType: "Utf8",
		ColumnType: "Utf8", IsNullable: "YES", OrdinalPosition: 4})

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), silent, read, platform.YDB, must.Must(builtin.New())))
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].FeatureChanges, qt.HasLen, 0)
	adopted, found, err := schemaext.FacetAs[*ydbschema.DesiredColumnFamilies](diff.TablesModified[0].Desired.Table.Facets, ydbschema.ColumnFamiliesKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(adopted.Families, qt.DeepEquals, held)

	unread := ydbFamilyCatalog()
	docs := objectidentity.NewBuilder(identifier.ForDialect(platform.YDB)).TableParts("", "docs")
	unread.FeatureCoverage = must.Must(ydbschema.ColumnFamiliesCoverage(schemaext.Observed, knownFamilies, []schemaext.SubjectCoverage{{
		Kind: ydbschema.ColumnFamiliesKind, Subject: docs,
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the table's column families hold a setting Ptah does not read"},
	}}))
	opts := config.DefaultCompareOptions()
	opts.Dialect = platform.YDB
	withheld, undecided, err := schemadiff.CompareReportingUndecidedAdditions(
		t.Context(), ydbFamilyDeclaration(ydbschema.ColumnFamily{Name: "cold"}), unread, opts, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(withheld.HasChanges(), qt.IsFalse)
	c.Assert(undecided.Features, qt.HasLen, 1)
	c.Assert(undecided.Features[0].Kind, qt.Equals, ydbschema.ColumnFamiliesKind)
	c.Assert(undecided.Features[0].Reason, qt.Equals, "the read found column families it could not describe")
}
