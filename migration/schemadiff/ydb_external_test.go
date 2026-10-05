package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// declaredWarehouse is a PostgreSQL data source whose password a secret holds,
// and an external table over an object storage source.
func declaredWarehouse() *schemamodel.Database {
	return &schemamodel.Database{
		ExternalDataSources: []schemamodel.ExternalDataSource{
			{Name: "pg", Schema: "ext", SourceType: "PostgreSQL", Location: "pg:5432", AuthMethod: "BASIC",
				Options: map[string]string{"DATABASE_NAME": "app", "LOGIN": "reader", "PASSWORD_SECRET_PATH": "ext/pg_password"}},
			{Name: "s3", Schema: "ext", SourceType: "ObjectStorage", Location: "https://s3.example.test/b/", AuthMethod: "NONE"},
		},
		ExternalTables: []schemamodel.ExternalTable{{
			Name: "events", Schema: "ext", DataSource: "ext/s3", Location: "events/",
			Columns: []schemamodel.ExternalColumn{{Name: "id", Type: "Int64", NotNull: true}, {Name: "name", Type: "Utf8"}},
			Options: map[string]string{"FORMAT": "json_each_row", "PARTITIONED_BY": `["id"]`},
		}},
	}
}

// heldWarehouse is what the reader describes after declaredWarehouse is
// applied at /local: option names in upper case, the secret's path and the
// data source written relative to the root, and the type names the server
// gives.
func heldWarehouse() catalog.Database {
	return catalog.Database{
		DatabasePath: "/local",
		ExternalDataSources: []catalog.ExternalDataSource{
			{Name: "pg", Schema: "ext", SourceType: "PostgreSQL", Location: "pg:5432", AuthMethod: "BASIC",
				Options: map[string]string{"DATABASE_NAME": "app", "LOGIN": "reader", "PASSWORD_SECRET_PATH": "ext/pg_password"}},
			{Name: "s3", Schema: "ext", SourceType: "ObjectStorage", Location: "https://s3.example.test/b/", AuthMethod: "NONE"},
		},
		ExternalTables: []catalog.ExternalTable{{
			Name: "events", Schema: "ext", DataSource: "ext/s3", Location: "events/",
			Columns: []catalog.ExternalColumn{{Name: "id", Type: "Int64", NotNull: true}, {Name: "name", Type: "Utf8"}},
			Options: map[string]string{"FORMAT": "json_each_row", "PARTITIONED_BY": `["id"]`},
		}},
	}
}

// TestCompare_YDBExternalObjectsAsDescribed finds nothing to do between a
// declaration and what the reader describes of it, whether the declaration
// writes a path relative to the root or absolute, and carries every declared
// external table.
func TestCompare_YDBExternalObjectsAsDescribed(t *testing.T) {
	absolute := declaredWarehouse()
	absolute.ExternalDataSources[0].Options["PASSWORD_SECRET_PATH"] = "/local/ext/pg_password"
	absolute.ExternalTables[0].DataSource = "/local/ext/s3"
	tests := []struct {
		name    string
		desired *schemamodel.Database
	}{
		{name: "paths relative to the root", desired: declaredWarehouse()},
		{name: "absolute paths", desired: absolute},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			held := heldWarehouse()
			diff := schemadiff.CompareWithDialect(test.desired, &held, platform.YDB)
			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%+v", diff))
			c.Assert(diff.DeclaredExternalTables, qt.DeepEquals, test.desired.ExternalTables)
		})
	}
}

// TestCompare_YDBExternalObjectChanges plans each difference: a missing
// object is added, an undeclared one removed, and one that differs in any
// part changed, carrying both sides.
func TestCompare_YDBExternalObjectChanges(t *testing.T) {
	c := qt.New(t)
	desired := declaredWarehouse()
	desired.ExternalDataSources = append(desired.ExternalDataSources, schemamodel.ExternalDataSource{
		Name: "ch", SourceType: "ClickHouse", Location: "ch:8123", AuthMethod: "NONE"})
	desired.ExternalDataSources[0].Options["LOGIN"] = "writer"
	desired.ExternalTables[0].Columns = append(desired.ExternalTables[0].Columns,
		schemamodel.ExternalColumn{Name: "extra", Type: "Utf8"})
	held := heldWarehouse()
	held.ExternalDataSources = append(held.ExternalDataSources, catalog.ExternalDataSource{
		Name: "old", SourceType: "ObjectStorage", Location: "https://old.example.test/", AuthMethod: "NONE"})
	held.ExternalTables = append(held.ExternalTables, catalog.ExternalTable{Name: "stale", DataSource: "old",
		Location: "x/", Columns: []catalog.ExternalColumn{{Name: "id", Type: "Int64"}}})

	diff := schemadiff.CompareWithDialect(desired, &held, platform.YDB)

	c.Assert(namesOf(diff.ExternalDataSourcesAdded), qt.Equals, `["ch"]`)
	c.Assert(namesOf(diff.ExternalDataSourcesRemoved), qt.Equals, `["old"]`)
	c.Assert(diff.ExternalDataSourcesChanged, qt.HasLen, 1)
	c.Assert(diff.ExternalDataSourcesChanged[0].Declared.Options["LOGIN"], qt.Equals, "writer")
	c.Assert(diff.ExternalDataSourcesChanged[0].Current.Options["LOGIN"], qt.Equals, "reader")
	c.Assert(diff.ExternalTablesAdded, qt.HasLen, 0)
	c.Assert(namesOf(diff.ExternalTablesRemoved), qt.Equals, `["stale"]`)
	c.Assert(diff.ExternalTablesChanged, qt.HasLen, 1)
	c.Assert(diff.ExternalTablesChanged[0].Declared.Columns, qt.HasLen, 3)
	c.Assert(diff.ExternalTablesChanged[0].Current.Columns, qt.HasLen, 2)
}

// namesOf is the JSON a diff writes for a list of objects.
func namesOf(list interface{ MarshalJSON() ([]byte, error) }) string {
	data, err := list.MarshalJSON()
	if err != nil {
		return err.Error()
	}
	return string(data)
}

// TestCompare_YDBExternalObjectsKeptWhereTheDesiredStateCannotNameThem plans
// no drop of a held external object when the desired state records that it
// does not describe the kind, and plans it where it does.
func TestCompare_YDBExternalObjectsKeptWhereTheDesiredStateCannotNameThem(t *testing.T) {
	tests := []struct {
		name        string
		desired     *schemamodel.Database
		wantSources int
		wantTables  int
	}{
		{name: "a document that cannot name either kind",
			desired: &schemamodel.Database{NotDescribed: coverage.Set{}.With(
				coverage.Object{Kind: coverage.ExternalDataSource, Reason: coverage.Unsupported, Provenance: coverage.DerivedFromFact},
				coverage.Object{Kind: coverage.ExternalTable, Reason: coverage.Unsupported, Provenance: coverage.DerivedFromFact},
			)}},
		{name: "a document that can, and names none", desired: &schemamodel.Database{}, wantSources: 2, wantTables: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			held := heldWarehouse()
			diff := schemadiff.CompareWithDialect(test.desired, &held, platform.YDB)
			c.Assert(diff.ExternalDataSourcesRemoved, qt.HasLen, test.wantSources)
			c.Assert(diff.ExternalTablesRemoved, qt.HasLen, test.wantTables)
		})
	}
}

// TestCompare_YDBExternalObjectsNotCreatedWhereTheReadDidNotLook withholds
// the creation of a declared object when the read recorded that it did not
// describe the kind, as the reader does on a server with external data
// sources turned off: CREATE EXTERNAL ... has no guard Ptah writes.
func TestCompare_YDBExternalObjectsNotCreatedWhereTheReadDidNotLook(t *testing.T) {
	c := qt.New(t)
	held := &catalog.Database{NotDescribed: coverage.Set{}.With(
		coverage.Object{Kind: coverage.ExternalDataSource, Name: "ext.s3", Reason: coverage.Unsupported,
			Provenance: coverage.Observed},
		coverage.Object{Kind: coverage.ExternalTable, Name: "ext.events", Reason: coverage.Unsupported,
			Provenance: coverage.Observed},
	)}

	diff := schemadiff.CompareWithDialect(declaredWarehouse(), held, platform.YDB)

	c.Assert(namesOf(diff.ExternalDataSourcesAdded), qt.Equals, `["ext.pg"]`)
	c.Assert(diff.ExternalTablesAdded, qt.HasLen, 0)
}

// The JSON a diff writes names each changed object.
func TestExternalChanges_MarshalJSON(t *testing.T) {
	c := qt.New(t)
	data, err := difftypes.ExternalTableChange{Declared: schemamodel.ExternalTable{Name: "events", Schema: "ext"}}.MarshalJSON()
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `"ext.events"`)
	data, err = difftypes.ExternalDataSourceChange{Declared: schemamodel.ExternalDataSource{Name: "s3"}}.MarshalJSON()
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `"s3"`)
}
