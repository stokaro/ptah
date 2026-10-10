package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/internal/atlasfilter"
)

// externalNames names each external object by its kind and path.
func externalNames(objects schemaext.Objects) []string {
	var names []string
	for _, ref := range objects.Refs() {
		names = append(names, string(ref.Kind)+" "+ydbexternal.Display(ref.Schema.Source, ref.Name.Source))
	}
	return names
}

var (
	filteredSource = ydbexternal.DataSource{SourceType: "ObjectStorage", AuthMethod: "NONE"}
	filteredTable  = ydbexternal.Table{DataSource: "s3", Location: "e/", Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}
	sourceS3       = string(ydbexternal.SourceKind) + " s3"
	sourceExtLake  = string(ydbexternal.SourceKind) + " ext/lake"
	tableEvents    = string(ydbexternal.TableKind) + " events"
	tableExtFiles  = string(ydbexternal.TableKind) + " ext/files"
)

// externalSides are the same two data sources and two external tables as a
// declaration and as a read.
func externalSides() (*schemamodel.Database, *catalog.Database) {
	declared := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		ydbexternal.DesiredSourceObject("", "s3", "", filteredSource), ydbexternal.DesiredSourceObject("ext", "lake", "", filteredSource),
		ydbexternal.DesiredTableObject("", "events", "", filteredTable), ydbexternal.DesiredTableObject("ext", "files", "", filteredTable)))}
	held := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		ydbexternal.ObservedSourceObject("", "s3", filteredSource), ydbexternal.ObservedSourceObject("ext", "lake", filteredSource),
		ydbexternal.ObservedTableObject("", "events", filteredTable), ydbexternal.ObservedTableObject("ext", "files", filteredTable)))}
	return declared, held
}

// TestScope_ExternalObjects selects a YDB data source and an external table
// on its own name, in the directory that holds it, each by its own type, on
// both sides of a comparison.
func TestScope_ExternalObjects(t *testing.T) {
	tests := []struct {
		name  string
		scope atlasfilter.Scope
		want  []string
	}{
		{name: "include a data source", scope: atlasfilter.Scope{Include: []string{"s3[type=external_data_source]"}}, want: []string{sourceS3}},
		{name: "include a table in a directory", scope: atlasfilter.Scope{Include: []string{"ext.files[type=external_table]"}},
			want: []string{tableExtFiles}},
		{name: "exclude by name", scope: atlasfilter.Scope{Exclude: []string{"events[type=external_table]"}},
			want: []string{sourceS3, sourceExtLake, tableExtFiles}},
		{name: "schema", scope: atlasfilter.Scope{Schemas: []string{"ext"}}, want: []string{sourceExtLake, tableExtFiles}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared, held := externalSides()

			generated, err := atlasfilter.ScopeGenerated(declared, test.scope)
			c.Assert(err, qt.IsNil)
			live, err := atlasfilter.ScopeDatabase(held, test.scope)
			c.Assert(err, qt.IsNil)

			c.Assert(externalNames(generated.FeatureObjects), qt.DeepEquals, test.want)
			c.Assert(externalNames(live.FeatureObjects), qt.DeepEquals, test.want)
			c.Assert(declared.FeatureObjects.Len(), qt.Equals, 4)
			c.Assert(held.FeatureObjects.Len(), qt.Equals, 4)
		})
	}
}

// TestExclude_ExternalObjects drops an excluded external object from the
// declaration as well as from the read, so an object the database holds is
// neither dropped nor created again.
func TestExclude_ExternalObjects(t *testing.T) {
	tests := []struct {
		name    string
		exclude []string
		want    []string
	}{
		{name: "a data source by name", exclude: []string{"s3[type=external_data_source]"}, want: []string{sourceExtLake, tableEvents, tableExtFiles}},
		{name: "every table in a directory", exclude: []string{"ext.*[type=external_table]"}, want: []string{sourceS3, sourceExtLake, tableEvents}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared, held := externalSides()

			generated, err := atlasfilter.ExcludeGenerated(declared, test.exclude)
			c.Assert(err, qt.IsNil)
			live, err := atlasfilter.ExcludeDatabase(held, test.exclude)
			c.Assert(err, qt.IsNil)

			c.Assert(externalNames(generated.FeatureObjects), qt.DeepEquals, test.want)
			c.Assert(externalNames(live.FeatureObjects), qt.DeepEquals, test.want)
		})
	}
}
