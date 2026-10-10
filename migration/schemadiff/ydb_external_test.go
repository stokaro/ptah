package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// warehouseSource is a PostgreSQL data source whose password a secret holds,
// with the secret's path written as secretPath.
func warehouseSource(secretPath string) ydbexternal.DataSource {
	return ydbexternal.DataSource{SourceType: "PostgreSQL", Location: "pg:5432", AuthMethod: "BASIC",
		Options: map[string]string{"DATABASE_NAME": "app", "LOGIN": "reader", "PASSWORD_SECRET_PATH": secretPath}}
}

// s3Source is an object storage data source.
func s3Source() ydbexternal.DataSource {
	return ydbexternal.DataSource{SourceType: "ObjectStorage", Location: "https://s3.example.test/b/", AuthMethod: "NONE"}
}

// eventsOver is an external table over the data source at source.
func eventsOver(source string) ydbexternal.Table {
	return ydbexternal.Table{DataSource: source, Location: "events/",
		Columns: []ydbexternal.Column{{Name: "id", Type: "Int64", NotNull: true}, {Name: "name", Type: "Utf8"}},
		Options: map[string]string{"FORMAT": "json_each_row", "PARTITIONED_BY": `["id"]`}}
}

// externalCoverage claims both external namespaces for representation.
func externalCoverage(representation schemaext.Representation) schemaext.Coverage {
	complete := schemaext.Knowledge{State: schemaext.Complete}
	sources := must.Must(ydbexternal.SourceCoverage(representation, complete, nil))
	return must.Must(sources.Combine(must.Must(ydbexternal.TableCoverage(representation, complete, nil))))
}

// declaredExternal declares objects from a source that describes both
// external namespaces.
func declaredExternal(objects ...schemaext.Object) *schemamodel.Database {
	return &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(objects...)), FeatureCoverage: externalCoverage(schemaext.Desired)}
}

// heldExternal is a read of the database /local that listed every external
// object and found objects.
func heldExternal(objects ...schemaext.Object) *catalog.Database {
	return &catalog.Database{DatabasePath: "/local", FeatureObjects: must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: externalCoverage(schemaext.Observed)}
}

// heldWarehouse is what the reader describes after the warehouse source, the
// object storage source and the table over it are applied at /local: the
// secret's path and the data source written relative to the root.
func heldWarehouse() *catalog.Database {
	return heldExternal(
		ydbexternal.ObservedSourceObject("ext", "pg", warehouseSource("ext/pg_password")),
		ydbexternal.ObservedSourceObject("ext", "s3", s3Source()),
		ydbexternal.ObservedTableObject("ext", "events", eventsOver("ext/s3")),
	)
}

// TestCompare_YDBExternalObjectsAsDescribed finds nothing to do between a
// declaration and what the reader describes of it, whether the declaration
// writes the paths relative to the root or absolute under it: the
// comparison reads them against the database the read describes.
func TestCompare_YDBExternalObjectsAsDescribed(t *testing.T) {
	tests := []struct {
		name          string
		secret, table string
	}{
		{name: "paths relative to the root", secret: "ext/pg_password", table: "ext/s3"},
		{name: "absolute paths", secret: "/local/ext/pg_password", table: "/local/ext/s3"}, // #nosec G101 -- a secret's path, not a credential
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := declaredExternal(
				ydbexternal.DesiredSourceObject("ext", "pg", "", warehouseSource(test.secret)),
				ydbexternal.DesiredSourceObject("ext", "s3", "", s3Source()),
				ydbexternal.DesiredTableObject("ext", "events", "", eventsOver(test.table)),
			)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, heldWarehouse(), platform.YDB, must.Must(builtin.New())))
			c.Assert(diff.FeatureChanges, qt.IsNil)
			c.Assert(diff.HasChanges(), qt.IsFalse)
		})
	}
}

// TestCompare_YDBExternalObjectChanges plans each difference: a missing
// object is created, an undeclared one dropped, and one that differs in any
// part changed, carrying both sides.
func TestCompare_YDBExternalObjectChanges(t *testing.T) {
	c := qt.New(t)
	writer := warehouseSource("ext/pg_password")
	writer.Options["LOGIN"] = "writer"
	wider := eventsOver("ext/s3")
	wider.Columns = append(wider.Columns, ydbexternal.Column{Name: "extra", Type: "Utf8"})
	clickhouse := ydbexternal.DataSource{SourceType: "ClickHouse", Location: "ch:8123", AuthMethod: "NONE"}
	old := ydbexternal.DataSource{SourceType: "ObjectStorage", Location: "https://old.example.test/", AuthMethod: "NONE"}
	stale := ydbexternal.Table{DataSource: "old", Location: "x/", Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}
	desired := declaredExternal(
		ydbexternal.DesiredSourceObject("", "ch", "", clickhouse),
		ydbexternal.DesiredSourceObject("ext", "pg", "", writer),
		ydbexternal.DesiredSourceObject("ext", "s3", "", s3Source()),
		ydbexternal.DesiredTableObject("ext", "events", "", wider),
	)
	held := heldWarehouse()
	held.FeatureObjects = must.Must(held.FeatureObjects.With(ydbexternal.ObservedSourceObject("", "old", old)))
	held.FeatureObjects = must.Must(held.FeatureObjects.With(ydbexternal.ObservedTableObject("", "stale", stale)))

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, held, platform.YDB, must.Must(builtin.New())))

	c.Assert(diff.FeatureChanges, qt.DeepEquals, []schemaext.ChangeRecord{
		{Subject: ydbexternal.SourceRef("", "ch"), Value: &ydbdiff.ExternalDataSource{After: &ydbexternal.DesiredSource{Spec: clickhouse}}},
		{Subject: ydbexternal.SourceRef("", "old"), Value: &ydbdiff.ExternalDataSource{Before: &ydbexternal.ObservedSource{Spec: old}}},
		{Subject: ydbexternal.SourceRef("ext", "pg"), Value: &ydbdiff.ExternalDataSource{
			Before: &ydbexternal.ObservedSource{Spec: warehouseSource("ext/pg_password")}, After: &ydbexternal.DesiredSource{Spec: writer}}},
		{Subject: ydbexternal.TableRef("", "stale"), Value: &ydbdiff.ExternalTable{Before: &ydbexternal.ObservedTable{Spec: stale}}},
		{Subject: ydbexternal.TableRef("ext", "events"), Value: &ydbdiff.ExternalTable{
			Before: &ydbexternal.ObservedTable{Spec: eventsOver("ext/s3")}, After: &ydbexternal.DesiredTable{Spec: wider}}},
	})
}

// TestCompare_YDBExternalTablesOverARecreatedSource asks for every declared
// table the database holds over a data source the plan drops and creates
// again, as a change whose operands describe the same table: on a target
// without CREATE OR REPLACE YDB keeps no table over a dropped source. A source
// replaced in place keeps its tables.
func TestCompare_YDBExternalTablesOverARecreatedSource(t *testing.T) {
	moved := s3Source()
	moved.Location = "https://s3.example.test/other/"
	desired := declaredExternal(
		ydbexternal.DesiredSourceObject("ext", "pg", "", warehouseSource("ext/pg_password")),
		ydbexternal.DesiredSourceObject("ext", "s3", "", moved),
		ydbexternal.DesiredTableObject("ext", "events", "", eventsOver("/local/ext/s3")),
	)
	sourceChange := schemaext.ChangeRecord{Subject: ydbexternal.SourceRef("ext", "s3"), Value: &ydbdiff.ExternalDataSource{
		Before: &ydbexternal.ObservedSource{Spec: s3Source()}, After: &ydbexternal.DesiredSource{Spec: moved}}}
	tests := []struct {
		name    string
		replace bool
		want    []schemaext.ChangeRecord
	}{
		{name: "without CREATE OR REPLACE", want: []schemaext.ChangeRecord{sourceChange,
			{Subject: ydbexternal.TableRef("ext", "events"), Value: &ydbdiff.ExternalTable{
				Before: &ydbexternal.ObservedTable{Spec: eventsOver("ext/s3")}, After: &ydbexternal.DesiredTable{Spec: eventsOver("/local/ext/s3")}}}}},
		{name: "with it", replace: true, want: []schemaext.ChangeRecord{sourceChange}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.YDB262().With(capability.ExternalDataSources, true).With(capability.ExternalObjectReplace, test.replace)

			diff := must.Must(schemadiff.CompareWithDatabaseInfo(t.Context(), desired, heldWarehouse(),
				catalog.ServerInfo{Dialect: platform.YDB, Capabilities: caps}, nil, must.Must(builtin.New())))

			c.Assert(diff.FeatureChanges, qt.DeepEquals, test.want)
		})
	}
}

// TestCompare_YDBExternalObjectsKeptWhereTheDesiredStateCannotNameThem plans
// no drop of a held external object when the desired state makes no claim
// about either namespace, as an HCL or DBML document does not, and plans it
// where it claims to describe them and names none.
func TestCompare_YDBExternalObjectsKeptWhereTheDesiredStateCannotNameThem(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		want    int
	}{
		{name: "a document that cannot name either kind", desired: &schemamodel.Database{}},
		{name: "a document that can, and names none", desired: declaredExternal(), want: 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), test.desired, heldWarehouse(), platform.YDB, must.Must(builtin.New())))
			c.Assert(diff.FeatureChanges, qt.HasLen, test.want)
		})
	}
}

// TestCompare_YDBExternalObjectsNotCreatedWhereTheReadDidNotLook withholds
// the creation of a declared object when the read recorded that it did not
// describe it, as the reader does on a server with external data sources
// turned off: CREATE EXTERNAL ... has no guard Ptah writes. The withheld
// creations are reported, and an object the read did look for is still
// created.
func TestCompare_YDBExternalObjectsNotCreatedWhereTheReadDidNotLook(t *testing.T) {
	c := qt.New(t)
	unread := schemaext.Knowledge{State: schemaext.Uninspected, Reason: ydbexternal.UnsupportedReason}
	complete := schemaext.Knowledge{State: schemaext.Complete}
	sources := must.Must(ydbexternal.SourceCoverage(schemaext.Observed, complete,
		[]schemaext.SubjectCoverage{{Kind: ydbexternal.SourceKind, Subject: ydbexternal.SourceRef("ext", "s3"), Knowledge: unread}}))
	tables := must.Must(ydbexternal.TableCoverage(schemaext.Observed, complete,
		[]schemaext.SubjectCoverage{{Kind: ydbexternal.TableKind, Subject: ydbexternal.TableRef("ext", "events"), Knowledge: unread}}))
	held := &catalog.Database{DatabasePath: "/local", FeatureCoverage: must.Must(sources.Combine(tables))}
	desired := declaredExternal(
		ydbexternal.DesiredSourceObject("ext", "pg", "", warehouseSource("ext/pg_password")),
		ydbexternal.DesiredSourceObject("ext", "s3", "", s3Source()),
		ydbexternal.DesiredTableObject("ext", "events", "", eventsOver("ext/s3")),
	)

	diff, diagnostics, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), desired, held, &config.CompareOptions{Dialect: platform.YDB}, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diagnostics.Features, qt.HasLen, 2)
	c.Assert(diff.FeatureChanges, qt.DeepEquals, []schemaext.ChangeRecord{{Subject: ydbexternal.SourceRef("ext", "pg"),
		Value: &ydbdiff.ExternalDataSource{After: &ydbexternal.DesiredSource{Spec: warehouseSource("ext/pg_password")}}}})
}

// TestCompare_YDBExternalFailurePath refuses a plan that drops a data source
// a declared external table reads, which would keep the table over a source
// that is gone, and any change on a target without external data sources.
func TestCompare_YDBExternalFailurePath(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		held    *catalog.Database
		wantErr string
	}{
		{name: "a declared table over a source the plan drops",
			desired: declaredExternal(ydbexternal.DesiredSourceObject("ext", "pg", "", warehouseSource("ext/pg_password")),
				ydbexternal.DesiredTableObject("ext", "events", "", eventsOver("ext/s3"))),
			held: heldWarehouse(),
			wantErr: ".*external table ext/events reads data source ext/s3, which the plan drops; declare the data source " +
				"or move the table to one the plan keeps.*"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff, err := schemadiff.CompareWithDialect(t.Context(), test.desired, test.held, platform.YDB, must.Must(builtin.New()))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(diff, qt.IsNil)
		})
	}
}
