package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbexternal"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

// externalSource holds a PostgreSQL data source and an object storage one, in
// the description local-ydb 26.2.1.14 gave of each, and an external table over
// the second.
func externalSource() fakeSource {
	return fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{
			"/local":     {entry("ext", Ydb_Scheme.Entry_DIRECTORY)},
			"/local/ext": {entry("pg", Ydb_Scheme.Entry_EXTERNAL_DATA_SOURCE), entry("s3", Ydb_Scheme.Entry_EXTERNAL_DATA_SOURCE), entry("events", Ydb_Scheme.Entry_EXTERNAL_TABLE)},
		},
		sources: map[string]*Ydb_Table.DescribeExternalDataSourceResult{
			// #nosec G101 -- the path of a secret, as the server describes one, not a credential
			"/local/ext/pg": {SourceType: new("PostgreSQL"), Location: new("pg:5432"), Properties: map[string]string{
				"AUTH_METHOD": "BASIC", "DATABASE_NAME": "app", "LOGIN": "reader",
				"PASSWORD_SECRET_PATH": "/local/ext/pg_password", "PROTOCOL": "native", "REFERENCES": "[]",
			}},
			"/local/ext/s3": {SourceType: new("ObjectStorage"), Location: new("https://s3.example.test/b/"),
				Properties: map[string]string{"AUTH_METHOD": "NONE", "REFERENCES": `["/local/ext/events"]`}},
		},
		external: map[string]*Ydb_Table.DescribeExternalTableResult{
			"/local/ext/events": {
				SourceType: new("ObjectStorage"), DataSourcePath: new("/local/ext/s3"), Location: new("events/"),
				Columns: []*Ydb_Table.ColumnMeta{
					{Name: "id", Type: primitive(Ydb.Type_INT64)},
					{Name: "amount", Type: optional(decimal(22, 9))},
				},
				Content: map[string]string{"FORMAT": `["csv_with_names"]`, "CSV_DELIMITER": `[";"]`, "PARTITIONED_BY": `["id"]`},
			},
		},
	}
}

// TestReader_ReadsExternalObjects reads both kinds as a declaration writes
// them: the auth method out of the properties, the server's REFERENCES left
// out, a secret's path and a table's data source relative to the root, each
// table option the one value of its array, and NOT NULL from a type that is
// not Optional.
func TestReader_ReadsExternalObjects(t *testing.T) {
	c := qt.New(t)
	caps := capability.YDB262().With(capability.ExternalDataSources, true)

	db, err := ydbschema.NewReaderFromSource(externalSource(), "/local", caps).ReadSchemaContext(context.Background())

	c.Assert(err, qt.IsNil)
	objects, err := db.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []schemaext.Object{
		ydbexternal.ObservedSourceObject("ext", "pg", ydbexternal.DataSource{SourceType: "PostgreSQL", Location: "pg:5432", AuthMethod: "BASIC",
			Options: map[string]string{"DATABASE_NAME": "app", "LOGIN": "reader", "PASSWORD_SECRET_PATH": "ext/pg_password", "PROTOCOL": "native"}}),
		ydbexternal.ObservedSourceObject("ext", "s3", ydbexternal.DataSource{SourceType: "ObjectStorage", Location: "https://s3.example.test/b/",
			AuthMethod: "NONE"}),
		ydbexternal.ObservedTableObject("ext", "events", ydbexternal.Table{DataSource: "ext/s3", Location: "events/",
			Columns: []ydbexternal.Column{{Name: "id", Type: "Int64", NotNull: true}, {Name: "amount", Type: "Decimal(22,9)"}},
			Options: map[string]string{"FORMAT": "csv_with_names", "CSV_DELIMITER": ";", "PARTITIONED_BY": `["id"]`}}),
	})
	c.Assert(db.FeatureCoverage.Lookup(ydbexternal.SourceKind, ydbexternal.SourceRef("ext", "gone")).State, qt.Equals, schemaext.Complete)
	c.Assert(db.FeatureCoverage.Lookup(ydbexternal.TableKind, ydbexternal.TableRef("ext", "gone")).State, qt.Equals, schemaext.Complete)
}

// On a line without the key both kinds are recorded as unread rather than
// described, so a plan neither keeps nor drops one, and an object the read
// did not meet is still known absent.
func TestReader_RecordsExternalObjectsOnALineWithoutThem(t *testing.T) {
	c := qt.New(t)

	db, err := ydbschema.NewReaderFromSource(externalSource(), "/local", capability.YDB262()).ReadSchemaContext(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
	unread := schemaext.Knowledge{State: schemaext.Uninspected, Reason: ydbexternal.UnsupportedReason}
	c.Assert(db.FeatureCoverage.Lookup(ydbexternal.SourceKind, ydbexternal.SourceRef("ext", "s3")), qt.Equals, unread)
	c.Assert(db.FeatureCoverage.Lookup(ydbexternal.SourceKind, ydbexternal.SourceRef("ext", "pg")), qt.Equals, unread)
	c.Assert(db.FeatureCoverage.Lookup(ydbexternal.TableKind, ydbexternal.TableRef("ext", "events")), qt.Equals, unread)
	c.Assert(db.FeatureCoverage.Lookup(ydbexternal.TableKind, ydbexternal.TableRef("ext", "gone")).State, qt.Equals, schemaext.Complete)
}

// A table option the server writes as anything but an array of one string is
// refused rather than read as some other value.
func TestReader_RefusesAnExternalTableOptionItCannotRead(t *testing.T) {
	c := qt.New(t)
	source := externalSource()
	source.external["/local/ext/events"].Content = map[string]string{"FORMAT": `["a","b"]`}
	caps := capability.YDB262().With(capability.ExternalDataSources, true)

	db, err := ydbschema.NewReaderFromSource(source, "/local", caps).ReadSchemaContext(context.Background())

	c.Assert(err, qt.ErrorMatches, `YDB external table /local/ext/events: external table option FORMAT is \["a","b"\], `+
		`where the server writes a JSON array of one string`)
	c.Assert(db, qt.IsNil)
}
