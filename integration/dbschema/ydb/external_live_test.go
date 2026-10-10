//go:build integration

package ydb_test

import (
	"context"
	"maps"
	"path"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine/builtin"
	ydbschema "ptah.run/internal/dbschema/ydb"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
	"ptah.run/migration/schemadiff"
)

// The directories the external object tests write into: the objects at the
// top of one, and a data source in a directory below it, so an external table
// reads a source the walk meets in another directory.
const (
	externalSchema       = "ptah_ydb_external"
	externalSourceSchema = "ptah_ydb_external/src"
)

var externalSchemas = []string{externalSchema, externalSourceSchema}

// The flags that turn the external objects on, and CREATE OR REPLACE of them.
var (
	externalSourcesOn = clusterFlag{yaml: "enable_external_data_sources", page: "EnableExternalDataSources", on: true}
	externalReplaceOn = clusterFlag{yaml: "enable_replace_if_exists_for_external_entities",
		page: "EnableReplaceIfExistsForExternalEntities", on: true}
)

// The variable the 26.2 declaration's secret takes its value from.
const externalSecretEnv = "PTAH_SECRET_LIVE_EXTERNAL_PG" // #nosec G101 -- a variable name, not a credential

// externalLine is a release line with what its declaration of a PostgreSQL
// source names the password by: on 26.2 a YDB secret's path, the secret
// declared beside the source, and on 25.1, which takes no path, a deprecated
// secret object the test makes itself.
type externalLine struct {
	line     string
	password map[string]string
	secrets  []schemaext.Object
	setup    []string
	teardown []string
	// statements is how many statements the first plan holds.
	statements int
}

var externalLines = []externalLine{
	{
		line:       "26.2",
		password:   map[string]string{"PASSWORD_SECRET_PATH": externalSchema + "/pg_password"},
		secrets:    []schemaext.Object{ydbsecret.DesiredObject(externalSchema, "pg_password", "", externalSecretEnv)},
		statements: 4,
	},
	{
		line:       "25.1",
		password:   map[string]string{"PASSWORD_SECRET_NAME": "ptah_ydb_external_pw"}, // #nosec G101 -- a secret object's name
		setup:      []string{"CREATE OBJECT ptah_ydb_external_pw (TYPE SECRET) WITH value = 'probe'"},
		teardown:   []string{"DROP OBJECT ptah_ydb_external_pw (TYPE SECRET)"},
		statements: 3,
	},
}

// externalColumns are the columns of the external table the declaration
// holds.
var externalColumns = []ydbexternal.Column{
	{Name: "id", Type: "Int64", NotNull: true}, {Name: "kind", Type: "Utf8"}, {Name: "amount", Type: "Decimal(22,9)"},
}

// externalWarehouse is the PostgreSQL source a line declares, with the
// password named as the line says.
func externalWarehouse(line externalLine) ydbexternal.DataSource {
	return ydbexternal.DataSource{SourceType: "PostgreSQL", Location: "pg.invalid:5432", AuthMethod: "BASIC",
		Options: merged(map[string]string{"DATABASE_NAME": "app", "LOGIN": "reader"}, line.password)}
}

// externalBucket is the object storage source at location.
func externalBucket(location string) ydbexternal.DataSource {
	return ydbexternal.DataSource{SourceType: "ObjectStorage", Location: location, AuthMethod: "NONE"}
}

// externalEvents is the external table over the bucket with columns, which
// carries a semicolon in an option.
func externalEvents(columns []ydbexternal.Column) ydbexternal.Table {
	return ydbexternal.Table{DataSource: externalSourceSchema + "/bucket", Location: "2026/", Columns: columns,
		Options: map[string]string{"FORMAT": "csv_with_names", "CSV_DELIMITER": ";", "PARTITIONED_BY": `["id"]`}}
}

// externalDeclaration declares an object storage source in the nested
// directory, a PostgreSQL source whose password is named as the line says,
// with the line's secrets, and an external table with columns over the first.
func externalDeclaration(line externalLine, location string, columns []ydbexternal.Column) *schemamodel.Database {
	objects := append(slices.Clone(line.secrets),
		ydbexternal.DesiredSourceObject(externalSchema, "warehouse", "", externalWarehouse(line)),
		ydbexternal.DesiredSourceObject(externalSourceSchema, "bucket", "", externalBucket(location)),
		ydbexternal.DesiredTableObject(externalSchema, "events", "", externalEvents(columns)),
	)
	return &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(objects...))}
}

// externalObjects is every external data source and external table db holds.
func externalObjects(c *qt.C, db *catalog.Database) []schemaext.Object {
	c.Helper()
	objects, err := db.FeatureObjects.Select(func(ref objectidentity.ID) bool {
		kind := schemaext.Kind(ref.Kind)
		return kind == ydbexternal.SourceKind || kind == ydbexternal.TableKind
	}).All()
	c.Assert(err, qt.IsNil)
	return objects
}

// externalTeardown drops the test's directory and what it holds, with the
// flag still on, and runs the statements a line names besides. Every test
// that registers it has made the directory by then.
func externalTeardown(c *qt.C, conn *dbschema.DatabaseConnection, extra []string) {
	c.Helper()
	dropper, ok := conn.SchemaWriter().(interface {
		DropDirectory(ctx context.Context, dir string) error
	})
	c.Assert(ok, qt.IsTrue, qt.Commentf("the YDB schema writer %T removes no directory", conn.SchemaWriter()))
	ctx := context.Background()
	c.Check(dropper.DropDirectory(ctx, externalSchema), qt.IsNil)
	for _, statement := range extra {
		c.Check(conn.Writer().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("execute: %s", statement))
	}
}

// TestYDBExternal_RoundTrip_NothingLeftToPlan is the external objects'
// round trip on both lines with EnableExternalDataSources on: two data
// sources and an external table rendered, applied, read back as declared,
// compared with nothing left to plan, and applied again with nothing planned.
// A data source moved to another location is then dropped and created again
// with the external table over it, since the line takes no CREATE OR REPLACE
// with the flag for it off, and nothing is left to plan after.
func TestYDBExternal_RoundTrip_NothingLeftToPlan(t *testing.T) {
	for _, test := range externalLines {
		t.Run(test.line, func(t *testing.T) {
			t.Setenv(externalSecretEnv, "probe")
			c := qt.New(t)
			line := lineNamed(c, test.line)
			setClusterFlags(c, line, externalSourcesOn)
			conn := openYDB(c, line)
			c.Cleanup(func() { externalTeardown(c, conn, test.teardown) })
			apply(c, conn, test.setup)
			declared := externalDeclaration(test, "https://storage.invalid/events/", externalColumns)

			first := planAgainst(c, conn, declared, externalSchemas)
			apply(c, conn, first)

			c.Assert(conn.Info().Capabilities.Has(capability.ExternalDataSources), qt.IsTrue)
			c.Assert(conn.Info().Capabilities.Has(capability.ExternalObjectReplace), qt.IsFalse)
			c.Assert(first, qt.HasLen, test.statements)
			c.Assert(externalObjects(c, readScoped(c, conn, externalSchemas)), qt.DeepEquals, []schemaext.Object{
				ydbexternal.ObservedSourceObject(externalSchema, "warehouse", externalWarehouse(test)),
				ydbexternal.ObservedSourceObject(externalSourceSchema, "bucket", externalBucket("https://storage.invalid/events/")),
				ydbexternal.ObservedTableObject(externalSchema, "events", externalEvents(externalColumns)),
			})
			c.Assert(planAgainst(c, conn, declared, externalSchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, declared, externalSchemas))
			c.Assert(planAgainst(c, conn, declared, externalSchemas), qt.HasLen, 0)

			moved := externalDeclaration(test, "https://storage.invalid/moved/", externalColumns)
			replacement := planAgainst(c, conn, moved, externalSchemas)
			apply(c, conn, replacement)

			c.Assert(heads(replacement), qt.DeepEquals, []string{
				"DROP EXTERNAL TABLE `ptah_ydb_external/events`",
				"DROP EXTERNAL DATA SOURCE `ptah_ydb_external/src/bucket`",
				"CREATE EXTERNAL DATA SOURCE `ptah_ydb_external/src/bucket` WITH (",
				"CREATE EXTERNAL TABLE `ptah_ydb_external/events` (",
			})
			c.Assert(planAgainst(c, conn, moved, externalSchemas), qt.HasLen, 0)
			c.Assert(externalObjects(c, readScoped(c, conn, externalSchemas))[1], qt.DeepEquals,
				ydbexternal.ObservedSourceObject(externalSourceSchema, "bucket", externalBucket("https://storage.invalid/moved/")))
		})
	}
}

// TestYDBExternal_ReplacedInPlace turns CREATE OR REPLACE on as well: a moved
// data source and a changed external table are each replaced in one
// statement, the table over the source stays, and nothing is left to plan.
func TestYDBExternal_ReplacedInPlace(t *testing.T) {
	for _, test := range externalLines {
		t.Run(test.line, func(t *testing.T) {
			t.Setenv(externalSecretEnv, "probe")
			c := qt.New(t)
			line := lineNamed(c, test.line)
			setClusterFlags(c, line, externalSourcesOn, externalReplaceOn)
			conn := openYDB(c, line)
			c.Cleanup(func() { externalTeardown(c, conn, test.teardown) })
			apply(c, conn, test.setup)
			apply(c, conn, planAgainst(c, conn, externalDeclaration(test, "https://storage.invalid/events/", externalColumns),
				externalSchemas))
			changed := externalDeclaration(test, "https://storage.invalid/moved/",
				append(slices.Clone(externalColumns), ydbexternal.Column{Name: "note", Type: "Utf8"}))

			replacement := planAgainst(c, conn, changed, externalSchemas)
			apply(c, conn, replacement)

			c.Assert(conn.Info().Capabilities.Has(capability.ExternalObjectReplace), qt.IsTrue)
			c.Assert(heads(replacement), qt.DeepEquals, []string{
				"CREATE OR REPLACE EXTERNAL DATA SOURCE `ptah_ydb_external/src/bucket` WITH (",
				"CREATE OR REPLACE EXTERNAL TABLE `ptah_ydb_external/events` (",
			})
			c.Assert(planAgainst(c, conn, changed, externalSchemas), qt.HasLen, 0)
			table := externalObjects(c, readScoped(c, conn, externalSchemas))[2].Value.(*ydbexternal.ObservedTable)
			c.Assert(table.Spec.Columns, qt.HasLen, 4)
		})
	}
}

// TestYDBExternal_FailurePath_RefusedWithTheFlagOff plans a declared external
// object against a line at its default flags: the declaration is refused by
// the key before anything is compared, and the connection that read the flags
// says the key is off.
func TestYDBExternal_FailurePath_RefusedWithTheFlagOff(t *testing.T) {
	for _, test := range externalLines {
		t.Run(test.line, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, lineNamed(c, test.line))
			info := conn.Info()

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), externalDeclaration(test, "https://storage.invalid/events/", externalColumns),
				readScoped(c, conn, externalSchemas), info, nil, must.Must(builtin.New()))

			c.Assert(err, qt.ErrorMatches, "external data source ptah_ydb_external/warehouse, which requires target "+
				"capability external_data_sources, unavailable on this ydb target")
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(diff, qt.IsNil)
			c.Assert(info.Capabilities.Has(capability.ExternalDataSources), qt.IsFalse)
		})
	}
}

// TestYDBExternal_TeardownsDropThem holds both teardowns to the order YDB
// needs: an external table in one directory reads a data source in a
// directory the walk meets first, and the source goes only once the table has.
func TestYDBExternal_TeardownsDropThem(t *testing.T) {
	statements := []string{
		"CREATE EXTERNAL DATA SOURCE `ptah_ydb_external/a/source` WITH (SOURCE_TYPE = 'ObjectStorage', " +
			"LOCATION = 'https://storage.invalid/b/', AUTH_METHOD = 'NONE')",
		"CREATE EXTERNAL TABLE `ptah_ydb_external/z/files` (id Int64) WITH (DATA_SOURCE = 'ptah_ydb_external/a/source', " +
			"LOCATION = 'f/', FORMAT = 'json_each_row')",
	}
	for _, line := range ydbLines {
		t.Run(line.name+" DropDirectory", func(t *testing.T) {
			c := qt.New(t)
			setClusterFlags(c, line, externalSourcesOn)
			conn := openYDB(c, line)
			c.Cleanup(func() { removeTeardownLeftovers(c, line, conn) })
			apply(c, conn, statements)
			dropper, ok := conn.SchemaWriter().(interface {
				DropDirectory(ctx context.Context, dir string) error
			})
			c.Assert(ok, qt.IsTrue)

			c.Assert(dropper.DropDirectory(c.Context(), externalSchema+"/z"), qt.IsNil)
			c.Assert(dropper.DropDirectory(c.Context(), externalSchema+"/a"), qt.IsNil)

			c.Assert(directoryNames(c, c.Context(), line, externalSchema), qt.HasLen, 0)
		})
		t.Run(line.name+" DropAllTables", func(t *testing.T) {
			c := qt.New(t)
			setClusterFlags(c, line, externalSourcesOn)
			conn := openYDB(c, line)
			c.Cleanup(func() { removeTeardownLeftovers(c, line, conn) })
			apply(c, conn, statements)

			c.Assert(conn.SchemaWriter().DropAllTables(c.Context()), qt.IsNil)

			c.Assert(externalObjects(c, readScoped(c, conn, nil)), qt.HasLen, 0)
			c.Assert(directoryNames(c, c.Context(), line), qt.Not(qt.Contains), externalSchema)
		})
	}
}

// TestYDBChecks_RefuseAReadOfAnExternalTable runs a check that reads an
// external table on a server where the read would fetch the table's files
// from the data source's location. It is refused before it runs, by db verify
// and by a migration's check alike, and the migration's body does not run. A
// check over a row table is the control.
//
// A view over the table is left to the unit tests of the proof: CREATE VIEW
// compiles the view's query, which lists the bucket, and a location no server
// answers holds the statement until the compilation times out. Measured with
// a local S3 server on 25.1.4.7 and 26.2.1.14, a read of such a view fetches
// the files as a read of the table does.
func TestYDBChecks_RefuseAReadOfAnExternalTable(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			setClusterFlags(c, line, externalSourcesOn)
			conn := openYDB(c, line)
			c.Cleanup(func() { externalTeardown(c, conn, nil) })
			apply(c, conn, []string{
				"CREATE EXTERNAL DATA SOURCE `ptah_ydb_external/source` WITH (SOURCE_TYPE = 'ObjectStorage', " +
					"LOCATION = 'https://storage.invalid/b/', AUTH_METHOD = 'NONE')",
				"CREATE EXTERNAL TABLE `ptah_ydb_external/files` (id Int64) WITH (DATA_SOURCE = " +
					"'ptah_ydb_external/source', LOCATION = 'f/', FORMAT = 'json_each_row')",
				"CREATE TABLE `ptah_ydb_external/rows` (id Int64 NOT NULL, PRIMARY KEY (id))",
			})

			report, err := migrator.VerifyChecks(c.Context(), conn, []migrator.Check{
				{Name: "external", Assert: "SELECT COUNT(*) = 0 FROM `ptah_ydb_external/files`"},
				{Name: "rows", Assert: "SELECT COUNT(*) = 0 FROM `ptah_ydb_external/rows`"},
			})
			guarded := newMigrator(c, conn, map[string]string{
				"0000000001_guarded.up.sql": "-- +ptah check name=\"no_files\" assert=\"SELECT COUNT(*) = 0 FROM " +
					"`ptah_ydb_external/files`\"\nCREATE TABLE `ptah_ydb_external/never` (id Int64 NOT NULL, " +
					"PRIMARY KEY (id));\n",
				"0000000001_guarded.down.sql": "DROP TABLE `ptah_ydb_external/never`;\n",
			}, migrator.RevisionTableFormatPtah, externalSchema).MigrateUp(c.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(report.Results, qt.HasLen, 2)
			c.Assert(report.Results[0].Status, qt.Equals, migrator.VerifyStatusErrored)
			c.Assert(report.Results[0].Err, qt.ErrorMatches, "check assertion cannot be proved read-only: the query "+
				"reads rows from outside the database: /local/ptah_ydb_external/files is an external table, whose "+
				"rows the server fetches from its data source")
			c.Assert(report.Results[1].Status, qt.Equals, migrator.VerifyStatusVerified)
			var checkErr *migrator.CheckFailedError
			c.Assert(guarded, qt.ErrorAs, &checkErr)
			c.Assert(checkErr.Name, qt.Equals, "no_files")
			// The refusal, not a read that failed: a read of the table runs
			// until its compilation times out against the unreachable
			// location, and fails the check too.
			c.Assert(guarded, qt.ErrorIs, ydbschema.ErrReadLeavesDatabase)
			c.Assert(report.Results[0].Err, qt.ErrorIs, ydbschema.ErrReadLeavesDatabase)
			c.Assert(tableNames(readScoped(c, conn, externalSchemas)), qt.DeepEquals, []string{externalSchema + "|rows"})
		})
	}
}

// removeTeardownLeftovers removes what a teardown test leaves when it fails
// before its teardown ran, and nothing when it passed: each statement is
// guarded, and a directory that is gone or not empty is left as it is.
func removeTeardownLeftovers(c *qt.C, line ydbLine, conn *dbschema.DatabaseConnection) {
	c.Helper()
	ctx := context.Background()
	c.Check(conn.Writer().ExecuteSQL(ctx, "DROP EXTERNAL TABLE IF EXISTS `ptah_ydb_external/z/files`"), qt.IsNil)
	c.Check(conn.Writer().ExecuteSQL(ctx, "DROP EXTERNAL DATA SOURCE IF EXISTS `ptah_ydb_external/a/source`"), qt.IsNil)
	driver, err := ydbsdk.Open(ctx, dbtarget.DriverDSN(c, line.engine))
	c.Assert(err, qt.IsNil)
	defer func() { _ = driver.Close(ctx) }()
	for _, dir := range []string{"ptah_ydb_external/z", "ptah_ydb_external/a", "ptah_ydb_external"} {
		_ = driver.Scheme().RemoveDirectory(ctx, path.Join(driver.Name(), dir))
	}
}

// merged is a copy of base with extra's entries added.
func merged(base, extra map[string]string) map[string]string {
	out := maps.Clone(base)
	maps.Copy(out, extra)
	return out
}

// heads is the first line of each statement.
func heads(statements []string) []string {
	out := make([]string, 0, len(statements))
	for _, statement := range statements {
		head, _, _ := strings.Cut(statement, "\n")
		out = append(out, head)
	}
	return out
}
