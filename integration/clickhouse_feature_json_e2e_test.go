//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasreport"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/featurejson"
)

// A ClickHouse TTL change is an owner change, and the JSON documents that
// carry a diff wrote none: `schema diff --format json` and `schema drift
// --format json` exited 2 with "feature data requires an explicit codec
// registry", and the compat diff template's `json` refused a side holding
// table settings (stokaro/ptah#4279). These run each command against a live
// table whose TTL differs from the declared one and read the document back
// through the codecs.

// clickHouseTTLFixture is a live table with a one-day TTL, a dev database
// beside it, and a schema file that declares a two-day TTL.
type clickHouseTTLFixture struct {
	url, devURL, schema string
}

func newClickHouseTTLFixture(c *qt.C) clickHouseTTLFixture {
	c.Helper()
	url := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
	devURL := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
	_, err := connectDevDialect(c, url).ExecContext(c.Context(),
		"CREATE TABLE events (id UInt64, at DateTime) ENGINE = MergeTree ORDER BY id TTL at + INTERVAL 1 DAY")
	c.Assert(err, qt.IsNil)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(
		"CREATE TABLE events (id UInt64, at DateTime) ENGINE = MergeTree ORDER BY id TTL at + INTERVAL 2 DAY;\n"), 0o600), qt.IsNil)
	return clickHouseTTLFixture{url: url, devURL: devURL, schema: schema}
}

// tableTTLChange returns the one ClickHouse table change of a decoded list.
func tableTTLChange(c *qt.C, changes []schemaext.ChangeRecord) *chdiff.Table {
	c.Helper()
	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Subject.Name.Source, qt.Equals, "events")
	change, ok := changes[0].Value.(*chdiff.Table)
	c.Assert(ok, qt.IsTrue, qt.Commentf("decoded %T", changes[0].Value))
	return change
}

// The diff document carries the TTL change as an envelope, beside the
// statement it plans, and reads back as the change the comparison made.
func TestSchemaDiffJSONEncodesAClickHouseTTLChangeE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newClickHouseTTLFixture(c)
	codecs := must.Must(builtin.New()).Codecs()

	got := clirun.Run(c, clirun.Ptah, clirun.Options{}, "schema", "diff", "--from", fixture.url, "--to", fixture.schema,
		"--dev-url", fixture.devURL, "--format", "json")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	stdout := got.Stdout
	var document struct {
		Statements []string `json:"statements"`
		Changes    struct {
			TablesModified []struct {
				TableName      string                   `json:"table_name"`
				FeatureChanges []schemaext.ChangeRecord `json:"feature_changes"`
			} `json:"tables_modified"`
		} `json:"changes"`
	}
	c.Assert(featurejson.Unmarshal(c.Context(), codecs, schemaext.Desired, []byte(stdout), &document), qt.IsNil, qt.Commentf("stdout:\n%s", stdout))
	c.Assert(document.Statements, qt.DeepEquals, []string{"ALTER TABLE events MODIFY TTL at + INTERVAL 2 DAY"})
	c.Assert(document.Changes.TablesModified, qt.HasLen, 1)
	change := tableTTLChange(c, document.Changes.TablesModified[0].FeatureChanges)
	c.Assert(change.Before.TTL, qt.Equals, "at + toIntervalDay(1)")
	c.Assert(change.After.TTL, qt.DeepEquals, chschema.Setting{State: chschema.Explicit, Value: "at + INTERVAL 2 DAY"})
	c.Assert(stdout, qt.Contains, `"kind": "ptah.run/clickhouse/table-change"`)
	c.Assert(stdout, qt.Contains, `"owner": "ptah.run/clickhouse"`)
}

// The drift document reports the drift, exits 1 as drift does, and carries
// the same change.
func TestSchemaDriftJSONEncodesAClickHouseTTLChangeE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newClickHouseTTLFixture(c)
	codecs := must.Must(builtin.New()).Codecs()

	got := clirun.Run(c, clirun.Ptah, clirun.Options{}, "schema", "drift", "--schema-file", fixture.schema,
		"--db-url", fixture.url, "--format", "json")

	c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stderr:\n%s", got.Stderr))
	stdout := got.Stdout
	var document struct {
		Drift bool `json:"drift"`
		Diff  struct {
			TablesModified []struct {
				FeatureChanges []schemaext.ChangeRecord `json:"feature_changes"`
			} `json:"tables_modified"`
		} `json:"diff"`
	}
	c.Assert(featurejson.Unmarshal(c.Context(), codecs, schemaext.Desired, []byte(stdout), &document), qt.IsNil, qt.Commentf("stdout:\n%s", stdout))
	c.Assert(document.Drift, qt.IsTrue)
	c.Assert(document.Diff.TablesModified, qt.HasLen, 1)
	change := tableTTLChange(c, document.Diff.TablesModified[0].FeatureChanges)
	c.Assert(change.Before.TTL, qt.Equals, "at + toIntervalDay(1)")
}

// With the template helpers on, the compat diff's `json` writes the current
// side with its table settings, and the settings read back.
func TestCompatSchemaDiffTemplateJSONEncodesTableSettingsE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newClickHouseTTLFixture(c)
	codecs := must.Must(builtin.New()).Codecs()

	got := clirun.Run(c, clirun.Compat, clirun.Options{Env: append(os.Environ(), atlasreport.SchemaDiffTemplateHelpersEnvVar+"=1")},
		"schema", "diff", "--from", fixture.url, "--to", "file://"+filepath.ToSlash(fixture.schema),
		"--dev-url", fixture.devURL, "--format", "{{ json .From }}")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	var from schemamodel.Database
	c.Assert(featurejson.Unmarshal(c.Context(), codecs, schemaext.Desired, []byte(got.Stdout), &from), qt.IsNil, qt.Commentf("stdout:\n%s", got.Stdout))
	c.Assert(from.Tables, qt.HasLen, 1)
	settings, found, err := schemaext.FacetAs[*chschema.DesiredTable](from.Tables[0].Facets, chschema.TableKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(settings.TTL, qt.DeepEquals, chschema.Setting{State: chschema.Explicit, Value: "at + toIntervalDay(1)"})
}
