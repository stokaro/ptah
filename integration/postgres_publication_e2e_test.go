//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbtarget"
)

// publicationTables are the tables every publication test declares and holds:
// three in public and one in audit.
const publicationTables = `CREATE SCHEMA audit;
CREATE TABLE orders (id int PRIMARY KEY);
CREATE TABLE items (id int PRIMARY KEY);
CREATE TABLE refunds (id int PRIMARY KEY);
CREATE TABLE audit.events (id int PRIMARY KEY);
`

// publicationTarget is a scratch PostgreSQL database holding
// [publicationTables], and a role that may own a publication there.
type publicationTarget struct {
	url string
	db  *sql.DB
	// replicator is a role with CREATE on the database, which the server
	// requires of a publication's new owner, and admin is the role the test
	// connects as.
	replicator, admin string
}

// newPublicationTarget creates the target and runs held there, with
// {replicator} expanded, after the tables.
func newPublicationTarget(c *qt.C, held string) publicationTarget {
	c.Helper()
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbtarget.URL(c, dbtarget.PostgreSQL)))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	target := publicationTarget{replicator: fmt.Sprintf("ptah_pub_replicator_%d", time.Now().UnixNano())}
	_, err = admin.ExecContext(c.Context(), "CREATE ROLE "+quoteE2EIdent(target.replicator)+" NOLOGIN")
	c.Assert(err, qt.IsNil)
	// Registered before the database, so it runs after the database that
	// holds the role's grant and publications is gone.
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), "DROP ROLE "+quoteE2EIdent(target.replicator))
		c.Check(err, qt.IsNil)
	})
	c.Assert(admin.QueryRowContext(c.Context(), "SELECT current_user").Scan(&target.admin), qt.IsNil)
	target.url = postgresScratchDevURL(c, "ptah_publication")
	target.db, err = sql.Open("pgx", postgresFamilyDriverURL(c, target.url))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(target.db.Close(), qt.IsNil) })
	execPostgresFamilySQL(c, c.Context(), target.url, publicationTables+
		"GRANT CREATE ON DATABASE "+quoteE2EIdent(databaseNameOf(c, target.url))+" TO "+quoteE2EIdent(target.replicator)+";\n"+
		target.expand(held))
	return target
}

// expand is text with {replicator} and {admin} replaced.
func (t publicationTarget) expand(text string) string {
	return strings.NewReplacer("{replicator}", t.replicator, "{admin}", t.admin).Replace(text)
}

// schemaFile writes [publicationTables] and publications, expanded, as the
// desired schema.
func (t publicationTarget) schemaFile(c *qt.C, publications string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(publicationTables+t.expand(publications)), 0o600), qt.IsNil)
	return path
}

// publications reads back each publication with its settings and owner, and
// each table and schema it names, as the catalog stores them.
func (t publicationTarget) publications(c *qt.C) []string {
	c.Helper()
	return queryStrings(c, t.db, `SELECT pubname || ' all=' || puballtables || ' publish=' ||
		concat_ws(',', CASE WHEN pubinsert THEN 'insert' END, CASE WHEN pubupdate THEN 'update' END,
			CASE WHEN pubdelete THEN 'delete' END, CASE WHEN pubtruncate THEN 'truncate' END) ||
		' root=' || pubviaroot || ' owner=' || pg_get_userbyid(pubowner)
	FROM pg_publication
	UNION ALL
	SELECT p.pubname || ' table ' || c.relnamespace::regnamespace || '.' || c.relname
	FROM pg_publication_rel r JOIN pg_publication p ON p.oid = r.prpubid JOIN pg_class c ON c.oid = r.prrelid
	UNION ALL
	SELECT p.pubname || ' schema ' || n.nspname
	FROM pg_publication_namespace s JOIN pg_publication p ON p.oid = s.pnpubid JOIN pg_namespace n ON n.oid = s.pnnspid
	ORDER BY 1`)
}

// publicationIDs reads back each publication's oid by name.
func (t publicationTarget) publicationIDs(c *qt.C) []string {
	c.Helper()
	return queryStrings(c, t.db, `SELECT pubname || '=' || oid FROM pg_publication ORDER BY 1`)
}

// TestSchemaApplyManagesPublicationsE2E applies a desired schema that
// declares publications to a database holding others, reads the catalog
// back, and applies it again. The second apply plans nothing, so the read,
// the comparison and the statements agree on every row. A publication the
// schema leaves out is dropped once the schema declares any; while it
// declares none, a publication a replication consumer made for itself stays.
func TestSchemaApplyManagesPublicationsE2E(t *testing.T) {
	tests := []struct {
		name, held, declared string
		want                 []string
	}{
		{
			name: "every form created",
			declared: `CREATE PUBLICATION "Orders Feed" FOR TABLE orders, audit.events WITH (publish = 'insert, update');
ALTER PUBLICATION "Orders Feed" OWNER TO {replicator};
CREATE PUBLICATION audit_feed FOR TABLES IN SCHEMA audit;
CREATE PUBLICATION everything FOR ALL TABLES WITH (publish_via_partition_root = true);
`,
			want: []string{
				"Orders Feed all=false publish=insert,update root=false owner={replicator}",
				"Orders Feed table audit.events",
				"Orders Feed table public.orders",
				"audit_feed all=false publish=insert,update,delete,truncate root=false owner={admin}",
				"audit_feed schema audit",
				"everything all=true publish=insert,update,delete,truncate root=true owner={admin}",
			},
		},
		{
			name:     "a member added and one dropped",
			held:     "CREATE PUBLICATION feed FOR TABLE orders, refunds;",
			declared: "CREATE PUBLICATION feed FOR TABLE orders, items;\n",
			want: []string{
				"feed all=false publish=insert,update,delete,truncate root=false owner={admin}",
				"feed table public.items",
				"feed table public.orders",
			},
		},
		{
			name:     "a list made FOR ALL TABLES",
			held:     "CREATE PUBLICATION feed FOR TABLE orders;",
			declared: "CREATE PUBLICATION feed FOR ALL TABLES;\n",
			want:     []string{"feed all=true publish=insert,update,delete,truncate root=false owner={admin}"},
		},
		{
			name:     "one left out dropped",
			held:     "CREATE PUBLICATION feed FOR TABLE orders; CREATE PUBLICATION old FOR TABLE items;",
			declared: "CREATE PUBLICATION feed FOR TABLE orders;\n",
			want: []string{
				"feed all=false publish=insert,update,delete,truncate root=false owner={admin}",
				"feed table public.orders",
			},
		},
		{
			name: "none declared drops none",
			held: "CREATE PUBLICATION dbz_publication FOR ALL TABLES;",
			want: []string{"dbz_publication all=true publish=insert,update,delete,truncate root=false owner={admin}"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newPublicationTarget(c, test.held)
			schema := target.schemaFile(c, test.declared)

			first := runPtahIn(c, "schema", "apply", "--db-url", target.url, "--auto-approve", "--schema-file", schema)
			second := runPtahIn(c, "schema", "apply", "--db-url", target.url, "--auto-approve", "--schema-file", schema)

			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
			c.Assert(target.publications(c), qt.DeepEquals, strings.Split(target.expand(strings.Join(test.want, "\n")), "\n"))
			c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", second.Stdout, second.Stderr))
			c.Assert(second.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// TestSchemaApplyChangesAPublicationInPlaceE2E reads each publication's oid
// before and after the apply. A change of members is made with ALTER
// PUBLICATION, so a subscriber keeps its publication; a change between a list
// of tables and FOR ALL TABLES cannot be, so the publication is dropped and
// created again.
func TestSchemaApplyChangesAPublicationInPlaceE2E(t *testing.T) {
	tests := []struct {
		name, held, declared string
		inPlace              bool
	}{
		{name: "a member added", held: "CREATE PUBLICATION feed FOR TABLE orders;",
			declared: "CREATE PUBLICATION feed FOR TABLE orders, items;\n", inPlace: true},
		{name: "the publish set changed", held: "CREATE PUBLICATION feed FOR TABLE orders;",
			declared: "CREATE PUBLICATION feed FOR TABLE orders WITH (publish = 'insert');\n", inPlace: true},
		{name: "a list made FOR ALL TABLES", held: "CREATE PUBLICATION feed FOR TABLE orders;",
			declared: "CREATE PUBLICATION feed FOR ALL TABLES;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newPublicationTarget(c, test.held)
			before := target.publicationIDs(c)

			got := runPtahIn(c, "schema", "apply", "--db-url", target.url, "--auto-approve",
				"--schema-file", target.schemaFile(c, test.declared))

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(slices.Equal(target.publicationIDs(c), before), qt.Equals, test.inPlace)
		})
	}
}

// TestSchemaApplyLeavesPublicationsToAWholeDatabaseReadE2E applies a schema
// that declares a publication with the read narrowed to public. A publication
// belongs to the database rather than to a schema, so the narrowed read
// claims nothing about publications: the plan neither creates the declared
// one nor drops the one the database holds, and says the schema is not shown
// to be synced rather than calling it synced.
func TestSchemaApplyLeavesPublicationsToAWholeDatabaseReadE2E(t *testing.T) {
	c := qt.New(t)
	target := newPublicationTarget(c, "CREATE PUBLICATION old FOR TABLE items;")

	got := runPtahIn(c, "schema", "apply", "--db-url", target.url, "--auto-approve", "--schemas", "public",
		"--schema-file", target.schemaFile(c, "CREATE PUBLICATION feed FOR TABLE orders;\n"))

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Not(qt.Contains), "PUBLICATION")
	c.Assert(got.Stdout, qt.Not(qt.Contains), "Schema is synced")
	c.Assert(target.publications(c), qt.DeepEquals, []string{
		target.expand("old all=false publish=insert,update,delete,truncate root=false owner={admin}"),
		"old table public.items",
	})
}

// TestSchemaInspectRoundTripsPublicationsE2E inspects a database holding a
// publication of each membership form, applies the inspected schema to an
// empty database, and reads both catalogs back. They agree, so the inspected
// schema runs on an empty database in the order it is written and loses
// nothing a publication publishes.
func TestSchemaInspectRoundTripsPublicationsE2E(t *testing.T) {
	tests := []struct{ name, format, file string }{
		{name: "sql", format: "sql", file: "schema.sql"},
		{name: "hcl", format: "hcl", file: "schema.hcl"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := newPublicationTarget(c, `CREATE PUBLICATION by_schema FOR TABLES IN SCHEMA audit;
CREATE PUBLICATION "Orders Feed" FOR TABLE orders, audit.events WITH (publish = 'insert, update');
CREATE PUBLICATION everything FOR ALL TABLES WITH (publish_via_partition_root = true);
CREATE PUBLICATION empty;`)
			inspected := runPtahIn(c, "schema", "inspect", "--db-url", source.url, "--format", test.format)
			c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
			schema := filepath.Join(c.TempDir(), test.file)
			c.Assert(os.WriteFile(schema, []byte(inspected.Stdout), 0o600), qt.IsNil)
			copied := postgresScratchDevURL(c, "ptah_publication_copy")
			copiedDB, err := sql.Open("pgx", postgresFamilyDriverURL(c, copied))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(copiedDB.Close(), qt.IsNil) })

			applied := runPtahIn(c, "schema", "apply", "--db-url", copied, "--auto-approve", "--schema-file", schema)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("schema:\n%s\nstdout:\n%s\nstderr:\n%s", inspected.Stdout, applied.Stdout, applied.Stderr))
			c.Assert(publicationTarget{db: copiedDB}.publications(c), qt.DeepEquals, source.publications(c))
		})
	}
}

// TestSchemaDiffReplaysAPublicationIntoADevDatabaseE2E replays a schema that
// declares a publication into a dev database twice. A publication belongs to
// the database, so dropping the schemas the replay created leaves it, and the
// second replay's CREATE PUBLICATION would fail on it. The cleanup drops the
// publication the replay created, whether the dev URL leaves the database
// whole or pins one schema.
func TestSchemaDiffReplaysAPublicationIntoADevDatabaseE2E(t *testing.T) {
	tests := []struct{ name, query string }{
		{name: "the whole database"},
		{name: "one schema pinned", query: "search_path=public"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev := postgresScratchDevURL(c, "ptah_publication_dev")
			devDB, err := sql.Open("pgx", postgresFamilyDriverURL(c, dev))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(devDB.Close(), qt.IsNil) })
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte("CREATE TABLE orders (id int PRIMARY KEY);\nCREATE PUBLICATION feed FOR TABLE orders;\n"), 0o600), qt.IsNil)
			devURL := withQuery(c, dev, test.query)

			first := runPtahIn(c, "schema", "diff", "--from", schema, "--to", schema, "--dev-url", devURL)
			second := runPtahIn(c, "schema", "diff", "--from", schema, "--to", schema, "--dev-url", devURL)

			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
			c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", second.Stdout, second.Stderr))
			c.Assert(second.Stdout, qt.Contains, "Schemas are synced, no changes to be made.")
			c.Assert(queryStrings(c, devDB, "SELECT pubname FROM pg_publication"), qt.HasLen, 0)
		})
	}
}

// withQuery is target with query added to its query string.
func withQuery(c *qt.C, target, query string) string {
	c.Helper()
	parsed, err := url.Parse(target)
	c.Assert(err, qt.IsNil)
	extra, err := url.ParseQuery(query)
	c.Assert(err, qt.IsNil)
	values := parsed.Query()
	maps.Copy(values, extra)
	parsed.RawQuery = values.Encode()
	return parsed.String()
}

// TestSchemaDriftComparesPublicationsOnceTheSchemaDeclaresOneE2E checks drift
// against a database holding two publications. While the desired schema
// declares none, the comparison leaves both alone and the report names them as
// not compared, so a report of no drift does not read as one that covered
// them. Once the schema declares one, the other is drift.
func TestSchemaDriftComparesPublicationsOnceTheSchemaDeclaresOneE2E(t *testing.T) {
	tests := []struct {
		name, declared  string
		wantDrift       bool
		wantNotCompared []string
	}{
		{name: "none declared", wantNotCompared: []string{"publication feed", "publication old"}},
		{name: "one declared", declared: "CREATE PUBLICATION feed FOR TABLE orders;\n", wantDrift: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newPublicationTarget(c, "CREATE PUBLICATION feed FOR TABLE orders; CREATE PUBLICATION old FOR TABLE items;")

			got := runPtahIn(c, "schema", "drift", "--db-url", target.url, "--schema-file", target.schemaFile(c, test.declared), "--format", "json")

			var report struct {
				Drift       bool                      `json:"drift"`
				NotCompared []catalog.UnmodeledObject `json:"not_compared"`
			}
			c.Assert(json.Unmarshal([]byte(got.Stdout), &report), qt.IsNil,
				qt.Commentf("exit %d\nstdout:\n%s\nstderr:\n%s", got.ExitCode, got.Stdout, got.Stderr))
			var notCompared []string
			for _, object := range report.NotCompared {
				notCompared = append(notCompared, object.Kind+" "+object.Name)
			}
			c.Assert(report.Drift, qt.Equals, test.wantDrift, qt.Commentf("%s", got.Stdout))
			c.Assert(notCompared, qt.DeepEquals, test.wantNotCompared)
		})
	}
}

// TestSchemaApplyPublishesGeneratedColumnsWhereTheServerHasThemE2E applies a
// publication that publishes stored generated columns. PostgreSQL 18 has the
// setting, so the apply creates the publication with it and a second apply is
// synced. An older line has no such setting and publishes no generated column,
// so the apply is refused, naming the capability, and creates nothing rather
// than a publication that publishes less than the schema declares.
func TestSchemaApplyPublishesGeneratedColumnsWhereTheServerHasThemE2E(t *testing.T) {
	c := qt.New(t)
	target := newPublicationTarget(c, "")
	schema := target.schemaFile(c, "CREATE PUBLICATION feed FOR TABLE orders WITH (publish_generated_columns = stored);\n")
	var supported bool
	c.Assert(target.db.QueryRowContext(c.Context(), "SELECT current_setting('server_version_num')::int >= 180000").Scan(&supported), qt.IsNil)
	type outcome struct {
		applied   bool
		stderr    string
		published []string
		second    string
	}
	want := map[bool]outcome{
		true: {applied: true, published: []string{"feed=s"}, second: "Schema is synced, no changes to be made."},
		false: {published: make([]string, 0), stderr: `publication "feed" declares publish_generated_columns = stored, which requires target capability ` +
			`publication_generated_columns`},
	}[supported]

	first := runPtahIn(c, "schema", "apply", "--db-url", target.url, "--auto-approve", "--schema-file", schema)
	second := runPtahIn(c, "schema", "apply", "--db-url", target.url, "--auto-approve", "--schema-file", schema)

	c.Assert(first.ExitCode == 0, qt.Equals, want.applied, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
	c.Assert(first.Stderr, qt.Contains, want.stderr)
	c.Assert(queryStrings(c, target.db, `SELECT pubname || '=' || coalesce(to_jsonb(p)->>'pubgencols', '') FROM pg_publication p`),
		qt.DeepEquals, want.published)
	c.Assert(second.Stdout, qt.Contains, want.second)
}
