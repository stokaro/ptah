//go:build integration

package partitionof_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql" // registers the mysql driver a raw refusal below runs through
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver a raw refusal below runs through

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/dbtarget"
)

// ptah runs the native binary's command tree with empty standard input, which a
// confirmation prompt reads as a declined answer.
func ptah(args ...string) (string, error) {
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// ptahStreams is ptah with the document a command writes kept apart from the notes it
// writes about the document.
func ptahStreams(args ...string) (stdout, stderr string, err error) {
	cmd := root.NewRootCommand()
	var out, diagnostics bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diagnostics)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), diagnostics.String(), err
}

// compat runs the Atlas-compatible command tree under its default policy.
func compat(args ...string) (string, error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// scratch is an empty database of its own. Its session time zone is fixed, because a
// bound on a timestamptz key reads back in the session's zone.
type scratch struct {
	url  string
	conn *dbschema.DatabaseConnection
}

func newScratch(c *qt.C, engine dbtarget.Engine) scratch {
	c.Helper()
	adminURL := dbtarget.URL(c, engine)
	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	random := make([]byte, 6)
	_, err = rand.Read(random)
	c.Assert(err, qt.IsNil)
	name := "ptah_partitionof_" + hex.EncodeToString(random)
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		c.Check(dropErr, qt.IsNil)
	})
	renamed, err := atlasurl.WithDatabaseName(adminURL, name)
	c.Assert(err, qt.IsNil)
	parsed, err := url.Parse(renamed)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("timezone", "UTC")
	parsed.RawQuery = query.Encode()
	conn, err := dbschema.ConnectToDatabase(c.Context(), parsed.String())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return scratch{url: parsed.String(), conn: conn}
}

func writeSchema(c *qt.C, source string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)
	return path
}

func (s scratch) exec(c *qt.C, statements string) {
	c.Helper()
	_, err := s.conn.ExecContext(c.Context(), statements)
	c.Assert(err, qt.IsNil)
}

// partitions reads every partition's parent and bound from the catalog.
func (s scratch) partitions(c *qt.C) map[string]string {
	c.Helper()
	rows, err := s.conn.QueryContext(c.Context(), `
		SELECT n.nspname || '.' || t.relname, p_n.nspname || '.' || p.relname || ' ' || pg_get_expr(t.relpartbound, t.oid)
		FROM pg_class t
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN pg_inherits i ON i.inhrelid = t.oid
		JOIN pg_class p ON p.oid = i.inhparent
		JOIN pg_namespace p_n ON p_n.oid = p.relnamespace
		WHERE t.relispartition AND t.relkind IN ('r', 'p') AND n.nspname NOT LIKE 'pg_temp%'
		ORDER BY 1`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	found := make(map[string]string)
	for rows.Next() {
		var name, bound string
		c.Assert(rows.Scan(&name, &bound), qt.IsNil)
		found[name] = bound
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}

// rowHome reads which partition holds the row whose id is given.
func (s scratch) rowHome(c *qt.C, id int) string {
	c.Helper()
	var home string
	c.Assert(s.conn.QueryRowContext(c.Context(), "SELECT tableoid::regclass::text FROM events WHERE id = $1", id).Scan(&home), qt.IsNil)
	return home
}

func (s scratch) count(c *qt.C, query string) int {
	c.Helper()
	var count int
	c.Assert(s.conn.QueryRowContext(c.Context(), query).Scan(&count), qt.IsNil)
	return count
}

func (s scratch) apply(c *qt.C, source string, schemas ...string) string {
	c.Helper()
	args := []string{"schema", "apply", "--db-url", s.url, "--schema-file", writeSchema(c, source), "--auto-approve"}
	args = append(args, schemaFlags(schemas)...)
	out, err := ptah(args...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return out
}

func schemaFlags(schemas []string) []string {
	return []string{"--schemas", strings.Join(append([]string{"public"}, schemas...), ",")}
}

func (s scratch) assertSynced(c *qt.C, source string, schemas ...string) {
	c.Helper()
	path := writeSchema(c, source)
	flags := schemaFlags(schemas)
	plan, err := ptah(append([]string{"schema", "plan", "--db-url", s.url, "--schema-file", path, "--dry-run"}, flags...)...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
	c.Assert(plan, qt.Contains, "Schema is synced", qt.Commentf("%s", plan))
	compared, err := ptah(append([]string{"schema", "compare", "--db-url", s.url, "--schema-file", path}, flags...)...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", compared))
	c.Assert(compared, qt.Contains, "No schema differences detected", qt.Commentf("%s", compared))
	drift, err := ptah(append([]string{"schema", "drift", "--db-url", s.url, "--schema-file", path}, flags...)...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", drift))
	c.Assert(drift, qt.Contains, "No schema drift detected", qt.Commentf("%s", drift))
}

const roundTripDocument = `
CREATE SCHEMA archive;

CREATE TABLE events (
    id bigint NOT NULL,
    happened_at timestamptz NOT NULL,
    PRIMARY KEY (id, happened_at)
) PARTITION BY RANGE (happened_at);

CREATE TABLE events_2026 PARTITION OF events
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE archive.events_2020 PARTITION OF events
    FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE "Events Rest" PARTITION OF public.events DEFAULT;

CREATE TABLE regions (code text NOT NULL) PARTITION BY LIST (code);
CREATE TABLE regions_eu PARTITION OF regions FOR VALUES IN ('de', 'fr');
CREATE TABLE regions_rest PARTITION OF regions DEFAULT;

CREATE TABLE "Hashed" (id bigint NOT NULL) PARTITION BY HASH (id);
CREATE TABLE "Hashed_0" PARTITION OF "Hashed" FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE "Hashed_1" PARTITION OF "Hashed" FOR VALUES WITH (modulus 2, remainder 1);

CREATE TABLE sales (region text NOT NULL, sold date NOT NULL) PARTITION BY LIST (region);
CREATE TABLE sales_eu PARTITION OF sales FOR VALUES IN ('eu') PARTITION BY RANGE (sold);
CREATE TABLE sales_eu_2026 PARTITION OF sales_eu FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
`

func roundTripPartitions() map[string]string {
	return map[string]string{
		"archive.events_2020":  "public.events FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2021-01-01 00:00:00+00')",
		"public.Events Rest":   "public.events DEFAULT",
		"public.Hashed_0":      "public.Hashed FOR VALUES WITH (modulus 2, remainder 0)",
		"public.Hashed_1":      "public.Hashed FOR VALUES WITH (modulus 2, remainder 1)",
		"public.events_2026":   "public.events FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2027-01-01 00:00:00+00')",
		"public.regions_eu":    "public.regions FOR VALUES IN ('de', 'fr')",
		"public.regions_rest":  "public.regions DEFAULT",
		"public.sales_eu":      "public.sales FOR VALUES IN ('eu')",
		"public.sales_eu_2026": "public.sales_eu FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
	}
}

// TestPartitionOfRoundTripE2E applies a document holding every bound form, a partition
// in another schema, quoted names and a partition of a partition, and reads the catalog
// back. The next plan is synced, because the server's spelling of each bound agrees
// with the document's, and a partition added and a partition removed each converge.
func TestPartitionOfRoundTripE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "postgres", engine: dbtarget.PostgreSQL},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev := newScratch(c, test.engine)

			dev.apply(c, roundTripDocument, "archive")
			want := roundTripPartitions()
			c.Assert(dev.partitions(c), qt.DeepEquals, want)
			dev.assertSynced(c, roundTripDocument, "archive")

			dev.exec(c, "INSERT INTO events VALUES (1, '2026-06-01'), (2, '2020-06-01'), (3, '2030-06-01')")
			c.Assert(dev.rowHome(c, 1), qt.Equals, "events_2026")
			c.Assert(dev.rowHome(c, 2), qt.Equals, "archive.events_2020")
			c.Assert(dev.rowHome(c, 3), qt.Equals, `"Events Rest"`)

			added := roundTripDocument + "CREATE TABLE events_2027 PARTITION OF events FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');\n"
			out := dev.apply(c, added, "archive")
			c.Assert(out, qt.Contains, `CREATE TABLE "events_2027" PARTITION OF "events" FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');`)
			want["public.events_2027"] = "public.events FOR VALUES FROM ('2027-01-01 00:00:00+00') TO ('2028-01-01 00:00:00+00')"
			c.Assert(dev.partitions(c), qt.DeepEquals, want)
			dev.assertSynced(c, added, "archive")

			out = dev.apply(c, roundTripDocument, "archive")
			c.Assert(out, qt.Contains, `DROP TABLE "events_2027";`)
			delete(want, "public.events_2027")
			c.Assert(dev.partitions(c), qt.DeepEquals, want)
			dev.assertSynced(c, roundTripDocument, "archive")
			c.Assert(dev.count(c, "SELECT count(*) FROM events"), qt.Equals, 3)
		})
	}
}

// TestPartitionDropWithRowsIsDestructiveE2E holds a partition's removal to what a table
// drop gets: a plan that says so, a prompt that declines without an answer, and the rows
// still there afterwards. The removal of the partition keeps the rows of its sibling.
func TestPartitionDropWithRowsIsDestructiveE2E(t *testing.T) {
	c := qt.New(t)
	dev := newScratch(c, dbtarget.PostgreSQL)
	const parent = `CREATE TABLE events (id bigint NOT NULL, happened_at timestamptz NOT NULL, PRIMARY KEY (id, happened_at)) PARTITION BY RANGE (happened_at);
`
	const keep = "CREATE TABLE events_2027 PARTITION OF events FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');\n"
	dev.apply(c, parent+keep+"CREATE TABLE events_2026 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');\n")
	dev.exec(c, "INSERT INTO events VALUES (1, '2026-06-01'), (2, '2027-06-01')")
	path := writeSchema(c, parent+keep)

	plan, err := ptah("schema", "plan", "--db-url", dev.url, "--schema-file", path, "--schemas", "public", "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
	c.Assert(plan, qt.Contains, `"destructive": true`)
	c.Assert(plan, qt.Contains, `"sql": "DROP TABLE \"events_2026\""`)
	c.Assert(plan, qt.Contains, `"reason": "DROP TABLE removes the table and all rows"`)

	declined, err := ptah("schema", "apply", "--db-url", dev.url, "--schema-file", path, "--schemas", "public")
	c.Assert(err, qt.ErrorMatches, `read schema apply confirmation: .*`, qt.Commentf("%s", declined))
	c.Assert(dev.count(c, "SELECT count(*) FROM events_2026"), qt.Equals, 1)

	drift, err := ptah("schema", "drift", "--db-url", dev.url, "--schema-file", path, "--schemas", "public")
	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", drift))
	c.Assert(drift, qt.Contains, "tables_removed: 1 (destructive)")

	dev.apply(c, parent+keep)
	c.Assert(dev.count(c, "SELECT count(*) FROM pg_class WHERE relname = 'events_2026'"), qt.Equals, 0)
	c.Assert(dev.rowHome(c, 2), qt.Equals, "events_2027")
	dev.assertSynced(c, parent+keep)
}

const childrenDocument = `
CREATE TABLE tenants (id bigint PRIMARY KEY);
CREATE TABLE countries (code text PRIMARY KEY);
CREATE TABLE events (
    id bigserial NOT NULL,
    tenant bigint NOT NULL REFERENCES tenants (id),
    country text,
    label text DEFAULT 'x',
    happened_at timestamptz NOT NULL,
    PRIMARY KEY (id, happened_at),
    CONSTRAINT uq_events UNIQUE (tenant, id, happened_at),
    CONSTRAINT id_positive CHECK (id > 0)
) PARTITION BY RANGE (happened_at);
CREATE INDEX idx_events_tenant ON events (tenant);
CREATE FUNCTION touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER events_row BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION touch();
COMMENT ON TABLE events IS 'parent';

CREATE TABLE events_2026 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
COMMENT ON TABLE events_2026 IS 'this year';
CREATE INDEX idx_events_2026_label ON events_2026 (label);
ALTER TABLE events_2026 ADD CONSTRAINT label_short CHECK (length(label) < 10);
ALTER TABLE events_2026 ADD CONSTRAINT events_2026_country_fkey FOREIGN KEY (country) REFERENCES countries (code);
GRANT SELECT ON events_2026 TO PUBLIC;
CREATE TRIGGER events_2026_row BEFORE UPDATE ON events_2026 FOR EACH ROW EXECUTE FUNCTION touch();

CREATE TABLE events_rest PARTITION OF events DEFAULT;
CREATE TABLE references_partition (id bigint PRIMARY KEY, event bigint NOT NULL, day timestamptz NOT NULL,
    CONSTRAINT references_partition_event FOREIGN KEY (event, day) REFERENCES events_2026 (id, happened_at));
CREATE VIEW recent AS SELECT * FROM events_2026;
`

// TestPartitionChildrenConvergeE2E pins what the server copies onto a partition and what
// the partition holds for itself. The copies of the parent's key, unique key, foreign key,
// check, index and row trigger plan nothing, and a removal of the parent's objects
// plans nothing on the partition; the partition's own objects are compared and removed.
func TestPartitionChildrenConvergeE2E(t *testing.T) {
	c := qt.New(t)
	dev := newScratch(c, dbtarget.PostgreSQL)

	dev.apply(c, childrenDocument)
	c.Assert(dev.count(c, "SELECT count(*) FROM pg_constraint WHERE conrelid = 'events_2026'::regclass AND (conparentid <> 0 OR NOT conislocal) AND contype IN ('p', 'u', 'f', 'c')"), qt.Equals, 4)
	c.Assert(dev.count(c, "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'events_2026'::regclass AND tgparentid <> 0 AND NOT tgisinternal"), qt.Equals, 1)
	dev.assertSynced(c, childrenDocument)

	grown := strings.Replace(childrenDocument, "    happened_at timestamptz NOT NULL,\n", "    happened_at timestamptz NOT NULL,\n    note text,\n", 1) +
		"CREATE INDEX idx_events_note ON events (note);\nALTER TABLE events ADD CONSTRAINT note_short CHECK (length(note) < 99);\n"
	out := dev.apply(c, grown)
	c.Assert(out, qt.Contains, `ALTER TABLE "events" ADD COLUMN "note" text;`)
	c.Assert(out, qt.Not(qt.Contains), `ALTER TABLE "events_2026" ADD COLUMN`)
	c.Assert(dev.count(c, "SELECT count(*) FROM information_schema.columns WHERE table_name = 'events_2026' AND column_name = 'note'"), qt.Equals, 1)
	dev.assertSynced(c, grown)

	shrunk := strings.NewReplacer(
		"ALTER TABLE events_2026 ADD CONSTRAINT label_short CHECK (length(label) < 10);\n", "",
		"CREATE INDEX idx_events_2026_label ON events_2026 (label);\n", "",
		"CREATE TRIGGER events_2026_row BEFORE UPDATE ON events_2026 FOR EACH ROW EXECUTE FUNCTION touch();\n", "",
	).Replace(grown)
	out = dev.apply(c, shrunk)
	c.Assert(out, qt.Contains, `ALTER TABLE "events_2026" DROP CONSTRAINT "label_short";`)
	c.Assert(out, qt.Contains, `DROP INDEX "idx_events_2026_label";`)
	c.Assert(out, qt.Contains, `DROP TRIGGER "events_2026_row" ON "events_2026";`)
	c.Assert(out, qt.Not(qt.Contains), `DROP TRIGGER "events_row"`)
	dev.assertSynced(c, shrunk)
}

// TestPartitionCopiesPlanNothingForASourceThatDoesNotDeclareThemE2E runs against a document
// that leaves the partition out. Each removal the plan holds has to be one the server
// takes, so the plan runs to the end: a DROP of a trigger copy or of a constraint copy
// answers `cannot drop` and stops it.
func TestPartitionCopiesPlanNothingForASourceThatDoesNotDeclareThemE2E(t *testing.T) {
	c := qt.New(t)
	dev := newScratch(c, dbtarget.PostgreSQL)
	const parent = `
CREATE TABLE tenants (id bigint PRIMARY KEY);
CREATE TABLE events (
    id bigint NOT NULL,
    tenant bigint NOT NULL REFERENCES tenants (id),
    happened_at timestamptz NOT NULL,
    PRIMARY KEY (id, happened_at),
    CONSTRAINT id_positive CHECK (id > 0)
) PARTITION BY RANGE (happened_at);
CREATE INDEX idx_events_tenant ON events (tenant);
CREATE FUNCTION touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER events_row BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION touch();
`
	dev.apply(c, parent+"CREATE TABLE events_2026 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');\n")

	out := dev.apply(c, parent)

	c.Assert(out, qt.Contains, `DROP TABLE "events_2026";`)
	c.Assert(out, qt.Not(qt.Contains), `DROP TRIGGER`)
	c.Assert(out, qt.Not(qt.Contains), `DROP CONSTRAINT`)
	c.Assert(out, qt.Not(qt.Contains), `DROP INDEX`)
	c.Assert(dev.count(c, "SELECT count(*) FROM pg_class WHERE relname = 'events_2026'"), qt.Equals, 0)
	dev.assertSynced(c, parent)
}

// TestPartitionMembershipChangesAreRefusedE2E names each change Ptah does not plan, and
// reads back that nothing moved.
func TestPartitionMembershipChangesAreRefusedE2E(t *testing.T) {
	const parent = "CREATE TABLE events (id bigint NOT NULL, day date NOT NULL) PARTITION BY RANGE (day);\n"
	const other = "CREATE TABLE other (id bigint NOT NULL, day date NOT NULL) PARTITION BY RANGE (day);\n"
	const partition = "CREATE TABLE e1 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');\n"
	const plain = "CREATE TABLE plain (id bigint NOT NULL, day date NOT NULL);\n"
	current := parent + other + partition + plain
	for _, test := range []struct {
		name, desired, want string
	}{
		{name: "another bound", desired: parent + other + plain + "CREATE TABLE e1 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2028-01-01');\n",
			want: `is a partition with bound FOR VALUES FROM ('2026-01-01') TO ('2027-01-01'), and the declaration gives it FOR VALUES FROM ('2026-01-01') TO ('2028-01-01'); a reviewed manual partition cutover is required`},
		{name: "another parent", desired: parent + other + plain + "CREATE TABLE e1 PARTITION OF other FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');\n",
			want: `is a partition of events, and the declaration makes it a partition of other; a reviewed manual partition cutover is required`},
		{name: "a partition declared as a table", desired: parent + other + plain + "CREATE TABLE e1 (id bigint NOT NULL, day date NOT NULL);\n",
			want: `is a partition of events, and the declaration makes it an ordinary table; a reviewed manual partition cutover is required`},
		{name: "a table declared as a partition", desired: parent + other + partition + "CREATE TABLE plain PARTITION OF events FOR VALUES FROM ('2030-01-01') TO ('2031-01-01');\n",
			want: `is an ordinary table, and the declaration makes it a partition of events; a reviewed manual partition cutover is required`},
		{name: "a parent left out", desired: other + plain + partition,
			want: `is declared a partition of public.events, which the database holds and the desired schema does not declare; dropping a partitioned table drops its partitions`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev := newScratch(c, dbtarget.PostgreSQL)
			dev.apply(c, current)
			before := dev.partitions(c)

			out, err := ptah("schema", "apply", "--db-url", dev.url, "--schema-file", writeSchema(c, test.desired), "--schemas", "public", "--auto-approve")

			c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
			c.Assert(err.Error(), qt.Contains, test.want)
			c.Assert(dev.partitions(c), qt.DeepEquals, before)
		})
	}
}

// TestPartitionMigrationsE2E generates a partition's migrations, applies them, holds the
// removal at the destructive gates, and rolls the removal back to a partition the rows
// route to again.
func TestPartitionMigrationsE2E(t *testing.T) {
	c := qt.New(t)
	dev := newScratch(c, dbtarget.PostgreSQL)
	dir := c.TempDir()
	const kept = `CREATE TABLE events (id bigint NOT NULL, day date NOT NULL, PRIMARY KEY (id, day)) PARTITION BY RANGE (day);
CREATE TABLE e1 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
`
	const removed = "CREATE TABLE e2 PARTITION OF events FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');\n"

	out, err := ptah("migrations", "generate", "--schema-file", writeSchema(c, kept+removed), "--db-url", dev.url, "--migrations-dir", dir, "--name", "init")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = ptah("migrations", "up", "--db-url", dev.url, "--migrations-dir", dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	first := migrationVersion(c, dir)
	c.Assert(dev.partitions(c), qt.DeepEquals, map[string]string{
		"public.e1": "public.events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
		"public.e2": "public.events FOR VALUES FROM ('2027-01-01') TO ('2028-01-01')",
	})
	dev.exec(c, "INSERT INTO events VALUES (1, '2026-06-01')")

	// A version names a second, so the second migration waits for the next one.
	time.Sleep(1100 * time.Millisecond)
	shrink := writeSchema(c, kept)
	out, err = ptah("migrations", "generate", "--schema-file", shrink, "--db-url", dev.url, "--migrations-dir", dir, "--name", "checked", "--check-destructive")
	c.Assert(err, qt.ErrorMatches, "destructive migration statements require AllowDestructive", qt.Commentf("%s", out))
	out, err = ptah("migrations", "generate", "--schema-file", shrink, "--db-url", dev.url, "--migrations-dir", dir, "--name", "shrink")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	up, down := migrationFile(c, dir, "shrink", "up"), migrationFile(c, dir, "shrink", "down")
	c.Assert(up, qt.Contains, `DROP TABLE "e2";`)
	c.Assert(down, qt.Contains, `CREATE TABLE "e2" PARTITION OF "events" FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');`)

	out, err = ptah("migrations", "up", "--db-url", dev.url, "--migrations-dir", dir)
	c.Assert(err, qt.ErrorMatches, `(?s)error running migrations: pending migrations contain destructive statements.*DS101.*`, qt.Commentf("%s", out))
	c.Assert(dev.partitions(c), qt.HasLen, 2)

	out, err = ptah("migrations", "up", "--db-url", dev.url, "--migrations-dir", dir, "--allow-destructive")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(dev.partitions(c), qt.HasLen, 1)

	out, err = ptah("migrations", "down", "--db-url", dev.url, "--migrations-dir", dir, "--target", first, "--confirm")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(dev.partitions(c)["public.e2"], qt.Equals, "public.events FOR VALUES FROM ('2027-01-01') TO ('2028-01-01')")
	dev.exec(c, "INSERT INTO events VALUES (2, '2027-06-01')")
	c.Assert(dev.rowHome(c, 2), qt.Equals, "e2")
	c.Assert(dev.rowHome(c, 1), qt.Equals, "e1")
}

func migrationVersion(c *qt.C, dir string) string {
	c.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*_init.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.HasLen, 1)
	version, _, found := strings.Cut(filepath.Base(matches[0]), "_")
	c.Assert(found, qt.IsTrue)
	return version
}

func migrationFile(c *qt.C, dir, name, direction string) string {
	c.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, fmt.Sprintf("*_%s.%s.sql", name, direction)))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.HasLen, 1)
	body, err := os.ReadFile(matches[0])
	c.Assert(err, qt.IsNil)
	return string(body)
}

const describedDocument = `
CREATE TABLE tenants (id bigint PRIMARY KEY);
CREATE TABLE events (
    id bigint NOT NULL,
    tenant bigint NOT NULL REFERENCES tenants (id),
    happened_at timestamptz NOT NULL,
    PRIMARY KEY (id, happened_at),
    CONSTRAINT id_positive CHECK (id > 0)
) PARTITION BY RANGE (happened_at);
CREATE INDEX idx_events_tenant ON events (tenant);
CREATE TABLE events_2026 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
ALTER TABLE events_2026 ADD CONSTRAINT id_small CHECK (id < 1000);
CREATE TABLE events_rest PARTITION OF events DEFAULT;

CREATE TABLE notes (id bigint NOT NULL, day date NOT NULL, tag text) PARTITION BY RANGE (day);
CREATE TABLE notes_2026 PARTITION OF notes FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
ALTER TABLE notes_2026 ADD PRIMARY KEY (id);
ALTER TABLE notes_2026 ADD CONSTRAINT notes_2026_tag_key UNIQUE (tag);
`

// TestPartitionDescriptionsE2E reads each export of a partition tree. SQL says what the
// tree is and rebuilds it on an empty database. HCL and DBML have no spelling for a
// partition, so they describe it as a table and say so; applying the HCL back to the same
// database changes nothing. Go annotations cannot declare one, and the command names each.
func TestPartitionDescriptionsE2E(t *testing.T) {
	c := qt.New(t)
	dev := newScratch(c, dbtarget.PostgreSQL)
	dev.apply(c, describedDocument)

	described, _, err := ptahStreams("schema", "inspect", "--db-url", dev.url, "--schemas", "public", "--format", "sql")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", described))
	c.Assert(described, qt.Contains, `CREATE TABLE "events_2026" PARTITION OF "events" FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2027-01-01 00:00:00+00');`)
	c.Assert(described, qt.Contains, `ALTER TABLE "events_2026" ADD CONSTRAINT "id_small" CHECK ((id < 1000));`)
	c.Assert(described, qt.Not(qt.Contains), `ALTER TABLE "events_2026" ADD CONSTRAINT "events_tenant_fkey"`)
	c.Assert(described, qt.Not(qt.Contains), `"events_2026_tenant_idx"`)
	c.Assert(described, qt.Contains, `ALTER TABLE "notes_2026" ADD PRIMARY KEY ("id");`)
	c.Assert(described, qt.Contains, `ALTER TABLE "notes_2026" ADD CONSTRAINT "notes_2026_tag_key" UNIQUE ("tag");`)

	empty := newScratch(c, dbtarget.PostgreSQL)
	empty.apply(c, described)
	c.Assert(empty.partitions(c), qt.DeepEquals, dev.partitions(c))
	empty.assertSynced(c, describedDocument)
	c.Assert(empty.count(c, "SELECT count(*) FROM pg_constraint WHERE conrelid = 'notes_2026'::regclass AND contype IN ('p', 'u') AND conislocal"), qt.Equals, 2)

	hcl, hclNotes, err := ptahStreams("schema", "inspect", "--db-url", dev.url, "--schemas", "public", "--format", "hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", hclNotes))
	c.Assert(hcl, qt.Contains, `table "events_2026"`)
	c.Assert(hclNotes, qt.Contains, "cannot be represented in HCL")
	document := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(document, []byte(hcl), 0o600), qt.IsNil)
	plan, err := ptah("schema", "plan", "--db-url", dev.url, "--schema-file", document, "--schemas", "public", "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
	c.Assert(plan, qt.Contains, "Schema is synced", qt.Commentf("%s", plan))

	models := c.TempDir()
	out, err := ptah("introspect", "--db-url", dev.url, "--schemas", "public", "--out", models)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `note: "events_2026" is a PostgreSQL partition of "events", which Go annotations cannot declare`)

	plan, err = ptah("schema", "plan", "--db-url", dev.url, "--root-dir", models, "--schemas", "public", "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
	c.Assert(plan, qt.Contains, "Schema is synced", qt.Commentf("%s", plan))
}

// TestEnginesWithoutThePartitionStatementRefuseItE2E asks each engine first, so the row
// holds what it claims, and then the reader on a connection to that engine. A silent read
// would plan a table with no columns.
func TestEnginesWithoutThePartitionStatementRefuseItE2E(t *testing.T) {
	const statement = "CREATE TABLE ptah_partitionof_child PARTITION OF ptah_partitionof_parent FOR VALUES IN (1)"
	for _, test := range []struct {
		name         string
		engine       dbtarget.Engine
		driver       string
		serverAnswer string
		want         string
	}{
		{name: "mysql", engine: dbtarget.MySQL, driver: "mysql", serverAnswer: `.*You have an error in your SQL syntax.*`, want: "which Ptah does not read on mysql"},
		{name: "mariadb", engine: dbtarget.MariaDB, driver: "mysql", serverAnswer: `.*You have an error in your SQL syntax.*`, want: "which Ptah does not read on mariadb"},
		{name: "cockroachdb", engine: dbtarget.CockroachDB, driver: "pgx", serverAnswer: `(?s).*syntax error.*`, want: "which Ptah does not read on cockroachdb"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := sql.Open(test.driver, dbtarget.DriverDSN(c, test.engine))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
			_, err = db.ExecContext(c.Context(), statement)
			c.Assert(err, qt.ErrorMatches, test.serverAnswer)

			out, err := ptah("schema", "compare", "--schema-file", writeSchema(c, statement+";\n"), "--db-url", dbtarget.URL(c, test.engine))

			c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
			c.Assert(err.Error(), qt.Contains, "CREATE TABLE ptah_partitionof_child PARTITION OF declares")
			c.Assert(err.Error(), qt.Contains, test.want)
		})
	}
}

// TestPartitionOfThroughTheCompatibilitySurfaceE2E drives a SQL schema through
// `migrate diff` and `schema apply`, which read it with the same owner as the native
// surface.
func TestPartitionOfThroughTheCompatibilitySurfaceE2E(t *testing.T) {
	c := qt.New(t)
	target, dev := newScratch(c, dbtarget.PostgreSQL), newScratch(c, dbtarget.PostgreSQL)
	path := writeSchema(c, `CREATE TABLE events (id bigint NOT NULL, happened_at timestamptz NOT NULL, PRIMARY KEY (id, happened_at)) PARTITION BY RANGE (happened_at);
CREATE TABLE events_2026 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
`)
	dir := c.TempDir()

	out, err := compat("migrate", "diff", "init", "--dir", "file://"+filepath.ToSlash(dir), "--to", "file://"+filepath.ToSlash(path), "--dev-url", dev.url)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	migrations, err := filepath.Glob(filepath.Join(dir, "*_init.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(migrations, qt.HasLen, 1)
	body, err := os.ReadFile(migrations[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(body), qt.Contains, `CREATE TABLE "events_2026" PARTITION OF "events" FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');`)

	out, err = compat("schema", "apply", "-u", target.url, "--to", "file://"+filepath.ToSlash(path), "--dev-url", dev.url, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(target.partitions(c), qt.DeepEquals, map[string]string{
		"public.events_2026": "public.events FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2027-01-01 00:00:00+00')",
	})

	out, err = compat("schema", "apply", "-u", target.url, "--to", "file://"+filepath.ToSlash(path), "--dev-url", dev.url, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema is synced")

	out, err = compat("schema", "inspect", "-u", target.url)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `table "events_2026"`)
	c.Assert(out, qt.Contains, "feature facet ptah.run/postgres/table-partition-member is not represented in HCL")
}
