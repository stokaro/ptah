//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/dbtarget"
)

// connectingRolePassword is the password of every role these tests connect as.
const connectingRolePassword = "ptah_role_password" // #nosec G101 -- a scratch role's password on a test server

// connectingRoles are the roles a read is compared across: none of them owns
// anything it reads, and the admin connection the fixture is built on is the
// control.
type connectingRoles struct {
	// owner owns the fixture, so that a grant reads the same whichever account
	// the server was started with.
	owner string
	// noPrivileges can connect and holds no table privilege.
	noPrivileges string
	// readAllData holds pg_read_all_data and nothing else, where the server has
	// that role.
	readAllData string
	// grantee is what the fixtures grant to.
	grantee string
}

// newConnectingRoles creates the roles and drops them after every database of the
// test, whose objects depend on them. Roles are server-wide, so the names carry a
// stamp.
func newConnectingRoles(c *qt.C) connectingRoles {
	c.Helper()
	stamp := time.Now().UnixNano()
	roles := connectingRoles{
		owner:        fmt.Sprintf("ptah_owner_%d", stamp),
		noPrivileges: fmt.Sprintf("ptah_noprivs_%d", stamp),
		readAllData:  fmt.Sprintf("ptah_readall_%d", stamp),
		grantee:      fmt.Sprintf("ptah_grantee_%d", stamp),
	}
	admin, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), fmt.Sprintf(
			`DROP ROLE IF EXISTS %s, %s, %s, %s`, roles.owner, roles.noPrivileges, roles.readAllData, roles.grantee))
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	_, err = admin.ExecContext(c.Context(), fmt.Sprintf(`
		CREATE ROLE %[1]s NOLOGIN;
		CREATE ROLE %[2]s LOGIN PASSWORD '%[5]s';
		CREATE ROLE %[3]s LOGIN PASSWORD '%[5]s';
		CREATE ROLE %[4]s NOLOGIN;
		DO $$ BEGIN
			IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'pg_read_all_data') THEN
				GRANT pg_read_all_data TO %[3]s;
			END IF;
		END $$;`, roles.owner, roles.noPrivileges, roles.readAllData, roles.grantee, connectingRolePassword))
	c.Assert(err, qt.IsNil)
	return roles
}

// rows is one row per role that holds less than the owner, for a test that reads
// as each.
func (r connectingRoles) rows() []connectingRoleRow {
	return []connectingRoleRow{
		{name: "a role with no table privileges", role: r.noPrivileges},
		{name: "a role holding pg_read_all_data", role: r.readAllData},
	}
}

type connectingRoleRow struct{ name, role string }

// scratchOwnedDatabase is a scratch database built by sql, run as the roles' owner
// with {grantee} standing for the grantee. It returns the admin URL.
func scratchOwnedDatabase(c *qt.C, roles connectingRoles, sql string) string {
	c.Helper()
	target, _ := scratchReplayDatabase(c)
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), fmt.Sprintf(`
		GRANT ALL ON SCHEMA public TO %[1]s;
		GRANT CREATE ON DATABASE %[2]s TO %[1]s;
		SET ROLE %[1]s;
		`, roles.owner, databaseNameOf(c, target))+strings.ReplaceAll(sql, "{grantee}", roles.grantee)+`
		;RESET ROLE;`)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", sql))
	return target
}

func databaseNameOf(c *qt.C, target string) string {
	c.Helper()
	parsed, err := url.Parse(target)
	c.Assert(err, qt.IsNil)
	return strings.TrimPrefix(parsed.Path, "/")
}

// connectedAs is target with its credentials replaced by role's.
func connectedAs(c *qt.C, target, role string) string {
	c.Helper()
	parsed, err := url.Parse(target)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(role, connectingRolePassword)
	return parsed.String()
}

// The two databases of a comparison, differing by what information_schema hides
// from each of the roles: a table and a column from one, constraints and a grant
// from the other.
const (
	differingLeftSQL = `
		CREATE TABLE only_here (id int PRIMARY KEY);
		CREATE TABLE parent (id int PRIMARY KEY);
		CREATE TABLE shared (id int PRIMARY KEY, extra text);
		CREATE TABLE child (id int PRIMARY KEY, parent_id int, amount int);
		GRANT SELECT ON child TO {grantee};`
	differingRightSQL = `
		CREATE TABLE parent (id int PRIMARY KEY);
		CREATE TABLE shared (id int PRIMARY KEY);
		CREATE TABLE child (id int PRIMARY KEY, parent_id int, amount int);
		GRANT SELECT ON child TO {grantee};
		ALTER TABLE child ADD CONSTRAINT child_amount_check CHECK (amount >= 0);
		ALTER TABLE child ADD CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES parent (id);
		GRANT UPDATE ON child TO {grantee};`
)

// TestSchemaDiffAnswersTheSameForEveryConnectingRoleE2E compares two databases as
// a role that cannot see the differences through information_schema: there a role
// with no table privileges finds no table, and pg_read_all_data no constraint or
// grant, and a read through it answers `Schemas are synced` at exit 0. The answer
// is pinned to what the owner's side holds, and to the admin connection's output,
// so a read that loses an object for every role at once fails too.
func TestSchemaDiffAnswersTheSameForEveryConnectingRoleE2E(t *testing.T) {
	c := qt.New(t)
	roles := newConnectingRoles(c)
	left := scratchOwnedDatabase(c, roles, differingLeftSQL)
	right := scratchOwnedDatabase(c, roles, differingRightSQL)
	wantLeftToRight := runPtahNative(c, "schema", "diff", "--from", left, "--to", right)
	wantRightToLeft := runPtahNative(c, "schema", "diff", "--from", right, "--to", left)
	for _, statement := range []string{
		`DROP TABLE "only_here"`,
		`DROP COLUMN "extra"`,
		`ADD CONSTRAINT "child_amount_check"`,
		`ADD CONSTRAINT "child_parent_fk"`,
		fmt.Sprintf(`GRANT UPDATE ON TABLE "child" TO %q`, roles.grantee),
	} {
		c.Assert(wantLeftToRight, qt.Contains, statement)
	}
	for _, statement := range []string{
		`CREATE TABLE "only_here"`,
		`ADD COLUMN "extra"`,
		`DROP CONSTRAINT "child_amount_check"`,
		`DROP CONSTRAINT "child_parent_fk"`,
		fmt.Sprintf(`REVOKE UPDATE ON TABLE "child" FROM %q`, roles.grantee),
	} {
		c.Assert(wantRightToLeft, qt.Contains, statement)
	}

	for _, row := range roles.rows() {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			from, to := connectedAs(c, left, row.role), connectedAs(c, right, row.role)

			c.Assert(runPtahNative(c, "schema", "diff", "--from", from, "--to", to), qt.Equals, wantLeftToRight)
			c.Assert(runPtahNative(c, "schema", "diff", "--from", to, "--to", from), qt.Equals, wantRightToLeft)
		})
	}
}

// TestCompatSchemaDiffAnswersTheSameForEveryConnectingRoleE2E is the same read
// through ptah-compat, which shares it.
func TestCompatSchemaDiffAnswersTheSameForEveryConnectingRoleE2E(t *testing.T) {
	c := qt.New(t)
	roles := newConnectingRoles(c)
	left := scratchOwnedDatabase(c, roles, differingLeftSQL)
	right := scratchOwnedDatabase(c, roles, differingRightSQL)
	want, err := runCompatVerb("schema", "diff", "--from", left, "--to", right)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", want))
	c.Assert(want, qt.Contains, `"only_here"`)

	for _, row := range roles.rows() {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := runCompatVerb("schema", "diff",
				"--from", connectedAs(c, left, row.role), "--to", connectedAs(c, right, row.role))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", got))
			c.Assert(got, qt.Equals, want)
		})
	}
}

// reportsAfterDatabaseLine is a drift report without the line that names the
// connection, which holds the role and differs by design.
func reportsAfterDatabaseLine(c *qt.C, report string) string {
	c.Helper()
	_, after, found := strings.Cut(report, "Database: ")
	c.Assert(found, qt.IsTrue, qt.Commentf("%s", report))
	_, findings, _ := strings.Cut(after, "\n")
	return findings
}

// declaredSharedTableSQL declares part of what [differingLeftSQL] holds.
const declaredSharedTableSQL = `CREATE TABLE shared (id integer PRIMARY KEY, extra text);`

func writeDeclaration(c *qt.C, sql string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(sql), 0o600), qt.IsNil)
	return path
}

// TestSchemaDriftAnswersTheSameForEveryConnectingRoleE2E is drift against a
// database holding tables the declaration leaves out. Through information_schema a
// role with no table privileges finds the declared table missing, rates that safe,
// and finds nothing the declaration leaves out.
func TestSchemaDriftAnswersTheSameForEveryConnectingRoleE2E(t *testing.T) {
	c := qt.New(t)
	roles := newConnectingRoles(c)
	database := scratchOwnedDatabase(c, roles, differingLeftSQL)
	declaration := writeDeclaration(c, declaredSharedTableSQL)
	wantReport, wantErr := runPtahNativeOutcome("schema", "drift", "--db-url", database, "--schema-file", declaration)
	c.Assert(wantErr, qt.ErrorMatches, ".*drift.*", qt.Commentf("%s", wantReport))
	want := reportsAfterDatabaseLine(c, wantReport)
	c.Assert(want, qt.Contains, "tables_removed: 3")

	for _, row := range roles.rows() {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			report, err := runPtahNativeOutcome("schema", "drift",
				"--db-url", connectedAs(c, database, row.role), "--schema-file", declaration)

			c.Assert(err, qt.ErrorMatches, ".*drift.*", qt.Commentf("%s", report))
			c.Assert(report, qt.Contains, "highest severity: destructive")
			c.Assert(reportsAfterDatabaseLine(c, report), qt.Equals, want)
		})
	}
}

// TestSchemaDriftFindsNoDriftForEveryConnectingRoleE2E is the control: a
// declaration of exactly what the database holds is no drift for a role that holds
// no privilege on it. A read that lost a column's details, or asked something this
// role is refused, fails here where the comparison above could still pass.
func TestSchemaDriftFindsNoDriftForEveryConnectingRoleE2E(t *testing.T) {
	c := qt.New(t)
	roles := newConnectingRoles(c)
	database := scratchOwnedDatabase(c, roles, declaredSharedTableSQL)
	declaration := writeDeclaration(c, declaredSharedTableSQL)

	for _, row := range roles.rows() {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			report := runPtahNative(c, "schema", "drift",
				"--db-url", connectedAs(c, database, row.role), "--schema-file", declaration)

			c.Assert(report, qt.Contains, "No schema drift detected.")
		})
	}
}

// withoutConnectionLine is a compare report without its first line, which names the
// connection and so the role.
func withoutConnectionLine(c *qt.C, report string) string {
	c.Helper()
	_, after, found := strings.Cut(report, "\n")
	c.Assert(found, qt.IsTrue, qt.Commentf("%s", report))
	return after
}

// TestSchemaCompareAnswersTheSameForEveryConnectingRoleE2E is the comparison of a
// declaration with a database through the third verb that reads one.
func TestSchemaCompareAnswersTheSameForEveryConnectingRoleE2E(t *testing.T) {
	c := qt.New(t)
	roles := newConnectingRoles(c)
	database := scratchOwnedDatabase(c, roles, differingLeftSQL)
	declaration := writeDeclaration(c, declaredSharedTableSQL)
	want := withoutConnectionLine(c, runPtahNative(c, "schema", "compare", "--db-url", database, "--schema-file", declaration))
	c.Assert(want, qt.Contains, "tables_removed (3): child, only_here, parent")

	for _, row := range roles.rows() {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			got := runPtahNative(c, "schema", "compare",
				"--db-url", connectedAs(c, database, row.role), "--schema-file", declaration)

			c.Assert(withoutConnectionLine(c, got), qt.Equals, want)
		})
	}
}

// everyObjectKindSQL holds each kind of object a read describes, in every form the
// read takes from a catalog that filters by privilege: a column of each type
// family and a domain, a key of each kind, a view with a check option, a grant of
// each kind. It runs on every PostgreSQL line Ptah supports, so MAINTAIN arrives
// through GRANT ALL. The role is refused USAGE on `extra`, which a name-based
// lookup needs.
const everyObjectKindSQL = `
	CREATE SCHEMA extra;
	CREATE TYPE mood AS ENUM ('sad', 'happy');
	CREATE DOMAIN positive_int AS integer NOT NULL CHECK (VALUE > 0);
	CREATE DOMAIN short_text AS varchar(12);
	CREATE DOMAIN money_amount AS numeric(9, 3);
	CREATE DOMAIN int_list AS int[];
	CREATE TYPE pair AS (a int, b text);
	CREATE TYPE float_range AS RANGE (subtype = float8);
	CREATE SEQUENCE free_seq INCREMENT 3 START 9;
	COMMENT ON SEQUENCE free_seq IS 'free';
	CREATE TABLE parent (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, code text NOT NULL UNIQUE);
	COMMENT ON TABLE parent IS 'the parent';
	COMMENT ON COLUMN parent.code IS 'the code';
	CREATE TABLE kitchen (
		id serial PRIMARY KEY,
		a_small smallint DEFAULT 3,
		a_big bigint,
		a_num numeric(10, 2) NOT NULL DEFAULT 1.5,
		a_num_plain numeric,
		a_real real,
		a_double double precision,
		a_varchar varchar(40) COLLATE "C",
		a_char char(3),
		a_text text DEFAULT 'x'::text,
		a_bit bit(4),
		a_varbit varbit(9),
		a_date date,
		a_time time(3),
		a_timetz timetz,
		a_ts timestamp(2),
		a_tstz timestamptz DEFAULT now(),
		a_interval interval(4),
		a_interval_field interval year to month,
		a_list varchar(100)[],
		a_ints int[],
		a_mood mood DEFAULT 'happy',
		a_pair pair,
		a_range float_range,
		a_period int4range,
		d_pos positive_int,
		d_short short_text,
		d_money money_amount,
		d_list int_list,
		ref bigint,
		gen_col int GENERATED ALWAYS AS (a_small * 2) STORED,
		from_seq int DEFAULT nextval('free_seq'),
		CONSTRAINT kitchen_ref_fk FOREIGN KEY (ref) REFERENCES parent(id)
			ON DELETE SET NULL ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED,
		CONSTRAINT kitchen_small_ck CHECK (a_small > 0),
		CONSTRAINT kitchen_uq UNIQUE (a_big, a_date),
		CONSTRAINT kitchen_period_ex EXCLUDE USING gist (a_period WITH &&)
	);
	COMMENT ON CONSTRAINT kitchen_small_ck ON kitchen IS 'small check';
	ALTER TABLE kitchen ADD CONSTRAINT kitchen_big_ck CHECK (a_big > 0) NOT VALID;
	CREATE INDEX kitchen_big_idx ON kitchen (a_big) WHERE a_big > 0;
	COMMENT ON INDEX kitchen_big_idx IS 'big idx';
	CREATE UNLOGGED TABLE scratchpad (id int PRIMARY KEY);
	CREATE TABLE partitioned (id int, at date, PRIMARY KEY (id, at)) PARTITION BY RANGE (at);
	CREATE TABLE partition_one PARTITION OF partitioned FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
	CREATE TABLE inherited_base (id int);
	CREATE TABLE inherited_child (x int) INHERITS (inherited_base);
	CREATE VIEW plain_view AS SELECT id, code FROM parent;
	COMMENT ON VIEW plain_view IS 'a view';
	CREATE VIEW local_check_view WITH (security_barrier = true) AS
		SELECT id FROM parent WHERE id > 0 WITH LOCAL CHECK OPTION;
	CREATE VIEW cascaded_check_view AS SELECT id FROM parent WHERE id > 0 WITH CASCADED CHECK OPTION;
	CREATE MATERIALIZED VIEW parent_ids AS SELECT id FROM parent WITH NO DATA;
	CREATE FUNCTION add_one(i int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT i + 1';
	CREATE FUNCTION touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
	CREATE PROCEDURE do_it(i int) LANGUAGE sql AS 'SELECT 1';
	CREATE TRIGGER kitchen_touch BEFORE INSERT ON kitchen FOR EACH ROW EXECUTE FUNCTION touch();
	ALTER TABLE parent ENABLE ROW LEVEL SECURITY;
	ALTER TABLE parent FORCE ROW LEVEL SECURITY;
	CREATE POLICY parent_policy ON parent FOR SELECT TO {grantee} USING (id > 0);
	GRANT SELECT, INSERT ON parent TO {grantee};
	GRANT SELECT ON parent TO PUBLIC;
	GRANT UPDATE (code) ON parent TO {grantee};
	GRANT SELECT ON kitchen TO {grantee} WITH GRANT OPTION;
	GRANT ALL ON scratchpad TO {grantee};
	GRANT SELECT ON plain_view TO {grantee};
	GRANT SELECT ON parent_ids TO {grantee};
	GRANT USAGE ON SEQUENCE free_seq TO {grantee};
	GRANT USAGE, SELECT ON SEQUENCE kitchen_id_seq TO {grantee};
	REVOKE ALL ON FUNCTION add_one(int) FROM PUBLIC;
	GRANT EXECUTE ON FUNCTION add_one(int) TO {grantee};
	GRANT USAGE ON SCHEMA extra TO {grantee};
	ALTER DEFAULT PRIVILEGES IN SCHEMA extra GRANT SELECT ON TABLES TO {grantee};
	ALTER DEFAULT PRIVILEGES GRANT SELECT ON SEQUENCES TO {grantee};
	CREATE TABLE extra.other (id int PRIMARY KEY, p bigint REFERENCES public.parent(id), d positive_int, m mood);
	CREATE TABLE extra.other_serial (id serial PRIMARY KEY);
	CREATE TABLE extra.partitioned (id int, at date, PRIMARY KEY (id, at)) PARTITION BY RANGE (at);
	CREATE VIEW extra.other_view AS SELECT o.id, k.a_big FROM extra.other o JOIN public.kitchen k ON k.id = o.id;
	CREATE FUNCTION extra.lookup() RETURNS bigint LANGUAGE sql AS 'SELECT count(*) FROM public.parent';
	REVOKE ALL ON SCHEMA extra FROM PUBLIC;`

// describedKinds counts what an inspect document holds of each kind, so that two
// empty documents cannot agree.
func describedKinds(c *qt.C, document string) map[string]int {
	c.Helper()
	var parsed map[string]json.RawMessage
	c.Assert(json.Unmarshal([]byte(document), &parsed), qt.IsNil)
	kinds := make(map[string]int)
	for _, kind := range []string{
		"Tables", "Fields", "Indexes", "Constraints", "Enums", "Functions", "Sequences", "Views",
		"MaterializedViews", "Triggers", "Roles", "Grants", "RevokedGrants", "feature_objects",
	} {
		var entries []json.RawMessage
		c.Assert(json.Unmarshal(parsed[kind], &entries), qt.IsNil, qt.Commentf("%s", kind))
		kinds[kind] = len(entries)
	}
	return kinds
}

// TestSchemaInspectDescribesTheSameForEveryConnectingRoleE2E reads every kind of
// object as a role that holds no privilege on any of them, including a schema it
// cannot use, and compares the whole document byte for byte with the admin's. Only
// a read that does not depend on the role can pass this: the corpus is the
// document, so a new read of a filtering view shows up here without this test
// naming it.
func TestSchemaInspectDescribesTheSameForEveryConnectingRoleE2E(t *testing.T) {
	c := qt.New(t)
	roles := newConnectingRoles(c)
	database := scratchOwnedDatabase(c, roles, everyObjectKindSQL)
	want, wantErr := inspectDocument(c, database)
	c.Assert(wantErr, qt.IsNil, qt.Commentf("%s", want))
	for kind, count := range describedKinds(c, want) {
		c.Assert(count > 0, qt.IsTrue, qt.Commentf("the fixture describes no %s", kind))
	}
	for _, carried := range []string{
		`"WithCheck": true`,
		"ptah.run/pgpolicy/policy",
		"ptah.run/postgres/exclusion-constraint",
		"ptah.run/pgdefaultprivilege/default-privilege",
		"ptah.run/pgtype/domain",
		"ptah.run/pgtype/composite",
		"ptah.run/pgtype/range",
		"ptah.run/postgres/table-partition",
	} {
		c.Assert(want, qt.Contains, carried)
	}

	for _, row := range roles.rows() {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := inspectDocument(c, connectedAs(c, database, row.role))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", got))
			c.Assert(got, qt.Equals, want)
		})
	}
}

// inspectDocument is `schema inspect` JSON alone. The notes on standard error
// count what was left out and are not the document.
func inspectDocument(c *qt.C, database string) (string, error) {
	c.Helper()
	cmd := root.NewRootCommand()
	var out, diagnostics bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diagnostics)
	cmd.SetArgs([]string{"schema", "inspect", "--db-url", database, "--format", "json"})
	err := cmd.Execute()
	return out.String(), err
}

// TestSchemaDiffNamesTheMissingUsageForARoleWithNoSchemaE2E is the read the catalog
// cannot answer for: PostgreSQL resolves a search_path of schemas the role holds no
// USAGE on to nothing, so the session has no schema to read. It is refused at exit
// 2, and the message names the privilege.
func TestSchemaDiffNamesTheMissingUsageForARoleWithNoSchemaE2E(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name: "a URL that names no schema",
			want: "the search_path of the connecting role resolves to no schema",
		},
		{
			name:  "a URL naming a schema that exists",
			query: "search_path=public",
			want:  `database URL selects schema "public", which does not exist in this database or holds no USAGE privilege`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			roles := newConnectingRoles(c)
			database := scratchOwnedDatabase(c, roles, `CREATE TABLE t (id int PRIMARY KEY);`)
			conn, err := dbschema.ConnectToDatabase(c.Context(), database)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			_, err = conn.ExecContext(c.Context(), `REVOKE USAGE ON SCHEMA public FROM PUBLIC`)
			c.Assert(err, qt.IsNil)
			from, err := url.Parse(connectedAs(c, database, roles.noPrivileges))
			c.Assert(err, qt.IsNil)
			query := from.Query()
			maps.Copy(query, mustParseQuery(c, test.query))
			from.RawQuery = query.Encode()

			cmd := root.NewRootCommand()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			code := root.RunContext(c.Context(), cmd, "schema", "diff", "--from", from.String(), "--to", from.String())

			c.Assert(code, qt.Equals, 2, qt.Commentf("%s", out.String()))
			c.Assert(out.String(), qt.Contains, test.want)
			c.Assert(out.String(), qt.Contains, "USAGE privilege")
			c.Assert(out.String(), qt.Not(qt.Contains), "Schemas are synced")
		})
	}
}

func mustParseQuery(c *qt.C, query string) url.Values {
	c.Helper()
	values, err := url.ParseQuery(query)
	c.Assert(err, qt.IsNil)
	return values
}
