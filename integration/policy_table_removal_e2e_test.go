//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// policyRemovalShape is a first schema whose tables hold row-security
// policies, a second schema without those tables, and the state each leaves
// in the catalog. Rolling the second migration back has to recreate the
// tables, switch row security on as before, and create the policies with
// their expressions, commands and roles, and the grants.
type policyRemovalShape struct {
	name          string
	first, second string
	firstState    rowSecurityState
	secondState   rowSecurityState
}

// rowSecurityState is what the catalog says about the relations of the public
// schema: each relation with its row-security switches, each policy, and each
// table grant to a role other than the owner.
type rowSecurityState struct {
	Relations []string
	Policies  []string
	Grants    []string
}

// policyRemovalShapes are the shapes a project with row-level security
// removes tables in. role names a role the server has and both schemas
// declare, so the plans manage its grants and neither creates nor drops it.
func policyRemovalShapes(role string) []policyRemovalShape {
	other := "CREATE TABLE other (id bigint PRIMARY KEY);\n"
	otherState := rowSecurityState{Relations: []string{"other table rls=false force=false"}}
	return []policyRemovalShape{
		{
			name: "a table with a policy",
			first: `CREATE TABLE docs (id bigint PRIMARY KEY, member_id bigint);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p_docs ON docs USING (member_id > 0);
`,
			second: other,
			firstState: rowSecurityState{
				Relations: []string{"docs table rls=true force=false"},
				Policies:  []string{"docs.p_docs PERMISSIVE {public} ALL using=(member_id > 0) check="},
			},
			secondState: otherState,
		},
		{
			name: "a forced table with a restrictive policy for one command and role, and grants",
			first: fmt.Sprintf(`CREATE ROLE %[1]s NOLOGIN;
CREATE TABLE docs (id bigint PRIMARY KEY, member_id bigint, author text);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
ALTER TABLE docs FORCE ROW LEVEL SECURITY;
CREATE POLICY p_docs ON docs USING (member_id > 0);
CREATE POLICY p_insert ON docs AS RESTRICTIVE FOR INSERT TO %[1]s WITH CHECK (author = current_user);
GRANT SELECT, INSERT ON docs TO %[1]s;
`, role),
			second: fmt.Sprintf("CREATE ROLE %s NOLOGIN;\n", role) + other,
			firstState: rowSecurityState{
				Relations: []string{"docs table rls=true force=true"},
				Policies: []string{
					"docs.p_docs PERMISSIVE {public} ALL using=(member_id > 0) check=",
					fmt.Sprintf("docs.p_insert RESTRICTIVE {%s} INSERT using= check=(author = CURRENT_USER)", role),
				},
				Grants: []string{"docs " + role + " INSERT", "docs " + role + " SELECT"},
			},
			secondState: otherState,
		},
		{
			name: "a policy that reads a removed view",
			first: `CREATE TABLE members (id bigint PRIMARY KEY, who text);
CREATE VIEW current_members AS SELECT id FROM members WHERE who = current_user;
CREATE TABLE docs (id bigint PRIMARY KEY, member_id bigint);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p_docs ON docs USING (member_id IN (SELECT id FROM current_members));
`,
			second: other,
			firstState: rowSecurityState{
				Relations: []string{"current_members view rls=false force=false", "docs table rls=true force=false", "members table rls=false force=false"},
				Policies:  []string{"docs.p_docs PERMISSIVE {public} ALL using=(member_id IN ( SELECT current_members.id\n   FROM current_members)) check="},
			},
			secondState: otherState,
		},
	}
}

// policyRemovalRole creates a role of this test's own on the PostgreSQL
// server and drops it when the test ends, after the databases that hold its
// grants are gone.
func policyRemovalRole(c *qt.C) string {
	c.Helper()
	admin, err := sql.Open("pgx", dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	role := fmt.Sprintf("ptah_policy_removal_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" NOLOGIN")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+pgx.Identifier{role}.Sanitize())
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	return role
}

// policyRemovalWriter writes the migration for one schema into dir on dev.
type policyRemovalWriter func(c *qt.C, dir, dev, name, schema string)

// writeGolangMigrate runs `ptah-compat migrate diff` into a golang-migrate
// directory, which holds an up and a down file per step.
func writeGolangMigrate(c *qt.C, dir, dev, name, schema string) {
	c.Helper()
	output, err := runCompatVerb("migrate", "diff", name,
		"--dir", "file://"+filepath.ToSlash(dir)+"?format=golang-migrate",
		"--to", "file://"+filepath.ToSlash(schema), "--dev-url", dev)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
}

// writeNative runs `ptah migrations generate --replay`, which writes an up and
// a down file per step.
func writeNative(c *qt.C, dir, dev, name, schema string) {
	c.Helper()
	runPtahNative(c, "migrations", "generate", "--replay", "--dev-url", dev,
		"--schema-file", schema, "--migrations-dir", dir, "--name", name)
}

// writePolicyRemovalSteps writes the shape's two migrations with write, on a
// dev database of their own, and returns the directory.
func writePolicyRemovalSteps(c *qt.C, shape policyRemovalShape, write policyRemovalWriter) string {
	c.Helper()
	dev := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_policy_removal_dev"), "public")
	work := c.TempDir()
	dir := filepath.Join(work, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	for _, step := range []struct{ name, schema string }{{"s1", shape.first}, {"s2", shape.second}} {
		path := filepath.Join(work, step.name+".sql")
		c.Assert(os.WriteFile(path, []byte(step.schema), 0o600), qt.IsNil)
		write(c, dir, dev, step.name, path)
	}
	return dir
}

// readRowSecurity reads the state of the public schema from db.
func readRowSecurity(c *qt.C, db *sql.DB) rowSecurityState {
	c.Helper()
	return rowSecurityState{
		Relations: queryRows(c, db, `SELECT c.relname || ' ' || CASE c.relkind WHEN 'v' THEN 'view' ELSE 'table' END
  || ' rls=' || c.relrowsecurity || ' force=' || c.relforcerowsecurity
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'v') ORDER BY 1`),
		Policies: queryRows(c, db, `SELECT tablename || '.' || policyname || ' ' || permissive || ' ' || roles::text || ' ' || cmd
  || ' using=' || coalesce(qual, '') || ' check=' || coalesce(with_check, '')
FROM pg_policies WHERE schemaname = 'public' ORDER BY 1`),
		Grants: queryRows(c, db, `SELECT table_name || ' ' || grantee || ' ' || privilege_type
FROM information_schema.role_table_grants
WHERE table_schema = 'public' AND grantee <> current_user ORDER BY 1`),
	}
}

func queryRows(c *qt.C, db *sql.DB, query string) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), query)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var result []string
	for rows.Next() {
		var row string
		c.Assert(rows.Scan(&row), qt.IsNil)
		result = append(result, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return result
}

// TestPolicyTableRemovalE2E_FilePairsApplyBothWays writes each shape through
// `ptah-compat migrate diff` into a golang-migrate directory and through
// `ptah migrations generate --replay`, and runs the files on PostgreSQL:
// both up, both down to an empty schema, and both up again, reading the
// catalog after each file. When the restored table claims a namespace of
// every captured kind, the second step writes no file: its rollback is
// refused, since no parent planning service accounts for default privileges
// on a table.
func TestPolicyTableRemovalE2E_FilePairsApplyBothWays(t *testing.T) {
	writers := []struct {
		name  string
		write policyRemovalWriter
	}{
		{name: "migrate diff golang-migrate", write: writeGolangMigrate},
		{name: "migrations generate", write: writeNative},
	}
	for _, writer := range writers {
		t.Run(writer.name, func(t *testing.T) {
			c := qt.New(t)
			role := policyRemovalRole(c)
			for _, shape := range policyRemovalShapes(role) {
				t.Run(shape.name, func(t *testing.T) {
					c := qt.New(t)
					envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
					pairs := readMigrationPairs(c, writePolicyRemovalSteps(c, shape, writer.write))
					c.Assert(pairs, qt.HasLen, 2)
					target, err := sql.Open("pgx", postgresScratchDevURL(c, "ptah_policy_removal_target"))
					c.Assert(err, qt.IsNil)
					c.Cleanup(func() { c.Check(target.Close(), qt.IsNil) })
					steps := []struct {
						sql  string
						want rowSecurityState
					}{
						{pairs[0].Up, shape.firstState},
						{pairs[1].Up, shape.secondState},
						{pairs[1].Down, shape.firstState},
						{pairs[0].Down, rowSecurityState{}},
						{pairs[0].Up, shape.firstState},
						{pairs[1].Up, shape.secondState},
					}
					for _, step := range steps {
						_, err := target.ExecContext(c.Context(), step.sql)
						c.Assert(err, qt.IsNil, qt.Commentf("%s", step.sql))
						c.Assert(readRowSecurity(c, target), qt.DeepEquals, step.want, qt.Commentf("%s", step.sql))
					}
				})
			}
		})
	}
}

// TestPolicyTableRemovalE2E_AtlasDirectoryRollsBackByPlan writes each shape
// into an Atlas directory, which holds no down files. The files run forward
// through `migrate apply`, back to an empty schema through `migrate down
// --plan`, which plans each rollback from the schema difference, and forward
// again, with the catalog read after each step. Writing an Atlas directory
// plans no rollback, so this is the control for the shapes themselves.
func TestPolicyTableRemovalE2E_AtlasDirectoryRollsBackByPlan(t *testing.T) {
	c := qt.New(t)
	role := policyRemovalRole(c)
	for _, shape := range policyRemovalShapes(role) {
		t.Run(shape.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
			dev := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_policy_removal_down_dev"), "public")
			dir := writePolicyRemovalSteps(c, shape, writeAtlas)
			versions := atlasVersions(c, dir)
			c.Assert(versions, qt.HasLen, 2)
			targetURL := postgresScratchDevURL(c, "ptah_policy_removal_target")
			target, err := sql.Open("pgx", targetURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(target.Close(), qt.IsNil) })
			atlasDir := "file://" + filepath.ToSlash(dir)
			steps := []struct {
				args []string
				want rowSecurityState
			}{
				{[]string{"migrate", "apply", "--url", targetURL, "--dir", atlasDir, "1"}, shape.firstState},
				{[]string{"migrate", "apply", "--url", targetURL, "--dir", atlasDir, "1"}, shape.secondState},
				{[]string{"migrate", "down", "--plan", "--to-version", versions[0], "--url", targetURL, "--dir", atlasDir, "--dev-url", dev}, shape.firstState},
				{[]string{"migrate", "down", "--plan", "--to-version", "0", "--url", targetURL, "--dir", atlasDir, "--dev-url", dev}, rowSecurityState{}},
				{[]string{"migrate", "apply", "--url", targetURL, "--dir", atlasDir}, shape.secondState},
			}
			for _, step := range steps {
				output, err := runCompatVerb(step.args...)
				c.Assert(err, qt.IsNil, qt.Commentf("%v\n%s", step.args, output))
				c.Assert(readRowSecurity(c, target), qt.DeepEquals, step.want, qt.Commentf("%v\n%s", step.args, output))
			}
		})
	}
}

// writeAtlas runs `ptah-compat migrate diff` into an Atlas directory.
func writeAtlas(c *qt.C, dir, dev, name, schema string) {
	c.Helper()
	output, err := runCompatVerb("migrate", "diff", name,
		"--dir", "file://"+filepath.ToSlash(dir),
		"--to", "file://"+filepath.ToSlash(schema), "--dev-url", dev)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
}

// atlasVersions lists the versions of an Atlas directory in order.
func atlasVersions(c *qt.C, dir string) []string {
	c.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	versions := make([]string, 0, len(files))
	for _, file := range files {
		version, _, found := strings.Cut(filepath.Base(file), "_")
		c.Assert(found, qt.IsTrue, qt.Commentf("%s", file))
		versions = append(versions, version)
	}
	slices.Sort(versions)
	return versions
}
