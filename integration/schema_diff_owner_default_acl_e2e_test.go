//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
)

// ownerTableACL reads the ACL of table t, as PostgreSQL spells it, with the two
// roles of a fixture named. A NULL ACL reads as the word.
func ownerTableACL(c *qt.C, target string, roles connectingRoles) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var acl string
	err = conn.QueryRowContext(c.Context(), `SELECT COALESCE(relacl::text, 'NULL') FROM pg_class WHERE relname = 't'`).Scan(&acl)
	c.Assert(err, qt.IsNil)
	return strings.NewReplacer(roles.owner, "owner", roles.grantee, "grantee").Replace(acl)
}

// ownedPublicDatabase is a scratch database whose public schema belongs to the
// fixture's owner, with sql run as that role, as in stokaro/ptah#5508. {grantee} and
// {owner} stand for the roles.
//
// A database read names the role that wrote a grant, so the owner of public is a role
// the other side of a comparison manages, whatever else it holds.
func ownedPublicDatabase(c *qt.C, roles connectingRoles, sql string) string {
	c.Helper()
	target, _ := scratchReplayDatabase(c)
	statements := strings.NewReplacer("{grantee}", roles.grantee, "{owner}", roles.owner).Replace(sql)
	execPostgresFamilySQL(c, c.Context(), target, fmt.Sprintf(`
		ALTER SCHEMA public OWNER TO %[1]s;
		GRANT CREATE ON DATABASE %[2]s TO %[1]s;
		SET ROLE %[1]s;
		%[3]s;
		RESET ROLE;`, roles.owner, databaseNameOf(c, target), statements))
	return target
}

// TestSchemaDiffLeavesAnOwnerWhoseACLWasWrittenE2E is two objects whose owner holds
// the same privileges. PostgreSQL stores no ACL until an object's first GRANT or
// REVOKE, and that statement writes the owner's entry out, so the only difference
// between the two databases is the grant to the grantee.
//
// Each direction names the owner in a plan only if the read reports its entry as
// grants, and the revoking direction then leaves the owner refused on its own table.
func TestSchemaDiffLeavesAnOwnerWhoseACLWasWrittenE2E(t *testing.T) {
	tests := []struct {
		name   string
		create string
		grant  string
		revoke string
		want   string
	}{
		{
			name:   "a table",
			create: `CREATE TABLE t (id int PRIMARY KEY);`,
			grant:  `GRANT SELECT ON t TO {grantee};`,
			want:   `GRANT SELECT ON TABLE "t" TO GRANTEE;`,
			revoke: `REVOKE SELECT ON TABLE "t" FROM GRANTEE;`,
		},
		{
			name:   "a sequence",
			create: `CREATE SEQUENCE t;`,
			grant:  `GRANT USAGE ON SEQUENCE t TO {grantee};`,
			want:   `GRANT USAGE ON SEQUENCE "t" TO GRANTEE;`,
			revoke: `REVOKE USAGE ON SEQUENCE "t" FROM GRANTEE;`,
		},
		{
			name:   "a schema",
			create: `CREATE SCHEMA app;`,
			grant:  `GRANT USAGE ON SCHEMA app TO {grantee};`,
			want:   `GRANT USAGE ON SCHEMA "app" TO GRANTEE;`,
			revoke: `REVOKE USAGE ON SCHEMA "app" FROM GRANTEE;`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			roles := newConnectingRoles(c)
			grantee := fmt.Sprintf("%q", roles.grantee)
			left := ownedPublicDatabase(c, roles, test.create)
			right := ownedPublicDatabase(c, roles, test.create+test.grant)

			leftToRight := runPtahNative(c, "schema", "diff", "--from", left, "--to", right)
			rightToLeft := runPtahNative(c, "schema", "diff", "--from", right, "--to", left)

			c.Assert(strings.TrimSpace(leftToRight), qt.Equals, strings.ReplaceAll(test.want, "GRANTEE", grantee))
			c.Assert(strings.TrimSpace(rightToLeft), qt.Equals, strings.ReplaceAll(test.revoke, "GRANTEE", grantee))
		})
	}
}

// TestSchemaDiffRevokeLeavesTheOwnerItsTableE2E runs what the comparison prints and
// reads the database back: the grant is gone, the owner keeps its entry and reads its
// own table, and the two databases then compare as synced. Running the statements of a
// read that takes the entry for grants leaves `permission denied` here, and a
// comparison that still reports the databases as synced with the grant in place.
func TestSchemaDiffRevokeLeavesTheOwnerItsTableE2E(t *testing.T) {
	c := qt.New(t)
	roles := newConnectingRoles(c)
	const table = `CREATE TABLE t (id int PRIMARY KEY);`
	left := ownedPublicDatabase(c, roles, table)
	right := ownedPublicDatabase(c, roles, table+`GRANT SELECT ON t TO {grantee};`)

	plan := runPtahNative(c, "schema", "diff", "--from", right, "--to", left)
	execPostgresFamilySQL(c, c.Context(), right, plan)
	execPostgresFamilySQL(c, c.Context(), right, fmt.Sprintf(`SET ROLE %s; SELECT count(*) FROM t; RESET ROLE;`, roles.owner))

	c.Assert(ownerTableACL(c, right, roles), qt.Equals, `{owner=arwdDxtm/owner}`)
	c.Assert(runPtahNative(c, "schema", "diff", "--from", right, "--to", left), qt.Contains, "Schemas are synced")
}

// TestSchemaDiffShowsWhatAnOwnerRevokedFromItselfE2E is the difference the rule above
// must not hide: an owner that gave a privilege up holds less than one that did not.
// Each direction plans that one privilege and nothing about the rest, the plan does
// what it says on the server, and the two databases then compare as synced. A read
// that reports what the owner still holds as grants plans the others, in the opposite
// direction, and an ACL emptied by REVOKE ALL reads as holding nothing to compare.
func TestSchemaDiffShowsWhatAnOwnerRevokedFromItselfE2E(t *testing.T) {
	tests := []struct {
		name    string
		revoke  string
		toLost  string
		toHeld  string
		lostACL string
	}{
		{
			name:    "one privilege",
			revoke:  `REVOKE SELECT ON t FROM {owner};`,
			toLost:  `REVOKE SELECT ON TABLE "t" FROM OWNER;`,
			toHeld:  `GRANT SELECT ON TABLE "t" TO OWNER;`,
			lostACL: `{owner=awdDxtm/owner}`,
		},
		{
			name:   "every privilege",
			revoke: `REVOKE ALL ON t FROM {owner};`,
			toLost: `REVOKE DELETE ON TABLE "t" FROM OWNER;
REVOKE INSERT ON TABLE "t" FROM OWNER;
REVOKE MAINTAIN ON TABLE "t" FROM OWNER;
REVOKE REFERENCES ON TABLE "t" FROM OWNER;
REVOKE SELECT ON TABLE "t" FROM OWNER;
REVOKE TRIGGER ON TABLE "t" FROM OWNER;
REVOKE TRUNCATE ON TABLE "t" FROM OWNER;
REVOKE UPDATE ON TABLE "t" FROM OWNER;`,
			toHeld: `GRANT DELETE ON TABLE "t" TO OWNER;
GRANT INSERT ON TABLE "t" TO OWNER;
GRANT MAINTAIN ON TABLE "t" TO OWNER;
GRANT REFERENCES ON TABLE "t" TO OWNER;
GRANT SELECT ON TABLE "t" TO OWNER;
GRANT TRIGGER ON TABLE "t" TO OWNER;
GRANT TRUNCATE ON TABLE "t" TO OWNER;
GRANT UPDATE ON TABLE "t" TO OWNER;`,
			lostACL: `{}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			roles := newConnectingRoles(c)
			owner := fmt.Sprintf("%q", roles.owner)
			const table = `CREATE TABLE t (id int PRIMARY KEY);`
			held := ownedPublicDatabase(c, roles, table)
			lost := ownedPublicDatabase(c, roles, table+test.revoke)
			restored := ownedPublicDatabase(c, roles, table+test.revoke)

			toLost := runPtahNative(c, "schema", "diff", "--from", held, "--to", lost)
			toHeld := runPtahNative(c, "schema", "diff", "--from", lost, "--to", held)

			c.Assert(strings.TrimSpace(toLost), qt.Equals, strings.ReplaceAll(test.toLost, "OWNER", owner))
			c.Assert(strings.TrimSpace(toHeld), qt.Equals, strings.ReplaceAll(test.toHeld, "OWNER", owner))

			execPostgresFamilySQL(c, c.Context(), restored, toHeld)
			c.Assert(ownerTableACL(c, restored, roles), qt.Equals, `{owner=arwdDxtm/owner}`)
			c.Assert(runPtahNative(c, "schema", "diff", "--from", restored, "--to", held), qt.Contains, "Schemas are synced")

			execPostgresFamilySQL(c, c.Context(), held, toLost)
			c.Assert(ownerTableACL(c, held, roles), qt.Equals, test.lostACL)
			c.Assert(runPtahNative(c, "schema", "diff", "--from", held, "--to", lost), qt.Contains, "Schemas are synced")
		})
	}
}

// TestSchemaDiffRebuildGivesAViewItsGrantsAndNotItsOwnersE2E is a view the plan drops
// and creates again. The plan gives back what somebody granted on it. What its owner
// held by default is what the owner of the new view holds, so a read that takes the
// owner's entry for grants adds a GRANT for each privilege, and only where the ACL was
// written.
func TestSchemaDiffRebuildGivesAViewItsGrantsAndNotItsOwnersE2E(t *testing.T) {
	c := qt.New(t)
	roles := newConnectingRoles(c)
	database := ownedPublicDatabase(c, roles, `
		CREATE TABLE t (id int PRIMARY KEY, a int);
		CREATE VIEW v AS SELECT id, a FROM t;
		GRANT SELECT ON v TO {grantee};`)
	declaration := writeDeclaration(c, fmt.Sprintf(`
		CREATE TABLE t (id int PRIMARY KEY, a int);
		CREATE VIEW v AS SELECT id FROM t;
		GRANT SELECT ON v TO %s;`, roles.grantee))

	got := runPtahNative(c, "schema", "diff", "--from", database, "--to", "file://"+declaration)

	c.Assert(got, qt.Contains, `DROP VIEW "v";`)
	c.Assert(got, qt.Contains, fmt.Sprintf(`GRANT SELECT ON TABLE "v" TO %q;`, roles.grantee))
	c.Assert(got, qt.Not(qt.Contains), roles.owner)
}

// ownerFixtureFile is a schema in one file format: the fixture's two roles, its table
// and a grant to the grantee, and in the other two fields the statement that grants
// the owner a privilege and the one that revokes it. OWNER and GRANTEE stand for the
// roles.
type ownerFixtureFile struct {
	name      string
	extension string
	body      string
	grant     string
	revoke    string
}

var ownerFixtureFiles = []ownerFixtureFile{
	{
		name:      "SQL",
		extension: "sql",
		body: `CREATE ROLE OWNER NOLOGIN;
CREATE ROLE GRANTEE NOLOGIN;
CREATE TABLE t (id int PRIMARY KEY);
GRANT SELECT ON t TO GRANTEE;
`,
		grant:  `GRANT SELECT ON t TO OWNER;`,
		revoke: `REVOKE SELECT ON t FROM OWNER;`,
	},
	{
		name:      "YAML",
		extension: "yaml",
		body: `roles:
  OWNER: {login: false}
  GRANTEE: {login: false}
tables:
  t:
    columns:
      id: {type: INTEGER, primary: true}
grants:
  to_grantee: {role: GRANTEE, privilege: SELECT, on_table: t}
`,
		grant:  `  to_owner: {role: OWNER, privilege: SELECT, on_table: t}`,
		revoke: "revokes:\n  from_owner: {role: OWNER, privilege: SELECT, on_table: t}",
	},
	{
		name:      "HCL",
		extension: "hcl",
		body: `schema "public" {}
role "OWNER" {}
role "GRANTEE" {}
table "t" {
  schema = schema.public
  column "id" {
    type = integer
  }
  primary_key {
    columns = [column.id]
  }
}
permission {
  to = role.GRANTEE
  for = table.t
  privileges = ["SELECT"]
}
`,
		grant:  "permission {\n  to = role.OWNER\n  for = table.t\n  privileges = [\"SELECT\"]\n}",
		revoke: "revoke {\n  from = role.OWNER\n  for = table.t\n  privileges = [\"SELECT\"]\n}",
	},
}

// write saves the schema with extra after it and returns the file's path.
func (f ownerFixtureFile) write(c *qt.C, roles connectingRoles, extra string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema."+f.extension)
	body := strings.NewReplacer("OWNER", roles.owner, "GRANTEE", roles.grantee).Replace(f.body + extra + "\n")
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// ownerGoSource is the same schema in Go annotations, in a directory of its own.
func ownerGoSource(c *qt.C, roles connectingRoles) string {
	c.Helper()
	dir := c.TempDir()
	source := fmt.Sprintf(`package entities

//ptah:schema:role name=%[1]q login="false"
//ptah:schema:role name=%[2]q login="false"
//ptah:schema:grant role=%[2]q privilege="SELECT" on_table="t"
//ptah:schema:table name="t"
type T struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}
`, roles.owner, roles.grantee)
	c.Assert(os.WriteFile(filepath.Join(dir, "entities.go"), []byte(source), 0o600), qt.IsNil)
	return dir
}

// TestSchemaDiffToAFileLeavesAnOwnerItDoesNotMentionE2E is a source that declares the
// owner role and says nothing about what it holds. The role is managed, so a read that
// reports the owner's entry as grants plans REVOKE statements on the owner, in every
// source format, as soon as the first grant writes the ACL out. An owner that gave a
// privilege up is no different: the file makes no claim about it.
func TestSchemaDiffToAFileLeavesAnOwnerItDoesNotMentionE2E(t *testing.T) {
	const table = `CREATE TABLE t (id int PRIMARY KEY);GRANT SELECT ON t TO {grantee};`
	states := []struct {
		name  string
		extra string
	}{
		{name: "its ACL written"},
		{name: "a privilege the owner revoked from itself", extra: `REVOKE UPDATE ON t FROM {owner};`},
	}
	for _, state := range states {
		t.Run(state.name, func(t *testing.T) {
			parent := qt.New(t)
			roles := newConnectingRoles(parent)
			database := ownedPublicDatabase(parent, roles, table+state.extra)

			for _, format := range ownerFixtureFiles {
				t.Run(format.name, func(t *testing.T) {
					c := qt.New(t)

					got := runPtahNative(c, "schema", "diff", "--from", database, "--to", "file://"+format.write(c, roles, ""))

					c.Assert(got, qt.Contains, "Schemas are synced")
				})
			}
			t.Run("Go annotations", func(t *testing.T) {
				c := qt.New(t)

				got := runPtahNative(c, "schema", "compare", "--db-url", database, "--root-dir", ownerGoSource(c, roles))

				c.Assert(got, qt.Contains, "No schema differences")
			})
		})
	}
}

// TestSchemaApplyDecidesWhatAFileSaysAboutAnOwnerE2E is a source that does name a
// privilege of the owner, against a table whose ACL nobody wrote. A declared revoke
// reaches what the owner holds by default, and a declared grant is held already.
// Each row then compares as synced, read back from the server.
func TestSchemaApplyDecidesWhatAFileSaysAboutAnOwnerE2E(t *testing.T) {
	for _, format := range ownerFixtureFiles {
		t.Run(format.name, func(t *testing.T) {
			c := qt.New(t)
			roles := newConnectingRoles(c)
			const table = `CREATE TABLE t (id int PRIMARY KEY);`

			revoked := ownedPublicDatabase(c, roles, table)
			file := format.write(c, roles, format.revoke)
			runPtahNative(c, "schema", "apply", "--db-url", revoked, "--schema-file", file, "--auto-approve")
			c.Assert(ownerTableACL(c, revoked, roles), qt.Equals, `{owner=awdDxtm/owner,grantee=r/owner}`)
			c.Assert(runPtahNative(c, "schema", "diff", "--from", revoked, "--to", "file://"+file), qt.Contains, "Schemas are synced")

			granted := ownedPublicDatabase(c, roles, table)
			file = format.write(c, roles, format.grant)
			runPtahNative(c, "schema", "apply", "--db-url", granted, "--schema-file", file, "--auto-approve")
			c.Assert(ownerTableACL(c, granted, roles), qt.Equals, `{owner=arwdDxtm/owner,grantee=r/owner}`)
			c.Assert(runPtahNative(c, "schema", "diff", "--from", granted, "--to", "file://"+file), qt.Contains, "Schemas are synced")
		})
	}
}

// TestSchemaInspectLeavesOutAnOwnerNothingNamesE2E is the description of a table whose
// ACL holds the owner's entry alone. No statement wrote it, so neither a grant nor the
// role is described, and strict mode takes the database as it takes one whose ACL was
// never written. A privilege the owner gave up is stated, with its role.
func TestSchemaInspectLeavesOutAnOwnerNothingNamesE2E(t *testing.T) {
	c := qt.New(t)
	roles := newConnectingRoles(c)
	target, _ := scratchReplayDatabase(c)
	execPostgresFamilySQL(c, c.Context(), target, fmt.Sprintf(`
		CREATE SCHEMA app AUTHORIZATION %[1]s;
		SET ROLE %[1]s;
		CREATE TABLE app.t (id int PRIMARY KEY);
		GRANT SELECT ON app.t TO %[2]s;
		REVOKE SELECT ON app.t FROM %[2]s;
		RESET ROLE;`, roles.owner, roles.grantee))

	written := runPtahNative(c, "schema", "inspect", "--db-url", target, "--schemas", "app", "--format", "sql")

	c.Assert(written, qt.Contains, `CREATE TABLE "app"."t"`)
	c.Assert(written, qt.Not(qt.Contains), roles.owner)

	strict := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), "PTAH_ATLAS_STRICT_COMPAT=1")},
		"schema", "inspect", "-u", target)

	c.Assert(strict.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", strict.Stdout, strict.Stderr))
	c.Assert(strict.Stdout, qt.Not(qt.Contains), roles.owner)

	// A grant on another table names the owner as its grantor. A selection without
	// that table has no reason left to describe the role.
	execPostgresFamilySQL(c, c.Context(), target, fmt.Sprintf(`
		SET ROLE %[1]s;
		CREATE TABLE app.other (id int PRIMARY KEY);
		GRANT SELECT ON app.other TO %[2]s;
		RESET ROLE;`, roles.owner, roles.grantee))
	selected := runPtahNative(c, "schema", "inspect", "--db-url", target, "--schemas", "app", "--include", "app.t", "--format", "sql")

	c.Assert(selected, qt.Contains, `CREATE TABLE "app"."t"`)
	c.Assert(selected, qt.Not(qt.Contains), roles.owner)

	execPostgresFamilySQL(c, c.Context(), target, fmt.Sprintf(`REVOKE SELECT ON app.t FROM %s`, roles.owner))
	lost := runPtahNative(c, "schema", "inspect", "--db-url", target, "--schemas", "app", "--format", "sql")

	c.Assert(lost, qt.Contains, fmt.Sprintf(`CREATE ROLE %q`, roles.owner))
	c.Assert(lost, qt.Contains, fmt.Sprintf(`REVOKE SELECT ON TABLE "app"."t" FROM %q;`, roles.owner))
	c.Assert(lost, qt.Not(qt.Contains), fmt.Sprintf(`TO %q`, roles.owner))
}
