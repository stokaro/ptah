//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// These tests read what an object's owner holds from a server, which decides what
// its default ACL holds and when the ACL is written. PostgreSQL and YugabyteDB share
// the read and differ by release: MAINTAIN is on PostgreSQL 17 and later only.

var ownerPrivilegeEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
	{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
}

// ownerPrivilegeKinds are the objects that have a default ACL and share this read.
// In each statement {schema}, {owner} and {grantee} stand for the fixture's names.
var ownerPrivilegeKinds = []struct {
	name       string
	create     string
	on         string
	objectType string
	objectName string
	// defaultACL is the object type's letter for acldefault.
	defaultACL string
	// privilege is one the owner holds by default and a grantee can hold.
	privilege string
}{
	{
		name:       "a table",
		create:     `CREATE TABLE {schema}.t (id int PRIMARY KEY)`,
		on:         `TABLE {schema}.t`,
		objectType: "TABLE", objectName: "t", defaultACL: "r", privilege: "SELECT",
	},
	{
		name:       "a partitioned table",
		create:     `CREATE TABLE {schema}.t (id int) PARTITION BY RANGE (id)`,
		on:         `TABLE {schema}.t`,
		objectType: "TABLE", objectName: "t", defaultACL: "r", privilege: "SELECT",
	},
	{
		name:       "a view",
		create:     `CREATE VIEW {schema}.t AS SELECT 1 AS id`,
		on:         `TABLE {schema}.t`,
		objectType: "TABLE", objectName: "t", defaultACL: "r", privilege: "SELECT",
	},
	{
		name:       "a materialized view",
		create:     `CREATE MATERIALIZED VIEW {schema}.t AS SELECT 1 AS id`,
		on:         `TABLE {schema}.t`,
		objectType: "TABLE", objectName: "t", defaultACL: "r", privilege: "SELECT",
	},
	{
		name: "a foreign table",
		create: `RESET ROLE;
			CREATE FOREIGN DATA WRAPPER {schema}_w;
			CREATE SERVER {schema}_s FOREIGN DATA WRAPPER {schema}_w;
			GRANT USAGE ON FOREIGN SERVER {schema}_s TO {owner};
			SET ROLE {owner};
			CREATE FOREIGN TABLE {schema}.t (id int) SERVER {schema}_s`,
		on:         `TABLE {schema}.t`,
		objectType: "TABLE", objectName: "t", defaultACL: "r", privilege: "SELECT",
	},
	{
		name:       "a sequence",
		create:     `CREATE SEQUENCE {schema}.t`,
		on:         `SEQUENCE {schema}.t`,
		objectType: "SEQUENCE", objectName: "t", defaultACL: "s", privilege: "USAGE",
	},
	{
		name:       "a sequence a column owns",
		create:     `CREATE TABLE {schema}.c (id serial PRIMARY KEY)`,
		on:         `SEQUENCE {schema}.c_id_seq`,
		objectType: "SEQUENCE", objectName: "c_id_seq", defaultACL: "s", privilege: "USAGE",
	},
	{
		name:       "a schema",
		create:     `SELECT 1`,
		on:         `SCHEMA {schema}`,
		objectType: "SCHEMA", defaultACL: "n", privilege: "USAGE",
	},
}

// ownerPrivilegeFixture is a schema a role of its own owns, and a role that holds
// nothing there.
type ownerPrivilegeFixture struct {
	conn    *dbschema.DatabaseConnection
	schema  string
	owner   string
	grantee string
}

func newOwnerPrivilegeFixture(c *qt.C, engine dbtarget.Engine) ownerPrivilegeFixture {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, engine))
	c.Assert(err, qt.IsNil)
	suffix := time.Now().UnixNano()
	f := ownerPrivilegeFixture{
		conn:    conn,
		schema:  fmt.Sprintf("ptah_owner_acl_%d", suffix),
		owner:   fmt.Sprintf("ptah_owner_acl_owner_%d", suffix),
		grantee: fmt.Sprintf("ptah_owner_acl_grantee_%d", suffix),
	}
	c.Cleanup(func() {
		for _, statement := range []string{
			"DROP SCHEMA IF EXISTS " + f.schema + " CASCADE",
			"DROP FOREIGN DATA WRAPPER IF EXISTS " + f.schema + "_w CASCADE",
			"DROP ROLE IF EXISTS " + f.owner,
			"DROP ROLE IF EXISTS " + f.grantee,
		} {
			_, dropErr := conn.ExecContext(context.Background(), statement)
			c.Check(dropErr, qt.IsNil, qt.Commentf("statement: %s", statement))
		}
		c.Check(conn.Close(), qt.IsNil)
	})
	f.exec(c, "CREATE ROLE {owner} NOLOGIN")
	f.exec(c, "CREATE ROLE {grantee} NOLOGIN")
	f.exec(c, "CREATE SCHEMA {schema} AUTHORIZATION {owner}")
	return f
}

// exec runs statement as the owner.
func (f ownerPrivilegeFixture) exec(c *qt.C, statement string) {
	c.Helper()
	spelled := strings.NewReplacer("{schema}", f.schema, "{owner}", f.owner, "{grantee}", f.grantee).Replace(statement)
	_, err := f.conn.ExecContext(c.Context(), spelled)
	c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", spelled))
}

func (f ownerPrivilegeFixture) read(c *qt.C) *catalog.Database {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), f.conn, []string{f.schema})
	c.Assert(err, qt.IsNil)
	return live
}

// rowsOn keeps what a read reports about the owner on one object.
func (f ownerPrivilegeFixture) rowsOn(live *catalog.Database, objectType, objectName string) []catalog.Grant {
	if objectType == "SCHEMA" {
		objectName = f.schema
	}
	var kept []catalog.Grant
	for _, grant := range live.Grants {
		if grant.ObjectType == objectType && grant.ObjectName == objectName && grant.Role == f.owner {
			kept = append(kept, grant)
		}
	}
	return kept
}

// defaultPrivileges asks the server which privileges the object type's default ACL
// gives the owner, sorted as the read sorts them.
func (f ownerPrivilegeFixture) defaultPrivileges(c *qt.C, defaultACL string) []string {
	c.Helper()
	rows, err := f.conn.QueryContext(c.Context(), fmt.Sprintf(`
		SELECT d.privilege_type
		FROM aclexplode(acldefault('%s', '%s'::regrole)) d
		WHERE d.grantee = '%s'::regrole
		ORDER BY 1`, defaultACL, f.owner, f.owner))
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var privileges []string
	for rows.Next() {
		var privilege string
		c.Assert(rows.Scan(&privilege), qt.IsNil)
		privileges = append(privileges, privilege)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return privileges
}

func heldPrivileges(rows []catalog.Grant) []string {
	var held []string
	for _, row := range rows {
		if row.OwnerDefault() {
			held = append(held, row.Privilege)
		}
	}
	return held
}

func revokedPrivileges(rows []catalog.Grant) []string {
	var revoked []string
	for _, row := range rows {
		if row.OwnerRevoked() {
			revoked = append(revoked, row.Privilege)
		}
	}
	return revoked
}

func privilegesWithOption(rows []catalog.Grant) []string {
	var named []string
	for _, row := range rows {
		if row.WithOption {
			named = append(named, row.Privilege)
		}
	}
	return named
}

// TestOwnerPrivileges_ReadTheSameWhetherTheACLIsWritten reads an object before and
// after its ACL is written out, by a grant that leaves nothing else behind. A read
// that takes the written entry for grants tells the two apart, and a comparison then
// grants the owner its privileges in one direction and revokes them in the other.
func TestOwnerPrivileges_ReadTheSameWhetherTheACLIsWritten(t *testing.T) {
	for _, engine := range ownerPrivilegeEngines {
		for _, kind := range ownerPrivilegeKinds {
			t.Run(engine.name+"/"+kind.name, func(t *testing.T) {
				c := qt.New(t)
				f := newOwnerPrivilegeFixture(c, engine.engine)
				f.exec(c, "SET ROLE {owner}; "+kind.create+"; RESET ROLE")

				before := f.read(c)
				f.exec(c, "GRANT "+kind.privilege+" ON "+kind.on+" TO {grantee}; REVOKE "+kind.privilege+" ON "+kind.on+" FROM {grantee}")
				after := f.read(c)

				got := f.rowsOn(after, kind.objectType, kind.objectName)
				c.Assert(got, qt.DeepEquals, f.rowsOn(before, kind.objectType, kind.objectName))
				c.Assert(heldPrivileges(got), qt.DeepEquals, f.defaultPrivileges(c, kind.defaultACL))
				c.Assert(revokedPrivileges(got), qt.HasLen, 0)
			})
		}
	}
}

// TestOwnerPrivileges_ReadWhatTheOwnerRevokedFromItself is the difference the read
// above must not hide. Each privilege the default ACL gives is held or revoked, so a
// comparison plans the one that differs. An owner that revokes everything leaves an
// ACL that explodes to no rows at all.
func TestOwnerPrivileges_ReadWhatTheOwnerRevokedFromItself(t *testing.T) {
	for _, engine := range ownerPrivilegeEngines {
		for _, kind := range ownerPrivilegeKinds {
			t.Run(engine.name+"/"+kind.name, func(t *testing.T) {
				c := qt.New(t)
				f := newOwnerPrivilegeFixture(c, engine.engine)
				f.exec(c, "SET ROLE {owner}; "+kind.create+"; RESET ROLE")

				f.exec(c, "REVOKE "+kind.privilege+" ON "+kind.on+" FROM {owner}")
				one := f.rowsOn(f.read(c), kind.objectType, kind.objectName)

				c.Assert(revokedPrivileges(one), qt.DeepEquals, []string{kind.privilege})
				c.Assert(heldPrivileges(one), qt.HasLen, len(f.defaultPrivileges(c, kind.defaultACL))-1)

				f.exec(c, "REVOKE ALL ON "+kind.on+" FROM {owner}")
				every := f.rowsOn(f.read(c), kind.objectType, kind.objectName)

				c.Assert(heldPrivileges(every), qt.HasLen, 0)
				c.Assert(revokedPrivileges(every), qt.DeepEquals, f.defaultPrivileges(c, kind.defaultACL))
			})
		}
	}
}

// TestOwnerPrivileges_ReportTheOptionTheACLHolds is what makes a declared grant option
// converge. PostgreSQL treats an owner as holding every grant option, so the ACL says
// nothing about what the owner may do, and a read that reports none plans the option
// on every run.
func TestOwnerPrivileges_ReportTheOptionTheACLHolds(t *testing.T) {
	for _, engine := range ownerPrivilegeEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			f := newOwnerPrivilegeFixture(c, engine.engine)
			f.exec(c, "SET ROLE {owner}; CREATE TABLE {schema}.t (id int PRIMARY KEY); RESET ROLE")

			f.exec(c, "SET ROLE {owner}; GRANT SELECT ON {schema}.t TO {owner} WITH GRANT OPTION; RESET ROLE")

			rows := f.rowsOn(f.read(c), "TABLE", "t")
			c.Assert(privilegesWithOption(rows), qt.DeepEquals, []string{"SELECT"})
			c.Assert(heldPrivileges(rows), qt.DeepEquals, f.defaultPrivileges(c, "r"))
		})
	}
}

// TestOwnerPrivileges_TheReadDescribesTheOwnerWhenSomethingNamesIt is the role
// scoping. Ownership is no reason to describe a role, and neither is the owner's own
// entry in a written ACL. Writing a grant is, because the grant names its grantor, and
// so is a privilege the owner gave up, which a description states as a revoke.
func TestOwnerPrivileges_TheReadDescribesTheOwnerWhenSomethingNamesIt(t *testing.T) {
	tests := []struct {
		name string
		acl  string
		want bool
	}{
		{
			name: "the ACL holds the owner's entry alone",
			acl:  "GRANT SELECT ON {schema}.t TO {grantee}; REVOKE SELECT ON {schema}.t FROM {grantee}",
		},
		{
			name: "the owner wrote a grant",
			acl:  "SET ROLE {owner}; GRANT SELECT ON {schema}.t TO {grantee}; RESET ROLE",
			want: true,
		},
		{
			name: "the owner revoked a privilege from itself",
			acl:  "REVOKE SELECT ON {schema}.t FROM {owner}",
			want: true,
		},
		{
			name: "the schema's ACL holds the owner's entry alone",
			acl:  "GRANT USAGE ON SCHEMA {schema} TO {grantee}; REVOKE USAGE ON SCHEMA {schema} FROM {grantee}",
		},
		{
			name: "the owner wrote a grant on the schema",
			acl:  "SET ROLE {owner}; GRANT USAGE ON SCHEMA {schema} TO {grantee}; RESET ROLE",
			want: true,
		},
		{
			name: "the owner revoked a privilege on the schema from itself",
			acl:  "REVOKE USAGE ON SCHEMA {schema} FROM {owner}",
			want: true,
		},
	}
	for _, engine := range ownerPrivilegeEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				f := newOwnerPrivilegeFixture(c, engine.engine)
				f.exec(c, "SET ROLE {owner}; CREATE TABLE {schema}.t (id int PRIMARY KEY); RESET ROLE")
				f.exec(c, test.acl)

				got := f.read(c)

				described := slices.ContainsFunc(got.Roles, func(role catalog.Role) bool { return role.Name == f.owner })
				c.Assert(described, qt.Equals, test.want)
			})
		}
	}
}
