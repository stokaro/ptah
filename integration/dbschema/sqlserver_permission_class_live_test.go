//go:build integration

package dbschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dbschema"
)

// sqlServerPermissionClassFixture holds a securable of each permission class
// the 2025 image can create without a CLR assembly, the role r, and a grant to
// r on each, beside a grant on a schema, on a table and on a procedure whose
// owner is rb_u rather than dbo.
var sqlServerPermissionClassFixture = []string{
	"CREATE ROLE r",
	"CREATE ROLE rb_other",
	"CREATE USER rb_u WITHOUT LOGIN",
	"CREATE APPLICATION ROLE ar WITH PASSWORD = 'Ar-unused-1!'",
	"CREATE TYPE dbo.money_t FROM int",
	"CREATE SCHEMA app AUTHORIZATION rb_u",
	"CREATE XML SCHEMA COLLECTION app.xc AS N'<xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">" +
		"<xsd:element name=\"a\" type=\"xsd:string\"/></xsd:schema>'",
	"CREATE CERTIFICATE ce ENCRYPTION BY PASSWORD = 'Ce-unused-1!' WITH SUBJECT = 'permission classes'",
	"CREATE SYMMETRIC KEY sk WITH ALGORITHM = AES_256 ENCRYPTION BY PASSWORD = 'Sk-unused-1!'",
	"CREATE ASYMMETRIC KEY ak WITH ALGORITHM = RSA_2048 ENCRYPTION BY PASSWORD = 'Ak-unused-1!'",
	"CREATE MESSAGE TYPE mt",
	"CREATE CONTRACT ct (mt SENT BY INITIATOR)",
	"CREATE QUEUE dbo.q",
	"CREATE SERVICE sv ON QUEUE dbo.q (ct)",
	"CREATE ROUTE rt WITH ADDRESS = 'LOCAL'",
	"CREATE REMOTE SERVICE BINDING rsb TO SERVICE 'far' WITH USER = rb_u",
	"CREATE FULLTEXT CATALOG fc",
	"CREATE FULLTEXT STOPLIST sl;",
	"CREATE SEARCH PROPERTY LIST spl;",
	"CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Mk-unused-1!'",
	"CREATE DATABASE SCOPED CREDENTIAL dc WITH IDENTITY = 'permission classes', SECRET = 'unused'",
	"CREATE TABLE dbo.u (id INT PRIMARY KEY)",
	"ALTER AUTHORIZATION ON OBJECT::dbo.u TO rb_u",
	"CREATE PROCEDURE dbo.p AS SELECT 1 AS one",
	"ALTER AUTHORIZATION ON OBJECT::dbo.p TO rb_u",
	"GRANT ALTER ON ROLE::rb_other TO r",
	"DENY VIEW DEFINITION ON ROLE::rb_other TO r",
	"GRANT IMPERSONATE ON USER::rb_u TO r",
	"GRANT ALTER ON APPLICATION ROLE::ar TO r",
	"GRANT EXECUTE ON TYPE::dbo.money_t TO r WITH GRANT OPTION",
	"GRANT ALTER ON XML SCHEMA COLLECTION::app.xc TO r",
	"GRANT CONTROL ON CERTIFICATE::ce TO r",
	"GRANT CONTROL ON SYMMETRIC KEY::sk TO r",
	"GRANT CONTROL ON ASYMMETRIC KEY::ak TO r",
	"GRANT CONTROL ON MESSAGE TYPE::mt TO r",
	"GRANT CONTROL ON CONTRACT::ct TO r",
	"GRANT SEND ON SERVICE::sv TO r",
	"GRANT CONTROL ON ROUTE::rt TO r",
	"GRANT CONTROL ON REMOTE SERVICE BINDING::rsb TO r",
	"GRANT CONTROL ON FULLTEXT CATALOG::fc TO r",
	"GRANT CONTROL ON FULLTEXT STOPLIST::sl TO r",
	"GRANT CONTROL ON SEARCH PROPERTY LIST::spl TO r",
	"GRANT CONTROL ON DATABASE SCOPED CREDENTIAL::dc TO r",
	"GRANT SELECT ON SCHEMA::app TO r",
	"GRANT SELECT ON dbo.u TO r",
	"GRANT EXECUTE ON dbo.p TO r",
}

// TestSQLServerLiveReaderNamesEachPermissionClass reads a permission on each
// securable class by the securable's own name.
//
// major_id holds an id in the number space of the permission's class, and the
// spaces overlap: OBJECT_NAME of a schema_id or of a role's principal_id names
// a system object, so a read that named every major_id as an object read
// `GRANT ALTER ON ROLE::rb_other` as a grant on sysclones and a grant on a
// type under no name. The schema models permissions on the database, a
// schema, an object and a column; a permission on any other class is kept out
// of the grants a comparison reads and reported as the statement that gives
// it. A grant on a schema, a table and a procedure is read under its name,
// with the owner of its securable, which a statement run by sysadmin names as
// its grantor.
//
// The database has a case-sensitive collation, so a name the read takes from a
// catalog view of another collation cannot pass unless the read settles it.
func TestSQLServerLiveReaderNamesEachPermissionClass(t *testing.T) {
	c := qt.New(t)
	conn := connectSQLServerCollationDatabase(c, provisionSQLServerCollationDatabase(c, sqlServerCSCollation))
	for _, statement := range sqlServerPermissionClassFixture {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{"dbo", "app"})

	c.Assert(err, qt.IsNil)
	c.Assert(grantsOfRole(live.Grants, "r"), qt.DeepEquals, []catalog.Grant{
		{Role: "r", Privilege: "EXECUTE", ObjectType: "TABLE", Schema: "dbo", ObjectName: "p", GrantedBy: "rb_u", ObjectOwner: "rb_u"},
		{Role: "r", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "u", GrantedBy: "rb_u", ObjectOwner: "rb_u"},
		{Role: "r", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "app", GrantedBy: "rb_u", ObjectOwner: "rb_u"},
	})
	reason := "Ptah compares permissions on the database, a schema, an object or a column, " +
		"and no permission on another securable class, so no plan grants, denies or revokes it"
	unmodeled := func(name string) catalog.UnmodeledObject {
		return catalog.UnmodeledObject{Kind: "permission", Name: name, Reason: reason}
	}
	c.Assert(live.UnmodeledObjects, qt.DeepEquals, []catalog.UnmodeledObject{
		unmodeled("GRANT ALTER ON ROLE::rb_other TO r"),
		unmodeled("GRANT ALTER ON APPLICATION ROLE::ar TO r"),
		unmodeled("GRANT ALTER ON XML SCHEMA COLLECTION::app.xc TO r"),
		unmodeled("GRANT CONTROL ON MESSAGE TYPE::mt TO r"),
		unmodeled("GRANT CONTROL ON CONTRACT::ct TO r"),
		unmodeled("GRANT CONTROL ON REMOTE SERVICE BINDING::rsb TO r"),
		unmodeled("GRANT CONTROL ON ROUTE::rt TO r"),
		unmodeled("GRANT CONTROL ON FULLTEXT CATALOG::fc TO r"),
		unmodeled("GRANT CONTROL ON SYMMETRIC KEY::sk TO r"),
		unmodeled("GRANT CONTROL ON CERTIFICATE::ce TO r"),
		unmodeled("GRANT CONTROL ON ASYMMETRIC KEY::ak TO r"),
		unmodeled("GRANT CONTROL ON FULLTEXT STOPLIST::sl TO r"),
		unmodeled("GRANT CONTROL ON SEARCH PROPERTY LIST::spl TO r"),
		unmodeled("GRANT CONTROL ON DATABASE SCOPED CREDENTIAL::dc TO r"),
		unmodeled("GRANT EXECUTE ON TYPE::dbo.money_t TO r WITH GRANT OPTION"),
		unmodeled("GRANT IMPERSONATE ON USER::rb_u TO r"),
		unmodeled("GRANT SEND ON SERVICE::sv TO r"),
		unmodeled("DENY VIEW DEFINITION ON ROLE::rb_other TO r"),
	})
}

// grantsOfRole is the rows of grants that role holds, in the read's order.
func grantsOfRole(grants []catalog.Grant, role string) []catalog.Grant {
	held := make([]catalog.Grant, 0)
	for _, grant := range grants {
		if grant.Role == role {
			held = append(held, grant)
		}
	}
	return held
}
