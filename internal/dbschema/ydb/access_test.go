package ydb_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

// owned is a directory's or a table's own entry: its owner and its permission
// entries, each a subject and the permission names it holds.
func owned(owner string, permissions ...*Ydb_Scheme.Permissions) *Ydb_Scheme.Entry {
	return &Ydb_Scheme.Entry{Owner: owner, Permissions: permissions}
}

func permission(subject string, names ...string) *Ydb_Scheme.Permissions {
	return &Ydb_Scheme.Permissions{Subject: subject, PermissionNames: names}
}

// accessSource is a database with a table in a directory, the access lists of
// the database, the directory and the table, and the principals .sys/auth_*
// reports: a cluster's own groups and user beside the declared ones.
func accessSource() fakeSource {
	table := plainTable()
	table.Self = owned("app", permission("readers", "ydb.granular.select_row"),
		permission("DDL-ADMINS", "ydb.granular.create_table", "ydb.granular.alter_schema"))
	return fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{
			"/local":      {entry("shop", Ydb_Scheme.Entry_DIRECTORY), entry(".sys", Ydb_Scheme.Entry_DIRECTORY)},
			"/local/shop": {entry("orders", Ydb_Scheme.Entry_TABLE)},
		},
		selves: map[string]*Ydb_Scheme.Entry{
			"/local":      owned("root", permission("USERS", "ydb.database.connect"), permission("app", "ydb.database.connect")),
			"/local/shop": owned("root@builtin", permission("readers", "ydb.generic.list"), permission("readers", "ydb.generic.list")),
		},
		tables: map[string]*Ydb_Table.DescribeTableResult{"/local/shop/orders": table},
		principals: ydbschema.Principals{
			Database: "/local",
			Users:    []ydbschema.User{{Name: "root", Enabled: true}, {Name: "app", Enabled: true}, {Name: "etl", Enabled: false}},
			Groups:   []string{"USERS", "readers", "DATA-READERS"},
			Memberships: []ydbschema.Membership{
				{Group: "readers", Member: "app"}, {Group: "USERS", Member: "app"}, {Group: "DATA-READERS", Member: "readers"},
			},
		},
	}
}

// TestReader_ReadsTheAccessModel_HappyPath reads the users and groups .sys
// reports, the principals the cluster made and the database's owner beside
// the description, every membership, the permission entries of the database, the directory and the
// table one grant per permission name, and each object's owner with whether
// that owner can log in. A password is not part of what is read.
func TestReader_ReadsTheAccessModel_HappyPath(t *testing.T) {
	c := qt.New(t)

	db := readFrom(c, accessSource())

	c.Assert(db.DatabasePath, qt.Equals, "/local")
	c.Assert(db.Roles, qt.DeepEquals, []catalog.Role{
		{Name: "app", Login: true, Inherit: true},
		{Name: "etl", Inherit: true},
		{Name: "readers", Inherit: true, Group: true},
	})
	c.Assert(db.RolesOutOfScope, qt.DeepEquals, []catalog.Role{
		{Name: "DATA-READERS", Inherit: true, Group: true},
		{Name: "USERS", Inherit: true, Group: true},
		{Name: "root", Login: true, Inherit: true},
	})
	c.Assert(db.RoleMemberships, qt.DeepEquals, []catalog.RoleMembership{
		{Role: "DATA-READERS", Member: "readers"},
		{Role: "USERS", Member: "app"},
		{Role: "readers", Member: "app"},
	})
	c.Assert(db.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "USERS", Privilege: "ydb.database.connect", ObjectType: "DATABASE"},
		{Role: "app", Privilege: "ydb.database.connect", ObjectType: "DATABASE"},
		{Role: "readers", Privilege: "ydb.generic.list", ObjectType: "SCHEMA", ObjectName: "shop"},
		{Role: "readers", Privilege: "ydb.granular.select_row", ObjectType: "TABLE", Schema: "shop", ObjectName: "orders"},
		{Role: "DDL-ADMINS", Privilege: "ydb.granular.create_table", ObjectType: "TABLE", Schema: "shop", ObjectName: "orders"},
		{Role: "DDL-ADMINS", Privilege: "ydb.granular.alter_schema", ObjectType: "TABLE", Schema: "shop", ObjectName: "orders"},
	})
	c.Assert(db.ObjectOwners, qt.DeepEquals, []catalog.ObjectOwner{
		{Kind: "database", Owner: "root", OwnerCanLogin: true},
		{Kind: "schema", Name: "shop", Owner: "root@builtin"},
		{Kind: "table", Schema: "shop", Name: "orders", Owner: "app", OwnerCanLogin: true},
	})
	c.Assert(db.NotDescribed.Objects, qt.HasLen, 0)
}

// TestReader_ScopedReadOfTheAccessModel reads the database's own entries and
// the principals whatever the scope, since neither belongs to a directory, and
// a directory's and a table's entries only where the scope reaches.
func TestReader_ScopedReadOfTheAccessModel(t *testing.T) {
	c := qt.New(t)
	source := accessSource()
	source.directories["/local/other"] = make([]*Ydb_Scheme.Entry, 0)
	source.directories["/local"] = append(source.directories["/local"], entry("other", Ydb_Scheme.Entry_DIRECTORY))
	source.selves["/local/other"] = owned("root", permission("app", "ydb.generic.list"))
	reader := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262())
	reader.SetSchemas([]string{"other"})

	db, err := reader.ReadSchemaContext(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(db.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "USERS", Privilege: "ydb.database.connect", ObjectType: "DATABASE"},
		{Role: "app", Privilege: "ydb.database.connect", ObjectType: "DATABASE"},
		{Role: "app", Privilege: "ydb.generic.list", ObjectType: "SCHEMA", ObjectName: "other"},
	})
	c.Assert(db.Roles, qt.HasLen, 3)
}

// TestReader_DescribesNoPrincipalInARealm reads a dev realm, a directory that
// stands in for a database: the users and groups are those of the database
// that holds it, so each is recorded as existing and none is described, while
// the realm's own permission entries and the memberships are read.
func TestReader_DescribesNoPrincipalInARealm(t *testing.T) {
	c := qt.New(t)
	source := accessSource()
	source.directories["/local/realm"] = []*Ydb_Scheme.Entry{entry("shop", Ydb_Scheme.Entry_DIRECTORY)}
	source.directories["/local/realm/shop"] = make([]*Ydb_Scheme.Entry, 0)
	source.selves["/local/realm/shop"] = owned("root@builtin", permission("app", "ydb.generic.list"))

	db, err := ydbschema.NewReaderFromSource(source, "/local/realm", capability.YDB262()).
		ReadSchemaContext(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(db.Roles, qt.IsNil)
	c.Assert(db.RolesOutOfScope, qt.DeepEquals, []catalog.Role{
		{Name: "DATA-READERS", Inherit: true, Group: true},
		{Name: "USERS", Inherit: true, Group: true},
		{Name: "app", Login: true, Inherit: true},
		{Name: "etl", Inherit: true},
		{Name: "readers", Inherit: true, Group: true},
		{Name: "root", Login: true, Inherit: true},
	})
	c.Assert(db.RoleMemberships, qt.HasLen, 3)
	c.Assert(db.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "app", Privilege: "ydb.generic.list", ObjectType: "SCHEMA", ObjectName: "shop"},
	})
}

// TestReader_RecordsRefusedPrincipals reads a database whose .sys the account
// may not read: the tables and their permission entries are read, and the
// principals are recorded as refused rather than described as none.
func TestReader_RecordsRefusedPrincipals(t *testing.T) {
	c := qt.New(t)
	source := accessSource()
	source.principals = ydbschema.Principals{}
	source.principalsErr = errors.Join(ydbschema.ErrPrincipalsRefused, errors.New("SCHEME_ERROR"))

	db := readFrom(c, source)

	c.Assert(db.Roles, qt.IsNil)
	c.Assert(db.RoleMemberships, qt.IsNil)
	c.Assert(db.Grants, qt.HasLen, 6)
	c.Assert(db.NotDescribed, qt.DeepEquals, coverage.Set{}.With(coverage.Refused(coverage.Role)))
}

// TestReader_FailsWhenThePrincipalsFail fails a read whose principals could not
// be read for a reason other than a refusal, rather than reading on without
// them.
func TestReader_FailsWhenThePrincipalsFail(t *testing.T) {
	c := qt.New(t)
	source := accessSource()
	source.principalsErr = errors.New("connection reset")

	db, err := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262()).ReadSchemaContext(context.Background())

	c.Assert(err, qt.ErrorMatches, `read the YDB users and groups: connection reset`)
	c.Assert(db, qt.IsNil)
}
