package clickhouserbac_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/clickhouserbac"
)

// managedSchema is a declaration naming one role, which is what makes a live
// row's role "managed" for [clickhouserbac.ValidateLive].
func managedSchema(roles ...string) *schemamodel.Database {
	declared := make([]schemamodel.Role, 0, len(roles))
	for _, name := range roles {
		declared = append(declared, schemamodel.Role{Name: name, Inherit: true})
	}
	return &schemamodel.Database{Roles: declared}
}

// partialRevoke is the second row ClickHouse leaves behind when a narrower
// scope is revoked from a broader grant. Measured on 26.7.3.19:
// `GRANT SELECT ON db.* TO r` followed by `REVOKE SELECT ON db.t FROM r`
// leaves the grant row and this one.
func partialRevoke(role, privilege, database, table string) catalog.Grant {
	return catalog.Grant{
		Role:            role,
		Privilege:       privilege,
		ObjectType:      "TABLE",
		Schema:          database,
		ObjectName:      table,
		IsPartialRevoke: true,
	}
}

func TestValidateLive_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		database *catalog.Database
	}{
		{
			name:     "no grants at all",
			desired:  managedSchema("reader"),
			database: &catalog.Database{},
		},
		{
			name:    "ordinary grants on a managed role",
			desired: managedSchema("reader"),
			database: &catalog.Database{Grants: []catalog.Grant{
				{Role: "reader", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "shop"},
				{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", Schema: "shop", ObjectName: "orders"},
			}},
		},
		{
			// The ownership boundary. A partial revoke on a role no declaration
			// names is somebody else's arrangement: Ptah neither compares it
			// nor plans against it, so there is nothing for it to get wrong.
			// Refusing here would make an unrelated role on a shared server
			// fail every migration.
			name:    "a partial revoke on a role nothing declares",
			desired: managedSchema("reader"),
			database: &catalog.Database{Grants: []catalog.Grant{
				partialRevoke("outsider", "SELECT", "shop", "orders"),
			}},
		},
		{
			name:    "a declaration with no roles leaves every live row alone",
			desired: &schemamodel.Database{},
			database: &catalog.Database{Grants: []catalog.Grant{
				partialRevoke("outsider", "SELECT", "shop", "orders"),
			}},
		},
		{
			name:     "a nil live schema is a caller with nothing to check",
			desired:  managedSchema("reader"),
			database: nil,
		},
		{
			name:    "a nil declaration is a caller with nothing to check",
			desired: nil,
			database: &catalog.Database{Grants: []catalog.Grant{
				partialRevoke("reader", "SELECT", "shop", "orders"),
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := clickhouserbac.ValidateLive(platform.ClickHouse, test.desired, test.database)
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestValidateLive_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		database *catalog.Database
		wantErr  string
	}{
		{
			name:    "a partial revoke on a managed role",
			desired: managedSchema("reader"),
			database: &catalog.Database{Grants: []catalog.Grant{
				partialRevoke("reader", "SELECT", "shop", "orders"),
			}},
			wantErr: `(?s).*role "reader" carries a partial revoke of SELECT on shop\.orders.*not grants with exceptions.*`,
		},
		{
			// The broader grant the exception applies to is present too, which
			// is the shape that reads as convergence: the comparator matches
			// the grant row and plans nothing, so without this refusal Ptah
			// would report a synced schema for a role whose privileges are
			// quietly narrower than the declaration says.
			name:    "the grant and its exception together",
			desired: managedSchema("reader"),
			database: &catalog.Database{Grants: []catalog.Grant{
				{Role: "reader", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "shop"},
				partialRevoke("reader", "SELECT", "shop", "orders"),
			}},
			wantErr: `(?s).*carries a partial revoke of SELECT on shop\.orders.*`,
		},
		{
			name:    "a database-scoped exception names the database scope",
			desired: managedSchema("reader"),
			database: &catalog.Database{Grants: []catalog.Grant{
				{
					Role: "reader", Privilege: "INSERT", ObjectType: "SCHEMA",
					ObjectName: "shop", IsPartialRevoke: true,
				},
			}},
			wantErr: `(?s).*partial revoke of INSERT on shop\.\*.*`,
		},
		{
			// Both halves are reported rather than the first one found, so an
			// operator fixing this learns the whole of what is in the way.
			name:    "every managed role with an exception is named",
			desired: managedSchema("reader", "writer"),
			database: &catalog.Database{Grants: []catalog.Grant{
				partialRevoke("writer", "INSERT", "shop", "orders"),
				partialRevoke("reader", "SELECT", "shop", "orders"),
			}},
			wantErr: `(?s).*role "reader" carries a partial revoke.*role "writer" carries a partial revoke.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := clickhouserbac.ValidateLive(platform.ClickHouse, test.desired, test.database)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestValidateLive_LeavesOtherDialectsAlone is the non-interference control.
//
// Only the ClickHouse reader ever sets IsPartialRevoke, but a live schema is
// not tagged with the dialect that produced it, so a check that fired on the
// field alone would refuse a comparison on any dialect whose reader later
// learned to set it. The dialect decides, and this row is what says so.
func TestValidateLive_LeavesOtherDialectsAlone(t *testing.T) {
	desired := managedSchema("reader")
	database := &catalog.Database{Grants: []catalog.Grant{
		partialRevoke("reader", "SELECT", "shop", "orders"),
	}}

	dialects := []string{
		platform.Postgres,
		platform.MySQL,
		platform.MariaDB,
		platform.SQLite,
		platform.SQLServer,
		platform.CockroachDB,
		platform.YugabyteDB,
		platform.Spanner,
	}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(clickhouserbac.ValidateLive(dialect, desired, database), qt.IsNil)
		})
	}
}

// TestValidateLive_RefusesTheSameStateOnClickHouse is the other half of the
// control above: without it, a ValidateLive that returned nil for every dialect
// would satisfy the non-interference test completely.
func TestValidateLive_RefusesTheSameStateOnClickHouse(t *testing.T) {
	c := qt.New(t)

	err := clickhouserbac.ValidateLive(
		platform.ClickHouse,
		managedSchema("reader"),
		&catalog.Database{Grants: []catalog.Grant{
			partialRevoke("reader", "SELECT", "shop", "orders"),
		}},
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `role "reader" carries a partial revoke`)
}

// TestValidateLive_IsDeterministic guards the diagnostic against the order the
// reader happened to return rows in. A message that names a different offender
// on each run is one no test can pin and no reader can trust.
func TestValidateLive_IsDeterministic(t *testing.T) {
	c := qt.New(t)

	desired := managedSchema("a", "b", "c")
	forward := &catalog.Database{Grants: []catalog.Grant{
		partialRevoke("c", "SELECT", "shop", "orders"),
		partialRevoke("a", "INSERT", "warehouse", "items"),
		partialRevoke("b", "SELECT", "shop", "items"),
	}}
	reversed := &catalog.Database{Grants: []catalog.Grant{
		partialRevoke("b", "SELECT", "shop", "items"),
		partialRevoke("a", "INSERT", "warehouse", "items"),
		partialRevoke("c", "SELECT", "shop", "orders"),
	}}

	first := clickhouserbac.ValidateLive(platform.ClickHouse, desired, forward)
	second := clickhouserbac.ValidateLive(platform.ClickHouse, desired, reversed)

	c.Assert(first, qt.IsNotNil)
	c.Assert(second, qt.IsNotNil)
	c.Assert(second.Error(), qt.Equals, first.Error())
}

// TestValidateLive_DoesNotMutateTheLiveSchema pins that the check is a read.
//
// It sorts the rows it reports on, and sorting the caller's slice in place
// would reorder a live description that later comparisons read — a check that
// changed the state it was checking.
func TestValidateLive_DoesNotMutateTheLiveSchema(t *testing.T) {
	c := qt.New(t)

	database := &catalog.Database{Grants: []catalog.Grant{
		partialRevoke("c", "SELECT", "shop", "orders"),
		partialRevoke("a", "INSERT", "warehouse", "items"),
	}}
	before := append([]catalog.Grant(nil), database.Grants...)

	err := clickhouserbac.ValidateLive(platform.ClickHouse, managedSchema("a", "c"), database)

	c.Assert(err, qt.IsNotNil)
	c.Assert(database.Grants, qt.DeepEquals, before)
}
