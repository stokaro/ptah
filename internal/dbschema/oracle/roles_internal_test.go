package oracle

// White-box testing required: the role read's degradation is decided inside
// the reader, from a driver error the exported API turns into either a
// description or a failure, and neither answer names which branch produced it.

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/sijms/go-ora/v3/network"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
)

// TestIsRoleReadDenied_SeparatesARefusalFromAFault holds the distinction the
// whole degradation rests on.
//
// A refusal is something to degrade around; everything else is a fault to
// surface. Reading ORA-03113 as a refusal would turn a dropped connection into
// a description that silently claims it did not look, and a run against a
// broken server would report a converged schema.
//
// The wrapped row is not a courtesy. database/sql hands a driver error back
// inside its own, so a check that compared rather than unwrapped would answer
// "fault" to every real refusal and fail the read this exists to survive.
func TestIsRoleReadDenied_SeparatesARefusalFromAFault(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "view not visible", err: network.NewOracleError(errViewNotVisible), want: true},
		{name: "insufficient privileges", err: network.NewOracleError(errInsufficientPrivileg), want: true},
		{name: "wrapped refusal", err: fmt.Errorf("read roles: %w", network.NewOracleError(errViewNotVisible)), want: true},
		{name: "end-of-file on communication channel", err: network.NewOracleError(3113), want: false},
		{name: "invalid identifier", err: network.NewOracleError(904), want: false},
		{name: "not an Oracle error", err: errors.New("dial tcp: connection refused"), want: false},
		{name: "no error", err: nil, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(isRoleReadDenied(test.err), qt.Equals, test.want)
		})
	}
}

// TestGrantObjectTypeFor_SpellsAViewTheWayADeclarationDoes pins the mapping
// that decides whether a view grant round-trips.
//
// schemamodel.Grant spells every relation target OnTable, so the declared side of
// a view grant carries TABLE. A reader passing ALL_TAB_PRIVS.TYPE through
// unmapped keys the same grant two ways, and the comparison plans a GRANT and
// a REVOKE of it on every run of an unchanged schema.
func TestGrantObjectTypeFor_SpellsAViewTheWayADeclarationDoes(t *testing.T) {
	tests := []struct {
		name        string
		catalogType string
		want        string
	}{
		{name: "table", catalogType: "TABLE", want: "TABLE"},
		{name: "view", catalogType: "VIEW", want: "TABLE"},
		{name: "sequence", catalogType: "SEQUENCE", want: "SEQUENCE"},
		{name: "lower case", catalogType: "sequence", want: "SEQUENCE"},
		{name: "padded", catalogType: "  VIEW  ", want: "TABLE"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(grantObjectTypeFor(test.catalogType), qt.Equals, test.want)
		})
	}
}

// TestReadRolesInto_DescribesWhatAPrivilegedAccountSees is the reading half.
func TestReadRolesInto_DescribesWhatAPrivilegedAccountSees(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringRoleCatalog)
	reader := NewOracleReader(db.SQL, "APP")

	schema := &catalog.Database{}
	c.Assert(reader.readRolesInto(t.Context(), schema), qt.IsNil)

	c.Assert(schema.Roles, qt.DeepEquals, []catalog.Role{
		{Name: "APP_APPLICATION", Inherit: true, PasswordState: catalog.RolePasswordAbsent},
		{Name: "APP_EXTERNAL", Inherit: true, PasswordState: catalog.RolePasswordAbsent},
		{Name: "APP_FUTURE", Inherit: true, PasswordState: catalog.RolePasswordUnknown},
		{Name: "APP_GLOBAL", Inherit: true, PasswordState: catalog.RolePasswordAbsent},
		{Name: "APP_NULL", Inherit: true, PasswordState: catalog.RolePasswordUnknown},
		{Name: "APP_READER", Inherit: true, PasswordState: catalog.RolePasswordAbsent},
		{Name: "APP_SECRET", Inherit: true, PasswordState: catalog.RolePasswordPresent},
	})
	c.Assert(schema.Grants, qt.DeepEquals, []catalog.Grant{
		{
			Role: "APP_READER", Privilege: "SELECT", ObjectType: "TABLE",
			Schema: "APP", ObjectName: "DOCS", GrantedBy: "APP",
		},
		{
			Role: "APP_READER", Privilege: "EXECUTE", ObjectType: "TABLE",
			Schema: "APP", ObjectName: "TALLY", GrantedBy: "APP",
		},
		{
			Role: "APP_READER", Privilege: "SELECT", ObjectType: "TABLE",
			Schema: "APP", ObjectName: "TITLES", GrantedBy: "APP",
		},
		{
			Role: "APP_READER", Privilege: "EXECUTE", ObjectType: "TABLE",
			Schema: "APP", ObjectName: "TOUCH", GrantedBy: "APP",
		},
	})
	c.Assert(schema.WideGrants, qt.DeepEquals, []catalog.WideGrant{
		{Role: "APP_READER", Privilege: "SELECT", Grant: "SELECT ANY TABLE"},
		{Role: "PUBLIC", Privilege: "READ", Grant: "READ ANY TABLE"},
		{Role: "APP_WRITER", Privilege: "INSERT", Grant: "INSERT ANY TABLE ON SCHEMA APP", Schema: "APP"},
		{Role: "PUBLIC", Privilege: "SELECT", Grant: "SELECT ANY TABLE ON SCHEMA APP", Schema: "APP"},
	})
	c.Assert(schema.WideGrantsPartial, qt.IsFalse)
	c.Assert(schema.NotDescribed.IsZero(), qt.IsTrue)
}

// TestReadRolesInto_AsksNoSchemaPrivilegeOnOracle21 reads the system
// privileges on every table of an Oracle 21 server and does not ask for those
// granted on a schema. The view of them does not exist there, and the server
// answers ORA-00942, as this catalog does, which the reader would take for a
// refusal and report the read partial.
func TestReadRolesInto_AsksNoSchemaPrivilegeOnOracle21(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, catalogWithoutSchemaPrivileges)
	reader := NewOracleReaderWithCapabilities(db.SQL, "APP", capability.Oracle21())

	schema := &catalog.Database{}
	c.Assert(reader.readRolesInto(t.Context(), schema), qt.IsNil)

	c.Assert(schema.WideGrants, qt.DeepEquals, []catalog.WideGrant{
		{Role: "APP_READER", Privilege: "SELECT", Grant: "SELECT ANY TABLE"},
		{Role: "PUBLIC", Privilege: "READ", Grant: "READ ANY TABLE"},
	})
	c.Assert(schema.WideGrantsPartial, qt.IsFalse)
}

// TestReadRolesInto_KeepsTheSystemPrivilegesWhereSchemaPrivilegesAreRefused
// keeps the system privileges on every table when the privileges granted on
// the schema are refused, and reports the read partial: a revoke on a table
// SELECT ANY TABLE covers is still refused, and one on a table the plan
// creates is planned, so the server says whether the role held the privilege
// on it.
func TestReadRolesInto_KeepsTheSystemPrivilegesWhereSchemaPrivilegesAreRefused(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, catalogWithoutSchemaPrivileges)
	reader := NewOracleReader(db.SQL, "APP")

	schema := &catalog.Database{}
	c.Assert(reader.readRolesInto(t.Context(), schema), qt.IsNil)

	c.Assert(schema.Roles, qt.HasLen, 7)
	c.Assert(schema.WideGrants, qt.DeepEquals, []catalog.WideGrant{
		{Role: "APP_READER", Privilege: "SELECT", Grant: "SELECT ANY TABLE"},
		{Role: "PUBLIC", Privilege: "READ", Grant: "READ ANY TABLE"},
	})
	c.Assert(schema.WideGrantsPartial, qt.IsTrue)
	c.Assert(schema.NotDescribed.IsZero(), qt.IsTrue)
}

// TestReadRolesInto_SurfacesAFaultInTheSchemaPrivileges fails the read when
// the privileges granted on the schema answer a fault rather than a refusal.
func TestReadRolesInto_SurfacesAFaultInTheSchemaPrivileges(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		return roleCatalogAnswerWith(query, args, nil, nil, network.NewOracleError(3113))
	})
	reader := NewOracleReader(db.SQL, "APP")

	schema := &catalog.Database{}
	err := reader.readRolesInto(t.Context(), schema)

	c.Assert(err, qt.ErrorMatches, `failed to read system privileges: .*`)
	c.Assert(schema.WideGrants, qt.IsNil)
	c.Assert(schema.WideGrantsPartial, qt.IsFalse)
}

// TestReadRolesInto_LeavesWideGrantsUnreadWhereSystemPrivilegesAreRefused
// keeps the roles and grants of an account that may read DBA_ROLES and not
// DBA_SYS_PRIVS, and says nothing about the grants on every table: nil, not
// an empty list, so a revoke on a table the plan creates is still planned.
func TestReadRolesInto_LeavesWideGrantsUnreadWhereSystemPrivilegesAreRefused(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, refusingSystemPrivilegeCatalog)
	reader := NewOracleReader(db.SQL, "APP")

	schema := &catalog.Database{}
	c.Assert(reader.readRolesInto(t.Context(), schema), qt.IsNil)

	c.Assert(schema.Roles, qt.HasLen, 7)
	c.Assert(schema.Grants, qt.HasLen, len(grantRows().Rows))
	c.Assert(schema.WideGrants, qt.IsNil)
	c.Assert(schema.NotDescribed.IsZero(), qt.IsTrue)
}

// TestReadRolesInto_RecordsWhatARefusedAccountDidNotLookAt is the other half,
// and the one a silent degradation passes without.
//
// The grant read is refused too even though ALL_TAB_PRIVS stays readable when
// DBA_ROLES does not: a description holding grants but no roles reads as
// "these privileges belong to roles that are not there", and the grant
// comparator decides a REVOKE from live rows alone.
func TestReadRolesInto_RecordsWhatARefusedAccountDidNotLookAt(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, refusingRoleCatalog)
	reader := NewOracleReader(db.SQL, "APP")

	schema := &catalog.Database{}
	c.Assert(reader.readRolesInto(t.Context(), schema), qt.IsNil)

	c.Assert(schema.Roles, qt.HasLen, 0)
	c.Assert(schema.Grants, qt.HasLen, 0)
	c.Assert(schema.WideGrants, qt.IsNil)
	c.Assert(schema.NotDescribed.Describes(coverage.Role, "APP_READER"), qt.IsFalse)
	// The record says WHY it did not look, and the reason is the strongest one
	// available: Ptah watched this server refuse the catalog rather than
	// assuming anything about it. A surface can turn that into "grant the
	// privilege"; it can turn a reasonless record into nothing
	// (stokaro/ptah#1346).
	c.Assert(schema.NotDescribed.Objects, qt.DeepEquals,
		[]coverage.Object{coverage.Refused(coverage.Role)})
}

// TestReadRolesInto_SurfacesAFaultRatherThanDescribingAroundIt is the control
// the two above need: the same shape, one error code apart, and the reader
// has to fail rather than record a limit it did not meet.
func TestReadRolesInto_SurfacesAFaultRatherThanDescribingAroundIt(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, faultingRoleCatalog)
	reader := NewOracleReader(db.SQL, "APP")

	schema := &catalog.Database{}
	err := reader.readRolesInto(t.Context(), schema)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "failed to read roles")
	c.Assert(schema.NotDescribed.IsZero(), qt.IsTrue)
}

// answeringRoleCatalog is a server that shows both catalogs.
func answeringRoleCatalog(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	return roleCatalogAnswerWith(query, args, nil, nil, nil)
}

// catalogWithoutSchemaPrivileges shows the roles, the object grants and the
// system privileges, and answers DBA_SCHEMA_PRIVS with ORA-00942, as Oracle
// 21 does to every account and Oracle 23 does to an account not granted it.
func catalogWithoutSchemaPrivileges(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	return roleCatalogAnswerWith(query, args, nil, nil, network.NewOracleError(errViewNotVisible))
}

// refusingSystemPrivilegeCatalog shows the roles and the object grants, and
// answers DBA_SYS_PRIVS with ORA-00942, as for an account granted DBA_ROLES
// alone.
func refusingSystemPrivilegeCatalog(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	return roleCatalogAnswer(query, nil, network.NewOracleError(errViewNotVisible))
}

// refusingRoleCatalog is the ordinary account: DBA_ROLES answers ORA-00942
// while ALL_TAB_PRIVS keeps working, which is the exact pair measured on
// 23.26.2.0.0.
func refusingRoleCatalog(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	return roleCatalogAnswer(query, network.NewOracleError(errViewNotVisible), network.NewOracleError(errViewNotVisible))
}

// faultingRoleCatalog answers the role read with a fault rather than a
// refusal.
func faultingRoleCatalog(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	return roleCatalogAnswer(query, network.NewOracleError(3113), nil)
}

// roleCatalogAnswer answers each catalog with the projection its query asks
// for, and hands roleErr back for the role read and sysErr for the read of
// system privileges.
//
// Answering per projection rather than per call is what makes a blanking
// mutant fail here: a query that stopped selecting AUTHENTICATION_TYPE would
// scan two columns into one and report an error, where a fake keyed on the
// call order would hand it the same rows and pass. DBA_SYS_PRIVS is matched
// first, because its query names DBA_ROLES in a subquery. ALL_TAB_PRIVS
// answers the rows of [grantRows] whose TYPE the query admits.
func roleCatalogAnswer(query string, roleErr, sysErr error) (dbtest.QueryResult, error) {
	return roleCatalogAnswerWith(query, nil, roleErr, sysErr, nil)
}

// roleCatalogAnswerWith is [roleCatalogAnswer] that also answers
// DBA_SCHEMA_PRIVS, with schemaErr, and with rows only for the schema APP the
// query is bound to, so a read that stops binding its schema loses them.
func roleCatalogAnswerWith(query string, args []driver.NamedValue, roleErr, sysErr, schemaErr error) (dbtest.QueryResult, error) {
	folded := strings.ToLower(query)
	switch {
	case strings.Contains(folded, "from dba_schema_privs"):
		answer := dbtest.QueryResult{Columns: []string{"GRANTEE", "PRIVILEGE"}}
		if len(args) == 1 && args[0].Value == "APP" {
			answer.Rows = [][]driver.Value{{"APP_WRITER", "INSERT ANY TABLE"}, {"PUBLIC", "SELECT ANY TABLE"}}
		}
		return answer, schemaErr
	case strings.Contains(folded, "from dba_sys_privs"):
		return dbtest.QueryResult{
			Columns: []string{"GRANTEE", "PRIVILEGE"},
			Rows:    [][]driver.Value{{"APP_READER", "SELECT ANY TABLE"}, {"PUBLIC", "READ ANY TABLE"}},
		}, sysErr
	case strings.Contains(folded, "from dba_roles"):
		return roleRows(), roleErr
	case strings.Contains(folded, "from all_tab_privs"):
		return grantRowsAdmittedBy(query), nil
	}
	return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
}

func roleRows() dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{"ROLE", "AUTHENTICATION_TYPE"},
		Rows: [][]driver.Value{
			{"APP_APPLICATION", "APPLICATION"},
			{"APP_EXTERNAL", "EXTERNAL"},
			{"APP_FUTURE", "FUTURE_AUTHENTICATION"},
			{"APP_GLOBAL", "GLOBAL"},
			{"APP_NULL", nil},
			{"APP_READER", "NONE"},
			{"APP_SECRET", "PASSWORD"},
		},
	}
}

// grantRows is one privilege on an object of each type the reader reads, in
// the order the query sorts them.
func grantRows() dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{"GRANTEE", "PRIVILEGE", "TABLE_NAME", "GRANTABLE", "TYPE", "GRANTOR"},
		Rows: [][]driver.Value{
			{"APP_READER", "SELECT", "DOCS", "NO", "TABLE", "APP"},
			{"APP_READER", "EXECUTE", "TALLY", "NO", "FUNCTION", "APP"},
			{"APP_READER", "SELECT", "TITLES", "NO", "VIEW", "APP"},
			{"APP_READER", "EXECUTE", "TOUCH", "NO", "PROCEDURE", "APP"},
		},
	}
}

// grantRowsAdmittedBy is the rows of [grantRows] whose TYPE query names as a
// quoted literal, the way the TYPE filter of [grantQuery] does. A reader that
// stops admitting a type loses that type's row here, as it would on a server,
// so a test comparing the grants it reads sees the loss.
func grantRowsAdmittedBy(query string) dbtest.QueryResult {
	rows := grantRows()
	rows.Rows = slices.DeleteFunc(rows.Rows, func(row []driver.Value) bool {
		return !strings.Contains(query, fmt.Sprintf("'%s'", row[4]))
	})
	return rows
}
