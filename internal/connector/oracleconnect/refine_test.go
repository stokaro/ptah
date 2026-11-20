package oracleconnect_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/connector"
	"ptah.run/internal/connector/oracleconnect"
	"ptah.run/internal/dbschema/dbtest"
)

// server scripts the three catalog reads the connector makes beyond the
// version: MAX_STRING_SIZE from DATABASE_PROPERTIES and the collation sorts
// from V$NLS_VALID_VALUES. A nil error answers the rows; an error is what the
// server says to an account that may not read the view.
type server struct {
	stringSize    []driver.Value
	stringSizeErr error
	sorts         [][]driver.Value
	sortsErr      error
}

func (s server) open(t *testing.T) *connector.Pool {
	db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, "database_properties"):
			if s.stringSizeErr != nil {
				return dbtest.QueryResult{}, s.stringSizeErr
			}
			return dbtest.QueryResult{Columns: []string{"PROPERTY_VALUE"}, Rows: rowsOf(s.stringSize)}, nil
		case strings.Contains(query, "v$nls_valid_values"):
			if s.sortsErr != nil {
				return dbtest.QueryResult{}, s.sortsErr
			}
			return dbtest.QueryResult{Columns: []string{"VALUE"}, Rows: s.sorts}, nil
		case strings.Contains(query, "product_component_version"):
			return dbtest.QueryResult{Columns: []string{"VERSION_FULL"}, Rows: [][]driver.Value{{"23.26.0.0.0"}}}, nil
		case strings.Contains(query, "SYS_CONTEXT"):
			return dbtest.QueryResult{Columns: []string{"SCHEMA"}, Rows: [][]driver.Value{{"PTAH"}}}, nil
		}
		return dbtest.QueryResult{}, errors.New("unexpected query: " + query)
	})
	return &connector.Pool{DB: db.SQL}
}

func rowsOf(values []driver.Value) [][]driver.Value {
	if values == nil {
		return nil
	}
	return [][]driver.Value{values}
}

var errNoPrivilege = errors.New(`ORA-00942: table or view "SYS"."DATABASE_PROPERTIES" does not exist`)

func TestRefine_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		stringSize   string
		wantStandard bool
	}{
		{name: "STANDARD", stringSize: "STANDARD", wantStandard: true},
		{name: "in lower case", stringSize: "standard", wantStandard: true},
		{name: "EXTENDED", stringSize: "EXTENDED", wantStandard: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			pool := server{stringSize: []driver.Value{tc.stringSize}}.open(t)

			got, err := oracleconnect.Service{}.Refine(context.Background(), pool, connector.Request{}, catalog.ServerInfo{}, capability.Oracle23())

			c.Assert(err, qt.IsNil)
			c.Assert(got.Has(capability.StandardStringSize), qt.Equals, tc.wantStandard)
			c.Assert(got.Has(capability.DomainTypes), qt.IsTrue)
		})
	}
}

// TestRefine_UnreadablePropertyLeavesTheCapabilityOff is the account that
// cannot read DATABASE_PROPERTIES. A database that revokes the PUBLIC grant
// answers ORA-00942 to an account with CREATE SESSION only, measured on Oracle
// Free 23.26 after `REVOKE READ ON sys.database_properties FROM PUBLIC` as
// SYSDBA. The revoke needs SYS: SYSTEM answers ORA-01927 because it did not make
// the grant, so no test account of the suite can reproduce it against a live
// server, and the connector's own read stands in. The connection is made, the
// capability stays off and the preset is untouched, as it is for every verb that
// declares no collation.
func TestRefine_UnreadablePropertyLeavesTheCapabilityOff(t *testing.T) {
	tests := []struct {
		name   string
		server server
	}{
		{name: "table or view does not exist", server: server{stringSizeErr: errNoPrivilege}},
		{name: "insufficient privileges", server: server{stringSizeErr: errors.New("ORA-01031: insufficient privileges")}},
		{name: "no such property", server: server{}},
		{name: "a value that cannot be scanned", server: server{stringSize: []driver.Value{nil}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			pool := tc.server.open(t)

			got, err := oracleconnect.Service{}.Refine(context.Background(), pool, connector.Request{}, catalog.ServerInfo{}, capability.Oracle23())

			c.Assert(err, qt.IsNil)
			c.Assert(got.Has(capability.StandardStringSize), qt.IsFalse)
			c.Assert(got.Len(), qt.Equals, capability.Oracle23().Len())
			c.Assert(got.Has(capability.DomainTypes), qt.IsTrue)
		})
	}
}

func TestDescribe_ReadsTheCollationSorts(t *testing.T) {
	c := qt.New(t)
	pool := server{sorts: [][]driver.Value{{"BINARY"}, {"french "}, {"GERMAN"}}}.open(t)

	info, err := oracleconnect.Service{}.Describe(context.Background(), pool, connector.Request{}, catalog.ServerInfo{})

	c.Assert(err, qt.IsNil)
	c.Assert(info.CollationNames, qt.DeepEquals, []string{"BINARY", "FRENCH", "GERMAN"})
	c.Assert(info.Schema, qt.Equals, "PTAH")
}

// TestDescribe_UnreadableSortsAreNotAnError is the view an account may not read:
// connecting does not need the list, and a plan that declares a collation leaves
// the name to the server.
func TestDescribe_UnreadableSortsAreNotAnError(t *testing.T) {
	c := qt.New(t)
	pool := server{sortsErr: errors.New("ORA-00942: table or view does not exist")}.open(t)

	info, err := oracleconnect.Service{}.Describe(context.Background(), pool, connector.Request{}, catalog.ServerInfo{})

	c.Assert(err, qt.IsNil)
	c.Assert(info.CollationNames, qt.IsNil)
	c.Assert(info.Schema, qt.Equals, "PTAH")
}
