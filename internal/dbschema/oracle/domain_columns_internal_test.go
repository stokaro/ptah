package oracle

// White-box testing required: adoptDomainColumns rewrites a read that the
// table, constraint and domain reads already produced, and the exported read
// reaches it only behind every other catalog query; driving it alone over a
// prepared read is what lets each row name the one column fact it is about.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/feature/oratype"
	"ptah.run/internal/dbschema/dbtest"
)

// TestAdoptDomainColumns_ReportsWhatTheDeclarationWrote pins each column fact
// the read leaves to the domain, with the catalog rows Oracle 23.26.3.0.0
// answered for a table typed by three domains.
//
// ADDR holds its NOT NULL only through the domain, so it reads as nullable;
// LOGIN states its own, which Oracle keeps as a constraint naming no domain.
// QTY carries the domain's default, which a declaration does not repeat.
// SCORE's CHECK is a copy of the domain's, so it is not a table constraint.
// OTHER is typed by a domain of another schema, which the read does not
// describe, so it keeps its base type; its copied constraint still belongs to
// that domain.
func TestAdoptDomainColumns_ReportsWhatTheDeclarationWrote(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, domainColumnCatalog)
	reader := NewOracleReader(db.SQL, "APP")
	read := domainColumnRead(c)

	err := reader.adoptDomainColumns(t.Context(), read)

	c.Assert(err, qt.IsNil)
	c.Assert(read.Tables[0].Columns, qt.DeepEquals, []catalog.Column{
		{Name: "ID", DataType: "NUMBER(10)", IsNullable: "NO"},
		{Name: "ADDR", DataType: "VARCHAR2(255)", IsNullable: "YES", DomainName: "D_EMAIL", DomainSchema: "APP", FormattedType: "D_EMAIL"},
		{Name: "LOGIN", DataType: "VARCHAR2(255)", IsNullable: "NO", DomainName: "D_EMAIL", DomainSchema: "APP", FormattedType: "D_EMAIL"},
		{Name: "QTY", DataType: "NUMBER(10)", IsNullable: "YES", DomainName: "D_QTY", DomainSchema: "APP", FormattedType: "D_QTY"},
		{Name: "SCORE", DataType: "NUMBER(5,2)", IsNullable: "YES", DomainName: "D_SCORE", DomainSchema: "APP", FormattedType: "D_SCORE"},
		{Name: "OTHER", DataType: "NUMBER", IsNullable: "NO"},
	})
	c.Assert(read.Constraints, qt.DeepEquals, []catalog.Constraint{{Name: "OWN_CHECK", TableName: "T", Type: "CHECK"}})
}

// domainColumnRead is the read before adoption: the table as ALL_TAB_COLS
// reports it, its constraints, and the domains the domain read described.
func domainColumnRead(c *qt.C) *catalog.Database {
	c.Helper()
	described := func(name string, domain *oratype.ObservedDomain) schemaext.Object {
		return must.Must(oratype.ObservedObject(oratype.Ref(oratype.DomainKind, "", name), domain))
	}
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "T", Columns: []catalog.Column{
			{Name: "ID", DataType: "NUMBER(10)", IsNullable: "NO"},
			{Name: "ADDR", DataType: "VARCHAR2(255)", IsNullable: "NO"},
			{Name: "LOGIN", DataType: "VARCHAR2(255)", IsNullable: "NO"},
			{Name: "QTY", DataType: "NUMBER(10)", IsNullable: "YES", ColumnDefault: new("5")},
			{Name: "SCORE", DataType: "NUMBER(5,2)", IsNullable: "YES"},
			{Name: "OTHER", DataType: "NUMBER", IsNullable: "NO"},
		}}},
		Constraints: []catalog.Constraint{
			{Name: "OWN_CHECK", TableName: "T", Type: "CHECK"},
			{Name: "SYS_C0001", TableName: "T", Type: "CHECK"},
			{Name: "SYS_C0002", TableName: "T", Type: "CHECK"},
		},
		FeatureObjects: must.Must(schemaext.NewObjects(
			described("D_EMAIL", &oratype.ObservedDomain{BaseType: "VARCHAR2(255)", NotNull: true}),
			described("D_QTY", &oratype.ObservedDomain{BaseType: "NUMBER(10)", Default: "5"}),
			described("D_SCORE", &oratype.ObservedDomain{BaseType: "NUMBER(5,2)", Check: "VALUE BETWEEN 0 AND 100"}),
		)),
	}
}

// domainColumnCatalog answers the two queries adoption sends.
func domainColumnCatalog(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	folded := strings.ToLower(query)
	switch {
	case strings.Contains(folded, "from all_tab_cols"):
		return dbtest.QueryResult{
			Columns: []string{"table_name", "column_name", "domain_owner", "domain_name"},
			Rows: [][]driver.Value{
				{"T", "ADDR", "APP", "D_EMAIL"},
				{"T", "LOGIN", "APP", "D_EMAIL"},
				{"T", "QTY", "APP", "D_QTY"},
				{"T", "SCORE", "APP", "D_SCORE"},
				{"T", "OTHER", "SHARED", "D_SCORE"},
			},
		}, nil
	case strings.Contains(folded, "from all_constraints"):
		return dbtest.QueryResult{
			Columns: []string{"constraint_name", "table_name", "column_name", "search_condition_vc"},
			Rows: [][]driver.Value{
				{"SYS_C0000", "T", "ADDR", `"ADDR" IS NOT NULL`},
				{"SYS_C0001", "T", "SCORE", `"SCORE">=0 AND "SCORE"<=100`},
				{"SYS_C0002", "T", "OTHER", `"OTHER">=0 AND "OTHER"<=100`},
			},
		}, nil
	}
	return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
}

// TestDomainColumnQueries_AskForTheDomainOrigin pins the predicates that keep
// each read to what a domain put there.
func TestDomainColumnQueries_AskForTheDomainOrigin(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fragment string
	}{
		{name: "typed columns only", query: domainTypedColumnQuery, fragment: "c.domain_name IS NOT NULL"},
		{name: "visible columns only", query: domainTypedColumnQuery, fragment: "c.hidden_column = 'NO'"},
		{name: "copies only", query: domainConstraintCopyQuery, fragment: "k.domain_name IS NOT NULL"},
		{name: "the copy's columns", query: domainConstraintCopyQuery, fragment: "kc.constraint_name = k.constraint_name"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.query, qt.Contains, test.fragment)
		})
	}
}

// TestReadSchema_AsksForDomainColumnsOnlyWhereThePresetHoldsDomains counts the
// queries SENT. ALL_TAB_COLS and ALL_CONSTRAINTS have no DOMAIN_NAME on Oracle
// 21.3, and a query naming it fails the statement there.
func TestReadSchema_AsksForDomainColumnsOnlyWhereThePresetHoldsDomains(t *testing.T) {
	tests := []struct {
		name      string
		caps      capability.Capabilities
		wantAsked bool
	}{
		{name: "Oracle 23", caps: capability.Oracle23(), wantAsked: true},
		{name: "Oracle 21", caps: capability.Oracle21(), wantAsked: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var asked bool
			db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
				asked = asked || strings.Contains(query, "domain_name IS NOT NULL")
				return dbtest.QueryResult{Columns: []string{"unused"}}, nil
			})

			_, err := NewOracleReaderWithCapabilities(db.SQL, "APP", test.caps).ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(asked, qt.Equals, test.wantAsked)
		})
	}
}
