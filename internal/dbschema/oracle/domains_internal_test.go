package oracle

// White-box testing required: which of a domain's CHECK constraints is the
// server's own restatement of NOT NULL is decided inside the reader, and the
// exported read returns the same shape whether it was recognized or not --
// the symptom is a plan that carries the same change forever, with a different
// constraint name each time.

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/feature/oratype"
	"ptah.run/internal/dbschema/dbtest"
)

// TestNotNullRestatement_MatchesTheColumnTheCatalogNames pins the recognition,
// and the column name is the whole point.
//
// Measured on 23.26.2.0.0: `CREATE DOMAIN email_d AS VARCHAR2(255) NOT NULL`
// stores its column as EMAIL_D, while `CREATE DOMAIN score_d AS NUMBER(5,2)
// CHECK (VALUE BETWEEN 0 AND 100)` stores it as VALUE -- the declaration's own
// reference to VALUE decides it. A rule written against the literal "VALUE"
// would leave the first domain reporting SYS_DOMAIN_C0043 as a declared CHECK
// (stokaro/ptah#1920).
func TestNotNullRestatement_MatchesTheColumnTheCatalogNames(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		columnName string
		want       bool
	}{
		{
			name:       "the domain-named column",
			expression: `"EMAIL_D" IS NOT NULL`,
			columnName: "EMAIL_D",
			want:       true,
		},
		{
			name:       "the VALUE column",
			expression: `"VALUE" IS NOT NULL`,
			columnName: "VALUE",
			want:       true,
		},
		{
			name:       "spacing the server chose",
			expression: "\"VALUE\"   IS\tNOT  NULL",
			columnName: "value",
			want:       true,
		},
		{
			name:       "a NOT NULL on another column entirely",
			expression: `"OTHER" IS NOT NULL`,
			columnName: "VALUE",
			want:       false,
		},
		{
			name:       "a user CHECK that merely mentions NULL",
			expression: `VALUE IS NOT NULL OR VALUE > 0`,
			columnName: "VALUE",
			want:       false,
		},
		{
			name:       "an ordinary user CHECK",
			expression: "VALUE BETWEEN 0 AND 100",
			columnName: "VALUE",
			want:       false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(notNullRestatement(test.expression, test.columnName), qt.Equals, test.want)
		})
	}
}

// TestDeclaredDomainChecks_KeepsWhatADeclarationCouldHaveWritten is the same
// rule at the level the reader uses it.
//
// The name of the dropped constraint is per-database -- SYS_DOMAIN_C0043 on
// one server and a different number on the next -- which is why reporting it
// would make the plan churn rather than merely be noisy.
func TestDeclaredDomainChecks_KeepsWhatADeclarationCouldHaveWritten(t *testing.T) {
	c := qt.New(t)

	kept := declaredDomainChecks([]domainCheck{
		{Name: "SYS_DOMAIN_C0043", Expression: `"EMAIL_D" IS NOT NULL`},
		{Name: "SYS_DOMAIN_C0045", Expression: "VALUE <> 'zzz'"},
		{Name: "SCORE_RANGE", Expression: "VALUE BETWEEN 0 AND 100"},
	}, "EMAIL_D")

	// The unnamed user CHECK stays: GENERATED does not separate it from the
	// NOT NULL restatement -- measured, an unnamed user CHECK is also
	// `GENERATED NAME` -- so the condition is what decides.
	c.Assert(kept, qt.DeepEquals, []domainCheck{
		{Name: "SYS_DOMAIN_C0045", Expression: "VALUE <> 'zzz'"},
		{Name: "SCORE_RANGE", Expression: "VALUE BETWEEN 0 AND 100"},
	})
}

// TestDomainQuery_AsksTheCatalogThatHasTheAnswer holds the views apart.
//
// ALL_DOMAINS carries no base type at all; ALL_DOMAIN_COLS does. And the views
// spell the owner differently -- OWNER against DOMAIN_OWNER -- which is a
// difference a reader that assumed one name would find at runtime.
func TestDomainQuery_AsksTheCatalogThatHasTheAnswer(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fragment string
	}{
		{name: "the base type", query: domainColumnQuery, fragment: "c.data_type"},
		{name: "the nullability", query: domainColumnQuery, fragment: "c.nullable"},
		{name: "the column order", query: domainColumnQuery, fragment: "ORDER BY c.domain_name, c.column_id"},
		{name: "the cols owner", query: domainColumnQuery, fragment: "c.owner = :1"},
		{name: "the column count", query: domainQuery, fragment: "d.cols"},
		{name: "the domain's type", query: domainQuery, fragment: "d.type"},
		{name: "the domains owner", query: domainQuery, fragment: "d.owner = :1"},
		{name: "the constraint owner", query: domainConstraintQuery, fragment: "c.domain_owner = :1"},
		{name: "checks only", query: domainConstraintQuery, fragment: "c.constraint_type = 'C'"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.query, qt.Contains, test.fragment)
		})
	}
}

// TestReadDomains_RecordsWhatTheModelCannotCarry pins each domain shape the
// model cannot describe, and that the read records it rather than leaving it
// out.
//
// The shapes are the ones ALL_DOMAINS reported on 23.26.3.0.0. A domain of two
// columns left out reads as absent, and a declaration of that name plans its
// creation. An enumeration domain has one NUMBER column, so the column count
// alone would describe it as a NUMBER domain. A flexible domain declares one
// column and lists two in ALL_DOMAIN_COLS, which a read keyed on the column
// rows reported as two domains of one name and failed.
func TestReadDomains_RecordsWhatTheModelCannotCarry(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, domainShapeCatalog)
	reader := NewOracleReader(db.SQL, "APP")

	read := &catalog.Database{}
	err := reader.readDomains(t.Context(), read)

	c.Assert(err, qt.IsNil)
	objects, err := read.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []schemaext.Object{{Ref: oratype.Ref(oratype.DomainKind, "", "D_SINGLE"),
		Value: &oratype.ObservedDomain{BaseType: "VARCHAR2(10)", NotNull: true}}})
	c.Assert(read.FeatureCoverage.SubjectRecords(), qt.DeepEquals, []schemaext.SubjectCoverage{
		undescribedDomainClaim("D_ENUM", "the domain is an enumeration domain, which the domain model cannot describe"),
		undescribedDomainClaim("D_FLEX", "the domain is a flexible domain, which the domain model cannot describe"),
		undescribedDomainClaim("D_LOST", "the read found 0 columns of a single-column domain"),
		undescribedDomainClaim("D_MULTI", "the domain has 2 columns, and the domain model describes a single-column domain"),
	})
}

// undescribedDomainClaim is the claim a read records about a domain it found and
// could not describe.
func undescribedDomainClaim(name, reason string) schemaext.SubjectCoverage {
	return schemaext.SubjectCoverage{Kind: oratype.DomainKind, Subject: oratype.Ref(oratype.DomainKind, "", name),
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: reason}}
}

// domainShapeCatalog answers the domain read with one domain of each shape
// ALL_DOMAINS reported on 23.26.3.0.0, and one whose column the read misses.
func domainShapeCatalog(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	folded := strings.ToLower(query)
	switch {
	case strings.Contains(folded, "from all_domain_constraints"):
		return dbtest.QueryResult{Columns: []string{"domain_name", "name", "search_condition"}}, nil
	case strings.Contains(folded, "from all_domain_cols"):
		return dbtest.QueryResult{
			Columns: []string{"domain_name", "column_name", "data_type", "data_length", "data_precision",
				"data_scale", "char_length", "nullable", "data_default"},
			Rows: [][]driver.Value{
				{"D_ENUM", "D_ENUM", "NUMBER", int64(22), nil, nil, int64(0), "Y", nil},
				{"D_FLEX", "A", "NUMBER", int64(22), nil, nil, int64(0), "Y", nil},
				{"D_FLEX", "K", "NUMBER", int64(22), nil, nil, int64(0), "Y", nil},
				{"D_MULTI", "LAT", "NUMBER", int64(22), nil, nil, int64(0), "Y", nil},
				{"D_MULTI", "LON", "NUMBER", int64(22), nil, nil, int64(0), "Y", nil},
				{"D_SINGLE", "D_SINGLE", "VARCHAR2", int64(10), nil, nil, int64(10), "N", nil},
			},
		}, nil
	case strings.Contains(folded, "from all_domains"):
		return dbtest.QueryResult{
			Columns: []string{"name", "cols", "type"},
			Rows: [][]driver.Value{
				{"D_ENUM", int64(1), "ENUMERATED"},
				{"D_FLEX", int64(1), "FLEXIBLE"},
				{"D_LOST", int64(1), "REGULAR"},
				{"D_MULTI", int64(2), "REGULAR"},
				{"D_SINGLE", int64(1), "REGULAR"},
			},
		}, nil
	}
	return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
}

// TestFormatDomainType_SpellsWhatTheCatalogMeans pins the facts of
// ALL_DOMAIN_COLS that mean something other than the same facts of
// ALL_TAB_COLS, with the rows 23.26.3.0.0 answered on an AL32UTF8 database.
func TestFormatDomainType_SpellsWhatTheCatalogMeans(t *testing.T) {
	tests := []struct {
		name                                 string
		dataType                             string
		length, charLength, precision, scale sql.NullInt64
		want                                 string
	}{
		{name: "a length in bytes", dataType: "VARCHAR2", length: valid(10), charLength: valid(10), want: "VARCHAR2(10)"},
		{name: "a length in characters", dataType: "VARCHAR2", length: valid(40), charLength: valid(10), want: "VARCHAR2(10 CHAR)"},
		{name: "a length whose unit cannot be told", dataType: "VARCHAR2", length: valid(4000), charLength: valid(4000),
			want: "VARCHAR2(4000)"},
		{name: "a fixed length in characters", dataType: "CHAR", length: valid(12), charLength: valid(3), want: "CHAR(3 CHAR)"},
		{name: "a national length", dataType: "NVARCHAR2", length: valid(20), charLength: valid(10), want: "NVARCHAR2(10)"},
		{name: "an integer", dataType: "NUMBER", length: valid(22), scale: valid(0), want: "NUMBER(*,0)"},
		{name: "a boolean", dataType: "UNDEFINED", length: valid(1), charLength: valid(0), want: "BOOLEAN"},
		{name: "another undecoded type", dataType: "UNDEFINED", length: valid(8), charLength: valid(0), want: "UNDEFINED"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(formatDomainType(test.dataType, test.length, test.charLength, test.precision, test.scale), qt.Equals, test.want)
		})
	}
}

// TestReadDomains_RecordsAnUndecodedBaseType pins that a domain whose base
// type the catalog reports as UNDEFINED is recorded as one the read cannot
// describe, rather than described over a type no declaration can name.
func TestReadDomains_RecordsAnUndecodedBaseType(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, undecodedDomainCatalog)
	read := &catalog.Database{}

	err := NewOracleReader(db.SQL, "APP").readDomains(t.Context(), read)

	c.Assert(err, qt.IsNil)
	c.Assert(read.FeatureObjects.Refs(), qt.HasLen, 0)
	c.Assert(read.FeatureCoverage.SubjectRecords(), qt.DeepEquals, []schemaext.SubjectCoverage{
		undescribedDomainClaim("D_ODD", "the catalog reports the domain's base type as UNDEFINED"),
	})
}

// undecodedDomainCatalog answers the domain read with one domain whose base
// type the DATA_TYPE decode does not name.
func undecodedDomainCatalog(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	folded := strings.ToLower(query)
	switch {
	case strings.Contains(folded, "from all_domain_constraints"):
		return dbtest.QueryResult{Columns: []string{"domain_name", "name", "search_condition"}}, nil
	case strings.Contains(folded, "from all_domain_cols"):
		return dbtest.QueryResult{
			Columns: []string{"domain_name", "column_name", "data_type", "data_length", "data_precision",
				"data_scale", "char_length", "nullable", "data_default"},
			Rows: [][]driver.Value{{"D_ODD", "D_ODD", "UNDEFINED", int64(8), nil, nil, int64(0), "Y", nil}},
		}, nil
	case strings.Contains(folded, "from all_domains"):
		return dbtest.QueryResult{Columns: []string{"name", "cols", "type"}, Rows: [][]driver.Value{{"D_ODD", int64(1), "REGULAR"}}}, nil
	}
	return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
}

// valid is a catalog number that is not NULL.
func valid(value int64) sql.NullInt64 { return sql.NullInt64{Int64: value, Valid: true} }
