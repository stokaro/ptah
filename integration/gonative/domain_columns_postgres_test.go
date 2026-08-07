//go:build integration

// Live guard for the last item of stokaro/ptah#1242: a column whose declared
// type is a PostgreSQL DOMAIN.
//
// The catalog subtlety this is about: information_schema.columns.data_type
// reports a domain column's BASE type -- "integer" for a column of
// `CREATE DOMAIN positive AS integer CHECK (VALUE > 0)` -- and records the
// domain separately in domain_name/domain_schema. pg_attribute joined to
// pg_type names the domain directly, with typtype = 'd'. A reader that trusts
// data_type therefore hands the rest of the pipeline a column that never had
// the domain, and no pure function downstream can notice.
//
// It takes a LIVE server to see this. Every fixture below is one throwaway
// database, so a failure names one shape.
//
// The domain is named `positive`, not `positive_int`, deliberately. The
// comparator folded a type spelling into a category by substring, so any name
// containing "int" compared equal to the base type by accident: measured on
// PostgreSQL 17.10 with ptah-compat, `schema diff --from X --to X` planned
// `ALTER TABLE "t" ALTER COLUMN "qty" TYPE positive` on the `positive` fixture
// and reported "Schemas are synced" on the `positive_int` one. The name of the
// domain decided whether the defect was visible.

package gonative_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
)

// domainColumnSeed is the fixture from the issue, plus a plain column of the
// domain's base type. The plain column is the control: it keeps a fix that
// reports every integer column as a domain from passing.
func domainColumnSeed() []string {
	return []string{
		"CREATE DOMAIN positive AS integer CHECK (VALUE > 0)",
		"CREATE TABLE t (id integer PRIMARY KEY, qty positive NOT NULL)",
	}
}

// TestPostgreSQLDomainColumn_ReaderKeepsTheDomain asserts what the reader
// reports, before anything renders or compares.
func TestPostgreSQLDomainColumn_ReaderKeepsTheDomain(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	dbURL := newBoundaryDatabase(c, dsn, boundaryCase{
		name:  "domain_reader",
		seed:  domainColumnSeed(),
		query: "search_path=public",
	})
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	live, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	c.Assert(err, qt.IsNil)
	converted := dbschematogo.ConvertDBSchemaToGoSchema(live)

	tests := []struct {
		name   string
		column string
		// wantDomain is DBColumn.DomainName: the fact information_schema
		// records and data_type erases.
		wantDomain string
		// wantFieldType is what the desired-state model carries, which is what
		// every renderer writes out.
		wantFieldType string
	}{
		{
			name:          "domain column",
			column:        "qty",
			wantDomain:    "positive",
			wantFieldType: "positive",
		},
		{
			name:          "plain column of the domain's base type",
			column:        "id",
			wantDomain:    "",
			wantFieldType: "integer",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			column := findLiveColumn(c, live.Tables, "t", test.column)
			c.Assert(column.DomainName, qt.Equals, test.wantDomain)
			// The base type stays available either way: the read adds a
			// spelling rather than replacing one.
			c.Assert(column.DataType, qt.Equals, "integer")

			field := findConvertedField(c, converted, test.column)
			c.Assert(field.Type, qt.Equals, test.wantFieldType)
		})
	}
}

// TestPostgreSQLDomainColumn_ApplyingItsOwnDescriptionChangesNothing is the
// round-trip property, and the one that makes a rendered domain worth
// something.
//
// Before this change the document said `type = sql("positive")` while the
// comparator read the same column back as `integer`, so a database was never in
// sync with itself: applying its own description planned
// `ALTER TABLE "t" ALTER COLUMN "qty" TYPE positive`, and `schema apply`
// executed it on every run and reported success.
//
// The assertion is on statements that touch the COLUMN rather than on an empty
// plan, because these fixtures also carry the owner-grant churn of #1276, which
// is a different issue with its own guard next door and is not this test's
// business.
func TestPostgreSQLDomainColumn_ApplyingItsOwnDescriptionChangesNothing(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	tests := []struct {
		name string
		// compatibility selects the surface: the compatibility binary omits
		// blocks the tool it stands in for refuses and tolerates names Ptah
		// does not model. Both surfaces must hold this property.
		compatibility bool
	}{
		{name: "native surface", compatibility: false},
		{name: "compatibility surface", compatibility: true},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			dbURL := newBoundaryDatabase(c, dsn, boundaryCase{
				name:  "domain_apply",
				seed:  domainColumnSeed(),
				query: "search_path=public",
			})
			conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

			document := boundaryInspect(c, dbURL, test.compatibility)
			c.Assert(document, qt.Contains, `type = sql("positive")`)

			plan := boundaryApplyBack(c, conn, document, test.compatibility)

			c.Assert(columnStatements(plan, "qty"), qt.DeepEquals, []string(nil))
		})
	}
}

// columnStatements keeps the planned statements that name a column, which for
// these fixtures means the type churn and nothing else: the grant statements
// name a table and a role.
func columnStatements(statements []string, column string) []string {
	var out []string
	for _, statement := range statements {
		if strings.Contains(statement, `"`+column+`"`) {
			out = append(out, statement)
		}
	}
	return out
}

func findLiveColumn(c *qt.C, tables []dbschematypes.DBTable, table, column string) dbschematypes.DBColumn {
	c.Helper()

	for _, candidate := range tables {
		for _, columnCandidate := range candidate.Columns {
			if candidate.Name == table && columnCandidate.Name == column {
				return columnCandidate
			}
		}
	}
	c.Fatalf("table %q has no column %q in the read schema", table, column)
	return dbschematypes.DBColumn{}
}

func findConvertedField(c *qt.C, converted *goschema.Database, column string) goschema.Field {
	c.Helper()

	for _, field := range converted.Fields {
		if field.Name == column {
			return field
		}
	}
	c.Fatalf("converted schema has no field %q", column)
	return goschema.Field{}
}
