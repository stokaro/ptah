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
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/migration/migrator"
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

// domainOverUserDefinedSeed is the shape the fixture above cannot reach: a
// domain whose BASE type is itself user-defined.
//
// The distinction is a catalog one and it decides which branch of the
// conversion runs. For `positive` above, information_schema reports data_type
// 'integer' -- the base type -- so a consumer that only knows about built-in
// spellings still lands somewhere that can be corrected. For `d_enum` here,
// measured on PostgreSQL 17.10, it reports data_type 'USER-DEFINED' with
// udt_name 'color': the BASE type again, but now under the branch that answers
// from udt_name. Only domain_name and format_type name the domain.
//
// The plain `color` column beside it is the control in the other direction: a
// USER-DEFINED column that is NOT a domain must keep answering with its own
// type name, so the rule stays gated on the domain rather than on USER-DEFINED.
// The composite and range columns are the same shape with the other two kinds
// of user-defined base type PostgreSQL has.
func domainOverUserDefinedSeed() []string {
	return []string{
		"CREATE TYPE color AS ENUM ('r','g','b')",
		"CREATE TYPE addr AS (street text, city text)",
		"CREATE TYPE myrange AS RANGE (subtype = integer)",
		"CREATE DOMAIN d_enum AS color CHECK (VALUE <> 'b')",
		"CREATE DOMAIN d_comp AS addr",
		"CREATE DOMAIN d_range AS myrange",
		"CREATE TABLE t (" +
			"id integer PRIMARY KEY, " +
			"c d_enum NOT NULL, " +
			"a d_comp, " +
			"r d_range, " +
			"plain color NOT NULL)",
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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

// TestPostgreSQLDomainColumn_OverUserDefinedBaseTypeKeepsTheDomain is the same
// claim as TestPostgreSQLDomainColumn_ReaderKeepsTheDomain for the shape whose
// base type is itself user-defined.
//
// This is the shape a fix for the `positive` fixture can silently break. The
// conversion answers a USER-DEFINED column from udt_name, and for a domain
// udt_name is the BASE type -- so consulting the domain only where data_type
// happens to be a built-in spelling flattens `c d_enum` to `color` and takes
// the domain's CHECK with it. Measured with ptah-compat against PostgreSQL
// 17.10 before this guard existed: `schema diff --from X --to X` on one
// database planned `ALTER TABLE "t" ALTER COLUMN "c" TYPE color;`, `schema
// apply` executed it and reported success, and afterwards
// information_schema.columns.domain_name for that column was NULL. The pinned
// community binary v1.3.0 reported the same database synced.
func TestPostgreSQLDomainColumn_OverUserDefinedBaseTypeKeepsTheDomain(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	dbURL := newBoundaryDatabase(c, dsn, boundaryCase{
		name:  "domain_over_user_defined_reader",
		seed:  domainOverUserDefinedSeed(),
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
		// wantDomain is DBColumn.DomainName, empty for the control column.
		wantDomain string
		// wantUDTName is the BASE type the catalog reports for a domain
		// column, and the column's own type for the control. The two being
		// equal for the control is exactly why the domain rows need the
		// domain: udt_name cannot tell them apart.
		wantUDTName string
		// wantFieldType is what the desired-state model carries, which is what
		// every renderer and the comparator's desired side then use.
		wantFieldType string
	}{
		{
			name:          "domain over an enum",
			column:        "c",
			wantDomain:    "d_enum",
			wantUDTName:   "color",
			wantFieldType: "d_enum",
		},
		{
			name:          "domain over a composite type",
			column:        "a",
			wantDomain:    "d_comp",
			wantUDTName:   "addr",
			wantFieldType: "d_comp",
		},
		{
			name:          "domain over a range type",
			column:        "r",
			wantDomain:    "d_range",
			wantUDTName:   "myrange",
			wantFieldType: "d_range",
		},
		{
			name:          "plain enum column is not a domain",
			column:        "plain",
			wantDomain:    "",
			wantUDTName:   "color",
			wantFieldType: "color",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			column := findLiveColumn(c, live.Tables, "t", test.column)
			c.Assert(column.DomainName, qt.Equals, test.wantDomain)
			c.Assert(column.UDTName, qt.Equals, test.wantUDTName)
			// The catalog reports the same data_type for all four, which is
			// why nothing downstream can separate them without the domain.
			c.Assert(column.DataType, qt.Equals, "USER-DEFINED")

			field := findConvertedField(c, converted, test.column)
			c.Assert(field.Type, qt.Equals, test.wantFieldType)
		})
	}
}

// TestPostgreSQLDomainColumn_OverUserDefinedBaseTypeApplyingItsOwnDescriptionChangesNothing
// is the round-trip property for the same shape: a database that applies its
// own inspected description must plan nothing for these columns.
//
// The `positive` version of this test next door passed throughout the change
// that broke this one, which is the whole reason this exists separately.
func TestPostgreSQLDomainColumn_OverUserDefinedBaseTypeApplyingItsOwnDescriptionChangesNothing(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)

	tests := []struct {
		name string
		// compatibility selects the surface, as next door: both must hold it.
		compatibility bool
	}{
		{name: "native surface", compatibility: false},
		{name: "compatibility surface", compatibility: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbURL := newBoundaryDatabase(c, dsn, boundaryCase{
				name:  "domain_over_user_defined_apply",
				seed:  domainOverUserDefinedSeed(),
				query: "search_path=public",
			})
			conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			document := boundaryInspect(c, dbURL, test.compatibility)
			c.Assert(document, qt.Contains, `type = sql("d_enum")`)
			// The control's spelling is asserted too, so a change that made
			// every USER-DEFINED column print sql(...) would not read as a
			// pass here.
			c.Assert(document, qt.Contains, `type = enum.color`)

			plan := boundaryApplyBack(c, conn, document, test.compatibility)

			c.Assert(columnStatements(plan, "c"), qt.DeepEquals, []string(nil))
			c.Assert(columnStatements(plan, "a"), qt.DeepEquals, []string(nil))
			c.Assert(columnStatements(plan, "r"), qt.DeepEquals, []string(nil))
			c.Assert(columnStatements(plan, "plain"), qt.DeepEquals, []string(nil))
		})
	}
}

// TestPostgreSQLDomainColumn_OverUserDefinedBaseTypeDescriptionReplaysOnAnEmptyDatabase
// is the round trip in its other direction: not "applying a database's own
// description changes nothing", but "the description RUNS at all".
//
// The two are different claims and only this one can see a statement ORDER
// defect. A self-apply plans nothing, so it executes nothing and any order is
// vacuously fine. Creating the same schema from empty has to name every type
// before the type that uses it, and PostgreSQL has no forward declaration:
// measured on 17.10 before this guard existed, ptah-compat emitted
// `CREATE DOMAIN "d_comp" AS addr;` five statements ahead of
// `CREATE TYPE "addr" AS ("street" text, "city" text);` and psql -v
// ON_ERROR_STOP=1 stopped at `ERROR: type "addr" does not exist`, exit 3.
//
// The plan is EXECUTED rather than inspected, because a text assertion on the
// order is a restatement of the emitter and would agree with it whatever it
// says. The server is the judge. What is asserted afterwards is the shape, not
// only the exit status: a script that ran but flattened the domains would
// otherwise pass.
func TestPostgreSQLDomainColumn_OverUserDefinedBaseTypeDescriptionReplaysOnAnEmptyDatabase(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	sourceURL := newBoundaryDatabase(c, dsn, boundaryCase{
		name:  "domain_over_user_defined_replay_source",
		seed:  domainOverUserDefinedSeed(),
		query: "search_path=public",
	})
	targetURL := newBoundaryDatabase(c, dsn, boundaryCase{
		name:  "domain_over_user_defined_replay_target",
		query: "search_path=public",
	})

	target, err := dbschema.ConnectToDatabase(c.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(target) })
	plan, err := atlasschema.PrepareApply(c.Context(), target, atlasschema.ApplyRuntimeOptions{
		ToURLs: []string{sourceURL},
		// The default the CLI parses out of an unset --tx-mode. One
		// transaction per statement list is also what makes a failure
		// unambiguous: the target is left empty rather than half built.
		TxMode: migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.HasChanges(), qt.IsTrue)
	c.Assert(plan.Execute(c.Context()), qt.IsNil, qt.Commentf("emitted script:\n%s", plan.SQL()))

	replayed, err := dbschema.ReadSchemaWithSchemas(target, nil)
	c.Assert(err, qt.IsNil)

	tests := []struct {
		name       string
		column     string
		wantDomain string
	}{
		{name: "domain over an enum", column: "c", wantDomain: "d_enum"},
		{name: "domain over a composite type", column: "a", wantDomain: "d_comp"},
		{name: "domain over a range type", column: "r", wantDomain: "d_range"},
		{name: "plain enum column is not a domain", column: "plain", wantDomain: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(findLiveColumn(c, replayed.Tables, "t", test.column).DomainName, qt.Equals, test.wantDomain)
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
