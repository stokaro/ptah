package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// domainColumnTableDiffs compares one database column of table t against a
// desired schema declaring the same column with desiredType, and returns the
// table diffs the comparator reported. It builds fixtures and asserts nothing,
// so both halves of this file read the same two sides.
func domainColumnTableDiffs(column types.DBColumn, desiredType string, desiredDomains []goschema.Domain) []difftypes.TableDiff {
	database := &types.DBSchema{
		Tables: []types.DBTable{{Name: "t", Type: "TABLE", Columns: []types.DBColumn{column}}},
	}
	desired := &goschema.Database{
		Tables:  []goschema.Table{{StructName: "T", Name: "t"}},
		Fields:  []goschema.Field{{StructName: "T", Name: column.Name, Type: desiredType}},
		Domains: desiredDomains,
	}
	return schemadiff.CompareWithDialect(desired, database, platform.Postgres).TablesModified
}

// TestCompareWithDialect_PostgresDomainColumnIdentityReportsAChange pins how a
// column typed by a PostgreSQL domain is compared: by the domain's IDENTITY,
// never through normalize.Type.
//
// normalize.Type matches by substring -- anything containing "int" is
// "integer", anything containing "text" is "text" -- which is right for type
// names and wrong for an identifier a schema author picked. Routing a domain
// name through it let the NAME decide whether a column changed, and the miss is
// destructive rather than cosmetic: the plan keeps the DROP DOMAIN ... CASCADE
// for the domain the column uses, so with no ALTER COLUMN ... TYPE ahead of it
// the CASCADE takes the column and its data. Measured on PostgreSQL 17.10,
// `ptah-compat schema apply --auto-approve` exited 0 and left a two-column
// table with neither column (stokaro/ptah#1138).
//
// The rows are catalog values as PostgreSQL 17.10 reports them. A domain column
// is recorded by information_schema under its BASE type, with domain_name
// naming the domain; format_type spells the domain the way the server does and
// qualifies it when the search path needs that.
//
// The other half of the rule -- the pairs that must stay silent -- is
// TestCompareWithDialect_PostgresDomainColumnIdentityReportsNoChange.
func TestCompareWithDialect_PostgresDomainColumnIdentityReportsAChange(t *testing.T) {
	tests := []struct {
		name string
		// column is the database side: one column of table t.
		column types.DBColumn
		// desiredType is what the desired schema declares for that column.
		desiredType string
		// desiredDomains are the domains the desired schema declares. It is the
		// only thing that can tell the comparator a desired type name belongs
		// to a domain, since a goschema field carries a type string and nothing
		// else.
		desiredDomains []goschema.Domain
		// wantChange is the type row the sole reported column change carries.
		wantChange string
	}{
		{
			// CREATE DOMAIN waypoint AS integer CHECK (VALUE > 0);
			// "waypoint" contains "int", so the normalizer called this equal to
			// a desired BIGINT and planned nothing.
			name:        "a domain whose name contains a type name is still a change",
			column:      types.DBColumn{Name: "a", DataType: "integer", UDTName: "int4", FormattedType: "waypoint", DomainName: "waypoint", IsNullable: "NO"},
			desiredType: "BIGINT",
			wantChange:  "waypoint -> BIGINT",
		},
		{
			// CREATE DOMAIN context AS integer; "context" contains "text".
			name:        "a domain named context is not text",
			column:      types.DBColumn{Name: "b", DataType: "integer", UDTName: "int4", FormattedType: "context", DomainName: "context", IsNullable: "NO"},
			desiredType: "TEXT",
			wantChange:  "context -> TEXT",
		},
		{
			name:        "another domain over the same base type is a change",
			column:      types.DBColumn{Name: "a", DataType: "integer", UDTName: "int4", FormattedType: "waypoint", DomainName: "waypoint", IsNullable: "NO"},
			desiredType: "milestone",
			wantChange:  "waypoint -> milestone",
		},
		{
			// The reviewer's shape for stokaro/ptah#1138. A domain's identity
			// is (schema, name); the name alone is not it. The database column
			// is declared with public.status and the desired schema types it
			// with other.status -- two different domains, with two different
			// CHECK constraints. Measured on PostgreSQL 17.10 with the name
			// alone reaching the comparator, `schema diff` emitted
			// DROP DOMAIN IF EXISTS "status" CASCADE and no ALTER, and
			// `schema apply --auto-approve` exited 0, said "Schema apply
			// completed successfully", and left the table with only its id
			// column.
			name:        "a domain in another schema is a different domain",
			column:      types.DBColumn{Name: "s", DataType: "text", UDTName: "text", FormattedType: "status", DomainName: "status", DomainSchema: "public", IsNullable: "NO"},
			desiredType: "other.status",
			wantChange:  "status -> other.status",
		},
		{
			// The same miss the other way round: the column is declared with
			// the domain in another schema and the desired schema declares its
			// own. Measured on the same server, that one is silent drift rather
			// than data loss -- the column is never converted and re-running
			// never converges.
			name:           "a column from another schema against the declared one is a change",
			column:         types.DBColumn{Name: "s", DataType: "text", UDTName: "text", FormattedType: "other.status", DomainName: "status", DomainSchema: "other", IsNullable: "NO"},
			desiredType:    "status",
			desiredDomains: []goschema.Domain{{Name: "status", BaseType: "TEXT"}},
			wantChange:     "other.status -> status",
		},
		{
			// An unqualified reference resolves through the declaration, so a
			// desired schema that declares its status in another schema means
			// that one even when the column spells the name bare.
			name:           "an unqualified reference means the domain that is declared",
			column:         types.DBColumn{Name: "s", DataType: "text", UDTName: "text", FormattedType: "status", DomainName: "status", DomainSchema: "public", IsNullable: "NO"},
			desiredType:    "status",
			desiredDomains: []goschema.Domain{{Name: "status", Schema: "other", BaseType: "TEXT"}},
			wantChange:     "status -> status",
		},
		{
			name:        "the same bare name in another schema is a different domain",
			column:      types.DBColumn{Name: "c", DataType: "USER-DEFINED", UDTName: "cube", FormattedType: "alt.alt_dom", DomainName: "alt_dom", IsNullable: "NO"},
			desiredType: "other.alt_dom",
			wantChange:  "alt.alt_dom -> other.alt_dom",
		},
		{
			// CREATE DOMAIN waypoints AS integer[]; a domain over an array is
			// reported with data_type ARRAY exactly like a plain array column,
			// and "waypoints" normalizes to "integer" just like the desired
			// BIGINT[] does.
			name:        "a domain over an array is a domain, not an array",
			column:      types.DBColumn{Name: "d", DataType: "ARRAY", UDTName: "_int4", FormattedType: "waypoints", DomainName: "waypoints", IsNullable: "NO"},
			desiredType: "BIGINT[]",
			wantChange:  "waypoints -> BIGINT[]",
		},
		{
			// The catalog fact on its own is enough: a caller that carries
			// DomainName without format_type still gets identity comparison.
			// The desired INTEGER is the discriminating spelling here -- it is
			// the domain's own base type, so normalization calls it equal and
			// the width rule sees nothing to report either.
			name:        "the domain name alone decides",
			column:      types.DBColumn{Name: "a", DataType: "integer", UDTName: "int4", DomainName: "waypoint", IsNullable: "NO"},
			desiredType: "INTEGER",
			wantChange:  "int4 -> INTEGER",
		},
		{
			// And so is the server's spelling on its own, which is what a
			// caller built before domain_name was read carries.
			name:        "the server's spelling alone decides",
			column:      types.DBColumn{Name: "a", DataType: "integer", UDTName: "int4", FormattedType: "waypoint", IsNullable: "NO"},
			desiredType: "BIGINT",
			wantChange:  "waypoint -> BIGINT",
		},
		{
			name:        "a plain array that really changed is reported",
			column:      types.DBColumn{Name: "e", DataType: "ARRAY", UDTName: "_varchar", FormattedType: "character varying(100)[]", IsNullable: "NO"},
			desiredType: "TEXT[]",
			wantChange:  "character varying(100)[] -> text",
		},
		{
			// The other direction of the same rule, and the pinned Atlas
			// community binary v1.3.0 plans it too: measured on two PostgreSQL
			// 17.10 databases that differ only in this column,
			// `ALTER TABLE "t" ALTER COLUMN "a" TYPE waypoint;`. Both sides
			// normalize to "integer", so without the declaration the desired
			// domain is invisible here.
			name:           "a plain column against a desired domain is a change",
			column:         types.DBColumn{Name: "a", DataType: "integer", UDTName: "int4", IsNullable: "NO"},
			desiredType:    "waypoint",
			desiredDomains: []goschema.Domain{{Name: "waypoint", BaseType: "INTEGER", Check: "VALUE > 0"}},
			wantChange:     "int4 -> waypoint",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			modified := domainColumnTableDiffs(test.column, test.desiredType, test.desiredDomains)

			c.Assert(modified, qt.HasLen, 1, qt.Commentf("the desired type is a real change and must be reported"))
			c.Assert(modified[0].ColumnsModified, qt.HasLen, 1)
			c.Assert(modified[0].ColumnsModified[0].Changes, qt.DeepEquals, map[string]string{"type": test.wantChange})
		})
	}
}

// TestCompareWithDialect_PostgresDomainColumnIdentityReportsNoChange is the
// other half of the rule, and it is what keeps identity comparison from
// degenerating into "any domain is a change": every row here is two spellings
// of ONE domain, or a plain type that really is unchanged. Without them a
// comparator that reported every domain column would pass
// TestCompareWithDialect_PostgresDomainColumnIdentityReportsAChange in full.
func TestCompareWithDialect_PostgresDomainColumnIdentityReportsNoChange(t *testing.T) {
	tests := []struct {
		name           string
		column         types.DBColumn
		desiredType    string
		desiredDomains []goschema.Domain
	}{
		{
			name:        "the same domain is not a change",
			column:      types.DBColumn{Name: "a", DataType: "integer", UDTName: "int4", FormattedType: "waypoint", DomainName: "waypoint", IsNullable: "NO"},
			desiredType: "waypoint",
		},
		{
			// And the boundary that keeps this from becoming "any domain is a
			// change": one domain, spelled qualified on the desired side and
			// reported by the catalog as the schema it is in.
			name:        "the same domain named with its schema is not a change",
			column:      types.DBColumn{Name: "s", DataType: "text", UDTName: "text", FormattedType: "status", DomainName: "status", DomainSchema: "public", IsNullable: "NO"},
			desiredType: "public.status",
		},
		{
			// A domain declared with no schema of its own lives in the schema
			// the connection reads, which is the schema the database side
			// reports for it. This is the ordinary single-schema case and it
			// must stay a no-op.
			name:           "a domain declared without a schema is the one in the default schema",
			column:         types.DBColumn{Name: "s", DataType: "text", UDTName: "text", FormattedType: "status", DomainName: "status", DomainSchema: "public", IsNullable: "NO"},
			desiredType:    "status",
			desiredDomains: []goschema.Domain{{Name: "status", BaseType: "TEXT"}},
		},
		{
			// The residual gap, pinned so it is visible rather than assumed
			// away: two declarations share the bare name in different schemas,
			// so an unqualified reference is left to the name. Measured on
			// PostgreSQL 17.10, one database whose column b is other.status
			// against one whose column b is public.status, read with both
			// schemas selected, reports "Schemas are synced" -- drift rather
			// than data loss, since neither domain is dropped. Closing it needs
			// the desired column to carry its domain's schema rather than a
			// type string, which is a model change (stokaro/ptah#1138).
			name:           "two same-named domains leave an unqualified reference undecided",
			column:         types.DBColumn{Name: "b", DataType: "integer", UDTName: "int4", FormattedType: "other.status", DomainName: "status", DomainSchema: "other", IsNullable: "NO"},
			desiredType:    "status",
			desiredDomains: []goschema.Domain{{Name: "status", BaseType: "TEXT"}, {Name: "status", Schema: "other", BaseType: "INTEGER"}},
		},
		{
			// A domain off the search path: format_type qualifies it, while
			// information_schema.domain_name stays bare. Whether the server
			// qualifies a name is a property of the search path, not of the
			// domain, so with nothing on the desired side to resolve the bare
			// spelling the two name one domain.
			name:        "a qualified spelling and a bare one name one domain",
			column:      types.DBColumn{Name: "c", DataType: "USER-DEFINED", UDTName: "cube", FormattedType: "alt.alt_dom", DomainName: "alt_dom", IsNullable: "NO"},
			desiredType: "alt_dom",
		},
		{
			// An array is not an identifier: its spelling is a type and must
			// keep normalizing like one, or every array column would report a
			// change against a desired schema that spells it differently.
			name:        "a plain array still compares as a type",
			column:      types.DBColumn{Name: "e", DataType: "ARRAY", UDTName: "_varchar", FormattedType: "character varying(100)[]", IsNullable: "NO"},
			desiredType: "character varying(100)[]",
		},
		{
			name:        "a plain column is untouched",
			column:      types.DBColumn{Name: "id", DataType: "integer", UDTName: "int4", IsNullable: "NO"},
			desiredType: "INTEGER",
		},
		{
			// And the boundary: with no domain declared anywhere, "waypoint" is
			// an ordinary type name on both sides and normalization decides. A
			// desired schema that means the domain has to declare it, which is
			// what every source Ptah reads does.
			name:        "an undeclared name is not a domain",
			column:      types.DBColumn{Name: "a", DataType: "integer", UDTName: "int4", IsNullable: "NO"},
			desiredType: "waypoint",
		},
		{
			name:           "a domain declared on both sides is not a change",
			column:         types.DBColumn{Name: "a", DataType: "integer", UDTName: "int4", FormattedType: "waypoint", DomainName: "waypoint", IsNullable: "NO"},
			desiredType:    "waypoint",
			desiredDomains: []goschema.Domain{{Name: "waypoint", BaseType: "INTEGER", Check: "VALUE > 0"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			modified := domainColumnTableDiffs(test.column, test.desiredType, test.desiredDomains)

			c.Assert(modified, qt.HasLen, 0, qt.Commentf("reported %+v for a column that did not change", modified))
		})
	}
}
