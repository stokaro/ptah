package compare_test

// Domain columns, database side. See stokaro/ptah#1242.
//
// Every fixture below is one PostgreSQL 17.10 column as the reader reports it.
// The values are measured, not invented: information_schema.columns.data_type
// for a column of domain `positive` is the domain's BASE type "integer",
// udt_name is "int4", and only format_type and domain_name name the domain.
//
// Both sides of a comparison are often the same database -- `schema diff`
// converts one side to the desired-state model and leaves the other as read --
// so the two must reach the same spelling for the same column or a database is
// never in sync with itself.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// domainColumn is the reader's report of `qty positive` where
// CREATE DOMAIN positive AS integer CHECK (VALUE > 0).
func domainColumn() types.DBColumn {
	return types.DBColumn{
		Name:          "qty",
		DataType:      "integer",
		UDTName:       "int4",
		FormattedType: "positive",
		DomainName:    "positive",
		IsNullable:    "YES",
	}
}

// domainOverEnumColumn is the reader's report of `qty d_enum` where
// CREATE TYPE color AS ENUM (...) and CREATE DOMAIN d_enum AS color.
//
// A domain over a USER-DEFINED base type does not report its base type in
// data_type the way `positive` above does. Measured on PostgreSQL 17.10, the
// catalog answers data_type 'USER-DEFINED', udt_name 'color' -- the BASE type --
// domain_name 'd_enum' and format_type 'd_enum'. Nothing in the tree built this
// shape until stokaro/ptah#1242 landed, so the branch that answers from udt_name
// before consulting the domain was invisible to every test.
func domainOverEnumColumn() types.DBColumn {
	return types.DBColumn{
		Name:          "qty",
		DataType:      "USER-DEFINED",
		UDTName:       "color",
		FormattedType: "d_enum",
		DomainName:    "d_enum",
		IsNullable:    "YES",
	}
}

// TestColumns_DomainColumnHappyPath is the no-churn property: a desired column
// that names the domain the database column is declared as must produce no
// change at all.
//
// Measured before this fix with ptah-compat against PostgreSQL 17.10:
// `schema diff --from X --to X` on one database holding this column planned
// `ALTER TABLE "t" ALTER COLUMN "qty" TYPE positive;` -- forever, on every run,
// and `schema apply` executed it every time and reported success. The pinned
// community binary v1.3.0 reported the same database "Schemas are synced, no
// changes to be made."
func TestColumns_DomainColumnHappyPath(t *testing.T) {
	tests := []struct {
		name    string
		genType string
		dbCol   types.DBColumn
	}{
		{
			name:    "desired names the domain the column is declared as",
			genType: "positive",
			dbCol:   domainColumn(),
		},
		{
			name:    "a domain outside the search path keeps its qualifier on both sides",
			genType: "doms.positive",
			dbCol: func() types.DBColumn {
				column := domainColumn()
				column.FormattedType = "doms.positive"
				return column
			}(),
		},
		{
			// The desired side comes from HCL, where an author may have written
			// the domain in any case. Domain names are identifiers, and
			// PostgreSQL folds an unquoted one to lower case.
			name:    "case does not make two spellings of one domain differ",
			genType: "POSITIVE",
			dbCol:   domainColumn(),
		},
		{
			name:    "a domain over an enum is the domain on this side too",
			genType: "d_enum",
			dbCol:   domainOverEnumColumn(),
		},
		{
			name:    "a domain over a composite type",
			genType: "d_comp",
			dbCol: func() types.DBColumn {
				column := domainOverEnumColumn()
				column.UDTName = "addr"
				column.FormattedType = "d_comp"
				column.DomainName = "d_comp"
				return column
			}(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			genCol := goschema.Field{Name: "qty", Type: test.genType, Nullable: true}

			result := compare.Columns(genCol, test.dbCol)

			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

// TestColumns_DomainColumnFailurePath is the other half: a domain column whose
// declared type really did change must still report it.
//
// The `bigint` row is why the comparison asks whether two domains are the SAME
// DOMAIN rather than running both spellings through the type-category
// normalizer. That normalizer folds by substring -- anything containing "int"
// becomes "integer" -- so a domain named `positive_int` compared equal to a
// desired `bigint` column and the drift was invisible, while the same fixture
// named `positive` reported it. The name of the domain decided whether the
// comparator worked.
func TestColumns_DomainColumnFailurePath(t *testing.T) {
	tests := []struct {
		name       string
		genType    string
		dbCol      types.DBColumn
		wantChange string
	}{
		{
			name:       "desired asks for the base type where the database has the domain",
			genType:    "integer",
			dbCol:      domainColumn(),
			wantChange: "positive -> integer",
		},
		{
			name:       "desired names a different domain",
			genType:    "nonnegative",
			dbCol:      domainColumn(),
			wantChange: "positive -> nonnegative",
		},
		{
			name:    "a domain whose name contains a type keyword is still compared as a domain",
			genType: "bigint",
			dbCol: func() types.DBColumn {
				column := domainColumn()
				column.FormattedType = "positive_int"
				column.DomainName = "positive_int"
				return column
			}(),
			wantChange: "positive_int -> bigint",
		},
		{
			name:       "desired asks for the base enum where the database has the domain",
			genType:    "color",
			dbCol:      domainOverEnumColumn(),
			wantChange: "d_enum -> color",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			genCol := goschema.Field{Name: "qty", Type: test.genType, Nullable: true}

			result := compare.Columns(genCol, test.dbCol)

			c.Assert(result.Changes["type"], qt.Equals, test.wantChange)
		})
	}
}

// TestColumns_NonDomainColumnKeepsCategoryComparison is the control. The domain
// rule is gated on the reader having reported a domain, and a column that is not
// one must keep comparing through the type-category normalizer, which is what
// makes "int4" and "integer" -- the same type under two catalog spellings --
// compare equal.
func TestColumns_NonDomainColumnKeepsCategoryComparison(t *testing.T) {
	tests := []struct {
		name       string
		genType    string
		dbCol      types.DBColumn
		wantChange string
	}{
		{
			name:    "catalog spelling of a plain column still folds to its category",
			genType: "integer",
			dbCol: types.DBColumn{
				Name: "qty", DataType: "integer", UDTName: "int4", IsNullable: "YES",
			},
			wantChange: "",
		},
		{
			name:    "a plain column of the domain's base type is not the domain",
			genType: "positive",
			dbCol: types.DBColumn{
				Name: "qty", DataType: "integer", UDTName: "int4", IsNullable: "YES",
			},
			wantChange: "integer -> positive",
		},
		{
			// The inverse of the domain-over-enum rows: a USER-DEFINED column
			// with no domain must keep answering with its own type name, so the
			// gate stays on DomainName rather than on DataType.
			name:    "a plain enum column with no domain compares as the enum",
			genType: "color",
			dbCol: types.DBColumn{
				Name: "qty", DataType: "USER-DEFINED", UDTName: "color", IsNullable: "YES",
			},
			wantChange: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			genCol := goschema.Field{Name: "qty", Type: test.genType, Nullable: true}

			result := compare.Columns(genCol, test.dbCol)

			c.Assert(result.Changes["type"], qt.Equals, test.wantChange)
		})
	}
}
