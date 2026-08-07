package schemadiff_test

// A database must be in sync with ITSELF. See stokaro/ptah#1242.
//
// `ptah-compat schema diff --from X --to X` reads one database once, converts
// one copy of the read to the desired-state model through dbschematogo and
// leaves the other as the reader produced it. Every column therefore has to
// reach the same spelling down both paths, or the comparison plans an ALTER
// that will never converge and `schema apply` executes it on every run while
// reporting success.
//
// The two paths live in different packages and neither package's own tests can
// see the disagreement: dbschematogo's tests assert what the desired side
// answers and compare's tests supply a desired side by hand. This file joins
// them, which is the only place the property is a property. Every DBColumn
// below is one column as the PostgreSQL 17.10 reader reports it, values
// measured from information_schema.columns and pg_catalog rather than invented.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestCompare_DomainColumnSelfDiffPlansNothing is the no-churn property for
// every domain column shape PostgreSQL can produce.
//
// A domain over a BUILT-IN base type reports that base type in data_type. A
// domain over a USER-DEFINED base type -- an enum, a composite, a range --
// reports data_type 'USER-DEFINED' with udt_name naming the BASE type, and only
// domain_name and format_type name the domain. The second shape reaches a
// different branch of the conversion than the first, so a fix that reads the
// domain on only one of them re-creates on the enum column exactly the churn it
// removed from the integer one -- and `schema apply` then drops the domain off
// the column for real.
func TestCompare_DomainColumnSelfDiffPlansNothing(t *testing.T) {
	tests := []struct {
		name   string
		column types.DBColumn
	}{
		{
			// CREATE DOMAIN positive AS integer CHECK (VALUE > 0);
			name: "domain over a built-in base type",
			column: types.DBColumn{
				Name:          "qty",
				DataType:      "integer",
				UDTName:       "int4",
				FormattedType: "positive",
				DomainName:    "positive",
				IsNullable:    "NO",
			},
		},
		{
			// CREATE DOMAIN doms.positive AS integer; a domain off the search
			// path keeps its qualifier on both sides or neither.
			name: "domain outside the search path",
			column: types.DBColumn{
				Name:          "qty",
				DataType:      "integer",
				UDTName:       "int4",
				FormattedType: "doms.positive",
				DomainName:    "positive",
				IsNullable:    "NO",
			},
		},
		{
			// CREATE TYPE color AS ENUM ('r','g','b');
			// CREATE DOMAIN d_enum AS color CHECK (VALUE <> 'b');
			name: "domain over an enum",
			column: types.DBColumn{
				Name:          "c",
				DataType:      "USER-DEFINED",
				UDTName:       "color",
				FormattedType: "d_enum",
				DomainName:    "d_enum",
				IsNullable:    "NO",
			},
		},
		{
			// CREATE TYPE addr AS (street text, city text);
			// CREATE DOMAIN d_comp AS addr;
			name: "domain over a composite type",
			column: types.DBColumn{
				Name:          "a",
				DataType:      "USER-DEFINED",
				UDTName:       "addr",
				FormattedType: "d_comp",
				DomainName:    "d_comp",
				IsNullable:    "YES",
			},
		},
		{
			// CREATE TYPE myrange AS RANGE (subtype=integer);
			// CREATE DOMAIN d_range AS myrange;
			name: "domain over a range type",
			column: types.DBColumn{
				Name:          "r",
				DataType:      "USER-DEFINED",
				UDTName:       "myrange",
				FormattedType: "d_range",
				DomainName:    "d_range",
				IsNullable:    "YES",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &types.DBSchema{
				Tables: []types.DBTable{{Name: "t", Columns: []types.DBColumn{test.column}}},
			}

			diff := schemadiff.CompareWithDialect(
				dbschematogo.ConvertDBSchemaToGoSchema(database), database, "postgres",
			)

			c.Assert(diff.TablesModified, qt.HasLen, 0)
			c.Assert(diff.HasChanges(), qt.IsFalse)
		})
	}
}

// TestCompare_NonDomainColumnSelfDiffPlansNothing is the control. The rule that
// keeps a domain column quiet is gated on the reader having reported a domain,
// so the columns that carry none must stay quiet for their own reasons -- an
// enum column answering with its type name, a composite column with its own, a
// plain integer through the ordinary catalog-spelling fold.
//
// Without this, reading the domain unconditionally -- answering FormattedType
// for every column that has one -- would look like a fix and would silently
// change what a plain enum column compares as.
func TestCompare_NonDomainColumnSelfDiffPlansNothing(t *testing.T) {
	tests := []struct {
		name   string
		column types.DBColumn
	}{
		{
			name: "plain enum column",
			column: types.DBColumn{
				Name:       "m",
				DataType:   "USER-DEFINED",
				UDTName:    "mood",
				IsNullable: "NO",
			},
		},
		{
			name: "plain composite column",
			column: types.DBColumn{
				Name:       "p",
				DataType:   "USER-DEFINED",
				UDTName:    "pt",
				IsNullable: "YES",
			},
		},
		{
			name: "plain integer column under its catalog spelling",
			column: types.DBColumn{
				Name:       "id",
				DataType:   "integer",
				UDTName:    "int4",
				IsNullable: "NO",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &types.DBSchema{
				Tables: []types.DBTable{{Name: "t", Columns: []types.DBColumn{test.column}}},
			}

			diff := schemadiff.CompareWithDialect(
				dbschematogo.ConvertDBSchemaToGoSchema(database), database, "postgres",
			)

			c.Assert(diff.TablesModified, qt.HasLen, 0)
			c.Assert(diff.HasChanges(), qt.IsFalse)
		})
	}
}
