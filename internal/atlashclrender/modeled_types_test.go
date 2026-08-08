package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// TestRenderForDialectWritesTypesTheBinaryCanRead pins how an INSPECTED column
// type reaches HCL (stokaro/ptah#1138).
//
// A type that came out of a database carries no record of how it was written,
// so the dialect's modeled set decides. The pinned Atlas community binary
// v1.3.0 accepts a modeled name written bare and refuses every other spelling
// of an unmodeled one -- both the bare identifier and the quoted string -- so
// the only two outputs that read back are the ones asserted here.
//
// The case rows carry the weight. Ptah's SQLite reader upper-cases every type
// it reads and its PostgreSQL reader reports NUMERIC(10,2) and VARCHAR(100), so
// before this every ordinary inspected column produced HCL that binary refused.
// An upper-case sized type is the worse of the two: it parses as a CALL, and the
// binary complains about a missing function rather than a missing type.
func TestRenderForDialectWritesTypesTheBinaryCanRead(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		columnType string
		want       string
	}{
		{
			name:       "a modeled name goes out bare",
			dialect:    platform.SQLite,
			columnType: "integer",
			want:       "    type = integer\n",
		},
		{
			name:       "a modeled name the reader upper-cased is lowered",
			dialect:    platform.SQLite,
			columnType: "INTEGER",
			want:       "    type = integer\n",
		},
		{
			name:       "a size survives the lowering",
			dialect:    platform.SQLite,
			columnType: "VARCHAR(100)",
			want:       "    type = varchar(100)\n",
		},
		{
			name:       "a type SQLite's schema does not model is wrapped",
			dialect:    platform.SQLite,
			columnType: "TIMESTAMP",
			want:       `    type = sql("TIMESTAMP")` + "\n",
		},
		{
			name:       "PostgreSQL lowers a sized type that would otherwise parse as a call",
			dialect:    platform.Postgres,
			columnType: "NUMERIC(10,2)",
			want:       "    type = numeric(10,2)\n",
		},
		{
			name:       "PostgreSQL wraps the catalog's multi-word spelling",
			dialect:    platform.Postgres,
			columnType: "timestamp with time zone",
			want:       `    type = sql("timestamp with time zone")` + "\n",
		},
		{
			name:       "PostgreSQL wraps an array",
			dialect:    platform.Postgres,
			columnType: "character varying(100)[]",
			want:       `    type = sql("character varying(100)[]")` + "\n",
		},
		{
			name:       "an alias of the dialect name resolves to the same set",
			dialect:    "pgx",
			columnType: "NUMERIC(10,2)",
			want:       "    type = numeric(10,2)\n",
		},
		{
			// A dialect with no measured set keeps whatever the IR holds, which
			// is what every caller had before this and what the parse-and-
			// re-render callers still want.
			name:       "an unmeasured dialect is left alone",
			dialect:    platform.ClickHouse,
			columnType: "Int64",
			want:       "    type = Int64\n",
		},
		{
			name:       "no dialect is left alone too",
			dialect:    "",
			columnType: "INTEGER",
			want:       "    type = INTEGER\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			result, err := atlashclrender.RenderForDialect(inspectedColumn(test.columnType), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, test.want)
		})
	}
}

// TestRenderForDialectKeepsAnAuthoredSQLCall pins that the modeled set never
// overrules what the schema author wrote.
//
// When the IR came from HCL the parser already recorded the sql() call, and
// re-deciding it from the type name would second-guess the author: `integer` is
// modeled, so a set-driven render would unwrap sql("integer") into a bare name
// and change a file the author controls.
func TestRenderForDialectKeepsAnAuthoredSQLCall(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "on a dialect with a measured set", dialect: platform.Postgres},
		{name: "on a dialect without one", dialect: platform.ClickHouse},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			db := inspectedColumn("integer")
			db.Fields[0].TypeRawSQL = true

			result, err := atlashclrender.RenderForDialect(db, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, `    type = sql("integer")`+"\n")
		})
	}
}

// TestRenderForDialectWrapsEveryArrayColumnType pins that an array reaches HCL
// through sql() whatever its element type is (stokaro/ptah#1138).
//
// Before this the modeled set was consulted through the element name, so an
// array split in half by an accident: `text[]` is absent from the set as a
// whole string and was wrapped, while `numeric(10,2)[]` found `numeric` in it
// and came out as the quoted string `"numeric(10,2)[]"` -- which the pinned
// Atlas community binary v1.3.0 refuses to read at all.
//
// The rows are the four spellings PostgreSQL's format_type produces for a sized
// element, each measured against that binary bare, quoted and wrapped, plus the
// unsized ones that were already right and must stay right. The scalar controls
// at the end are what keeps this from being "wrap everything": drop the
// bracket and the same name goes out bare, exactly as it did before.
func TestRenderForDialectWrapsEveryArrayColumnType(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		want       string
	}{
		{
			name:       "a sized element type whose name is modeled",
			columnType: "numeric(10,2)[]",
			want:       `    type = sql("numeric(10,2)[]")` + "\n",
		},
		{
			name:       "a bit array",
			columnType: "bit(8)[]",
			want:       `    type = sql("bit(8)[]")` + "\n",
		},
		{
			name:       "a character array",
			columnType: "character(5)[]",
			want:       `    type = sql("character(5)[]")` + "\n",
		},
		{
			name:       "a sized multi-word element type",
			columnType: "timestamp(3) with time zone[]",
			want:       `    type = sql("timestamp(3) with time zone[]")` + "\n",
		},
		{
			name:       "an unsized element type whose name is modeled",
			columnType: "integer[]",
			want:       `    type = sql("integer[]")` + "\n",
		},
		{
			name:       "an element type that is not modeled at all",
			columnType: "text[]",
			want:       `    type = sql("text[]")` + "\n",
		},
		{
			name:       "an enum element type",
			columnType: "status[]",
			want:       `    type = sql("status[]")` + "\n",
		},
		{
			name:       "the same name without the bracket still goes out bare",
			columnType: "numeric(10,2)",
			want:       "    type = numeric(10,2)\n",
		},
		{
			name:       "and so does an unsized one",
			columnType: "integer",
			want:       "    type = integer\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			result, err := atlashclrender.RenderForDialect(inspectedColumn(test.columnType), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, test.want)
		})
	}
}

// TestRenderIgnoresTheDialect pins that the plain entry point is unchanged, so
// every caller that parses HCL and renders it back keeps the behavior it had.
func TestRenderIgnoresTheDialect(t *testing.T) {
	c := qt.New(t)

	result, err := atlashclrender.Render(inspectedColumn("NUMERIC(10,2)"))

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains, "    type = NUMERIC(10,2)\n")
}

// inspectedColumn builds the IR a database read produces for a one-column
// table: a type with no record of how it was written.
func inspectedColumn(columnType string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Probe", Name: "probe"}},
		Fields: []goschema.Field{{StructName: "Probe", Name: "c", Type: columnType}},
	}
}
