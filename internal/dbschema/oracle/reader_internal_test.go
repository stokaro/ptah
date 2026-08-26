package oracle

// White-box testing required: Oracle catalog normalization is a package-local
// correctness primitive with no direct exported API.

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
)

func number(value int64) sql.NullInt64 { return sql.NullInt64{Int64: value, Valid: true} }

// TestFormatColumnType_ComposesTheDeclaredSpelling holds the property that made
// a live schema fail to match itself.
//
// DATA_TYPE alone is not the type: a VARCHAR2(200) and a VARCHAR2(4000) both
// report VARCHAR2, and a NUMBER(10) and a bare NUMBER both report NUMBER, so
// reading only that column answered `type = NUMBER` for every integer column.
func TestFormatColumnType_ComposesTheDeclaredSpelling(t *testing.T) {
	tests := []struct {
		name       string
		dataType   string
		charLength sql.NullInt64
		precision  sql.NullInt64
		scale      sql.NullInt64
		want       string
	}{
		{name: "integer", dataType: "NUMBER", precision: number(10), scale: number(0), want: "NUMBER(10)"},
		{name: "boolean width", dataType: "NUMBER", precision: number(1), scale: number(0), want: "NUMBER(1)"},
		{name: "decimal keeps its scale", dataType: "NUMBER", precision: number(5), scale: number(2), want: "NUMBER(5,2)"},
		// An unconstrained NUMBER reports a NULL precision and a NULL scale,
		// measured on 23.26 -- not a zero, which is what a plain int64 scan
		// would have recorded.
		{name: "unconstrained number", dataType: "NUMBER", want: "NUMBER"},
		{name: "varchar2", dataType: "VARCHAR2", charLength: number(200), want: "VARCHAR2(200)"},
		{name: "char", dataType: "CHAR", charLength: number(3), want: "CHAR(3)"},
		{name: "raw", dataType: "RAW", charLength: number(16), want: "RAW(16)"},
		{name: "clob carries no width", dataType: "CLOB", want: "CLOB"},
		{name: "json carries no width", dataType: "JSON", want: "JSON"},
		// A column declared TIMESTAMP reports DATA_TYPE `TIMESTAMP(6)` with
		// DATA_SCALE 6. Appending the scale again would write TIMESTAMP(6)(6).
		{name: "timestamp already carries its precision", dataType: "TIMESTAMP(6)", scale: number(6), want: "TIMESTAMP(6)"},
		{name: "interval already carries its precision", dataType: "INTERVAL DAY(2) TO SECOND(6)", precision: number(2), scale: number(6), want: "INTERVAL DAY(2) TO SECOND(6)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := formatColumnType(test.dataType, test.charLength, test.precision, test.scale)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestAssignDefault_SeparatesOraclesOwnBookkeeping pins the three things
// DATA_DEFAULT can hold.
//
// Reading it as a user default made a schema Ptah had just applied read back
// wrong twice over: a virtual column reported its expression as a DEFAULT and
// nothing saying it was generated, and an identity column reported the nextval
// of the sequence Oracle created for it -- a name that differs in every
// database, so no two catalogs would ever compare equal.
func TestAssignDefault_SeparatesOraclesOwnBookkeeping(t *testing.T) {
	tests := []struct {
		name          string
		identity      bool
		virtual       bool
		value         sql.NullString
		wantDefault   string
		wantGenerated string
		wantKind      string
	}{
		{
			name:        "a declared default",
			value:       sql.NullString{String: "0", Valid: true},
			wantDefault: "0",
		},
		{
			name:          "a virtual column's expression",
			virtual:       true,
			value:         sql.NullString{String: `"size"*2`, Valid: true},
			wantGenerated: `"size"*2`,
			wantKind:      "VIRTUAL",
		},
		{
			name:     "an identity column's sequence",
			identity: true,
			value:    sql.NullString{String: `"PTAH"."ISEQ$$_73294".nextval`, Valid: true},
		},
		{
			name:  "no default at all",
			value: sql.NullString{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			var column catalog.Column
			assignDefault(&column, test.identity, test.virtual, test.value)

			c.Assert(stringValue(column.ColumnDefault), qt.Equals, test.wantDefault)
			c.Assert(stringValue(column.GeneratedExpression), qt.Equals, test.wantGenerated)
			c.Assert(column.GeneratedKind, qt.Equals, test.wantKind)
		})
	}
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// TestNumberPointer_AnswersNilForWhatThisModelCannotHold pins the value that
// ended the first live read of an Oracle schema.
//
// A sequence created with no MAXVALUE gets Oracle's default of 10^28 - 1, and
// ALL_SEQUENCES reports it in full. Scanning that into an int64 failed with
// `value out of range`; answering nil says this model carries no bound there,
// which is true, where a clamped number would be a bound nobody declared.
func TestNumberPointer_AnswersNilForWhatThisModelCannotHold(t *testing.T) {
	var (
		one      = int64(1)
		minInt64 = int64(-9223372036854775808)
		twenty   = int64(20)
	)

	tests := []struct {
		name  string
		value sql.NullString
		want  *int64
	}{
		{name: "a plain increment", value: sql.NullString{String: "1", Valid: true}, want: &one},
		{name: "a negative bound", value: sql.NullString{String: "-9223372036854775808", Valid: true}, want: &minInt64},
		{name: "Oracle's default MAXVALUE", value: sql.NullString{String: "9999999999999999999999999999", Valid: true}},
		{name: "NULL", value: sql.NullString{}},
		{name: "whitespace around the number", value: sql.NullString{String: "  20  ", Valid: true}, want: &twenty},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := numberPointer(test.value)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestDropStatement_CarriesTheClausesCleanupNeeds holds the two clauses that
// make a repeated cleanup work.
//
// CASCADE CONSTRAINTS means a cycle of foreign keys does not decide the order,
// and PURGE keeps the table out of the recycle bin -- where it would keep its
// storage and be listed by USER_OBJECTS on the next run.
func TestDropStatement_CarriesTheClausesCleanupNeeds(t *testing.T) {
	tests := []struct {
		name string
		kind string
		want string
	}{
		{name: "table", kind: "TABLE", want: `DROP TABLE "ORA_POSTS" CASCADE CONSTRAINTS PURGE`},
		{name: "view", kind: "VIEW", want: `DROP VIEW "ORA_POSTS"`},
		{name: "sequence", kind: "SEQUENCE", want: `DROP SEQUENCE "ORA_POSTS"`},
		{name: "materialized view", kind: "MATERIALIZED VIEW", want: `DROP MATERIALIZED VIEW "ORA_POSTS"`},
		{name: "type", kind: "TYPE", want: `DROP TYPE "ORA_POSTS" FORCE`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(dropStatement(test.kind, "ORA_POSTS"), qt.Equals, test.want)
		})
	}
}

// TestQueriesExcludeTheRecycleBin pins the filters that keep dropped objects out
// of a read.
//
// Oracle does not delete a dropped table: it renames it to BIN$... and keeps it
// until the bin is purged, and all_tables lists it like any other. Measured
// against 23.26 -- after one apply dropped a table, dba_recyclebin held it and
// the next plan answered
//
//	ALTER TABLE APPUSER."BIN$WZaxbiSHASjgYwUAEawI+Q==$0" DROP CONSTRAINT ...
//
// which compounds: every apply that drops a table leaves an entry the next read
// treats as live. all_tables carries the flag; the other catalogs do not, so
// they are filtered by the name Oracle gives a recycled object.
func TestQueriesExcludeTheRecycleBin(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "tables use the catalog flag", query: tableQuery, want: "t.dropped = 'NO'"},
		{name: "columns exclude recycled tables", query: columnQuery, want: "c.table_name NOT LIKE 'BIN$%'"},
		{name: "constraints exclude recycled tables", query: constraintQuery, want: "c.table_name NOT LIKE 'BIN$%'"},
		{name: "referenced keys exclude recycled tables", query: referencedKeyQuery, want: "c.table_name NOT LIKE 'BIN$%'"},
		{name: "indexes exclude recycled tables", query: indexQuery, want: "i.table_name NOT LIKE 'BIN$%'"},
		{name: "indexes exclude recycled indexes", query: indexQuery, want: "i.index_name NOT LIKE 'BIN$%'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(tt.query, qt.Contains, tt.want)
		})
	}
}

// TestWithoutGeneratedKeys_KeepsWhatTheDeclarationCanMatch pins which key
// constraints survive a read.
//
// Oracle names an inline key itself -- `id INTEGER PRIMARY KEY` becomes
// SYS_C008644 -- and the declaration has no name to compare that against, so a
// plan drops it and the next apply recreates it under a fresh number. The fact
// is not lost: markKeyColumns runs first and puts IsPrimaryKey on the column,
// which is the shape the declared side uses.
//
// A constraint the user named arrives as 'USER NAME' and is kept, because that
// one does have a counterpart to compare against.
func TestWithoutGeneratedKeys_KeepsWhatTheDeclarationCanMatch(t *testing.T) {
	tests := []struct {
		name      string
		generated map[string]bool
		want      []string
	}{
		{
			name:      "an inline key Oracle named is dropped",
			generated: map[string]bool{"SYS_C008644": true},
			want:      []string{"orders_total_check", "uq_email", "fk_author"},
		},
		{
			name:      "nothing generated leaves the list alone",
			generated: make(map[string]bool),
			want:      []string{"SYS_C008644", "orders_total_check", "uq_email", "fk_author"},
		},
		{
			name:      "a user-named unique is kept even beside a generated one",
			generated: map[string]bool{"SYS_C008644": true, "SYS_C008700": true},
			want:      []string{"orders_total_check", "uq_email", "fk_author"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			constraints := []catalog.Constraint{
				{Name: "SYS_C008644", Type: "PRIMARY KEY"},
				{Name: "orders_total_check", Type: "CHECK"},
				{Name: "uq_email", Type: "UNIQUE"},
				{Name: "fk_author", Type: "FOREIGN KEY"},
			}

			kept := withoutGeneratedKeys(constraints, tt.generated)

			names := make([]string, 0, len(kept))
			for _, constraint := range kept {
				names = append(names, constraint.Name)
			}
			c.Assert(names, qt.DeepEquals, tt.want)
		})
	}
}
