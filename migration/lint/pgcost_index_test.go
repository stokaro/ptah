package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// pgCollated is one baseline column of orders declared with a collation, or
// with the database default when collation is empty.
func pgCollated(name, columnType, collation string) lint.BaselineColumn {
	column := pgColumn(name, columnType, false)
	column.Collation = collation
	return column
}

// pgIndex is one baseline index of orders over whole columns.
func pgIndex(name string, columns ...string) lint.BaselineIndex {
	parts := make([]lint.BaselineIndexPart, 0, len(columns))
	for _, column := range columns {
		parts = append(parts, lint.BaselineIndexPart{Column: column})
	}
	return lint.BaselineIndex{Version: 2, Table: "orders", Name: name, Parts: parts}
}

func analyzePGState(c *qt.C, alter string, columns []lint.BaselineColumn, indexes []lint.BaselineIndex) lint.Analysis {
	c.Helper()
	analysis, err := lint.AnalyzeFS(fixture(pgFS(alter)), lint.Options{
		Dialect:         "postgres",
		DirFormat:       migrationfile.DirFormatAtlas,
		Selection:       lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Baseline:        columns,
		BaselineIndexes: indexes,
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

// TestTypeRewriteRule_NamesTheIndexesACollationChangeRebuilds pins PG301 to
// the collation change measured to rebuild every index on the column while
// the heap stays, and to the index names it carries into the message.
func TestTypeRewriteRule_NamesTheIndexesACollationChangeRebuilds(t *testing.T) {
	tests := []struct {
		name    string
		alter   string
		column  lint.BaselineColumn
		indexes []lint.BaselineIndex
		want    []string
	}{
		{
			name:    "a collation named on a default-collated indexed column",
			alter:   `ALTER TABLE orders ALTER COLUMN body TYPE text COLLATE "C";`,
			column:  pgCollated("body", "text", ""),
			indexes: []lint.BaselineIndex{pgIndex("orders_body", "body")},
			want: []string{
				`changes the collation from the database default to "C"`,
				"keeps the table but rebuilds every index on the column (orders_body)",
				"reading the table once per index",
			},
		},
		{
			name:    "every index on the column is named",
			alter:   `ALTER TABLE orders ALTER COLUMN body TYPE text COLLATE "en-US-x-icu";`,
			column:  pgCollated("body", "text", "C"),
			indexes: []lint.BaselineIndex{pgIndex("orders_body", "body"), pgIndex("orders_body_note", "body", "note"), pgIndex("orders_note", "note")},
			want:    []string{`from "C" to "en-US-x-icu"`, "(orders_body, orders_body_note)"},
		},
		{
			name:    "a clause with no COLLATE resets a declared collation",
			alter:   "ALTER TABLE orders ALTER COLUMN body TYPE text;",
			column:  pgCollated("body", "text", "C"),
			indexes: []lint.BaselineIndex{pgIndex("orders_body", "body")},
			want:    []string{`from "C" to the database default`},
		},
		{
			name:    "an unquoted name folds to lower case",
			alter:   "ALTER TABLE orders ALTER COLUMN body TYPE text COLLATE POSIX;",
			column:  pgCollated("body", "text", "C"),
			indexes: []lint.BaselineIndex{pgIndex("orders_body", "body")},
			want:    []string{`from "C" to "posix"`},
		},
		{
			name:    "an in-place widening with a collation change is the rebuild, not the catalog edit",
			alter:   `ALTER TABLE orders ALTER COLUMN note TYPE varchar(20) COLLATE "POSIX";`,
			column:  pgCollated("note", "character varying(10)", ""),
			indexes: []lint.BaselineIndex{pgIndex("orders_note", "note")},
			want:    []string{`TYPE character varying(20) changes the collation`, "(orders_note)"},
		},
		{
			name:    "a prefix or expression part still reads the column",
			alter:   `ALTER TABLE orders ALTER COLUMN body TYPE text COLLATE "C";`,
			column:  pgCollated("body", "text", ""),
			indexes: []lint.BaselineIndex{{Version: 2, Table: "orders", Name: "orders_lower", Parts: []lint.BaselineIndexPart{{Column: ""}, {Column: "body"}}}},
			want:    []string{"(orders_lower)"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzePGState(c, test.alter, []lint.BaselineColumn{test.column}, test.indexes)
			c.Assert(pgCostCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"PG301"})
			message := messageOf(analysis.Findings(), "PG301")
			for _, want := range test.want {
				c.Assert(message, qt.Contains, want)
			}
		})
	}
}

// TestTypeRewriteRule_ARewriteDecidesBeforeTheCollation: a narrowing that
// rewrites the table rewrites every index with it, so the collation clause
// adds nothing to the finding.
func TestTypeRewriteRule_ARewriteDecidesBeforeTheCollation(t *testing.T) {
	c := qt.New(t)
	analysis := analyzePGState(c, `ALTER TABLE orders ALTER COLUMN body TYPE varchar(20) COLLATE "C";`,
		[]lint.BaselineColumn{pgCollated("body", "text", "")}, []lint.BaselineIndex{pgIndex("orders_body", "body")})
	message := messageOf(analysis.Findings(), "PG301")
	c.Assert(message, qt.Contains, "rewrites the whole table")
	c.Assert(message, qt.Not(qt.Contains), "rebuilds every index on the column (")
}

// TestTypeRewriteRule_StaysQuietWhereTheCollationDoesNotMove pins the
// measured no-ops: a restated collation, "default" on a default-collated
// column, and a change on a column no index reads, which is a catalog edit.
func TestTypeRewriteRule_StaysQuietWhereTheCollationDoesNotMove(t *testing.T) {
	tests := []struct {
		name    string
		alter   string
		column  lint.BaselineColumn
		indexes []lint.BaselineIndex
	}{
		{
			name:    "the collation restated on an indexed column",
			alter:   `ALTER TABLE orders ALTER COLUMN body TYPE text COLLATE "C";`,
			column:  pgCollated("body", "text", "C"),
			indexes: []lint.BaselineIndex{pgIndex("orders_body", "body")},
		},
		{
			name:    "default named on a default-collated column",
			alter:   `ALTER TABLE orders ALTER COLUMN body TYPE text COLLATE "default";`,
			column:  pgCollated("body", "text", ""),
			indexes: []lint.BaselineIndex{pgIndex("orders_body", "body")},
		},
		{
			name:    "a schema-qualified spelling of the same collation",
			alter:   `ALTER TABLE orders ALTER COLUMN body TYPE text COLLATE pg_catalog."C";`,
			column:  pgCollated("body", "text", "C"),
			indexes: []lint.BaselineIndex{pgIndex("orders_body", "body")},
		},
		{
			name:    "a change on a column no index reads",
			alter:   `ALTER TABLE orders ALTER COLUMN body TYPE text COLLATE "C";`,
			column:  pgCollated("body", "text", ""),
			indexes: []lint.BaselineIndex{pgIndex("orders_note", "note")},
		},
		{
			name:   "a change with no indexes in the state at all",
			alter:  `ALTER TABLE orders ALTER COLUMN body TYPE text COLLATE "C";`,
			column: pgCollated("body", "text", ""),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzePGState(c, test.alter, []lint.BaselineColumn{test.column}, test.indexes)
			c.Assert(pgCostCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
		})
	}
}

// TestPrimaryKeyScanRule_ResolvesTheIndexAUsingIndexFormNames pins PG304 to
// the USING INDEX form, whose columns are the index's: found in the state
// the version starts from, or among the unique indexes the file builds
// earlier.
func TestPrimaryKeyScanRule_ResolvesTheIndexAUsingIndexFormNames(t *testing.T) {
	tests := []struct {
		name    string
		alter   string
		columns []lint.BaselineColumn
		indexes []lint.BaselineIndex
		want    []string
	}{
		{
			name:    "the index in the baseline",
			alter:   "ALTER TABLE orders ADD CONSTRAINT orders_pk PRIMARY KEY USING INDEX orders_id;",
			columns: []lint.BaselineColumn{pgColumn("id", "integer", false)},
			indexes: []lint.BaselineIndex{{Version: 2, Table: "orders", Name: "orders_id", Unique: true, Parts: []lint.BaselineIndexPart{{Column: "id"}}}},
			want: []string{
				"ADD PRIMARY KEY USING INDEX orders_id on orders sets id NOT NULL",
				"the index is already built, so the scan PostgreSQL still runs is the one that checks it",
				"CHECK (id IS NOT NULL)",
			},
		},
		{
			name:    "the index built earlier in the same file",
			alter:   "CREATE UNIQUE INDEX orders_id ON orders (id);\nALTER TABLE orders ADD PRIMARY KEY USING INDEX orders_id;",
			columns: []lint.BaselineColumn{pgColumn("id", "integer", false)},
			want:    []string{"ADD PRIMARY KEY USING INDEX orders_id on orders sets id NOT NULL"},
		},
		{
			name:    "a two-column index names the nullable one",
			alter:   "ALTER TABLE orders ADD PRIMARY KEY USING INDEX orders_id_total;",
			columns: []lint.BaselineColumn{pgColumn("id", "integer", true), pgColumn("total", "integer", false)},
			indexes: []lint.BaselineIndex{{Version: 2, Table: "orders", Name: "orders_id_total", Unique: true, Parts: []lint.BaselineIndexPart{{Column: "id"}, {Column: "total"}}}},
			want:    []string{"sets total NOT NULL"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzePGState(c, test.alter, test.columns, test.indexes)
			c.Assert(pgCostCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"PG304"})
			message := messageOf(analysis.Findings(), "PG304")
			for _, want := range test.want {
				c.Assert(message, qt.Contains, want)
			}
		})
	}
}

// TestPrimaryKeyScanRule_StaysQuietWhereTheUsingIndexFormSetsNothingNotNull
// pins the two quiet USING INDEX shapes: every key column already NOT NULL,
// and an index nothing can resolve.
func TestPrimaryKeyScanRule_StaysQuietWhereTheUsingIndexFormSetsNothingNotNull(t *testing.T) {
	tests := []struct {
		name    string
		alter   string
		columns []lint.BaselineColumn
		indexes []lint.BaselineIndex
	}{
		{
			name:    "the key column is NOT NULL already",
			alter:   "ALTER TABLE orders ADD PRIMARY KEY USING INDEX orders_id;",
			columns: []lint.BaselineColumn{pgColumn("id", "integer", true)},
			indexes: []lint.BaselineIndex{{Version: 2, Table: "orders", Name: "orders_id", Unique: true, Parts: []lint.BaselineIndexPart{{Column: "id"}}}},
		},
		{
			name:    "an index neither the file nor the state knows",
			alter:   "ALTER TABLE orders ADD PRIMARY KEY USING INDEX orders_elsewhere;",
			columns: []lint.BaselineColumn{pgColumn("id", "integer", false)},
			indexes: []lint.BaselineIndex{{Version: 2, Table: "orders", Name: "orders_id", Unique: true, Parts: []lint.BaselineIndexPart{{Column: "id"}}}},
		},
		{
			name:    "an index over an expression names no column",
			alter:   "ALTER TABLE orders ADD PRIMARY KEY USING INDEX orders_expr;",
			columns: []lint.BaselineColumn{pgColumn("id", "integer", false)},
			indexes: []lint.BaselineIndex{{Version: 2, Table: "orders", Name: "orders_expr", Unique: true, Parts: []lint.BaselineIndexPart{{Column: ""}}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzePGState(c, test.alter, test.columns, test.indexes)
			c.Assert(pgCostCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
		})
	}
}
