package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// ordersIndex is one baseline index of orders over whole columns.
func ordersIndex(name string, unique bool, columns ...string) lint.BaselineIndex {
	parts := make([]lint.BaselineIndexPart, 0, len(columns))
	for _, column := range columns {
		parts = append(parts, lint.BaselineIndexPart{Column: column})
	}
	return lint.BaselineIndex{Version: 2, Table: "orders", Name: name, Unique: unique, Parts: parts}
}

// ordersColumn is one baseline column of orders, for the NULLS NOT DISTINCT
// rows that need to know whether a column can hold NULL.
func ordersColumn(name string, notNull bool) lint.BaselineColumn {
	return lint.BaselineColumn{Version: 2, Table: "orders", Name: name, ColumnType: "text", NotNull: notNull}
}

func analyzeUniqueState(c *qt.C, dialect, migration string, columns []lint.BaselineColumn, indexes []lint.BaselineIndex) lint.Analysis {
	c.Helper()
	analysis, err := lint.AnalyzeFS(fixture(uniqueFS(migration)), lint.Options{
		Dialect:         dialect,
		DirFormat:       migrationfile.DirFormatAtlas,
		Selection:       lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Baseline:        columns,
		BaselineIndexes: indexes,
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

// TestIndexMadeUniqueRule_RecognizesAReplacementUnderANewName pins MF102 to
// an index the file drops and rebuilds as unique under another name, which
// only the dropped index's columns, as the state records them, can show.
func TestIndexMadeUniqueRule_RecognizesAReplacementUnderANewName(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		migration string
		indexes   []lint.BaselineIndex
		want      []string
	}{
		{
			name:      "PostgreSQL DROP INDEX then CREATE UNIQUE INDEX under a new name",
			dialect:   "postgres",
			migration: "DROP INDEX orders_email_idx;\nCREATE UNIQUE INDEX orders_email_uq ON orders (email);",
			indexes:   []lint.BaselineIndex{ordersIndex("orders_email_idx", false, "email")},
			want: []string{
				"CREATE UNIQUE INDEX orders_email_uq replaces the index orders_email_idx dropped earlier, which covered the same columns, with a unique one under a new name",
				"leaves the table without the index it had",
			},
		},
		{
			name:      "MySQL in one statement",
			dialect:   "mysql",
			migration: "ALTER TABLE orders DROP INDEX k_email, ADD UNIQUE KEY uq_email (email);",
			indexes:   []lint.BaselineIndex{ordersIndex("k_email", false, "email")},
			want:      []string{"ADD UNIQUE uq_email replaces the index k_email dropped earlier, which covered the same columns"},
		},
		{
			name:      "the same columns in another order",
			dialect:   "postgres",
			migration: "DROP INDEX orders_email_code;\nCREATE UNIQUE INDEX orders_code_email ON orders (code, email);",
			indexes:   []lint.BaselineIndex{ordersIndex("orders_email_code", false, "email", "code")},
			want:      []string{"replaces the index orders_email_code dropped earlier, which covered the same columns"},
		},
		{
			name:      "the dropped index is found among several",
			dialect:   "postgres",
			migration: "DROP INDEX orders_code_idx;\nDROP INDEX orders_email_idx;\nCREATE UNIQUE INDEX orders_email_uq ON orders (email);",
			indexes:   []lint.BaselineIndex{ordersIndex("orders_code_idx", false, "code"), ordersIndex("orders_email_idx", false, "email")},
			want:      []string{"replaces the index orders_email_idx dropped earlier"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeUniqueState(c, test.dialect, test.migration, nil, test.indexes)
			c.Assert(uniqueCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"MF102"})
			message := messageOf(analysis.Findings(), "MF102")
			for _, want := range test.want {
				c.Assert(message, qt.Contains, want)
			}
		})
	}
}

// TestIndexMadeUniqueRule_LeavesADifferentIndexToMF101 pins what a dropped
// index must cover to count as replaced: exactly the new key's columns,
// whole, as the state records them.
func TestIndexMadeUniqueRule_LeavesADifferentIndexToMF101(t *testing.T) {
	tests := []struct {
		name      string
		migration string
		indexes   []lint.BaselineIndex
	}{
		{
			name:      "a dropped index over other columns",
			migration: "DROP INDEX orders_code_idx;\nCREATE UNIQUE INDEX orders_email_uq ON orders (email);",
			indexes:   []lint.BaselineIndex{ordersIndex("orders_code_idx", false, "code")},
		},
		{
			name:      "a dropped index over a superset of the columns",
			migration: "DROP INDEX orders_email_code;\nCREATE UNIQUE INDEX orders_email_uq ON orders (email);",
			indexes:   []lint.BaselineIndex{ordersIndex("orders_email_code", false, "email", "code")},
		},
		{
			name:      "a dropped index keyed by a prefix",
			migration: "DROP INDEX orders_email_idx;\nCREATE UNIQUE INDEX orders_email_uq ON orders (email);",
			indexes:   []lint.BaselineIndex{{Version: 2, Table: "orders", Name: "orders_email_idx", Parts: []lint.BaselineIndexPart{{Column: "email", Prefix: 5}}}},
		},
		{
			name:      "a dropped index the state does not know",
			migration: "DROP INDEX orders_email_idx;\nCREATE UNIQUE INDEX orders_email_uq ON orders (email);",
			indexes:   []lint.BaselineIndex{ordersIndex("orders_code_idx", false, "code")},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeUniqueState(c, "postgres", test.migration, nil, test.indexes)
			c.Assert(uniqueCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"MF101"})
		})
	}
}

// TestUniqueRules_StayQuietWhereTheRowsAreProvenUnique pins the exemption a
// unique index or primary key in the state grants: over the new key's
// columns or a subset of them, the build cannot meet a duplicate.
func TestUniqueRules_StayQuietWhereTheRowsAreProvenUnique(t *testing.T) {
	tests := []struct {
		name      string
		migration string
		columns   []lint.BaselineColumn
		indexes   []lint.BaselineIndex
	}{
		{
			name:      "a unique index over the same column",
			migration: "CREATE UNIQUE INDEX orders_email_uq ON orders (email);",
			indexes:   []lint.BaselineIndex{ordersIndex("orders_email_old", true, "email")},
		},
		{
			name:      "the primary key over a subset of the new key",
			migration: "CREATE UNIQUE INDEX orders_id_email ON orders (id, email);",
			indexes:   []lint.BaselineIndex{{Version: 2, Table: "orders", Name: "orders_pkey", Unique: true, Primary: true, Parts: []lint.BaselineIndexPart{{Column: "id"}}}},
		},
		{
			name:      "a unique constraint added over a covered column",
			migration: "ALTER TABLE orders ADD CONSTRAINT orders_email_uq UNIQUE (email);",
			indexes:   []lint.BaselineIndex{ordersIndex("orders_email_old", true, "email")},
		},
		{
			name:      "a unique index dropped and rebuilt under the same name",
			migration: "DROP INDEX orders_email_uq;\nCREATE UNIQUE INDEX orders_email_uq ON orders (email);",
			indexes:   []lint.BaselineIndex{ordersIndex("orders_email_uq", true, "email")},
		},
		{
			name:      "NULLS NOT DISTINCT over a NOT NULL covered column",
			migration: "CREATE UNIQUE INDEX orders_email_uq ON orders (email) NULLS NOT DISTINCT;",
			columns:   []lint.BaselineColumn{ordersColumn("email", true)},
			indexes:   []lint.BaselineIndex{ordersIndex("orders_email_old", true, "email")},
		},
		{
			name:      "a schema-qualified reference to the covered table",
			migration: "CREATE UNIQUE INDEX orders_email_uq ON public.orders (email);",
			indexes:   []lint.BaselineIndex{ordersIndex("orders_email_old", true, "email")},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeUniqueState(c, "postgres", test.migration, test.columns, test.indexes)
			c.Assert(uniqueCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
		})
	}
}

// TestUniqueRules_KeepReportingWhereTheStateProvesNothing pins what does
// not count as proof: a unique index over a superset, a partial or
// incomplete one, one keyed by a prefix or an expression, NULLS NOT DISTINCT
// over a nullable column, and a table two schemas carry.
func TestUniqueRules_KeepReportingWhereTheStateProvesNothing(t *testing.T) {
	tests := []struct {
		name      string
		migration string
		columns   []lint.BaselineColumn
		indexes   []lint.BaselineIndex
	}{
		{
			name:      "a unique index over a superset of the new key",
			migration: "CREATE UNIQUE INDEX orders_email_uq ON orders (email);",
			indexes:   []lint.BaselineIndex{ordersIndex("orders_email_code", true, "email", "code")},
		},
		{
			name:      "a non-unique index over the same column",
			migration: "CREATE UNIQUE INDEX orders_email_uq ON orders (email);",
			indexes:   []lint.BaselineIndex{ordersIndex("orders_email_idx", false, "email")},
		},
		{
			name:      "a partial unique index",
			migration: "CREATE UNIQUE INDEX orders_email_uq ON orders (email);",
			indexes:   []lint.BaselineIndex{{Version: 2, Table: "orders", Name: "orders_email_live", Unique: true, Partial: true, Parts: []lint.BaselineIndexPart{{Column: "email"}}}},
		},
		{
			name:      "a unique index whose key parts the reader could not name",
			migration: "CREATE UNIQUE INDEX orders_email_uq ON orders (email);",
			indexes:   []lint.BaselineIndex{{Version: 2, Table: "orders", Name: "orders_email_x", Unique: true, Incomplete: true, Parts: []lint.BaselineIndexPart{{Column: "email"}}}},
		},
		{
			name:      "a unique index keyed by a prefix",
			migration: "CREATE UNIQUE INDEX orders_email_uq ON orders (email);",
			indexes:   []lint.BaselineIndex{{Version: 2, Table: "orders", Name: "orders_email_p", Unique: true, Parts: []lint.BaselineIndexPart{{Column: "email", Prefix: 5}}}},
		},
		{
			name:      "a unique index over an expression",
			migration: "CREATE UNIQUE INDEX orders_email_uq ON orders (email);",
			indexes:   []lint.BaselineIndex{{Version: 2, Table: "orders", Name: "orders_email_lower", Unique: true, Parts: []lint.BaselineIndexPart{{Column: ""}}}},
		},
		{
			name:      "NULLS NOT DISTINCT over a nullable covered column",
			migration: "CREATE UNIQUE INDEX orders_email_uq ON orders (email) NULLS NOT DISTINCT;",
			columns:   []lint.BaselineColumn{ordersColumn("email", false)},
			indexes:   []lint.BaselineIndex{ordersIndex("orders_email_old", true, "email")},
		},
		{
			name:      "a table two schemas carry resolves to neither",
			migration: "CREATE UNIQUE INDEX orders_email_uq ON orders (email);",
			indexes: []lint.BaselineIndex{
				{Version: 2, Schema: "public", Table: "orders", Name: "orders_email_old", Unique: true, Parts: []lint.BaselineIndexPart{{Column: "email"}}},
				{Version: 2, Schema: "archive", Table: "orders", Name: "orders_email_old", Unique: true, Parts: []lint.BaselineIndexPart{{Column: "email"}}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeUniqueState(c, "postgres", test.migration, test.columns, test.indexes)
			c.Assert(uniqueCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"MF101"})
		})
	}
}

// TestUniqueRules_NameTheirInputWhenTheRunSuppliesNone: the two rules read
// the state as a refinement, so a run without it still reports from the text
// and names the refinement it went without.
func TestUniqueRules_NameTheirInputWhenTheRunSuppliesNone(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeUnique(c, "postgres", "CREATE UNIQUE INDEX orders_email_uq ON orders (email);")
	c.Assert(uniqueCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"MF101"})
	c.Assert(analysis.BaselineVersions(), qt.DeepEquals, []int64{2})
	var unmet []string
	for _, entry := range analysis.UnmetInputs() {
		unmet = append(unmet, entry.Rule+":"+entry.Input.String())
	}
	c.Assert(unmet, qt.DeepEquals, []string{
		"MF101:baseline schema that refines the statement text",
		"MF102:baseline schema that refines the statement text",
	})
}

// TestUniqueRules_AskForNothingWhereNothingIsBuilt: a file with no unique
// index costs no catalog read.
func TestUniqueRules_AskForNothingWhereNothingIsBuilt(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeUnique(c, "postgres", "CREATE INDEX orders_email_idx ON orders (email);")
	c.Assert(analysis.BaselineVersions(), qt.HasLen, 0)
	c.Assert(analysis.UnmetInputs(), qt.HasLen, 0)
}
