package oraclecollation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/oraclecollation"
)

// collationChange is a diff that changes the collation of column c of table t,
// where t held observed before the change.
func collationChange(observed schemacapture.TableObservation) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "t",
		Current:   observed,
		ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName: "c",
			Changes:    map[string]string{"collation": " -> BINARY_CI"},
		}},
	}}}
}

func TestRefuseKeyedColumnChange_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "no diff"},
		{name: "a column nothing uses", diff: collationChange(schemacapture.TableObservation{
			Indexes:     []catalog.Index{{Name: "t_other", Columns: []string{"other"}}},
			Constraints: []catalog.Constraint{{Name: "t_pk", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}}},
			Table:       catalog.Table{Columns: []catalog.Column{{Name: "g", GeneratedExpression: new(`UPPER("OTHER")`)}}},
		})},
		{name: "a column whose name is part of a longer one in a generated column", diff: collationChange(schemacapture.TableObservation{
			Table: catalog.Table{Columns: []catalog.Column{{Name: "g", GeneratedExpression: new(`UPPER("CC") || C_2`)}}},
		})},
		{name: "a function-based index over another column", diff: collationChange(schemacapture.TableObservation{
			Indexes: []catalog.Index{{Name: "T_UPPER_IDX", Parts: []catalog.IndexPart{{Expr: `UPPER("CC")`}}}},
		})},
		{name: "a diff that changes something else", diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
			TableName: "t",
			Current:   schemacapture.TableObservation{Indexes: []catalog.Index{{Name: "t_c", Columns: []string{"c"}}}},
			ColumnsModified: []difftypes.ColumnDiff{{
				ColumnName: "c", Changes: map[string]string{"type": "VARCHAR2(20) -> VARCHAR2(40)"},
			}},
		}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oraclecollation.RefuseKeyedColumnChange(tc.diff), qt.IsNil)
		})
	}
}

// TestRefuseKeyedColumnChange_FailurePath names the object that holds the
// column. Each row is a statement Oracle Free 23.26 refused with ORA-43923 or
// ORA-43922 while the plan would have run earlier statements first.
func TestRefuseKeyedColumnChange_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		observed schemacapture.TableObservation
		want     string
	}{
		{
			name:     "an index",
			observed: schemacapture.TableObservation{Indexes: []catalog.Index{{Name: "t_c_idx", Columns: []string{"x", "C"}}}},
			want:     `.*collation of column "c" of table "t" \(USING_NLS_COMP -> BINARY_CI\).*index "t_c_idx" uses the column \(ORA-43923 or ORA-43922\).*`,
		},
		{
			name: "a primary key",
			observed: schemacapture.TableObservation{Constraints: []catalog.Constraint{
				{Name: "t_pk", Type: "PRIMARY KEY", ColumnName: "c", ColumnNames: []string{"c"}},
			}},
			want: `.*primary key constraint "t_pk" uses the column.*`,
		},
		{
			name: "a unique key over two columns",
			observed: schemacapture.TableObservation{Constraints: []catalog.Constraint{
				{Name: "t_uq", Type: "UNIQUE", ColumnName: "a", ColumnNames: []string{"a", "c"}},
			}},
			want: `.*unique constraint "t_uq" uses the column.*`,
		},
		{
			name: "a foreign key",
			observed: schemacapture.TableObservation{Constraints: []catalog.Constraint{
				{Name: "t_fk", Type: "FOREIGN KEY", ColumnName: "c"},
			}},
			want: `.*foreign key constraint "t_fk" uses the column.*`,
		},
		{
			name: "a check",
			observed: schemacapture.TableObservation{Constraints: []catalog.Constraint{
				{Name: "t_ck", Type: "CHECK", ColumnNames: []string{"c"}},
			}},
			want: `.*check constraint "t_ck" uses the column.*`,
		},
		{
			name: "a function-based index",
			observed: schemacapture.TableObservation{Indexes: []catalog.Index{{
				Name: "T_UPPER_IDX", Columns: []string{`UPPER("C")`}, Parts: []catalog.IndexPart{{Expr: `UPPER("C")`}},
			}}},
			want: `.*index "T_UPPER_IDX" uses the column.*`,
		},
		{
			name: "a function-based index over two columns",
			observed: schemacapture.TableObservation{Indexes: []catalog.Index{{
				Name: "T_CAT_IDX", Parts: []catalog.IndexPart{{Name: "A"}, {Expr: `"A"||"C"`}},
			}}},
			want: `.*index "T_CAT_IDX" uses the column.*`,
		},
		{
			name: "a primary key Oracle named itself, carried on the column",
			observed: schemacapture.TableObservation{Table: catalog.Table{Columns: []catalog.Column{
				{Name: "c", IsPrimaryKey: true},
			}}},
			want: `.*the primary key uses the column.*`,
		},
		{
			name: "a unique key Oracle named itself, carried on the column",
			observed: schemacapture.TableObservation{Table: catalog.Table{Columns: []catalog.Column{
				{Name: "C", IsUnique: true},
			}}},
			want: `.*a unique key uses the column.*`,
		},
		{
			name: "a generated column",
			observed: schemacapture.TableObservation{Table: catalog.Table{Columns: []catalog.Column{
				{Name: "g", GeneratedExpression: new(`UPPER("C")`)},
			}}},
			want: `.*the generated column "g" uses the column.*`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			err := oraclecollation.RefuseKeyedColumnChange(collationChange(tc.observed))
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, tc.want)
			c.Assert(err, qt.ErrorMatches, `.*drop it, apply the collation change, and declare it again in a later migration`)
		})
	}
}

// TestRefuseKeyedColumnChange_FailurePathNamesAnIndexTheSameDiffDrops covers
// the declaration an author writes first: the index is removed and the
// collation changed in one file. The plan drops the index after it changes
// the column, so the refusal says to apply the two apart, in the other order.
func TestRefuseKeyedColumnChange_FailurePathNamesAnIndexTheSameDiffDrops(t *testing.T) {
	c := qt.New(t)
	diff := collationChange(schemacapture.TableObservation{
		Indexes: []catalog.Index{{Name: "T_C_IDX", Columns: []string{"C"}}},
	})
	diff.IndexesRemoved = []difftypes.IndexRef{{Name: "t_c_idx", TableName: "T"}}
	diff.TablesModified[0].TableName = "t"

	err := oraclecollation.RefuseKeyedColumnChange(diff)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*the same diff drops the index, but a plan changes columns before it drops indexes.*`)
}

// TestRefuseKeyedColumnChange_FailurePathNamesTheCollationTheColumnHolds prints
// the collation the column compares with where the declaration inherited it: an
// empty side of the change reads as the collation Oracle gives a column that
// names none, and a column that takes its table's default reads as that default.
func TestRefuseKeyedColumnChange_FailurePathNamesTheCollationTheColumnHolds(t *testing.T) {
	tests := []struct {
		name   string
		column catalog.Column
		change string
		want   string
	}{
		{name: "an inherited collation", column: catalog.Column{Name: "c", IsUnique: true}, change: " -> BINARY_CI", want: `.*\(USING_NLS_COMP -> BINARY_CI\).*`},
		{name: "the table default", column: catalog.Column{Name: "c", IsUnique: true, EffectiveCollate: "BINARY_CI"}, change: "BINARY_CI -> BINARY", want: `.*\(BINARY_CI -> BINARY\).*`},
		{name: "a declared collation", column: catalog.Column{Name: "c", IsUnique: true, Collate: "BINARY_AI", EffectiveCollate: "BINARY_AI"}, change: "BINARY_AI -> ", want: `.*\(BINARY_AI -> USING_NLS_COMP\).*`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			diff := collationChange(schemacapture.TableObservation{Table: catalog.Table{Columns: []catalog.Column{tc.column}}})
			diff.TablesModified[0].ColumnsModified[0].Changes["collation"] = tc.change

			err := oraclecollation.RefuseKeyedColumnChange(diff)

			c.Assert(err, qt.ErrorMatches, tc.want)
		})
	}
}

// TestRefuseKeyedColumnChange_FailurePathNamesAHolderTheSameDiffAdds covers a
// generated column or an inline CHECK the plan adds before it changes the
// column: Oracle answers ORA-43922 to the change once they stand, and the add
// has committed by then.
func TestRefuseKeyedColumnChange_FailurePathNamesAHolderTheSameDiffAdds(t *testing.T) {
	tests := []struct {
		name  string
		added schemamodel.Field
		want  string
	}{
		{name: "a generated column", added: schemamodel.Field{Name: "g", GeneratedExpression: "UPPER(c)"},
			want: `.*the generated column "g" the same diff adds uses the column.*`},
		{name: "a generated column that quotes the name", added: schemamodel.Field{Name: "g", GeneratedExpression: `UPPER("C")`},
			want: `.*the generated column "g" the same diff adds uses the column.*`},
		{name: "an inline CHECK", added: schemamodel.Field{Name: "x", Check: "c <> 'a'"},
			want: `.*the CHECK of the column "x" the same diff adds uses the column.*`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			diff := collationChange(schemacapture.TableObservation{})
			diff.TablesModified[0].ColumnsAdded = difftypes.ColumnChanges{tc.added}

			err := oraclecollation.RefuseKeyedColumnChange(diff)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, tc.want)
			c.Assert(err, qt.ErrorMatches, `.*apply the collation change on its own first and add it in a later migration`)
		})
	}
}

// TestRefuseKeyedColumnChange_HappyPathLeavesAnAddedColumnThatNamesAnother is
// the control: a column the diff adds that does not name the column holds
// nothing.
func TestRefuseKeyedColumnChange_HappyPathLeavesAnAddedColumnThatNamesAnother(t *testing.T) {
	c := qt.New(t)
	diff := collationChange(schemacapture.TableObservation{})
	diff.TablesModified[0].ColumnsAdded = difftypes.ColumnChanges{
		{Name: "g", GeneratedExpression: "UPPER(other)"},
		{Name: "x", Check: "cc > 0"},
		{Name: "plain"},
	}

	c.Assert(oraclecollation.RefuseKeyedColumnChange(diff), qt.IsNil)
}

// TestRefuseKeyedColumnChange_HappyPathReadsIdentifiersOnly holds the match to
// identifier tokens: a column name inside a string literal or a comment, or a
// quoted identifier that differs in case from the column, names nothing.
func TestRefuseKeyedColumnChange_HappyPathReadsIdentifiersOnly(t *testing.T) {
	tests := []struct {
		name       string
		column     string
		expression string
	}{
		{name: "a string literal", column: "a", expression: `UPPER(ab) || 'a'`},
		{name: "a literal with a doubled quote", column: "a", expression: `ab || 'it''s a'`},
		{name: "a literal that is the whole expression", column: "c", expression: `'c'`},
		{name: "an alternative-quoted literal", column: "a", expression: `UPPER(ab) || q'[a it's a]'`},
		{name: "a national alternative-quoted literal", column: "a", expression: `NQ'{a it's a}' || ab`},
		{name: "a block comment", column: "a", expression: `UPPER(ab) /* a */`},
		{name: "a line comment", column: "a", expression: "UPPER(ab) -- a\n"},
		{name: "a quoted identifier of another case", column: "abc", expression: `UPPER("Abc")`},
		{name: "a longer quoted identifier", column: "a", expression: `UPPER("A_B")`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			column := tc.column
			diff := collationChange(schemacapture.TableObservation{Table: catalog.Table{Columns: []catalog.Column{
				{Name: "g", GeneratedExpression: &tc.expression},
			}}})
			diff.TablesModified[0].ColumnsModified[0].ColumnName = column

			c.Assert(oraclecollation.RefuseKeyedColumnChange(diff), qt.IsNil)
		})
	}
}

// TestRefuseKeyedColumnChange_FailurePathReadsQuotedIdentifiers is the control:
// the same expressions with the column as an identifier, quoted or not, hold it.
// A quoted identifier names the column in the spelling the declaration gives or
// in the upper-case form Oracle stores an unquoted name as.
func TestRefuseKeyedColumnChange_FailurePathReadsQuotedIdentifiers(t *testing.T) {
	tests := []struct {
		name       string
		column     string
		expression string
	}{
		{name: "an unquoted identifier beside a literal", column: "a", expression: `UPPER(a) || 'a'`},
		{name: "a quoted upper-case identifier", column: "a", expression: `UPPER("A")`},
		{name: "a quoted identifier in the declared spelling", column: "abc", expression: `UPPER("abc")`},
		{name: "a quoted identifier with a doubled quote", column: `a"b`, expression: `UPPER("a""b")`},
		{name: "a qualified name", column: "c", expression: `UPPER(t.c)`},
		{name: "a name after a comment", column: "c", expression: "/* c */ UPPER(c)"},
		{name: "a name after an apostrophe inside an alternative-quoted literal", column: "a", expression: `q'[it's]' || a`},
		{name: "a name after a national alternative-quoted literal", column: "a", expression: `NQ'[it's]' || a`},
		{name: "a name after a literal with another delimiter", column: "a", expression: `q'{it's}' || UPPER("A")`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			diff := collationChange(schemacapture.TableObservation{Table: catalog.Table{Columns: []catalog.Column{
				{Name: "g", GeneratedExpression: &tc.expression},
			}}})
			diff.TablesModified[0].ColumnsModified[0].ColumnName = tc.column

			err := oraclecollation.RefuseKeyedColumnChange(diff)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*the generated column "g" uses the column.*`)
		})
	}
}
