package atlasfilter_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasfilter"
)

// An exclusion names a primary key, a UNIQUE constraint and a CHECK
// constraint through their table, under the types the community binary names
// them by and under `constraint`. The fixture is the table public.kinds with
// the key kinds_pkey, the unique constraint kinds_code_key and the check
// code_len, on both sides of a comparison. The database side also holds the
// two indexes the server builds for the key and the unique constraint, under
// their names.
//
// integration/constraint_selector_e2e_test.go runs the same selectors through
// the shipped binaries against PostgreSQL.

func observedSelectorConstraints() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Schema: "public", Name: "kinds", Columns: []catalog.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "code", IsUnique: true},
		}}},
		Indexes: []catalog.Index{
			{Schema: "public", TableName: "kinds", Name: "kinds_pkey", Columns: []string{"id"}, IsPrimary: true, IsUnique: true},
			{Schema: "public", TableName: "kinds", Name: "kinds_code_key", Columns: []string{"code"}, IsUnique: true},
		},
		Constraints: []catalog.Constraint{
			{Schema: "public", TableName: "kinds", Name: "kinds_pkey", Type: "PRIMARY KEY", ColumnNames: []string{"id"}},
			{Schema: "public", TableName: "kinds", Name: "kinds_code_key", Type: "UNIQUE", ColumnNames: []string{"code"}},
			{Schema: "public", TableName: "kinds", Name: "code_len", Type: "CHECK", ColumnNames: []string{"code"}},
		},
	}
}

func desiredSelectorConstraints() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Schema: "public", Name: "kinds", StructName: "Kinds"}},
		Constraints: []schemamodel.Constraint{
			{StructName: "Kinds", Table: "kinds", Name: "kinds_pkey", Type: "PRIMARY KEY", Columns: []string{"id"}},
			{StructName: "Kinds", Table: "kinds", Name: "kinds_code_key", Type: "UNIQUE", Columns: []string{"code"}},
			{StructName: "Kinds", Table: "kinds", Name: "code_len", Type: "CHECK", Columns: []string{"code"}, CheckExpression: "length(code) > 0"},
		},
	}
}

func TestExcludeNamesAConstraintByItsType_HappyPath(t *testing.T) {
	tests := []struct {
		name, exclude string
		// want are the constraints both sides keep, and wantIndexes the
		// indexes the database side keeps. An index leaves with the
		// constraint it enforces, whichever type named the constraint.
		want, wantIndexes []string
		// wantFlags are the primary-key and unique flags of id and code on
		// the database side. Each flag leaves with its constraint.
		wantFlags [2]bool
	}{
		{
			name:        "a unique constraint as an index",
			exclude:     "public.kinds.kinds_code_key[type=index]",
			want:        []string{"kinds_pkey", "code_len"},
			wantIndexes: []string{"kinds_pkey"},
			wantFlags:   [2]bool{true, false},
		},
		{
			name:        "every index of the table",
			exclude:     "public.kinds.*[type=index]",
			want:        []string{"code_len"},
			wantIndexes: make([]string, 0),
			wantFlags:   [2]bool{false, false},
		},
		{
			name:        "a unique constraint as a constraint",
			exclude:     "public.kinds.kinds_code_key[type=constraint]",
			want:        []string{"kinds_pkey", "code_len"},
			wantIndexes: []string{"kinds_pkey"},
			wantFlags:   [2]bool{true, false},
		},
		{
			name:        "a unique constraint with no type",
			exclude:     "public.kinds.kinds_code_key",
			want:        []string{"kinds_pkey", "code_len"},
			wantIndexes: []string{"kinds_pkey"},
			wantFlags:   [2]bool{true, false},
		},
		{
			name:        "a unique constraint through its table",
			exclude:     "kinds.kinds_code_key[type=constraint]",
			want:        []string{"kinds_pkey", "code_len"},
			wantIndexes: []string{"kinds_pkey"},
			wantFlags:   [2]bool{true, false},
		},
		{
			name:        "a primary key as an index",
			exclude:     "public.kinds.kinds_pkey[type=index]",
			want:        []string{"kinds_code_key", "code_len"},
			wantIndexes: []string{"kinds_code_key"},
			wantFlags:   [2]bool{false, true},
		},
		{
			name:        "a primary key as a constraint",
			exclude:     "public.kinds.kinds_pkey[type=constraint]",
			want:        []string{"kinds_code_key", "code_len"},
			wantIndexes: []string{"kinds_code_key"},
			wantFlags:   [2]bool{false, true},
		},
		{
			name:        "a check constraint as a check",
			exclude:     "public.kinds.code_len[type=check]",
			want:        []string{"kinds_pkey", "kinds_code_key"},
			wantIndexes: []string{"kinds_pkey", "kinds_code_key"},
			wantFlags:   [2]bool{true, true},
		},
		{
			name:        "a check constraint as a constraint",
			exclude:     "public.kinds.code_len[type=constraint]",
			want:        []string{"kinds_pkey", "kinds_code_key"},
			wantIndexes: []string{"kinds_pkey", "kinds_code_key"},
			wantFlags:   [2]bool{true, true},
		},
		{
			name:        "every check of the table",
			exclude:     "public.kinds.*[type=check]",
			want:        []string{"kinds_pkey", "kinds_code_key"},
			wantIndexes: []string{"kinds_pkey", "kinds_code_key"},
			wantFlags:   [2]bool{true, true},
		},
		{
			name:        "every check",
			exclude:     "*[type=check]",
			want:        []string{"kinds_pkey", "kinds_code_key"},
			wantIndexes: []string{"kinds_pkey", "kinds_code_key"},
			wantFlags:   [2]bool{true, true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			current, currentReport, currentErr := atlasfilter.ExcludeDatabaseScopeReport(observedSelectorConstraints(), realmScope(test.exclude))
			declared, declaredReport, declaredErr := atlasfilter.ExcludeGeneratedScopeReport(desiredSelectorConstraints(), realmScope(test.exclude))

			c.Assert(currentErr, qt.IsNil)
			c.Assert(declaredErr, qt.IsNil)
			c.Assert(constraintNames(current.Constraints), qt.DeepEquals, test.want)
			c.Assert(generatedConstraintNames(declared.Constraints), qt.DeepEquals, test.want)
			c.Assert(indexNames(current.Indexes), qt.DeepEquals, test.wantIndexes)
			c.Assert(current.Tables, qt.HasLen, 1)
			c.Assert([2]bool{current.Tables[0].Columns[0].IsPrimaryKey, current.Tables[0].Columns[1].IsUnique}, qt.Equals, test.wantFlags)
			c.Assert(declared.Tables, qt.HasLen, 1)
			c.Assert(currentReport.Unmatched, qt.IsNil)
			c.Assert(declaredReport.Unmatched, qt.IsNil)
		})
	}
}

// TestExcludeNamesAConstraintByItsType_FailurePath names each constraint
// under a type that is not one of its own. None names a constraint, so both
// sides keep all three with their indexes, and the report says the selector
// matched nothing. `[type=unique]` is no type the community binary names a
// constraint by either: it accepts the selector and plans the DROP.
// A declaration's primary key is on its table or its field, not in the
// constraint list, and it often carries no name. A selector names it by the
// name it was given, by the name PostgreSQL gives an unnamed key, or by
// `PRIMARY`, the MySQL family's name for every key. Named, the key leaves the
// table and its field, and the column stays, NOT NULL where the key made it
// so: on PostgreSQL and on a dialect nothing describes, and not on SQLite.
func TestExcludeNamesADeclaredPrimaryKey_HappyPath(t *testing.T) {
	tests := []struct {
		name, exclude, dialect string
		table                  schemamodel.Table
		primaryField           bool
		// wantKey is the table's key columns after the exclusion, joined,
		// and wantPrimary and wantNullable the id field's flags. Every row
		// declares id without NOT NULL.
		wantKey                   string
		wantPrimary, wantNullable bool
	}{
		{
			name: "an unnamed table key by the PostgreSQL name", exclude: "public.kinds.kinds_pkey[type=index]", dialect: "postgres",
			table: schemamodel.Table{PrimaryKey: []string{"id"}}, primaryField: true,
		},
		{
			name: "an unnamed table key as a constraint", exclude: "public.kinds.kinds_pkey[type=constraint]", dialect: "postgres",
			table: schemamodel.Table{PrimaryKey: []string{"id"}},
		},
		{
			name: "a named table key by its name", exclude: "kinds.c_pk", dialect: "postgres",
			table: schemamodel.Table{PrimaryKey: []string{"id"}, PrimaryKeyName: "c_pk"}, primaryField: true,
		},
		{
			name: "a named table key by the MySQL name", exclude: "kinds.PRIMARY[type=index]", dialect: "mysql",
			table: schemamodel.Table{PrimaryKey: []string{"id"}, PrimaryKeyName: "c_pk"}, primaryField: true,
		},
		{
			name: "a column key by the PostgreSQL name", exclude: "public.kinds.kinds_pkey", dialect: "postgres",
			primaryField: true,
		},
		{
			name: "a column key on a dialect nothing describes", exclude: "public.kinds.kinds_pkey",
			primaryField: true,
		},
		{
			name: "a column key on SQLite, where a key column may hold NULL", exclude: "public.kinds.kinds_pkey", dialect: "sqlite",
			primaryField: true, wantNullable: true,
		},
		{
			name: "another constraint keeps the key", exclude: "public.kinds.kinds_code_key", dialect: "postgres",
			table:   schemamodel.Table{PrimaryKey: []string{"id"}},
			wantKey: "id", primaryField: true, wantPrimary: true, wantNullable: true,
		},
		{
			name: "a named key keeps its name from the PostgreSQL default", exclude: "kinds.kinds_pkey", dialect: "postgres",
			table:   schemamodel.Table{PrimaryKey: []string{"id"}, PrimaryKeyName: "c_pk"},
			wantKey: "id", primaryField: true, wantPrimary: true, wantNullable: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := test.table
			table.Schema, table.Name, table.StructName = "public", "kinds", "Kinds"
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{table},
				Fields: []schemamodel.Field{
					{StructName: "Kinds", Name: "id", Type: "int", Primary: test.primaryField, Nullable: true},
					{StructName: "Kinds", Name: "code", Type: "text", Unique: true},
				},
			}
			scope := realmScope(test.exclude)
			scope.Dialect = test.dialect

			declared, _, err := atlasfilter.ExcludeGeneratedScopeReport(desired, scope)

			c.Assert(err, qt.IsNil)
			c.Assert(declared.Tables, qt.HasLen, 1)
			c.Assert(strings.Join(declared.Tables[0].PrimaryKey, ","), qt.Equals, test.wantKey)
			c.Assert(declared.Fields, qt.HasLen, 2)
			c.Assert(declared.Fields[0].Primary, qt.Equals, test.wantPrimary)
			c.Assert(declared.Fields[0].Nullable, qt.Equals, test.wantNullable)
		})
	}
}

// A selector naming the key of a table that declares none names nothing, and
// the report says so.
func TestExcludeNamesADeclaredPrimaryKey_FailurePath(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Schema: "public", Name: "kinds", StructName: "Kinds"}},
		Fields: []schemamodel.Field{{StructName: "Kinds", Name: "id", Type: "int"}},
	}

	declared, report, err := atlasfilter.ExcludeGeneratedScopeReport(desired, realmScope("public.kinds.kinds_pkey"))

	c.Assert(err, qt.IsNil)
	c.Assert(declared.Fields, qt.HasLen, 1)
	c.Assert(report.Unmatched, qt.DeepEquals, []string{"public.kinds.kinds_pkey"})
}

func TestExcludeNamesAConstraintByItsType_FailurePath(t *testing.T) {
	tests := []struct {
		name, exclude string
	}{
		{name: "a check as an index", exclude: "public.kinds.code_len[type=index]"},
		{name: "a unique constraint as a check", exclude: "public.kinds.kinds_code_key[type=check]"},
		{name: "a primary key as a check", exclude: "public.kinds.kinds_pkey[type=check]"},
		{name: "a unique constraint as a foreign key", exclude: "public.kinds.kinds_code_key[type=foreign_key]"},
		{name: "a type no binary names", exclude: "public.kinds.kinds_code_key[type=unique]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			current, currentReport, currentErr := atlasfilter.ExcludeDatabaseScopeReport(observedSelectorConstraints(), realmScope(test.exclude))
			declared, declaredReport, declaredErr := atlasfilter.ExcludeGeneratedScopeReport(desiredSelectorConstraints(), realmScope(test.exclude))

			c.Assert(currentErr, qt.IsNil)
			c.Assert(declaredErr, qt.IsNil)
			c.Assert(constraintNames(current.Constraints), qt.DeepEquals, []string{"kinds_pkey", "kinds_code_key", "code_len"})
			c.Assert(generatedConstraintNames(declared.Constraints), qt.DeepEquals, []string{"kinds_pkey", "kinds_code_key", "code_len"})
			c.Assert(indexNames(current.Indexes), qt.DeepEquals, []string{"kinds_pkey", "kinds_code_key"})
			c.Assert(currentReport.Unmatched, qt.DeepEquals, []string{test.exclude})
			c.Assert(declaredReport.Unmatched, qt.DeepEquals, []string{test.exclude})
		})
	}
}

// TestExcludeRemovesTheIndexOfAnExcludedConstraint holds an index to the
// constraint it enforces on each side. A selector that names the constraint
// and not the index removes both, and an index that only shares the name, on
// another table, is no index of the constraint and stays.
func TestExcludeRemovesTheIndexOfAnExcludedConstraint(t *testing.T) {
	c := qt.New(t)
	observed := observedSelectorConstraints()
	observed.Tables = append(observed.Tables, catalog.Table{Schema: "public", Name: "sorts"})
	observed.Indexes = append(observed.Indexes, catalog.Index{Schema: "public", TableName: "sorts", Name: "kinds_code_key", IsUnique: true})
	desired := desiredSelectorConstraints()
	desired.Tables = append(desired.Tables, schemamodel.Table{Schema: "public", Name: "sorts", StructName: "Sorts"})
	desired.Indexes = []schemamodel.Index{
		{StructName: "Kinds", TableName: "kinds", Name: "kinds_code_key", Fields: []string{"code"}, Unique: true},
		{StructName: "Sorts", TableName: "sorts", Name: "kinds_code_key", Fields: []string{"code"}, Unique: true},
	}

	current, _, currentErr := atlasfilter.ExcludeDatabaseScopeReport(observed, realmScope("public.kinds.kinds_code_key[type=constraint]"))
	declared, _, declaredErr := atlasfilter.ExcludeGeneratedScopeReport(desired, realmScope("public.kinds.kinds_code_key[type=constraint]"))

	c.Assert(currentErr, qt.IsNil)
	c.Assert(declaredErr, qt.IsNil)
	c.Assert(indexTables(current.Indexes), qt.DeepEquals, []string{"kinds.kinds_pkey", "sorts.kinds_code_key"})
	c.Assert(generatedIndexTables(declared.Indexes), qt.DeepEquals, []string{"Sorts.kinds_code_key"})
	c.Assert(constraintNames(current.Constraints), qt.DeepEquals, []string{"kinds_pkey", "code_len"})
	c.Assert(generatedConstraintNames(declared.Constraints), qt.DeepEquals, []string{"kinds_pkey", "code_len"})
}

func indexTables(indexes []catalog.Index) []string {
	names := make([]string, 0, len(indexes))
	for _, index := range indexes {
		names = append(names, index.TableName+"."+index.Name)
	}
	return names
}

// generatedIndexTables names each declared index through the struct that
// declares it.
func generatedIndexTables(indexes []schemamodel.Index) []string {
	names := make([]string, 0, len(indexes))
	for _, index := range indexes {
		names = append(names, index.StructName+"."+index.Name)
	}
	return names
}

// TestExcludeNamesAColumnUniqueByTheServersName declares UNIQUE on the column
// code, with no name. The server names it kinds_code_key, as PostgreSQL does,
// or code, as the MySQL family does, and a selector naming it by either name
// under a type a UNIQUE constraint answers takes the declaration out. A
// selector of another type, or another name, leaves it.
func TestExcludeNamesAColumnUniqueByTheServersName(t *testing.T) {
	tests := []struct {
		name, exclude string
		wantUnique    bool
	}{
		{name: "the PostgreSQL name as an index", exclude: "public.kinds.kinds_code_key[type=index]"},
		{name: "the PostgreSQL name as a constraint", exclude: "public.kinds.kinds_code_key[type=constraint]"},
		{name: "the PostgreSQL name with no type", exclude: "kinds.kinds_code_key"},
		{name: "the MySQL name as an index", exclude: "kinds.code[type=index]"},
		{name: "the PostgreSQL name as a check", exclude: "public.kinds.kinds_code_key[type=check]", wantUnique: true},
		{name: "another name", exclude: "public.kinds.kinds_name_key[type=index]", wantUnique: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{Schema: "public", Name: "kinds", StructName: "Kinds"}},
				Fields: []schemamodel.Field{
					{StructName: "Kinds", Name: "id", Type: "int", Primary: true},
					{StructName: "Kinds", Name: "code", Type: "text", Unique: true},
				},
			}

			declared, _, err := atlasfilter.ExcludeGeneratedScopeReport(desired, realmScope(test.exclude))

			c.Assert(err, qt.IsNil)
			c.Assert(declared.Fields, qt.HasLen, 2)
			c.Assert(declared.Fields[1].Unique, qt.Equals, test.wantUnique)
			c.Assert(desired.Fields[1].Unique, qt.IsTrue)
		})
	}
}
