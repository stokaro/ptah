package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// spelledDatabase writes every kind RespellReadNames respells once in
// database db, and keeps a table and a grant in database other beside them.
func spelledDatabase(db, other string) *schemamodel.Database {
	return &schemamodel.Database{
		Schemas:           []schemamodel.Schema{{Name: db}},
		Tables:            []schemamodel.Table{{Name: "items", Schema: db}, {Name: "kept", Schema: other}, {Name: "plain"}},
		Sequences:         []schemamodel.Sequence{{Name: "s", Schema: db}, {Name: db + ".q"}},
		Enums:             []schemamodel.Enum{{Name: "mood", Schema: db}, {Name: db + ".shade"}},
		Functions:         []schemamodel.Function{{Name: db + ".f"}},
		Views:             []schemamodel.View{{Name: db + ".v"}, {Name: db + `."a.b"`}},
		MaterializedViews: []schemamodel.MaterializedView{{Name: db + ".mv"}},
		Fields:            []schemamodel.Field{{Name: "item_id", Foreign: db + ".items(id)"}},
		EmbeddedFields:    []schemamodel.EmbeddedField{{Ref: db + ".items"}},
		Indexes:           []schemamodel.Index{{Name: "i", TableName: db + ".items"}},
		Constraints:       []schemamodel.Constraint{{Name: "fk", Table: db + ".other", ForeignTable: db + ".items"}},
		Triggers:          []schemamodel.Trigger{{Name: "tr", Table: db + ".items", ExecuteFunction: db + ".f()"}},
		Grants: []schemamodel.Grant{
			{Role: "r", Privileges: []string{"SELECT"}, OnTable: db + ".items"},
			{Role: "r", Privileges: []string{"SELECT"}, OnSchema: db},
			{Role: "r", Privileges: []string{"EXECUTE"}, OnRoutine: db + ".f"},
			{Role: "r", Privileges: []string{"USAGE"}, OnSequence: db + ".s"},
			{Role: "r", Privileges: []string{"SELECT"}, OnTable: other + ".items"},
		},
		RevokedGrants:              []schemamodel.Grant{{Role: "r", Privileges: []string{"SELECT"}, OnTable: db + ".items"}},
		ManagedData:                []schemamodel.ManagedData{{Table: "items", Schema: db}, {Table: db + ".other"}},
		Dependencies:               map[string][]string{db + ".other": {db + ".items"}},
		FunctionDependencies:       map[string][]string{db + ".f": {db + ".g"}},
		SelfReferencingForeignKeys: map[string][]schemamodel.SelfReferencingFK{db + ".items": {{Foreign: db + ".items(id)"}}},
	}
}

// spelledTable writes every reference to a table once, as table qualified
// with db, or alone where db is empty, beside the table it declares.
func spelledTable(db, table string) *schemamodel.Database {
	name := table
	if db != "" {
		name = db + "." + table
	}
	return &schemamodel.Database{
		Tables:         []schemamodel.Table{{Name: table, Schema: db}, {Name: "other", Schema: db}},
		Fields:         []schemamodel.Field{{Name: "item_id", Foreign: name + "(id)"}},
		EmbeddedFields: []schemamodel.EmbeddedField{{Ref: name}},
		Indexes:        []schemamodel.Index{{Name: "i", TableName: name}},
		Constraints:    []schemamodel.Constraint{{Name: "fk", Table: "other", ForeignTable: name}},
		Triggers:       []schemamodel.Trigger{{Name: "tr", Table: name}},
		Grants:         []schemamodel.Grant{{Role: "r", Privileges: []string{"SELECT"}, OnTable: name}},
		RevokedGrants:  []schemamodel.Grant{{Role: "r", Privileges: []string{"INSERT"}, OnTable: name}},
		ManagedData:    []schemamodel.ManagedData{{Table: name}},
		Dependencies:   map[string][]string{"other": {name}},
		SelfReferencingForeignKeys: map[string][]schemamodel.SelfReferencingFK{
			name: {{Foreign: name + "(id)"}},
		},
	}
}

// On a server that folds names, every name the document qualifies with another
// case of a database the read found is qualified with the read's spelling. A
// name in a database the read did not find, and an unqualified one, keep what
// they were written with, and the input is not modified.
func TestRespellReadNames_HappyPath(t *testing.T) {
	tests := []struct {
		name              string
		read              schemaprep.ReadNames
		written, other    string // the databases the document names
		wantDB, wantOther string // the databases after the step
	}{
		{
			name: "upper case against a folded session", read: schemaprep.ReadNames{Session: "fa"},
			written: "FA", other: "FB", wantDB: "fa", wantOther: "FB",
		},
		{
			name: "mixed case against a folded session", read: schemaprep.ReadNames{Session: "fa"},
			written: "Fa", other: "FB", wantDB: "fa", wantOther: "FB",
		},
		{
			name: "the folded name against a session that kept its spelling", read: schemaprep.ReadNames{Session: "R_LC"},
			written: "r_lc", other: "FB", wantDB: "R_LC", wantOther: "FB",
		},
		{
			name: "the session's own spelling", read: schemaprep.ReadNames{Session: "fa"},
			written: "fa", other: "FB", wantDB: "fa", wantOther: "FB",
		},
		{
			name: "every database a whole server holds", read: schemaprep.ReadNames{Databases: []string{"fa", "fb"}},
			written: "FA", other: "Fb", wantDB: "fa", wantOther: "fb",
		},
		{
			name: "a whole server that lacks one of them", read: schemaprep.ReadNames{Databases: []string{"fa", "fc"}},
			written: "FA", other: "FB", wantDB: "fa", wantOther: "FB",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := spelledDatabase(test.written, test.other)

			got, err := schemaprep.RespellReadNames(database, test.read, catalog.NameFoldingFolds)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, spelledDatabase(test.wantDB, test.wantOther))
			c.Assert(database, qt.DeepEquals, spelledDatabase(test.written, test.other))
		})
	}
}

// On a server that folds names, a table the document declares or references in
// another case than the read found it takes the read's spelling, in the
// session's database and in a database the name qualifies, and a reference
// takes the spelling of a table the document declares.
func TestRespellReadNames_Tables(t *testing.T) {
	tests := []struct {
		name                   string
		read                   schemaprep.ReadNames
		writtenDB, writtenName string
		wantDB, wantName       string
	}{
		{
			name:      "a table of the session's database",
			read:      schemaprep.ReadNames{Session: "fa", Relations: []schemaprep.ReadRelation{{Name: "items"}}},
			writtenDB: "", writtenName: "Items", wantDB: "", wantName: "items",
		},
		{
			name:      "a table the read qualifies with the session's database",
			read:      schemaprep.ReadNames{Session: "fa", Relations: []schemaprep.ReadRelation{{Database: "fa", Name: "items"}}},
			writtenDB: "", writtenName: "ITEMS", wantDB: "", wantName: "items",
		},
		{
			name:      "a table and its database in another case",
			read:      schemaprep.ReadNames{Session: "fa", Relations: []schemaprep.ReadRelation{{Name: "items"}}},
			writtenDB: "FA", writtenName: "Items", wantDB: "fa", wantName: "items",
		},
		{
			name: "a table of a whole server",
			read: schemaprep.ReadNames{
				Databases: []string{"fa", "fb"},
				Relations: []schemaprep.ReadRelation{{Database: "fa", Name: "items"}, {Database: "fb", Name: "Items"}},
			},
			writtenDB: "Fb", writtenName: "ITEMS", wantDB: "fb", wantName: "Items",
		},
		{
			name:      "a table the read did not find",
			read:      schemaprep.ReadNames{Session: "fa", Relations: []schemaprep.ReadRelation{{Name: "orders"}}},
			writtenDB: "", writtenName: "Items", wantDB: "", wantName: "Items",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := spelledTable(test.writtenDB, test.writtenName)

			got, err := schemaprep.RespellReadNames(database, test.read, catalog.NameFoldingFolds)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, spelledTable(test.wantDB, test.wantName))
			c.Assert(database, qt.DeepEquals, spelledTable(test.writtenDB, test.writtenName))
		})
	}
}

// A reference that spells a table the document declares in another case takes
// the declared spelling, so a first apply on a server that folds names finds
// the table its foreign key names.
func TestRespellReadNames_ReferenceToADeclaredTable(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables:      []schemamodel.Table{{Name: "items"}, {Name: "other"}},
		Constraints: []schemamodel.Constraint{{Name: "fk", Table: "other", ForeignTable: "Items"}},
		Grants:      []schemamodel.Grant{{Role: "r", Privileges: []string{"SELECT"}, OnTable: "ITEMS"}},
	}

	got, err := schemaprep.RespellReadNames(database, schemaprep.ReadNames{Session: "fa"}, catalog.NameFoldingFolds)

	c.Assert(err, qt.IsNil)
	c.Assert(got.Constraints[0].ForeignTable, qt.Equals, "items")
	c.Assert(got.Grants[0].OnTable, qt.Equals, "items")
}

// Nothing is respelled where the server keeps names apart by case, where the
// read names no database, and where the document names databases the read did
// not find.
func TestRespellReadNames_KeepsWhatItCannotRespell(t *testing.T) {
	tests := []struct {
		name    string
		read    schemaprep.ReadNames
		folding catalog.NameFolding
		written string
	}{
		{
			name: "a session on a server that keeps case apart", read: schemaprep.ReadNames{Session: "fa"},
			folding: catalog.NameFoldingExact, written: "FA",
		},
		{
			name: "a whole server that keeps case apart", read: schemaprep.ReadNames{Databases: []string{"fa", "fc"}},
			folding: catalog.NameFoldingExact, written: "FA",
		},
		{
			name:    "a table on a server that keeps case apart",
			read:    schemaprep.ReadNames{Session: "fa", Relations: []schemaprep.ReadRelation{{Name: "items"}}},
			folding: catalog.NameFoldingExact, written: "Items",
		},
		{name: "no database read", folding: catalog.NameFoldingFolds, written: "FA"},
		{name: "an empty session", read: schemaprep.ReadNames{Session: " "}, folding: catalog.NameFoldingFolds, written: "FA"},
		{
			name: "another database on a server that folds", read: schemaprep.ReadNames{Session: "fa"},
			folding: catalog.NameFoldingFolds, written: "FB",
		},
		{
			name:    "unknown folding and every name spelled as the read spells it",
			read:    schemaprep.ReadNames{Session: "fa", Relations: []schemaprep.ReadRelation{{Name: "items"}}},
			folding: catalog.NameFoldingUnknown, written: "fa",
		},
		{
			name:    "unknown folding and names the read did not find",
			read:    schemaprep.ReadNames{Session: "fb", Relations: []schemaprep.ReadRelation{{Name: "orders"}}},
			folding: catalog.NameFoldingUnknown, written: "FA",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := schemaprep.RespellReadNames(spelledDatabase(test.written, "FC"), test.read, test.folding)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, spelledDatabase(test.written, "FC"))
		})
	}
}

// A name that matches two spellings is refused, naming both, rather than
// respelled to either: a server that folds names cannot hold them. So is a
// document that declares two tables such a server reads as one.
func TestRespellReadNames_FailurePath(t *testing.T) {
	t.Run("two databases the read found", func(t *testing.T) {
		c := qt.New(t)
		read := schemaprep.ReadNames{Databases: []string{"fa", "fb", "Fa"}}

		got, err := schemaprep.RespellReadNames(spelledDatabase("FA", "FB"), read, catalog.NameFoldingFolds)

		c.Assert(err, qt.ErrorMatches, `database "FA" matches "fa" and "Fa", names that differ only in case, `+
			`on a server that compares database and table names without regard to case; no spelling is chosen`)
		var ambiguous *schemaprep.AmbiguousNameError
		c.Assert(err, qt.ErrorAs, &ambiguous)
		c.Assert(ambiguous.Matches, qt.DeepEquals, []string{"fa", "Fa"})
		c.Assert(got, qt.IsNil)
	})
	t.Run("two tables the document declares", func(t *testing.T) {
		c := qt.New(t)
		database := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "Items"}, {Name: "items"}}}
		read := schemaprep.ReadNames{Session: "fa", Relations: []schemaprep.ReadRelation{{Name: "items"}}}

		got, err := schemaprep.RespellReadNames(database, read, catalog.NameFoldingFolds)

		c.Assert(err, qt.ErrorMatches, `table "items" matches "Items" and "items", names that differ only in case, .*`)
		c.Assert(got, qt.IsNil)
	})
}

// Where no connection read how the server compares names, a database or table
// name that matches another only without regard to case is refused, naming
// the pair and how to go on, because the plan for it depends on the setting.
func TestRespellReadNames_UnknownFolding(t *testing.T) {
	tests := []struct {
		name     string
		database *schemamodel.Database
		read     schemaprep.ReadNames
		want     schemaprep.UnknownFoldingError
	}{
		{
			name: "a database", database: spelledDatabase("FA", "FB"), read: schemaprep.ReadNames{Session: "fa"},
			want: schemaprep.UnknownFoldingError{Kind: "database", Written: "FA", Found: "fa"},
		},
		{
			name: "a table", database: spelledTable("", "Items"),
			read: schemaprep.ReadNames{Session: "fa", Relations: []schemaprep.ReadRelation{{Name: "items"}}},
			want: schemaprep.UnknownFoldingError{Kind: "table", Written: "Items", Found: "items"},
		},
		{
			name: "a reference to a declared table",
			database: &schemamodel.Database{
				Tables:      []schemamodel.Table{{Name: "items"}, {Name: "other"}},
				Constraints: []schemamodel.Constraint{{Name: "fk", Table: "other", ForeignTable: "Items"}},
			},
			read: schemaprep.ReadNames{Session: "fa"},
			want: schemaprep.UnknownFoldingError{Kind: "table", Written: "Items", Found: "items"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := schemaprep.RespellReadNames(test.database, test.read, catalog.NameFoldingUnknown)

			var unknown *schemaprep.UnknownFoldingError
			c.Assert(err, qt.ErrorAs, &unknown)
			c.Assert(*unknown, qt.Equals, test.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

// The refusal names the pair and the two ways on.
func TestUnknownFoldingError_Error(t *testing.T) {
	c := qt.New(t)
	err := &schemaprep.UnknownFoldingError{Kind: "table", Written: "Items", Found: "items"}

	c.Assert(err.Error(), qt.Equals, `table "Items" and "items" differ only in case, and whether they name one table `+
		`depends on the server's lower_case_table_names, which this comparison does not know; `+
		`compare against a database connection, which reads the setting, or spell both names the same way`)
}

// A nil database stays nil.
func TestRespellReadNames_Nil(t *testing.T) {
	c := qt.New(t)

	got, err := schemaprep.RespellReadNames(nil, schemaprep.ReadNames{Session: "fa"}, catalog.NameFoldingFolds)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.IsNil)
}
