package atlasfilter_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
)

// includeOutcomeGenerated models one schema holding a plainly named table and
// a table whose name literally contains dots, which is the case a shape check
// over selector text can never decide: `a.b.c` is both a legal table name and
// the positional spelling of schema.table.column.
func includeOutcomeGenerated() *goschema.Database {
	return &goschema.Database{
		Schemas: []goschema.Schema{{Name: "main"}},
		Tables: []goschema.Table{
			{StructName: "User", Schema: "main", Name: "users"},
			{StructName: "Dotted", Schema: "main", Name: "a.b.c"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER"},
			{StructName: "User", Name: "email", Type: "TEXT"},
			{StructName: "Dotted", Name: "id", Type: "INTEGER"},
		},
	}
}

// includeOutcomeDatabase mirrors includeOutcomeGenerated for the introspected
// side, so one row set covers both projections.
func includeOutcomeDatabase() *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Schemas: []dbschematypes.DBSchemaInfo{{Name: "main"}},
		Tables: []dbschematypes.DBTable{
			{
				Schema: "main",
				Name:   "users",
				Columns: []dbschematypes.DBColumn{
					{Name: "id", DataType: "integer"},
					{Name: "email", DataType: "text"},
				},
			},
			{
				Schema: "main",
				Name:   "a.b.c",
				Columns: []dbschematypes.DBColumn{
					{Name: "id", DataType: "integer"},
				},
			},
		},
	}
}

func includeOutcomeScope(include []string) atlasfilter.Scope {
	return atlasfilter.Scope{Include: include, DefaultSchema: "main"}
}

// outcomeGeneratedTableNames tolerates a nil projection so a row that expects
// a projection still reports its assertion rather than panicking.
func outcomeGeneratedTableNames(db *goschema.Database) []string {
	var names []string
	if db == nil {
		return nil
	}
	for _, table := range db.Tables {
		names = append(names, table.Name)
	}
	return names
}

func outcomeDatabaseTableNames(db *dbschematypes.DBSchema) []string {
	var names []string
	if db == nil {
		return nil
	}
	for _, table := range db.Tables {
		names = append(names, table.Name)
	}
	return names
}

// errorText renders an error for exact comparison, so a row can pin the bytes
// of the message and the absence of one in the same assertion.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// TestIncludeSelectionOutcome pins the outcome check that replaced the shape
// check: a non-empty --include selection that matches no top-level object is
// reported as such, whatever the selector looked like.
//
// The shape check could only close the literal-separator spelling. path.Match
// treats "." as an ordinary character — only "/" separates — so every
// metacharacter that can stand for a dot walked past it, and `[.]` was an
// escape the check itself created by reading bracket runs as identifier
// quoting. Each of those spellings has a row here, because a rule whose
// escapes were sampled rather than enumerated is how the last one shipped.
func TestIncludeSelectionOutcome(t *testing.T) {
	c := qt.New(t)

	projections := []struct {
		name string
		// project runs one side's projection and returns the surviving
		// top-level table names together with the projection error, so the
		// row set below covers the generated and introspected sides alike.
		project func(include []string) ([]string, error)
	}{
		{
			name: "generated",
			project: func(include []string) ([]string, error) {
				got, err := atlasfilter.ScopeGenerated(includeOutcomeGenerated(), includeOutcomeScope(include))
				return outcomeGeneratedTableNames(got), err
			},
		},
		{
			name: "database",
			project: func(include []string) ([]string, error) {
				got, err := atlasfilter.ScopeDatabase(includeOutcomeDatabase(), includeOutcomeScope(include))
				return outcomeDatabaseTableNames(got), err
			},
		},
	}

	tests := []struct {
		name    string
		include []string
		// wantTables are the top-level tables the projection keeps.
		wantTables []string
		// wantErr is the exact error text, empty when the selection matched.
		wantErr string
	}{
		{
			name:    "typo matches nothing",
			include: []string{"no_such_table"},
			wantErr: `the --include selection matched no objects: "no_such_table"`,
		},
		{
			name:    "star stands for the separator",
			include: []string{"main.users*email"},
			wantErr: `the --include selection matched no objects: "main.users*email"`,
		},
		{
			name:    "question mark stands for the separator",
			include: []string{"main.users?email"},
			wantErr: `the --include selection matched no objects: "main.users?email"`,
		},
		{
			name:    "character class stands for the separator",
			include: []string{"main.users[.]email"},
			wantErr: `the --include selection matched no objects: "main.users[.]email"`,
		},
		{
			// The one spelling the deleted shape check did catch. It is not
			// special: it matches nothing for the same reason as its four
			// siblings above, and it is now reported for that reason.
			name:    "literal separator reaches a child",
			include: []string{"main.users.email"},
			wantErr: `the --include selection matched no objects: "main.users.email"`,
		},
		{
			// The proof the shape check is gone. A table is literally named
			// a.b.c and the bare selector matches it; the old rule refused
			// this input before any database was contacted.
			name:       "bare dotted table name",
			include:    []string{"a.b.c"},
			wantTables: []string{"a.b.c"},
		},
		{
			// The escape spelling the shape check shipped as a workaround.
			// It still parses and still selects, so nobody who adopted it
			// breaks.
			name:       "escaped dotted table name",
			include:    []string{`a\.b\.c`},
			wantTables: []string{"a.b.c"},
		},
		{
			// The other documented workaround: the qualified candidate quotes
			// a dotted identifier, so this is the schema-qualified spelling.
			name:       "quoted dotted table name",
			include:    []string{`main."a.b.c"`},
			wantTables: []string{"a.b.c"},
		},
		{
			// Backtick and bracket runs were arms of the deleted scanner. They
			// still parse, and they still match nothing, because the qualified
			// candidate is emitted with double quotes and path.Match reads
			// brackets as a character class. Pinned so the answer is stated
			// rather than assumed.
			name:    "backtick quoting selects nothing",
			include: []string{"main.`a.b.c`"},
			wantErr: "the --include selection matched no objects: \"main.`a.b.c`\"",
		},
		{
			name:    "bracket quoting selects nothing",
			include: []string{"main.[a.b.c]"},
			wantErr: `the --include selection matched no objects: "main.[a.b.c]"`,
		},
		{
			name:       "matching selector",
			include:    []string{"users"},
			wantTables: []string{"users"},
		},
		{
			// One matching selector in the union is enough; the union as a
			// whole matched something.
			name:       "one of several selectors matches",
			include:    []string{"no_such_table,users"},
			wantTables: []string{"users"},
		},
		{
			// A blank value carries no selection at all, so the scope is not
			// positive and the unfiltered path is kept.
			name:       "blank value keeps everything",
			include:    []string{"  "},
			wantTables: []string{"users", "a.b.c"},
		},
	}

	for _, projection := range projections {
		for _, test := range tests {
			c.Run(projection.name+"/"+test.name, func(c *qt.C) {
				names, err := projection.project(test.include)

				c.Assert(names, qt.DeepEquals, test.wantTables)
				c.Assert(errorText(err), qt.Equals, test.wantErr)
				// The signal has to be recognizable by type, because each verb
				// decides its own exit from it.
				var empty *atlasfilter.EmptySelectionError
				c.Assert(errors.As(err, &empty), qt.Equals, test.wantErr != "")
			})
		}
	}
}

// TestSchemaScopeAloneNeverReportsEmptySelection pins the deliberate asymmetry
// between the two positive selectors. --schema is the sibling selector whose
// zero-match answer was measured on the pinned Atlas community binary: exit 0
// and silent on every verb. Narrowing to a schema that holds nothing stays an
// ordinary answer here; only --include, which has no such oracle, reports.
func TestSchemaScopeAloneNeverReportsEmptySelection(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		// project runs one side's projection under a schema-only scope.
		project func(schemas []string) ([]string, error)
	}{
		{
			name: "generated",
			project: func(schemas []string) ([]string, error) {
				got, err := atlasfilter.ScopeGenerated(includeOutcomeGenerated(), atlasfilter.Scope{
					Schemas:       schemas,
					DefaultSchema: "main",
				})
				return outcomeGeneratedTableNames(got), err
			},
		},
		{
			name: "database",
			project: func(schemas []string) ([]string, error) {
				got, err := atlasfilter.ScopeDatabase(includeOutcomeDatabase(), atlasfilter.Scope{
					Schemas:       schemas,
					DefaultSchema: "main",
				})
				return outcomeDatabaseTableNames(got), err
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			names, err := test.project([]string{"no_such_schema"})

			c.Assert(err, qt.IsNil)
			c.Assert(names, qt.IsNil)
		})
	}
}
