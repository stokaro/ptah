package schemadiff_test

import (
	"context"
	"database/sql"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// A SQL Server database compares object names under its collation, and only
// the server knows the collation. The comparison asks it about every name it
// keys, and pairs two names the way the answer does: under a case-insensitive
// collation a name that differs from a held one only in case is the held
// object, changed; under a case-sensitive one it is another object
// (stokaro/ptah#6722).

// collationConnection is a SQL Server connection that answers the question
// the comparison asks the way a server under one collation does: names that
// differ only in case are one name under a case-insensitive collation, and
// every name is its own under a case-sensitive one. It holds no session, so no
// declaration is spelled by the server.
type collationConnection struct {
	collation       string
	caseInsensitive bool
}

func (c collationConnection) Info() catalog.ServerInfo {
	semantics := identifier.ForSQLServerCatalog(c.collation)
	return catalog.ServerInfo{Dialect: platform.SQLServer, Schema: semantics.DefaultSchema, IdentifierSemantics: semantics}
}

func (c collationConnection) ResolveIdentifierSemantics(_ context.Context, names []string) (identifier.Semantics, error) {
	semantics := c.Info().IdentifierSemantics
	names = slices.DeleteFunc(append(slices.Clone(names), semantics.DefaultSchema), func(name string) bool { return name == "" })
	slices.Sort(names)
	names = slices.Compact(names)
	class := func(name string) string {
		if c.caseInsensitive {
			return strings.ToLower(name)
		}
		return name
	}
	keys := make(map[string]string, len(names))
	for _, name := range names {
		if _, seen := keys[class(name)]; !seen {
			keys[class(name)] = name
		}
	}
	resolved := make([]identifier.ResolvedName, 0, len(names))
	for _, name := range names {
		resolved = append(resolved, identifier.ResolvedName{Name: name, Key: keys[class(name)]})
	}
	return semantics.WithResolvedNames(resolved), nil
}

func (collationConnection) WithRolledBackTransaction(context.Context, string, func(context.Context, *sql.Tx) error) (bool, error) {
	return false, nil
}

var (
	caseInsensitiveDatabase = collationConnection{collation: "SQL_Latin1_General_CP1_CI_AS", caseInsensitive: true}
	caseSensitiveDatabase   = collationConnection{collation: "Latin1_General_100_CS_AS"}
)

// caseOnlyNamesake is a database holding one object and a declaration naming
// it in another case, with the changes the comparison reports under each kind
// of collation, in the words [namedObjectChanges] writes them.
type caseOnlyNamesake struct {
	name        string
	desired     *schemamodel.Database
	held        *catalog.Database
	insensitive []string
	sensitive   []string
}

// caseOnlyNamesakes is one row for every kind of object a SQL Server schema
// holds by name. The declared object differs from the held one in its
// definition too, so a pair is a modification rather than nothing.
func caseOnlyNamesakes() []caseOnlyNamesake {
	orders := schemamodel.Table{StructName: "Order", Name: "orders"}
	heldOrders := catalog.Table{Name: "orders"}
	return []caseOnlyNamesake{
		{
			name: "procedure",
			desired: &schemamodel.Database{Functions: []schemamodel.Function{
				{Name: "MIXEDCASE", Kind: schemamodel.FunctionKindProcedure, Body: "BEGIN SELECT 2 END"},
			}},
			held: &catalog.Database{Functions: []catalog.Function{
				{Name: "MixedCase", Kind: schemamodel.FunctionKindProcedure, Body: "BEGIN SELECT 1 END"},
			}},
			insensitive: []string{"routine modified MIXEDCASE"},
			sensitive:   []string{"procedure removed MixedCase", "routine added MIXEDCASE"},
		},
		{
			name: "function",
			desired: &schemamodel.Database{Functions: []schemamodel.Function{
				{Name: "dbo.MIXEDFN", Returns: "int", Body: "BEGIN RETURN 2 END"},
			}},
			held: &catalog.Database{Functions: []catalog.Function{
				{Name: "MixedFn", Returns: "int", Body: "BEGIN RETURN 1 END"},
			}},
			insensitive: []string{"routine modified dbo.MIXEDFN"},
			sensitive:   []string{"function removed MixedFn", "routine added dbo.MIXEDFN"},
		},
		{
			name:        "view",
			desired:     &schemamodel.Database{Views: []schemamodel.View{{Name: "MIXEDVIEW", Body: "SELECT 2 AS a"}}},
			held:        &catalog.Database{Views: []catalog.View{{Name: "MixedView", Body: "SELECT 1 AS a"}}},
			insensitive: []string{"view modified MIXEDVIEW"},
			sensitive:   []string{"view added MIXEDVIEW", "view removed MixedView"},
		},
		{
			name:        "sequence",
			desired:     &schemamodel.Database{Sequences: []schemamodel.Sequence{{Name: "MIXEDSEQ", Increment: new(int64(2))}}},
			held:        &catalog.Database{Sequences: []catalog.Sequence{{Name: "MixedSeq", Increment: new(int64(1))}}},
			insensitive: []string{"sequence modified MIXEDSEQ"},
			sensitive:   []string{"sequence added MIXEDSEQ", "sequence removed MixedSeq"},
		},
		{
			name: "trigger",
			desired: &schemamodel.Database{Tables: []schemamodel.Table{orders}, Triggers: []schemamodel.Trigger{
				{Name: "MIXEDTRG", Table: "orders", Timing: "AFTER", Event: "INSERT", Body: "BEGIN SELECT 2 END"},
			}},
			held: &catalog.Database{Tables: []catalog.Table{heldOrders}, Triggers: []catalog.Trigger{
				{Name: "MixedTrg", Table: "orders", Timing: "AFTER", Event: "INSERT", Body: "BEGIN SELECT 1 END"},
			}},
			insensitive: []string{"trigger modified MIXEDTRG on orders"},
			sensitive:   []string{"trigger added MIXEDTRG on orders", "trigger removed MixedTrg on orders"},
		},
		{
			name:        "table",
			desired:     &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "Mixed", Name: "MIXEDTABLE"}}},
			held:        &catalog.Database{Tables: []catalog.Table{{Name: "MixedTable"}}},
			insensitive: nil,
			sensitive:   []string{"table added MIXEDTABLE", "table removed MixedTable"},
		},
		{
			name:        "schema",
			desired:     &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "SALES"}}},
			held:        &catalog.Database{Schemas: []catalog.Schema{{Name: "Sales"}}},
			insensitive: nil,
			sensitive:   []string{"schema added SALES"},
		},
	}
}

// namedObjectChanges lists the schemas, tables, views, sequences, routines and
// triggers diff adds, removes and modifies, one line each, sorted.
func namedObjectChanges(diff *difftypes.SchemaDiff) []string {
	var changes []string
	note := func(kind, change string, names ...string) {
		for _, name := range names {
			changes = append(changes, kind+" "+change+" "+name)
		}
	}
	for _, schema := range diff.SchemasAdded {
		note("schema", "added", schema.Name)
	}
	note("schema", "removed", diff.SchemasRemoved...)
	note("table", "added", diff.TablesAdded.Names()...)
	note("table", "removed", diff.TablesRemoved.Names()...)
	note("view", "added", diff.ViewsAdded.Names()...)
	note("view", "removed", diff.ViewsRemoved.Names()...)
	for _, view := range diff.ViewsModified {
		note("view", "modified", view.ViewName)
	}
	note("sequence", "added", diff.SequencesAdded.Names()...)
	note("sequence", "removed", diff.SequencesRemoved.Names()...)
	for _, sequence := range diff.SequencesModified {
		note("sequence", "modified", sequence.SequenceName)
	}
	note("routine", "added", diff.FunctionsAdded.Names()...)
	note("function", "removed", diff.FunctionsRemoved.Names()...)
	note("procedure", "removed", diff.ProceduresRemoved.Names()...)
	for _, routine := range diff.FunctionsModified {
		note("routine", "modified", routine.FunctionName)
	}
	for _, trigger := range diff.TriggersAdded {
		note("trigger", "added", trigger.TriggerName+" on "+trigger.TableName)
	}
	for _, trigger := range diff.TriggersRemoved {
		note("trigger", "removed", trigger.TriggerName+" on "+trigger.TableName)
	}
	for _, trigger := range diff.TriggersModified {
		note("trigger", "modified", trigger.TriggerName+" on "+trigger.TableName)
	}
	slices.Sort(changes)
	return changes
}

// TestCompareWithDatabase_SQLServerCaseInsensitiveCollationPairsCaseOnlyNames_HappyPath
// declares each kind of object in another case than the database holds it,
// under a case-insensitive collation. Each pair is the held object, changed,
// and nothing is created beside it.
func TestCompareWithDatabase_SQLServerCaseInsensitiveCollationPairsCaseOnlyNames_HappyPath(t *testing.T) {
	for _, test := range caseOnlyNamesakes() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabase(c.Context(), caseInsensitiveDatabase, test.desired, test.held, nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(namedObjectChanges(diff), qt.DeepEquals, test.insensitive)
		})
	}
}

// TestCompareWithDatabase_SQLServerCaseSensitiveCollationKeepsCaseOnlyNamesApart_HappyPath
// is the control: the same declarations under a case-sensitive collation name
// other objects, so each is created and the held one dropped, as two names
// the server keeps apart are.
func TestCompareWithDatabase_SQLServerCaseSensitiveCollationKeepsCaseOnlyNamesApart_HappyPath(t *testing.T) {
	for _, test := range caseOnlyNamesakes() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabase(c.Context(), caseSensitiveDatabase, test.desired, test.held, nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(namedObjectChanges(diff), qt.DeepEquals, test.sensitive)
		})
	}
}

// TestCompareWithDatabase_SQLServerCaseSensitiveCollationKeepsTwoProcedures_HappyPath
// declares two procedures whose names differ only in case on a database
// holding one of them. Under a case-sensitive collation they are two
// procedures, and the plan creates the second beside the first.
func TestCompareWithDatabase_SQLServerCaseSensitiveCollationKeepsTwoProcedures_HappyPath(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Functions: []schemamodel.Function{
		{Name: "MixedCase", Kind: schemamodel.FunctionKindProcedure, Body: "BEGIN SELECT 1 END"},
		{Name: "MIXEDCASE", Kind: schemamodel.FunctionKindProcedure, Body: "BEGIN SELECT 2 END"},
	}}
	held := &catalog.Database{Functions: []catalog.Function{
		{Name: "MixedCase", Kind: schemamodel.FunctionKindProcedure, Body: "BEGIN SELECT 1 END", Volatility: "VOLATILE"},
	}}

	diff, err := schemadiff.CompareWithDatabase(c.Context(), caseSensitiveDatabase, desired, held, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(namedObjectChanges(diff), qt.DeepEquals, []string{"routine added MIXEDCASE"})
}

// TestCompareWithDatabase_SQLServerCaseInsensitiveCollationNamesakeDeclarations_FailurePath
// declares two objects of one kind whose names differ only in case. Under a
// case-insensitive collation they are one object, so one declaration would be
// compared and the other dropped in silence; the comparison refuses instead,
// naming both.
func TestCompareWithDatabase_SQLServerCaseInsensitiveCollationNamesakeDeclarations_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		want    string
	}{
		{
			name: "views",
			desired: &schemamodel.Database{Views: []schemamodel.View{
				{Name: "MixedView", Body: "SELECT 1 AS a"}, {Name: "MIXEDVIEW", Body: "SELECT 2 AS a"},
			}},
			want: `target views dbo\.MixedView and dbo\.MIXEDVIEW may have the same catalog identity`,
		},
		{
			name: "procedures",
			desired: &schemamodel.Database{Functions: []schemamodel.Function{
				{Name: "MixedCase", Kind: schemamodel.FunctionKindProcedure, Body: "BEGIN SELECT 1 END"},
				{Name: "dbo.MIXEDCASE", Kind: schemamodel.FunctionKindProcedure, Body: "BEGIN SELECT 2 END"},
			}},
			want: `target routines dbo\.MixedCase and dbo\.MIXEDCASE may have the same catalog identity`,
		},
		{
			name: "sequences",
			desired: &schemamodel.Database{Sequences: []schemamodel.Sequence{
				{Name: "s1"}, {Name: "S1"},
			}},
			want: `target sequences dbo\.s1 and dbo\.S1 may have the same catalog identity`,
		},
		{
			name: "triggers",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
				Triggers: []schemamodel.Trigger{
					{Name: "trg", Table: "orders", Timing: "AFTER", Event: "INSERT", Body: "BEGIN SELECT 1 END"},
					{Name: "TRG", Table: "orders", Timing: "AFTER", Event: "UPDATE", Body: "BEGIN SELECT 2 END"},
				},
			},
			want: `target triggers dbo\.orders trigger trg and dbo\.orders trigger TRG may have the same catalog identity`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabase(c.Context(), caseInsensitiveDatabase, test.desired, &catalog.Database{}, nil, must.Must(builtin.New()))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `(?s).*`+test.want+`.*`)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestCompareWithDatabaseInfo_SQLServerSnapshotMissingAnObjectName_FailurePath
// hands the comparison a resolved snapshot that does not say how the server
// compares a view's name. Without the answer the name would keep its spelling
// and could pair wrongly, so the snapshot is refused.
func TestCompareWithDatabaseInfo_SQLServerSnapshotMissingAnObjectName_FailurePath(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics("SQL_Latin1_General_CP1_CI_AS", []string{"dbo"}, []string{"MixedView"})

	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(),
		&schemamodel.Database{Views: []schemamodel.View{{Name: "MIXEDVIEW", Body: "SELECT 2 AS a"}}},
		&catalog.Database{Views: []catalog.View{{Name: "MixedView", Body: "SELECT 1 AS a"}}},
		catalog.ServerInfo{Dialect: platform.SQLServer, IdentifierSemantics: semantics},
		nil, must.Must(builtin.New()))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*identifier semantics snapshot does not resolve "MIXEDVIEW".*`)
	c.Assert(diff, qt.IsNil)
}

// TestCompareWithDatabaseInfo_SQLServerResolvedObjectNames_HappyPath hands
// the comparison a snapshot that puts the declared and the held view name in
// one class, as a case-insensitive server answers. The view is changed, not
// created beside the held one.
func TestCompareWithDatabaseInfo_SQLServerResolvedObjectNames_HappyPath(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics("SQL_Latin1_General_CP1_CI_AS", []string{"dbo"}, []string{"MIXEDVIEW", "MixedView"})

	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(),
		&schemamodel.Database{Views: []schemamodel.View{{Name: "MIXEDVIEW", Body: "SELECT 2 AS a"}}},
		&catalog.Database{Views: []catalog.View{{Name: "MixedView", Body: "SELECT 1 AS a"}}},
		catalog.ServerInfo{Dialect: platform.SQLServer, IdentifierSemantics: semantics},
		nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(namedObjectChanges(diff), qt.DeepEquals, []string{"view modified MIXEDVIEW"})
}

// TestValidateDesiredSchema_SQLServerSnapshotMissingADeclaredObjectName_FailurePath
// validates a desired schema against a resolved snapshot that does not say
// how the server compares a declared view's name. No comparison ran to ask
// about the names, so validation itself refuses the snapshot.
func TestValidateDesiredSchema_SQLServerSnapshotMissingADeclaredObjectName_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		missing string
	}{
		{name: "view", desired: &schemamodel.Database{Views: []schemamodel.View{{Name: "MIXEDVIEW", Body: "SELECT 2 AS a"}}}, missing: "MIXEDVIEW"},
		{name: "procedure", desired: &schemamodel.Database{Functions: []schemamodel.Function{
			{Name: "MIXEDCASE", Kind: schemamodel.FunctionKindProcedure, Body: "BEGIN SELECT 2 END"},
		}}, missing: "MIXEDCASE"},
		{name: "sequence", desired: &schemamodel.Database{Sequences: []schemamodel.Sequence{{Name: "MIXEDSEQ"}}}, missing: "MIXEDSEQ"},
		{name: "trigger", desired: &schemamodel.Database{
			Tables:   []schemamodel.Table{{StructName: "Order", Name: "orders"}},
			Triggers: []schemamodel.Trigger{{Name: "MIXEDTRG", Table: "orders", Timing: "AFTER", Event: "INSERT", Body: "BEGIN SELECT 2 END"}},
		}, missing: "MIXEDTRG"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			semantics := resolvedSQLServerSemantics("SQL_Latin1_General_CP1_CI_AS", []string{"dbo"}, []string{"orders"})

			err := schemadiff.ValidateDesiredSchema(c.Context(), must.Must(builtin.New()), test.desired,
				catalog.ServerInfo{Dialect: platform.SQLServer, IdentifierSemantics: semantics})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*identifier semantics snapshot does not resolve "`+test.missing+`".*`)
		})
	}
}
