package planner_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// A plan that creates an object and drops another the server reads as the same
// one loses it: on a case-insensitive SQL Server database `CREATE OR ALTER
// PROCEDURE [MIXEDCASE]` changes dbo.MixedCase, and the DROP after it removes
// it (stokaro/ptah#6722). The comparison pairs such names, so a diff asking
// for both comes from a kind it does not pair, and the plan refuses it.

// sqlServerCollation is a live SQL Server snapshot whose catalog put each
// group of names in one class, as a server answers for the names it was asked
// about.
func sqlServerCollation(collation string, groups ...[]string) *identifier.Semantics {
	var resolved []identifier.ResolvedName
	for _, group := range groups {
		for _, name := range group {
			resolved = append(resolved, identifier.ResolvedName{Name: name, Key: slices.Min(group)})
		}
	}
	semantics := identifier.ForSQLServerCatalog(collation).WithResolvedNames(resolved)
	return &semantics
}

// caseInsensitiveProcedures is a case-insensitive snapshot in which MIXEDCASE
// and MixedCase are one name.
func caseInsensitiveProcedures() *identifier.Semantics {
	return sqlServerCollation("SQL_Latin1_General_CP1_CI_AS", []string{"dbo"}, []string{"MIXEDCASE", "MixedCase"})
}

func procedure(name, body string) difftypes.RoutineChange {
	return difftypes.RoutineChange{Function: schemamodel.Function{Name: name, Kind: schemamodel.FunctionKindProcedure, Body: body}}
}

// The rows without a snapshot plan SQL Server under its offline rules, which
// compare no name, so each such refusal says that no connection answered.
func TestGenerateSchemaDiffAST_RefusesCreatedAndDroppedNamesakes_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		diff    difftypes.SchemaDiff
		want    string
	}{
		{
			name:    "a procedure under a case-insensitive collation",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				IdentifierSemantics: caseInsensitiveProcedures(),
				FunctionsAdded:      difftypes.FunctionChanges{procedure("MIXEDCASE", "BEGIN SELECT 2 END")},
				ProceduresRemoved:   difftypes.FunctionChanges{procedure("dbo.MixedCase", "BEGIN SELECT 1 END")},
			},
			want: "the plan creates procedure MIXEDCASE and drops procedure dbo.MixedCase, and sqlserver reads them as one name, " +
				"so the drop would remove the object the create makes; declare the object as the database spells it",
		},
		{
			name:    "a view no server was asked about",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				ViewsAdded:   difftypes.ViewChanges{{Name: "MIXEDVIEW", Body: "SELECT 2 AS a"}},
				ViewsRemoved: difftypes.ViewChanges{{Name: "dbo.MixedView", Body: "SELECT 1 AS a"}},
			},
			want: "the plan creates view MIXEDVIEW and drops view dbo.MixedView, which differ only in case, so under a " +
				"case-insensitive collation the drop would remove the object the create makes, and without a connection " +
				"Ptah cannot tell which collation the database uses; declare the object as the database spells it",
		},
		{
			name:    "a table",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				TablesAdded:   difftypes.TableChanges{{Name: "MIXEDTABLE"}},
				TablesRemoved: difftypes.TableRemovals{{Name: "MixedTable"}},
			},
			want: "the plan creates table MIXEDTABLE and drops table MixedTable, which differ only in case",
		},
		{
			name:    "a sequence",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				SequencesAdded:   difftypes.SequenceChanges{{Name: "MIXEDSEQ"}},
				SequencesRemoved: difftypes.SequenceChanges{{Name: "MixedSeq", Schema: "dbo"}},
			},
			want: "the plan creates sequence MIXEDSEQ and drops sequence dbo.MixedSeq",
		},
		{
			name:    "a schema",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				SchemasAdded:   []schemamodel.Schema{{Name: "SALES"}},
				SchemasRemoved: []string{"Sales"},
			},
			want: "the plan creates schema SALES and drops schema Sales",
		},
		{
			// Offline, a name with a non-ASCII rune has no class, so it may
			// be any name of its kind in its schema, however unrelated.
			name:    "two non-ASCII tables offline",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				TablesAdded:   difftypes.TableChanges{{Name: "Z\u00e4hler"}},
				TablesRemoved: difftypes.TableRemovals{{Name: "Gr\u00f6\u00dfe"}},
			},
			want: "the plan creates table Z\u00e4hler and drops table Gr\u00f6\u00dfe, and without a connection Ptah " +
				"cannot tell whether they are one name under the database collation",
		},
		{
			// An accent-insensitive collation reads café as cafe, and one
			// of the two keys is an ASCII fold.
			name:    "an ASCII view and an accented one offline",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				ViewsAdded:   difftypes.ViewChanges{{Name: "cafe", Body: "SELECT 2 AS a"}},
				ViewsRemoved: difftypes.ViewChanges{{Name: "dbo.caf\u00e9", Body: "SELECT 1 AS a"}},
			},
			want: "the plan creates view cafe and drops view dbo.caf\u00e9, and without a connection Ptah cannot tell " +
				"whether they are one name under the database collation",
		},
		{
			// A name the snapshot does not resolve has no class, so it may be
			// any other name of its kind: the refusal is what a kind the
			// comparison did not ask the server about meets.
			name:    "names a resolved snapshot does not cover",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				IdentifierSemantics: caseInsensitiveProcedures(),
				ViewsAdded:          difftypes.ViewChanges{{Name: "recent", Body: "SELECT 2 AS a"}},
				ViewsRemoved:        difftypes.ViewChanges{{Name: "archived", Body: "SELECT 1 AS a"}},
			},
			want: "the plan creates view recent and drops view archived, and sqlserver was not asked whether they are one name, " +
				"so the drop may remove the object the create makes",
		},
		{
			// MySQL folds a routine name whatever its table rule says, which
			// compares names exactly.
			name:    "a MySQL routine whose names differ only in case",
			dialect: platform.MySQL,
			diff: difftypes.SchemaDiff{
				FunctionsAdded: difftypes.FunctionChanges{{Function: schemamodel.Function{
					Name: "FOO", Returns: "INT", Language: "SQL", Body: "RETURN 2",
				}}},
				FunctionsRemoved: difftypes.FunctionChanges{{Function: schemamodel.Function{
					Name: "foo", Returns: "INT", Language: "SQL", Body: "RETURN 1",
				}}},
			},
			want: "the plan creates function FOO and drops function foo, and mysql reads them as one name",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := planner.GenerateSchemaDiffAST(t.Context(), must.Must(builtin.New()), &test.diff, test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, ".*"+test.want+".*")
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestGenerateSchemaDiffAST_RefusesCreatedAndDroppedNamesakes_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		diff    difftypes.SchemaDiff
	}{
		{
			// The control for the first failure row: the same names, which a
			// case-sensitive server puts in two classes, are two procedures.
			name:    "a procedure under a case-sensitive collation",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				IdentifierSemantics: sqlServerCollation("Latin1_General_100_CS_AS", []string{"dbo"}, []string{"MIXEDCASE"}, []string{"MixedCase"}),
				FunctionsAdded:      difftypes.FunctionChanges{procedure("MIXEDCASE", "BEGIN SELECT 2 END")},
				ProceduresRemoved:   difftypes.FunctionChanges{procedure("dbo.MixedCase", "BEGIN SELECT 1 END")},
			},
		},
		{
			name:    "two views every collation keeps apart",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				ViewsAdded:   difftypes.ViewChanges{{Name: "recent", Body: "SELECT 2 AS a"}},
				ViewsRemoved: difftypes.ViewChanges{{Name: "dbo.archived", Body: "SELECT 1 AS a"}},
			},
		},
		{
			// The control for the offline non-ASCII row: a server that was
			// asked put the two names in two classes.
			name:    "two non-ASCII tables a server put apart",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				IdentifierSemantics: sqlServerCollation("SQL_Latin1_General_CP1_CI_AS",
					[]string{"dbo"}, []string{"Gr\u00f6\u00dfe"}, []string{"Z\u00e4hler"}),
				TablesAdded:   difftypes.TableChanges{{Name: "Z\u00e4hler"}},
				TablesRemoved: difftypes.TableRemovals{{Name: "Gr\u00f6\u00dfe"}},
			},
		},
		{
			// A name with no class is a namesake only of names in a schema
			// that may be its own.
			name:    "a non-ASCII table offline beside a drop in another schema",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				TablesAdded:   difftypes.TableChanges{{Name: "sales.Gr\u00f6\u00dfe"}},
				TablesRemoved: difftypes.TableRemovals{{Name: "dbo.orders"}},
			},
		},
		{
			// The control for the MySQL routine row.
			name:    "two MySQL routines of different names",
			dialect: platform.MySQL,
			diff: difftypes.SchemaDiff{
				FunctionsAdded: difftypes.FunctionChanges{{Function: schemamodel.Function{
					Name: "foo", Returns: "INT", Language: "SQL", Body: "RETURN 2",
				}}},
				FunctionsRemoved: difftypes.FunctionChanges{{Function: schemamodel.Function{
					Name: "bar", Returns: "INT", Language: "SQL", Body: "RETURN 1",
				}}},
			},
		},
		{
			// A routine created beside one of its name that is dropped is an
			// overload, which PostgreSQL addresses by its argument list.
			name:    "a PostgreSQL overload",
			dialect: platform.Postgres,
			diff: difftypes.SchemaDiff{
				FunctionsAdded: difftypes.FunctionChanges{{Function: schemamodel.Function{
					Name: "f", Parameters: "a integer", Returns: "integer", Language: "sql", Body: "SELECT 1",
				}}},
				FunctionsRemoved: difftypes.FunctionChanges{{
					Function:  schemamodel.Function{Name: "public.f", Parameters: "a text", Returns: "integer", Language: "sql", Body: "SELECT 1"},
					Signature: new("text"),
				}},
			},
		},
		{
			name:    "a PostgreSQL view whose names differ only in case",
			dialect: platform.Postgres,
			diff: difftypes.SchemaDiff{
				ViewsAdded:   difftypes.ViewChanges{{Name: "MixedView", Body: "SELECT 2 AS a"}},
				ViewsRemoved: difftypes.ViewChanges{{Name: "mixedview", Body: "SELECT 1 AS a"}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := planner.GenerateSchemaDiffAST(t.Context(), must.Must(builtin.New()), &test.diff, test.dialect)

			c.Assert(err, qt.IsNil)
		})
	}
}

// A trigger whose name differs from one the plan drops only in case is not
// refused: the planner drops the old trigger first, so the plan runs. Measured
// on SQL Server 2025 under a case-insensitive collation, the DROP TRIGGER
// frees the name and the CREATE TRIGGER after it succeeds.
func TestGenerateSchemaDiffSQLStatements_DropsACaseOnlyTriggerNamesakeFirst_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := difftypes.SchemaDiff{
		TriggersAdded: []difftypes.TriggerRef{{TriggerName: "MIXEDTRG", TableName: "orders", Desired: schemamodel.Trigger{
			Name: "MIXEDTRG", Table: "orders", Timing: "AFTER", Event: "INSERT", Body: "BEGIN SET NOCOUNT ON; END",
		}}},
		TriggersRemoved: []difftypes.TriggerRef{{TriggerName: "MixedTrg", TableName: "dbo.orders"}},
	}

	statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), must.Must(builtin.New()), &diff, platform.SQLServer)

	c.Assert(err, qt.IsNil)
	c.Assert(statementHeads(statements), qt.DeepEquals, []string{"DROP TRIGGER", "CREATE TRIGGER"})
}

// statementHeads is the first two words of each statement, which say what it
// does and in which order.
func statementHeads(statements []string) []string {
	heads := make([]string, 0, len(statements))
	for _, statement := range statements {
		words := strings.Fields(statement)
		heads = append(heads, strings.Join(words[:min(2, len(words))], " "))
	}
	return heads
}
