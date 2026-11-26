package planner_test

import (
	"slices"
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

func TestGenerateSchemaDiffAST_RefusesCreatedAndDroppedNamesakes_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		diff difftypes.SchemaDiff
		want string
	}{
		{
			name: "a procedure under a case-insensitive collation",
			diff: difftypes.SchemaDiff{
				IdentifierSemantics: caseInsensitiveProcedures(),
				FunctionsAdded:      difftypes.FunctionChanges{procedure("MIXEDCASE", "BEGIN SELECT 2 END")},
				ProceduresRemoved:   difftypes.FunctionChanges{procedure("dbo.MixedCase", "BEGIN SELECT 1 END")},
			},
			want: "the plan creates procedure MIXEDCASE and drops procedure dbo.MixedCase, and sqlserver may read the two names as one",
		},
		{
			name: "a view no server was asked about",
			diff: difftypes.SchemaDiff{
				ViewsAdded:   difftypes.ViewChanges{{Name: "MIXEDVIEW", Body: "SELECT 2 AS a"}},
				ViewsRemoved: difftypes.ViewChanges{{Name: "dbo.MixedView", Body: "SELECT 1 AS a"}},
			},
			want: "the plan creates view MIXEDVIEW and drops view dbo.MixedView",
		},
		{
			name: "a table",
			diff: difftypes.SchemaDiff{
				TablesAdded:   difftypes.TableChanges{{Name: "MIXEDTABLE"}},
				TablesRemoved: difftypes.TableRemovals{{Name: "MixedTable"}},
			},
			want: "the plan creates table MIXEDTABLE and drops table MixedTable",
		},
		{
			name: "a sequence",
			diff: difftypes.SchemaDiff{
				SequencesAdded:   difftypes.SequenceChanges{{Name: "MIXEDSEQ"}},
				SequencesRemoved: difftypes.SequenceChanges{{Name: "MixedSeq", Schema: "dbo"}},
			},
			want: "the plan creates sequence MIXEDSEQ and drops sequence dbo.MixedSeq",
		},
		{
			name: "a trigger",
			diff: difftypes.SchemaDiff{
				TriggersAdded:   []difftypes.TriggerRef{{TriggerName: "MIXEDTRG", TableName: "orders"}},
				TriggersRemoved: []difftypes.TriggerRef{{TriggerName: "MixedTrg", TableName: "dbo.orders"}},
			},
			want: "the plan creates trigger MIXEDTRG on orders and drops trigger MixedTrg on dbo.orders",
		},
		{
			name: "a schema",
			diff: difftypes.SchemaDiff{
				SchemasAdded:   []schemamodel.Schema{{Name: "SALES"}},
				SchemasRemoved: []string{"Sales"},
			},
			want: "the plan creates schema SALES and drops schema Sales",
		},
		{
			// A name the snapshot does not resolve has no class, so it may be
			// any other name of its kind: the refusal is what a kind the
			// comparison did not ask the server about meets.
			name: "names a resolved snapshot does not cover",
			diff: difftypes.SchemaDiff{
				IdentifierSemantics: caseInsensitiveProcedures(),
				ViewsAdded:          difftypes.ViewChanges{{Name: "recent", Body: "SELECT 2 AS a"}},
				ViewsRemoved:        difftypes.ViewChanges{{Name: "archived", Body: "SELECT 1 AS a"}},
			},
			want: "the plan creates view recent and drops view archived",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := planner.GenerateSchemaDiffAST(t.Context(), must.Must(builtin.New()), &test.diff, platform.SQLServer)

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
