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

func function(name, body string) difftypes.RoutineChange {
	return difftypes.RoutineChange{Function: schemamodel.Function{Name: name, Returns: "int", Body: body}}
}

// The rows without a snapshot plan SQL Server under its offline rules, which
// say of no two names that they are two, so each such refusal says that no
// connection answered.
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
			want: "the plan creates procedure dbo.MIXEDCASE and drops procedure dbo.MixedCase, and sqlserver reads them as one name, " +
				"so the drop would remove the object the create makes; declare the object as the database spells it",
		},
		{
			name:    "a view no server was asked about",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				ViewsAdded:   difftypes.ViewChanges{{Name: "MIXEDVIEW", Body: "SELECT 2 AS a"}},
				ViewsRemoved: difftypes.ViewChanges{{Name: "dbo.MixedView", Body: "SELECT 1 AS a"}},
			},
			want: "the plan creates view dbo.MIXEDVIEW and drops view dbo.MixedView, which differ only in case, so under a " +
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
			want: "the plan creates table dbo.MIXEDTABLE and drops table dbo.MixedTable, which differ only in case",
		},
		{
			name:    "a sequence",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				SequencesAdded:   difftypes.SequenceChanges{{Name: "MIXEDSEQ"}},
				SequencesRemoved: difftypes.SequenceChanges{{Name: "MixedSeq", Schema: "dbo"}},
			},
			want: "the plan creates sequence dbo.MIXEDSEQ and drops sequence dbo.MixedSeq",
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
			want: "the plan creates table dbo.Z\u00e4hler and drops table dbo.Gr\u00f6\u00dfe, and without a connection Ptah " +
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
			want: "the plan creates view dbo.cafe and drops view dbo.caf\u00e9, and without a connection Ptah cannot tell " +
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
			want: "the plan creates view dbo.recent and drops view dbo.archived, and sqlserver was not asked whether they are one name, " +
				"so the drop may remove the object the create makes",
		},
		{
			// Hungarian_CI_AS reads ccs as cscs, and the offline rules cannot
			// see it, so no rule says the two are two (stokaro/ptah#6746).
			name:    "a contraction offline",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				FunctionsAdded:   difftypes.FunctionChanges{function("dbo.cscs", "BEGIN RETURN 2 END")},
				FunctionsRemoved: difftypes.FunctionChanges{function("dbo.ccs", "BEGIN RETURN 1 END")},
			},
			want: "the plan creates function dbo.cscs and drops function dbo.ccs, and without a connection Ptah cannot tell " +
				"whether they are one name under the database collation, so the drop may remove the object the create makes; " +
				"compare with a connection to the database, which answers how it compares the names",
		},
		{
			name:    "a contraction a Hungarian server reads as one name",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				IdentifierSemantics: sqlServerCollation("Hungarian_CI_AS", []string{"dbo"}, []string{"ccs", "cscs"}),
				FunctionsAdded:      difftypes.FunctionChanges{function("dbo.cscs", "BEGIN RETURN 2 END")},
				FunctionsRemoved:    difftypes.FunctionChanges{function("dbo.ccs", "BEGIN RETURN 1 END")},
			},
			want: "the plan creates function dbo.cscs and drops function dbo.ccs, and sqlserver reads them as one name",
		},
		{
			name:    "two ASCII views offline",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				ViewsAdded:   difftypes.ViewChanges{{Name: "recent", Body: "SELECT 2 AS a"}},
				ViewsRemoved: difftypes.ViewChanges{{Name: "dbo.archived", Body: "SELECT 1 AS a"}},
			},
			want: "the plan creates view dbo.recent and drops view dbo.archived, and without a connection Ptah cannot tell",
		},
		{
			// Two schemas may be one schema too.
			name:    "a table in another schema offline",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				TablesAdded:   difftypes.TableChanges{{Name: "sales.orders"}},
				TablesRemoved: difftypes.TableRemovals{{Name: "dbo.orders"}},
			},
			want: "the plan creates table sales.orders and drops table dbo.orders, and without a connection Ptah cannot tell",
		},
		{
			// SQL Server keeps views and routines in one namespace, and the
			// plan creates the procedure while the view holds the name
			// (stokaro/ptah#6741).
			name:    "a procedure named as a view the plan drops",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				IdentifierSemantics: sqlServerCollation("SQL_Latin1_General_CP1_CI_AS", []string{"dbo"}, []string{"REPORT", "Report"}),
				FunctionsAdded:      difftypes.FunctionChanges{procedure("dbo.REPORT", "BEGIN SELECT 2 END")},
				ViewsRemoved:        difftypes.ViewChanges{{Name: "dbo.Report", Body: "SELECT 1 AS a"}},
			},
			want: "the plan creates procedure dbo.REPORT and drops view dbo.Report, and sqlserver keeps both kinds in one " +
				"namespace in a schema, where it may read the two names as one, so the create would fail while the view " +
				"exists; make the change in two migrations, the first dropping the view dbo.Report and the second " +
				"creating the procedure dbo.REPORT",
		},
		{
			// A function and a procedure are two kinds there, and the plan
			// creates the procedure while the function holds the name.
			name:    "a procedure in place of a function of one name",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				FunctionsAdded:   difftypes.FunctionChanges{procedure("dbo.x", "BEGIN SELECT 1 END")},
				FunctionsRemoved: difftypes.FunctionChanges{function("dbo.x", "BEGIN RETURN 1 END")},
			},
			want: "the plan creates procedure dbo.x and drops function dbo.x, and sqlserver keeps both kinds in one " +
				"namespace in a schema, where the two are one name, so the create would fail while the function exists; " +
				"make the change in two migrations, the first dropping the function dbo.x and the second creating the " +
				"procedure dbo.x",
		},
		{
			name:    "a table spelled as a view the plan drops, offline",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				TablesAdded:  difftypes.TableChanges{{Name: "Report"}},
				ViewsRemoved: difftypes.ViewChanges{{Name: "dbo.Report", Body: "SELECT 1 AS a"}},
			},
			want: "the plan creates table dbo.Report and drops view dbo.Report, and sqlserver keeps both kinds in one namespace " +
				"in a schema, where the two are one name",
		},
		{
			name:    "a sequence named as a table the plan drops, offline",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				SequencesAdded: difftypes.SequenceChanges{{Name: "TAB1"}},
				TablesRemoved:  difftypes.TableRemovals{{Name: "dbo.Tab1"}},
			},
			want: "the plan creates sequence dbo.TAB1 and drops table dbo.Tab1, and sqlserver keeps both kinds in one namespace " +
				"in a schema, where it may read the two names as one",
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
		{
			// An unqualified routine is in the default schema, so the two are
			// one routine, which SQL Server cannot hold twice: CREATE OR
			// ALTER FUNCTION [ccs] replaces dbo.ccs, and the DROP FUNCTION
			// after it removes it.
			name:    "a routine created and dropped under one spelling offline",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				FunctionsAdded:   difftypes.FunctionChanges{function("ccs", "BEGIN RETURN 2 END")},
				FunctionsRemoved: difftypes.FunctionChanges{function("dbo.ccs", "BEGIN RETURN 1 END")},
			},
			want: "the plan creates function dbo.ccs and drops function dbo.ccs, which are one routine on sqlserver, " +
				"since it has no routine overloading, and a plan cannot both create it and drop it; a routine whose " +
				"definition changes is a modification, which the planner replaces",
		},
		{
			name:    "a routine created and dropped under one spelling with a connection",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				IdentifierSemantics: sqlServerCollation("Latin1_General_100_CI_AS", []string{"dbo"}, []string{"f"}),
				FunctionsAdded:      difftypes.FunctionChanges{function("dbo.f", "BEGIN RETURN 2 END")},
				FunctionsRemoved:    difftypes.FunctionChanges{function("f", "BEGIN RETURN 1 END")},
			},
			want: "the plan creates function dbo.f and drops function dbo.f, which are one routine on sqlserver",
		},
		{
			// The schema is spelled under the target's own rules, which the
			// offline rules leave to the collation, so DBO and dbo are two
			// spellings that may be one schema.
			name:    "a routine whose schema differs only in case offline",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				FunctionsAdded:   difftypes.FunctionChanges{function("DBO.f", "BEGIN RETURN 2 END")},
				FunctionsRemoved: difftypes.FunctionChanges{function("dbo.f", "BEGIN RETURN 1 END")},
			},
			want: "the plan creates function DBO.f and drops function dbo.f, which differ only in case, so under a " +
				"case-insensitive collation the drop would remove the object the create makes",
		},
		{
			name:    "a MySQL routine created and dropped under one name",
			dialect: platform.MySQL,
			diff: difftypes.SchemaDiff{
				FunctionsAdded: difftypes.FunctionChanges{{Function: schemamodel.Function{
					Name: "foo", Returns: "INT", Language: "SQL", Body: "RETURN 2",
				}}},
				FunctionsRemoved: difftypes.FunctionChanges{{Function: schemamodel.Function{
					Name: "foo", Returns: "INT", Language: "SQL", Body: "RETURN 1",
				}}},
			},
			want: "the plan creates function foo and drops function foo, which are one routine on mysql",
		},
		{
			// Every planner creates a schema only where none exists and drops
			// it after, so the schema the plan keeps is dropped.
			name:    "a SQL Server schema created and dropped under one spelling",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				SchemasAdded:   []schemamodel.Schema{{Name: "sales"}},
				SchemasRemoved: []string{"sales"},
			},
			want: "the plan creates schema sales and drops schema sales, which are one schema, so the drop would " +
				"remove the schema the create keeps",
		},
		{
			name:    "a PostgreSQL schema created and dropped under one spelling",
			dialect: platform.Postgres,
			diff: difftypes.SchemaDiff{
				SchemasAdded:   []schemamodel.Schema{{Name: "sales"}},
				SchemasRemoved: []string{"sales"},
			},
			want: "the plan creates schema sales and drops schema sales, which are one schema",
		},
		{
			// A MySQL schema is a database, and its drop is DROP DATABASE.
			name:    "a MySQL database created and dropped under one spelling",
			dialect: platform.MySQL,
			diff: difftypes.SchemaDiff{
				SchemasAdded:   []schemamodel.Schema{{Name: "sales"}},
				SchemasRemoved: []string{"sales"},
			},
			want: "the plan creates schema sales and drops schema sales, which are one schema",
		},
		{
			// SQL Server compares a role name under the database collation,
			// and CREATE ROLE [APP] beside the role App fails with error
			// 15023 under a case-insensitive one.
			name:    "a role whose name differs only in case offline",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				RolesAdded:   difftypes.RoleChanges{{Name: "APP"}},
				RolesRemoved: difftypes.RoleChanges{{Name: "App"}},
			},
			want: "the plan creates role APP and drops role App, which differ only in case, so under a case-insensitive " +
				"collation the create would fail while the role the plan drops exists",
		},
		{
			name:    "a role a server reads as one name",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				IdentifierSemantics: sqlServerCollation("Latin1_General_100_CI_AS", []string{"dbo"}, []string{"APP", "App"}),
				RolesAdded:          difftypes.RoleChanges{{Name: "APP"}},
				RolesRemoved:        difftypes.RoleChanges{{Name: "App"}},
			},
			want: "the plan creates role APP and drops role App, and sqlserver reads them as one name, so the create " +
				"would fail while the role the plan drops exists",
		},
		{
			name:    "two non-ASCII roles offline",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				RolesAdded:   difftypes.RoleChanges{{Name: "Z\u00e4hler"}},
				RolesRemoved: difftypes.RoleChanges{{Name: "Gr\u00f6\u00dfe"}},
			},
			want: "the plan creates role Z\u00e4hler and drops role Gr\u00f6\u00dfe, and without a connection Ptah " +
				"cannot tell whether they are one name under the database collation, so the create may fail",
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
			// The control for the offline ASCII row: a server that was asked
			// put the two names in two classes.
			name:    "two views a server put apart",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				IdentifierSemantics: sqlServerCollation("SQL_Latin1_General_CP1_CI_AS",
					[]string{"dbo"}, []string{"recent"}, []string{"archived"}),
				ViewsAdded:   difftypes.ViewChanges{{Name: "recent", Body: "SELECT 2 AS a"}},
				ViewsRemoved: difftypes.ViewChanges{{Name: "dbo.archived", Body: "SELECT 1 AS a"}},
			},
		},
		{
			// The control for the contraction rows: a server of another
			// collation put ccs and cscs in two classes.
			name:    "a contraction a Latin server reads as two names",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				IdentifierSemantics: sqlServerCollation("SQL_Latin1_General_CP1_CI_AS", []string{"dbo"}, []string{"ccs"}, []string{"cscs"}),
				FunctionsAdded:      difftypes.FunctionChanges{function("dbo.cscs", "BEGIN RETURN 2 END")},
				FunctionsRemoved:    difftypes.FunctionChanges{function("dbo.ccs", "BEGIN RETURN 1 END")},
			},
		},
		{
			// CREATE VIEW fails while the view exists, so a view created and
			// dropped under one spelling is not judged here.
			name:    "a SQL Server view created and dropped under one spelling",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				ViewsAdded:   difftypes.ViewChanges{{Name: "recent", Body: "SELECT 2 AS a"}},
				ViewsRemoved: difftypes.ViewChanges{{Name: "dbo.recent", Body: "SELECT 1 AS a"}},
			},
		},
		{
			// A role is created with CREATE ROLE, which fails while a namesake
			// exists, so nothing is lost, and offline two ASCII names that
			// differ beyond case are two names.
			name:    "two ASCII roles offline",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				RolesAdded:   difftypes.RoleChanges{{Name: "reader"}},
				RolesRemoved: difftypes.RoleChanges{{Name: "writer"}},
			},
		},
		{
			// The control for the resolved role row.
			name:    "two roles a case-sensitive server put apart",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				IdentifierSemantics: sqlServerCollation("Latin1_General_100_CS_AS", []string{"dbo"}, []string{"APP"}, []string{"App"}),
				RolesAdded:          difftypes.RoleChanges{{Name: "APP"}},
				RolesRemoved:        difftypes.RoleChanges{{Name: "App"}},
			},
		},
		{
			// PostgreSQL states how it compares a role name.
			name:    "a PostgreSQL role whose name differs only in case",
			dialect: platform.Postgres,
			diff: difftypes.SchemaDiff{
				RolesAdded:   difftypes.RoleChanges{{Name: "APP"}},
				RolesRemoved: difftypes.RoleChanges{{Name: "app"}},
			},
		},
		{
			// MySQL keeps functions and procedures in two namespaces, so a
			// function and a procedure of one name are two routines.
			name:    "a MySQL procedure created where a function of its name is dropped",
			dialect: platform.MySQL,
			diff: difftypes.SchemaDiff{
				FunctionsAdded: difftypes.FunctionChanges{{Function: schemamodel.Function{
					Name: "foo", Kind: schemamodel.FunctionKindProcedure, Language: "SQL", Body: "SELECT 2",
				}}},
				FunctionsRemoved: difftypes.FunctionChanges{{Function: schemamodel.Function{
					Name: "foo", Returns: "INT", Language: "SQL", Body: "RETURN 1",
				}}},
			},
		},
		{
			// The control for the cross-kind rows: under a case-sensitive
			// collation REPORT and Report are two names.
			name:    "a procedure beside a view the plan drops under a case-sensitive collation",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				IdentifierSemantics: sqlServerCollation("Latin1_General_100_CS_AS", []string{"dbo"}, []string{"REPORT"}, []string{"Report"}),
				FunctionsAdded:      difftypes.FunctionChanges{procedure("dbo.REPORT", "BEGIN SELECT 2 END")},
				ViewsRemoved:        difftypes.ViewChanges{{Name: "dbo.Report", Body: "SELECT 1 AS a"}},
			},
		},
		{
			// Two ASCII names of two kinds that differ beyond case are two
			// names offline: a create beside the other object fails on the
			// server, and nothing is lost.
			name:    "a table beside a view of another name the plan drops, offline",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				TablesAdded:  difftypes.TableChanges{{Name: "orders"}},
				ViewsRemoved: difftypes.ViewChanges{{Name: "dbo.recent_orders", Body: "SELECT 1 AS a"}},
			},
		},
		{
			// SQL Server's shared namespace is its descriptor's, and the
			// PostgreSQL descriptor states none.
			name:    "a PostgreSQL function replaced by a procedure of one name",
			dialect: platform.Postgres,
			diff: difftypes.SchemaDiff{
				FunctionsAdded: difftypes.FunctionChanges{{Function: schemamodel.Function{
					Name: "x", Kind: schemamodel.FunctionKindProcedure, Language: "sql", Body: "SELECT 1",
				}}},
				FunctionsRemoved: difftypes.FunctionChanges{{Function: schemamodel.Function{
					Name: "x", Returns: "integer", Language: "sql", Body: "SELECT 1",
				}}},
			},
		},
		{
			// PostgreSQL keeps routines apart from relations.
			name:    "a PostgreSQL function named as a view the plan drops",
			dialect: platform.Postgres,
			diff: difftypes.SchemaDiff{
				FunctionsAdded: difftypes.FunctionChanges{{Function: schemamodel.Function{
					Name: "report", Returns: "integer", Language: "sql", Body: "SELECT 1",
				}}},
				ViewsRemoved: difftypes.ViewChanges{{Name: "public.report", Body: "SELECT 1 AS a"}},
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
			name:    "a name a snapshot does not cover beside a drop in another schema",
			dialect: platform.SQLServer,
			diff: difftypes.SchemaDiff{
				IdentifierSemantics: sqlServerCollation("SQL_Latin1_General_CP1_CI_AS",
					[]string{"dbo"}, []string{"sales"}, []string{"orders"}),
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
			// The control for the routine rows spelled alike: a routine
			// created beside one of its name that is dropped is an overload,
			// which PostgreSQL addresses by its argument list.
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

// SQL Server compares names under the database collation, so semantics of
// another shape say which names are one by a rule the server does not follow.
// The namesake checks would trust them: under the exact rule MIXEDCASE and
// MixedCase are two procedures, and the plan would drop the one it changes.
func TestGenerateSchemaDiffAST_RefusesSemanticsThatIgnoreTheCatalog_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		comparison identifier.Comparison
	}{
		{name: "the exact rule", comparison: identifier.ComparisonExact},
		{name: "an ASCII fold", comparison: identifier.ComparisonASCIIInsensitive},
		{name: "a fold that leaves other names unknown", comparison: identifier.ComparisonASCIIFoldedNonASCIIUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := difftypes.SchemaDiff{
				IdentifierSemantics: &identifier.Semantics{
					DefaultSchema:  "dbo",
					IndexNamespace: identifier.IndexNamespaceTable,
					TableNames:     test.comparison,
					ColumnNames:    test.comparison,
					IndexNames:     test.comparison,
				},
				FunctionsAdded:    difftypes.FunctionChanges{procedure("MIXEDCASE", "BEGIN SELECT 2 END")},
				ProceduresRemoved: difftypes.FunctionChanges{procedure("dbo.MixedCase", "BEGIN SELECT 1 END")},
			}

			nodes, err := planner.GenerateSchemaDiffAST(t.Context(), must.Must(builtin.New()), &diff, platform.SQLServer)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*the identifier semantics compare table names by the "`+string(test.comparison)+
				`" rule, and sqlserver compares names under the database collation; plan with the semantics a `+
				`connection resolved, or with none.*`)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// The controls: the offline rules of a target whose catalog compares names,
// a snapshot a connection resolved, and the exact rule on a target that
// states it.
func TestGenerateSchemaDiffAST_PlansSemanticsOfTheTargetsShape_HappyPath(t *testing.T) {
	offline := identifier.ForDialect(platform.SQLServer)
	tests := []struct {
		name      string
		dialect   string
		semantics *identifier.Semantics
	}{
		{name: "the offline SQL Server rules", dialect: platform.SQLServer, semantics: &offline},
		{name: "a resolved SQL Server snapshot", dialect: platform.SQLServer, semantics: caseInsensitiveProcedures()},
		{name: "the exact rule on PostgreSQL", dialect: platform.Postgres, semantics: new(identifier.ForDialect(platform.Postgres))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := difftypes.SchemaDiff{
				IdentifierSemantics: test.semantics,
				ViewsAdded:          difftypes.ViewChanges{{Name: "recent", Body: "SELECT 1 AS a"}},
			}

			_, err := planner.GenerateSchemaDiffAST(t.Context(), must.Must(builtin.New()), &diff, test.dialect)

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
