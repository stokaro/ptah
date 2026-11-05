package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgtype"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// The routines the rows below remove: reader depends on the table archive, and
// the others reach it by calling reader.
var (
	archiveReader = styleRoutine("reader", "", "bigint", "RETURN (SELECT count(*) FROM archive)")
	readerCaller  = styleRoutine("caller", "", "bigint", "RETURN reader() + 1")
	archiveTable  = styleTable("archive", styleKey("id"))
)

// defaultCalling is a column named code whose default calls routine.
func defaultCalling(routine string) catalog.Column {
	return catalog.Column{Name: "code", DataType: "bigint", UDTName: "int8", IsNullable: "YES", ColumnDefault: new(routine + "()")}
}

// routineProcedure is routine as a procedure.
func routineProcedure(routine catalog.Function) catalog.Function {
	routine.Kind = schemamodel.FunctionKindProcedure
	return routine
}

// removedRoutineCallersRows are removals in which a routine that a removed
// relation or column depends on has something calling it. Each drop goes ahead
// of what it depends on and after what calls it, so nothing relies on CASCADE,
// which would also take a view in a schema the run leaves out.
var removedRoutineCallersRows = []struct {
	name   string
	change func(*catalog.Database)
	want   []string
}{
	{
		name: "a function calls the reader of a removed table",
		change: func(h *catalog.Database) {
			h.Tables = append(h.Tables, archiveTable)
			h.Functions = []catalog.Function{archiveReader, readerCaller}
		},
		want: []string{`DROP FUNCTION "public"."caller"()`, `DROP FUNCTION "public"."reader"()`, `DROP TABLE "public"."archive"`},
	},
	{
		name: "a chain of callers",
		change: func(h *catalog.Database) {
			h.Tables = append(h.Tables, archiveTable)
			h.Functions = []catalog.Function{
				archiveReader,
				readerCaller,
				styleRoutine("top", "", "bigint", "RETURN caller() + 1"),
			}
		},
		want: []string{
			`DROP FUNCTION "public"."top"()`,
			`DROP FUNCTION "public"."caller"()`,
			`DROP FUNCTION "public"."reader"()`,
			`DROP TABLE "public"."archive"`,
		},
	},
	{
		name: "a procedure calls the reader",
		change: func(h *catalog.Database) {
			h.Tables = append(h.Tables, archiveTable)
			h.Functions = []catalog.Function{
				archiveReader,
				routineProcedure(styleRoutine("run_it", "", "", "BEGIN ATOMIC SELECT reader(); END")),
			}
		},
		want: []string{`DROP PROCEDURE "public"."run_it"()`, `DROP FUNCTION "public"."reader"()`, `DROP TABLE "public"."archive"`},
	},
	{
		name: "a routine reads two removed tables",
		change: func(h *catalog.Database) {
			h.Tables = append(h.Tables, archiveTable, styleTable("ledger", styleKey("id")))
			h.Functions = []catalog.Function{
				styleRoutine("both", "", "bigint", "RETURN (SELECT count(*) FROM archive) + (SELECT count(*) FROM ledger)"),
			}
		},
		want: []string{`DROP FUNCTION "public"."both"()`, `DROP TABLE "public"."archive"`, `DROP TABLE "public"."ledger"`},
	},
	{
		name: "a routine reads a removed view and a removed table",
		change: func(h *catalog.Database) {
			h.Tables = append(h.Tables, archiveTable)
			h.Views = []catalog.View{itemSkusView}
			h.Functions = []catalog.Function{
				styleRoutine("both", "", "bigint", "RETURN (SELECT count(*) FROM archive) + (SELECT count(*) FROM item_skus)"),
			}
		},
		want: []string{`DROP FUNCTION "public"."both"()`, `DROP VIEW "public"."item_skus"`, `DROP TABLE "public"."archive"`},
	},
	{
		name: "a function calls the reader of a dropped column",
		change: func(h *catalog.Database) {
			h.Tables[0].Columns = append(h.Tables[0].Columns, styleColumn("email"))
			h.Functions = []catalog.Function{
				styleRoutine("reader", "", "bigint", "RETURN (SELECT count(email) FROM owners)"),
				readerCaller,
			}
		},
		want: []string{`DROP FUNCTION "public"."caller"()`, `DROP FUNCTION "public"."reader"()`, `ALTER TABLE "owners" DROP COLUMN "email"`},
	},
	{
		name: "two readers call each other",
		change: func(h *catalog.Database) {
			h.Tables = append(h.Tables, archiveTable)
			h.Functions = []catalog.Function{
				styleRoutine("reader", "", "bigint", "RETURN (SELECT count(*) FROM archive) + caller()"),
				readerCaller,
			}
		},
		want: []string{`DROP FUNCTION "public"."caller"(), "public"."reader"()`, `DROP TABLE "public"."archive"`},
	},
	{
		name: "a function calls a routine that takes the removed table's row",
		change: func(h *catalog.Database) {
			h.Tables = append(h.Tables, archiveTable)
			h.Functions = []catalog.Function{
				styleRoutine("reader", "r archive", "bigint", "RETURN r.id"),
				styleRoutine("caller", "", "bigint", "RETURN reader(NULL::archive)"),
			}
		},
		want: []string{`DROP FUNCTION "public"."caller"()`, `DROP FUNCTION "public"."reader"(r archive)`, `DROP TABLE "public"."archive"`},
	},
	{
		name: "a removed table's default calls the reader of a table it would drop after",
		change: func(h *catalog.Database) {
			h.Tables = append(h.Tables, archiveTable, styleTable("orders", styleKey("id"), defaultCalling("reader")))
			h.Functions = []catalog.Function{archiveReader}
		},
		want: []string{`DROP TABLE "public"."orders"`, `DROP FUNCTION "public"."reader"()`, `DROP TABLE "public"."archive"`},
	},
	{
		name: "a dropped column's default calls the reader of a removed table",
		change: func(h *catalog.Database) {
			h.Tables[0].Columns = append(h.Tables[0].Columns, defaultCalling("reader"))
			h.Tables = append(h.Tables, archiveTable)
			h.Functions = []catalog.Function{archiveReader}
		},
		want: []string{`ALTER TABLE "owners" DROP COLUMN "code"`, `DROP FUNCTION "public"."reader"()`, `DROP TABLE "public"."archive"`},
	},
	{
		name: "a routine reads a generated column that leaves ahead of the views",
		change: func(h *catalog.Database) {
			h.Tables[0].Columns = append(h.Tables[0].Columns, catalog.Column{
				Name: "code", DataType: "bigint", UDTName: "int8", IsNullable: "YES", GeneratedExpression: new("reader(id)"),
			})
			h.Views = []catalog.View{itemSkusView}
			h.Functions = []catalog.Function{
				styleRoutine("reader", "n integer", "bigint", "RETURN (SELECT count(*) FROM item_skus) + n"),
				styleRoutine("reads_code", "", "bigint", "RETURN (SELECT count(code) FROM owners)"),
			}
		},
		want: []string{
			`DROP FUNCTION "public"."reads_code"()`,
			`ALTER TABLE "owners" DROP COLUMN "code"`,
			`DROP FUNCTION "public"."reader"(n integer)`,
			`DROP VIEW "public"."item_skus"`,
		},
	},
	{
		name: "a removed view calls the reader of a removed table",
		change: func(h *catalog.Database) {
			h.Tables = append(h.Tables, archiveTable)
			h.Views = []catalog.View{{Name: "calling", Schema: "public", Body: "SELECT reader() AS n"}}
			h.Functions = []catalog.Function{archiveReader}
		},
		want: []string{`DROP VIEW "public"."calling"`, `DROP FUNCTION "public"."reader"()`, `DROP TABLE "public"."archive"`},
	},
	{
		name: "a removed view calls the reader of a removed view it would drop after",
		change: func(h *catalog.Database) {
			h.Views = []catalog.View{itemSkusView, {Name: "z_calling", Schema: "public", Body: "SELECT reader() AS n"}}
			h.Functions = []catalog.Function{styleRoutine("reader", "", "bigint", "RETURN (SELECT count(*) FROM item_skus)")}
		},
		want: []string{`DROP VIEW "public"."z_calling"`, `DROP FUNCTION "public"."reader"()`, `DROP VIEW "public"."item_skus"`},
	},
	{
		name: "a view with a column of the routine's name does not call it",
		change: func(h *catalog.Database) {
			h.Views = []catalog.View{{Name: "totals", Schema: "public", Body: "SELECT count(*) AS total FROM items"}}
			h.Functions = []catalog.Function{styleRoutine("total", "", "bigint", "RETURN (SELECT total FROM totals)")}
		},
		want: []string{`DROP FUNCTION "public"."total"()`, `DROP VIEW "public"."totals"`},
	},
	{
		name: "a body in quotes holds nothing back, so its routine stays in the routine step",
		change: func(h *catalog.Database) {
			h.Tables = append(h.Tables, archiveTable)
			h.Functions = []catalog.Function{
				archiveReader,
				quotedRoutine("caller", "", "plpgsql", "begin return reader(); end"),
			}
		},
		want: []string{`DROP FUNCTION "public"."reader"()`, `DROP TABLE "public"."archive"`, `DROP FUNCTION "public"."caller"()`},
	},
}

// TestRemovedRoutineCallers_HappyPath pins the plan of each row in the exact
// style. A row whose caller stayed in the routine step, or whose reader did,
// plans `DROP TABLE "public"."archive" CASCADE`.
func TestRemovedRoutineCallers_HappyPath(t *testing.T) {
	for _, test := range removedRoutineCallersRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := stylePlan(c, styleDeclared(), styleHeldWith(test.change),
				planext.Options{Capabilities: capability.ForDialect(platform.Postgres), Statements: planext.StatementStyleExact})
			c.Assert(drops(got), qt.DeepEquals, test.want)
		})
	}
}

// TestRemovedRoutineCallers_OnCockroachDBAQuotedBodyHoldsItsReads is the engine
// that records what a quoted body reads and calls.
func TestRemovedRoutineCallers_OnCockroachDBAQuotedBodyHoldsItsReads(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	held := styleHeldWith(func(h *catalog.Database) {
		h.Tables = append(h.Tables, archiveTable)
		h.Functions = []catalog.Function{
			quotedRoutine("reader", "", "plpgsql", "begin return (select count(*) from archive); end"),
			quotedRoutine("caller", "", "plpgsql", "begin return reader(); end"),
		}
	})
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), styleDeclared(), held,
		catalog.ServerInfo{Dialect: platform.CockroachDB, Schema: "public"}, nil, runtime)
	c.Assert(err, qt.IsNil)

	got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, platform.CockroachDB,
		planext.Options{Capabilities: capability.ForDialect(platform.CockroachDB), Statements: planext.StatementStyleExact})

	c.Assert(err, qt.IsNil)
	c.Assert(drops(got), qt.DeepEquals, []string{`DROP FUNCTION "public"."caller"()`, `DROP FUNCTION "public"."reader"()`, `DROP TABLE "public"."archive"`})
}

// TestRemovedRoutineCallers_TheAnnotatedStyleKeepsTheRoutineStep is the control:
// that style writes CASCADE on every drop and removes routines after the tables.
func TestRemovedRoutineCallers_TheAnnotatedStyleKeepsTheRoutineStep(t *testing.T) {
	c := qt.New(t)
	got := stylePlan(c, styleDeclared(), styleHeldWith(func(h *catalog.Database) {
		h.Tables = append(h.Tables, archiveTable)
		h.Functions = []catalog.Function{archiveReader, readerCaller}
	}), planext.Options{Capabilities: capability.ForDialect(platform.Postgres), Statements: planext.StatementStyleAnnotated})
	c.Assert(got, qt.DeepEquals, []string{
		"-- WARNING: This will delete all data!\nDROP TABLE IF EXISTS \"public\".\"archive\" CASCADE",
		"-- WARNING: Ensure no other objects depend on this function\nDROP FUNCTION IF EXISTS \"public\".\"caller\"()",
		"-- WARNING: Ensure no other objects depend on this function\nDROP FUNCTION IF EXISTS \"public\".\"reader\"()",
	})
}

// removedRoutineRefusal is the refusal each row of the failure path reaches.
func removedRoutineRefusal(c *qt.C, held *catalog.Database, declared *schemamodel.Database) error {
	c.Helper()
	runtime := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), declared, held,
		catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public"}, nil, runtime)
	c.Assert(err, qt.IsNil)
	got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, platform.Postgres,
		planext.Options{Capabilities: capability.ForDialect(platform.Postgres), Statements: planext.StatementStyleExact})
	c.Assert(got, qt.IsNil)
	return err
}

// TestRemovedRoutineCallers_FailurePath removes a routine that has to leave after
// something that calls it and before something it depends on, where the plan
// cannot drop the first before the second. The table steps follow the view and
// column steps, and a domain leaves with the types, after the tables.
func TestRemovedRoutineCallers_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		change func(*catalog.Database)
		want   string
	}{
		{
			name: "a table's default calls the reader of that table",
			change: func(h *catalog.Database) {
				h.Tables = append(h.Tables, styleTable("archive", styleKey("id"), defaultCalling("reader")))
				h.Functions = []catalog.Function{archiveReader}
			},
			want: `table public\.archive calls routine public\.reader, which depends on table public\.archive`,
		},
		{
			name: "two removed tables whose defaults call the reader of the other",
			change: func(h *catalog.Database) {
				h.Tables = append(h.Tables,
					styleTable("archive", styleKey("id"), defaultCalling("ledger_reader")),
					styleTable("ledger", styleKey("id"), defaultCalling("reader")))
				h.Functions = []catalog.Function{
					archiveReader,
					styleRoutine("ledger_reader", "", "bigint", "RETURN (SELECT count(*) FROM ledger)"),
				}
			},
			want: `table public\.archive calls routine public\.ledger_reader, which depends on table public\.ledger`,
		},
		{
			name: "a removed table's default calls the reader of a removed view",
			change: func(h *catalog.Database) {
				h.Tables = append(h.Tables, styleTable("orders", styleKey("id"), defaultCalling("reader")))
				h.Views = []catalog.View{itemSkusView}
				h.Functions = []catalog.Function{styleRoutine("reader", "", "bigint", "RETURN (SELECT count(*) FROM item_skus)")}
			},
			want: `table public\.orders calls routine public\.reader, which depends on view public\.item_skus`,
		},
		{
			name: "a removed table's default calls the caller of a reader of a dropped column",
			change: func(h *catalog.Database) {
				h.Tables[0].Columns = append(h.Tables[0].Columns, styleColumn("email"))
				h.Tables = append(h.Tables, styleTable("orders", styleKey("id"), defaultCalling("caller")))
				h.Functions = []catalog.Function{
					styleRoutine("reader", "", "bigint", "RETURN (SELECT count(email) FROM owners)"),
					readerCaller,
				}
			},
			want: `table public\.orders calls routine public\.caller, which depends on column owners\.email`,
		},
		{
			name: "a dropped column's default calls the reader of a removed view",
			change: func(h *catalog.Database) {
				h.Tables[0].Columns = append(h.Tables[0].Columns, defaultCalling("reader"))
				h.Views = []catalog.View{itemSkusView}
				h.Functions = []catalog.Function{styleRoutine("reader", "", "bigint", "RETURN (SELECT count(*) FROM item_skus)")}
			},
			want: `column owners\.code calls routine public\.reader, which depends on view public\.item_skus`,
		},
		{
			name: "a removed view calls the reader of itself",
			change: func(h *catalog.Database) {
				h.Views = []catalog.View{{Name: "calling", Schema: "public", Body: "SELECT reader() AS n"}}
				h.Functions = []catalog.Function{styleRoutine("reader", "", "bigint", "RETURN (SELECT count(*) FROM calling)")}
			},
			want: `view public\.calling calls routine public\.reader, which depends on view public\.calling`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := removedRoutineRefusal(c, styleHeldWith(test.change), styleDeclared())
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s)unsupported feature: `+test.want+`, and the plan has no order .*`)
		})
	}
}

// TestRemovedRoutineCallers_ADomainThatCallsTheReaderIsRefused removes a domain
// whose CHECK calls a routine that reads a removed table. The domain leaves with
// the types, after the tables, and the routine would have to leave between them.
func TestRemovedRoutineCallers_ADomainThatCallsTheReaderIsRefused(t *testing.T) {
	c := qt.New(t)
	declared := styleDeclared()
	declared.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Desired))
	held := styleHeldWith(func(h *catalog.Database) {
		h.Tables = append(h.Tables, archiveTable)
		h.Functions = []catalog.Function{styleRoutine("reader", "n integer", "boolean", "RETURN (SELECT count(*) FROM archive) >= n")}
		h.FeatureObjects = must.Must(schemaext.NewObjects(schemaext.Object{
			Ref: pgtype.Ref(pgtype.DomainKind, "public", "d"), Value: &pgtype.ObservedDomain{BaseType: "integer", Check: "reader(VALUE)"},
		}))
		h.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Observed))
	})

	err := removedRoutineRefusal(c, held, declared)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s)unsupported feature: domain d calls routine public\.reader, which depends on table public\.archive, .*`)
}
