package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgtype"
)

// styleRoutine is a function the database holds.
func styleRoutine(name, parameters, returns, body string) catalog.Function {
	return catalog.Function{Name: name, Schema: "public", Parameters: parameters, Returns: returns,
		Language: "sql", Volatility: "STABLE", Body: body, IdentityArguments: new(parameters)}
}

// itemSkusView is a view over items.sku, as the database holds it.
var itemSkusView = catalog.View{Name: "item_skus", Schema: "public", Body: "SELECT id, sku FROM items"}

// removedViewWithRoutine removes item_skus together with a routine the
// database holds.
func removedViewWithRoutine(routine catalog.Function) styleChange {
	return styleChange{declared: styleDeclared(), held: styleHeldWith(func(h *catalog.Database) {
		h.Views = []catalog.View{itemSkusView}
		h.Functions = []catalog.Function{routine}
	})}
}

// removedViewWithRoutineCalled removes item_skus, a routine the database
// holds, and the view sku_list, which calls list_skus.
func removedViewWithRoutineCalled(routine catalog.Function) styleChange {
	return styleChange{declared: styleDeclared(), held: styleHeldWith(func(h *catalog.Database) {
		h.Views = []catalog.View{itemSkusView, {Name: "sku_list", Schema: "public", Body: "SELECT sku FROM list_skus()"}}
		h.Functions = []catalog.Function{routine}
	})}
}

// removedViewAndTableWithRoutine removes item_skus and the table archive
// together with a routine.
func removedViewAndTableWithRoutine(routine catalog.Function) styleChange {
	return styleChange{declared: styleDeclared(), held: styleHeldWith(func(h *catalog.Database) {
		h.Tables = append(h.Tables, styleTable("archive", styleKey("id")))
		h.Views = []catalog.View{itemSkusView}
		h.Functions = []catalog.Function{routine}
	})}
}

// removedViewWithRoutineADefaultCalls removes item_skus, the routine
// next_sku, whose SQL-standard body reads it, and the table orders, whose
// column default calls next_sku.
func removedViewWithRoutineADefaultCalls() styleChange {
	return styleChange{declared: styleDeclared(), held: styleHeldWith(func(h *catalog.Database) {
		h.Views = []catalog.View{itemSkusView}
		h.Functions = []catalog.Function{styleRoutine("next_sku", "", "integer", "BEGIN ATOMIC SELECT count(*)::int FROM item_skus; END")}
		h.Tables = append(h.Tables, styleTable("orders", styleKey("id"),
			catalog.Column{Name: "code", DataType: "integer", UDTName: "int4", IsNullable: "YES", ColumnDefault: new("next_sku()")}))
	})}
}

// removedViewWithRoutineBesideACheck removes item_skus and a routine that
// returns its rows, and a CHECK on owners, which the plan keeps.
func removedViewWithRoutineBesideACheck(routine catalog.Function) styleChange {
	change := removedViewWithRoutine(routine)
	change.held.Constraints = append(change.held.Constraints, catalog.Constraint{Name: "owners_name_check", TableName: "owners",
		Schema: "public", Type: "CHECK", ColumnName: "name", ColumnNames: []string{"name"}, CheckClause: new("(name <> '')")})
	return change
}

// removedTableWithRoutine removes the table archive together with a routine.
func removedTableWithRoutine(routine catalog.Function) styleChange {
	return styleChange{declared: styleDeclared(), held: styleHeldWith(func(h *catalog.Database) {
		h.Tables = append(h.Tables, styleTable("archive", styleKey("id")))
		h.Functions = []catalog.Function{routine}
	})}
}

// droppedColumnWithRoutine drops owners.email and removes a routine.
func droppedColumnWithRoutine(routine catalog.Function) styleChange {
	return styleChange{declared: styleDeclared(), held: styleHeldWith(func(h *catalog.Database) {
		h.Tables[0].Columns = append(h.Tables[0].Columns, styleColumn("email"))
		h.Functions = []catalog.Function{routine}
	})}
}

// droppedColumnBesideDomain drops owners.email and adds the domain
// positive_int, a feature change that removes nothing.
func droppedColumnBesideDomain() styleChange {
	declared := styleDeclared()
	declared.FeatureObjects = must.Must(schemaext.NewObjects(schemaext.Object{
		Ref: pgtype.Ref(pgtype.DomainKind, "public", "positive_int"), Value: &pgtype.DesiredDomain{BaseType: "integer"},
	}))
	return styleChange{declared: declared, held: styleHeldWith(func(h *catalog.Database) {
		h.Tables[0].Columns = append(h.Tables[0].Columns, styleColumn("email"))
		h.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Observed))
	})}
}

// droppedColumnBesideRemovedDomain drops owners.email and removes the domain
// positive_int, which reads no table.
func droppedColumnBesideRemovedDomain() styleChange {
	declared := styleDeclared()
	declared.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Desired))
	return styleChange{declared: declared, held: styleHeldWith(func(h *catalog.Database) {
		h.Tables[0].Columns = append(h.Tables[0].Columns, styleColumn("email"))
		h.FeatureObjects = must.Must(schemaext.NewObjects(schemaext.Object{
			Ref: pgtype.Ref(pgtype.DomainKind, "public", "positive_int"), Value: &pgtype.ObservedDomain{BaseType: "integer"},
		}))
		h.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Observed))
	})}
}

// droppedColumnBesideComposite drops owners.email and removes the composite
// type wrap, whose attribute r is the row type of owners.
func droppedColumnBesideComposite() styleChange {
	declared := styleDeclared()
	declared.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Desired))
	return styleChange{declared: declared, held: styleHeldWith(func(h *catalog.Database) {
		h.Tables[0].Columns = append(h.Tables[0].Columns, styleColumn("email"))
		h.FeatureObjects = must.Must(schemaext.NewObjects(schemaext.Object{
			Ref:   pgtype.Ref(pgtype.CompositeKind, "public", "wrap"),
			Value: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "r", Type: "owners"}}},
		}))
		h.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Observed))
	})}
}

// removedTableBesideComposite removes the table archive and the composite
// type wrap, whose attribute r is of type rowType.
func removedTableBesideComposite(rowType string) styleChange {
	declared := styleDeclared()
	declared.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Desired))
	return styleChange{declared: declared, held: styleHeldWith(func(h *catalog.Database) {
		h.Tables = append(h.Tables, styleTable("archive", styleKey("id")))
		h.FeatureObjects = must.Must(schemaext.NewObjects(schemaext.Object{
			Ref: pgtype.Ref(pgtype.CompositeKind, "public", "wrap"),
			Value: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{
				{Name: "r", Type: rowType}, {Name: "n", Type: "integer"},
			}},
		}))
		h.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Observed))
	})}
}

// droppedColumnUnderTrigger drops owners.email, which a trigger the
// declaration removes fires on.
func droppedColumnUnderTrigger() styleChange {
	return styleChange{declared: styleDeclared(), held: styleHeldWith(func(h *catalog.Database) {
		h.Tables[0].Columns = append(h.Tables[0].Columns, styleColumn("email"))
		h.Triggers = []catalog.Trigger{{Name: "owners_email_audit", Schema: "public", Table: "owners", Timing: "BEFORE",
			Event: "UPDATE OF email", ForEach: "ROW", ExecuteFunction: "audit_email"}}
	})}
}

// droppedGeneratedPair drops owners.email and owners.email_lower, which is
// computed from it.
func droppedGeneratedPair() styleChange {
	return styleChange{declared: styleDeclared(), held: styleHeldWith(func(h *catalog.Database) {
		generated := styleColumn("email_lower")
		generated.GeneratedExpression = new("lower((email)::text)")
		generated.GeneratedKind = "STORED"
		h.Tables[0].Columns = append(h.Tables[0].Columns, styleColumn("email"), generated)
	})}
}

// observedPolicy is a policy on table whose USING clause is using.
func observedPolicy(table, name, using string) schemaext.Object {
	return must.Must(pgpolicy.ObservedPolicyObject(pgpolicy.PolicyRef("", table, name),
		pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}},
			Using: new(using), Composition: pgpolicy.Permissive}))
}

// removedTablesWithPolicy removes allowed and docs, whose policy reads
// allowed, as a first migration's rollback does.
func removedTablesWithPolicy() styleChange {
	declared := styleDeclared()
	declared.FeatureCoverage = must.Must(pgpolicy.CompleteCoverage(schemaext.Desired))
	return styleChange{declared: declared, held: styleHeldWith(func(h *catalog.Database) {
		docs := styleTable("docs", styleKey("id"))
		docs.Facets = must.Must(schemaext.NewFacets(&pgpolicy.ObservedTableState{Enabled: true}))
		h.Tables = append(h.Tables, styleTable("allowed", styleKey("id"), styleColumn("who")), docs)
		h.FeatureObjects = must.Must(schemaext.NewObjects(observedPolicy("docs", "docs_allowed",
			"(EXISTS ( SELECT 1 FROM allowed WHERE ((allowed.who)::text = CURRENT_USER)))")))
		h.FeatureCoverage = must.Must(pgpolicy.CompleteCoverage(schemaext.Observed))
	})}
}

// droppedColumnUnderRemovedPolicy drops allowed.who, which a policy of the
// removed table docs reads.
func droppedColumnUnderRemovedPolicy() styleChange {
	declared := styleDeclaredWith(func(d *schemamodel.Database) {
		d.Tables = append(d.Tables, schemamodel.Table{StructName: "Allowed", Name: "allowed"})
		d.Fields = append(d.Fields, schemamodel.Field{StructName: "Allowed", Name: "id", Type: "integer", Primary: true})
	})
	declared.FeatureCoverage = must.Must(pgpolicy.CompleteCoverage(schemaext.Desired))
	return styleChange{declared: declared, held: styleHeldWith(func(h *catalog.Database) {
		docs := styleTable("docs", styleKey("id"))
		docs.Facets = must.Must(schemaext.NewFacets(&pgpolicy.ObservedTableState{Enabled: true}))
		h.Tables = append(h.Tables, styleTable("allowed", styleKey("id"), styleColumn("who")), docs)
		h.FeatureObjects = must.Must(schemaext.NewObjects(observedPolicy("docs", "docs_allowed",
			"(EXISTS ( SELECT 1 FROM allowed WHERE ((allowed.who)::text = CURRENT_USER)))")))
		h.FeatureCoverage = must.Must(pgpolicy.CompleteCoverage(schemaext.Observed))
	})}
}

// removedTablesWithPolicyCycle removes a and b, each with a policy that reads
// the other.
func removedTablesWithPolicyCycle() styleChange {
	declared := styleDeclared()
	declared.FeatureCoverage = must.Must(pgpolicy.CompleteCoverage(schemaext.Desired))
	return styleChange{declared: declared, held: styleHeldWith(func(h *catalog.Database) {
		for _, name := range []string{"a", "b"} {
			table := styleTable(name, styleKey("id"), styleColumn("who"))
			table.Facets = must.Must(schemaext.NewFacets(&pgpolicy.ObservedTableState{Enabled: true}))
			h.Tables = append(h.Tables, table)
		}
		h.FeatureObjects = must.Must(schemaext.NewObjects(
			observedPolicy("a", "pa", "(EXISTS ( SELECT 1 FROM b WHERE (b.who = CURRENT_USER)))"),
			observedPolicy("b", "pb", "(EXISTS ( SELECT 1 FROM a WHERE (a.who = CURRENT_USER)))"),
		))
		h.FeatureCoverage = must.Must(pgpolicy.CompleteCoverage(schemaext.Observed))
	})}
}

// removedViewWithPolicy removes the view current_owners and the table docs,
// whose policy reads the view.
func removedViewWithPolicy() styleChange {
	declared := styleDeclared()
	declared.FeatureCoverage = must.Must(pgpolicy.CompleteCoverage(schemaext.Desired))
	return styleChange{declared: declared, held: styleHeldWith(func(h *catalog.Database) {
		docs := styleTable("docs", styleKey("id"), styleColumn("owner_id"))
		docs.Facets = must.Must(schemaext.NewFacets(&pgpolicy.ObservedTableState{Enabled: true}))
		h.Tables = append(h.Tables, docs)
		h.Views = []catalog.View{{Name: "current_owners", Schema: "public", Body: "SELECT id FROM owners"}}
		h.FeatureObjects = must.Must(schemaext.NewObjects(observedPolicy("docs", "docs_owned",
			"((owner_id)::text IN ( SELECT (current_owners.id)::text FROM current_owners))")))
		h.FeatureCoverage = must.Must(pgpolicy.CompleteCoverage(schemaext.Observed))
	})}
}

// enumStatusHeld is the enum status and the table tickets(id, s status
// DEFAULT 'a') as the database holds them.
func enumStatusHeld(h *catalog.Database) {
	h.Enums = append(h.Enums, catalog.Enum{Name: "status", Schema: "public", Values: []string{"a", "b"}})
	h.Tables = append(h.Tables, styleTable("tickets", styleKey("id"),
		catalog.Column{Name: "s", DataType: "USER-DEFINED", UDTName: "status", IsNullable: "YES", ColumnDefault: new("'a'::status")}))
}

// enumColumnBecomesText turns tickets.s into text and removes the enum.
func enumColumnBecomesText() styleChange {
	return styleChange{declared: styleDeclaredWith(func(d *schemamodel.Database) {
		d.Tables = append(d.Tables, schemamodel.Table{StructName: "Ticket", Name: "tickets"})
		d.Fields = append(d.Fields,
			schemamodel.Field{StructName: "Ticket", Name: "id", Type: "integer", Primary: true},
			schemamodel.Field{StructName: "Ticket", Name: "s", Type: "text", Nullable: true, Default: "a", DefaultSet: true})
	}), held: styleHeldWith(enumStatusHeld)}
}

// textColumnBecomesEnum turns tickets.s, a text column with a default, into
// the enum status.
func textColumnBecomesEnum() styleChange {
	return styleChange{declared: styleDeclaredWith(func(d *schemamodel.Database) {
		d.Enums = append(d.Enums, schemamodel.Enum{Name: "status", Values: []string{"a", "b"}})
		d.Tables = append(d.Tables, schemamodel.Table{StructName: "Ticket", Name: "tickets"})
		d.Fields = append(d.Fields,
			schemamodel.Field{StructName: "Ticket", Name: "id", Type: "integer", Primary: true},
			schemamodel.Field{StructName: "Ticket", Name: "s", Type: "status", Nullable: true, Default: "a", DefaultSet: true})
	}), held: styleHeldWith(func(h *catalog.Database) {
		h.Enums = append(h.Enums, catalog.Enum{Name: "status", Schema: "public", Values: []string{"a", "b"}})
		h.Tables = append(h.Tables, styleTable("tickets", styleKey("id"),
			catalog.Column{Name: "s", DataType: "text", UDTName: "text", IsNullable: "YES", ColumnDefault: new("'a'::text")}))
	})}
}

// textColumnBecomesDomainOverEnum turns tickets.s, a text column with a
// default, into the domain dstatus over the enum status.
func textColumnBecomesDomainOverEnum() styleChange {
	return styleChange{declared: styleDeclaredWith(func(d *schemamodel.Database) {
		d.Enums = append(d.Enums, schemamodel.Enum{Name: "status", Values: []string{"a", "b"}})
		d.FeatureObjects = must.Must(schemaext.NewObjects(schemaext.Object{
			Ref: pgtype.Ref(pgtype.DomainKind, "", "dstatus"), Value: &pgtype.DesiredDomain{BaseType: "status"},
		}))
		d.Tables = append(d.Tables, schemamodel.Table{StructName: "Ticket", Name: "tickets"})
		d.Fields = append(d.Fields,
			schemamodel.Field{StructName: "Ticket", Name: "id", Type: "integer", Primary: true},
			schemamodel.Field{StructName: "Ticket", Name: "s", Type: "dstatus", Nullable: true, Default: "a", DefaultSet: true})
	}), held: styleHeldWith(func(h *catalog.Database) {
		h.Enums = append(h.Enums, catalog.Enum{Name: "status", Schema: "public", Values: []string{"a", "b"}})
		h.Tables = append(h.Tables, styleTable("tickets", styleKey("id"),
			catalog.Column{Name: "s", DataType: "text", UDTName: "text", IsNullable: "YES", ColumnDefault: new("'a'::text")}))
		h.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Observed))
	})}
}

// textArrayBecomesEnumArray turns tickets.s, a text[] column with a default,
// into an array of the enum status.
func textArrayBecomesEnumArray() styleChange {
	return styleChange{declared: styleDeclaredWith(func(d *schemamodel.Database) {
		d.Enums = append(d.Enums, schemamodel.Enum{Name: "status", Values: []string{"a", "b"}})
		d.Tables = append(d.Tables, schemamodel.Table{StructName: "Ticket", Name: "tickets"})
		d.Fields = append(d.Fields,
			schemamodel.Field{StructName: "Ticket", Name: "id", Type: "integer", Primary: true},
			schemamodel.Field{StructName: "Ticket", Name: "s", Type: "status[]", Nullable: true, Default: "'{a}'", DefaultSet: true})
	}), held: styleHeldWith(func(h *catalog.Database) {
		h.Enums = append(h.Enums, catalog.Enum{Name: "status", Schema: "public", Values: []string{"a", "b"}})
		h.Tables = append(h.Tables, styleTable("tickets", styleKey("id"),
			catalog.Column{Name: "s", DataType: "ARRAY", UDTName: "_text", IsNullable: "YES", ColumnDefault: new("'{a}'::text[]")}))
	})}
}

// integerColumnBecomesBigint widens tickets.n, an integer column with a
// default, to bigint.
func integerColumnBecomesBigint() styleChange {
	return styleChange{declared: styleDeclaredWith(func(d *schemamodel.Database) {
		d.Tables = append(d.Tables, schemamodel.Table{StructName: "Ticket", Name: "tickets"})
		d.Fields = append(d.Fields,
			schemamodel.Field{StructName: "Ticket", Name: "id", Type: "integer", Primary: true},
			schemamodel.Field{StructName: "Ticket", Name: "n", Type: "bigint", Nullable: true, Default: "1", DefaultSet: true})
	}), held: styleHeldWith(func(h *catalog.Database) {
		h.Tables = append(h.Tables, styleTable("tickets", styleKey("id"),
			catalog.Column{Name: "n", DataType: "integer", UDTName: "int4", IsNullable: "YES", ColumnDefault: new("1")}))
	})}
}

// domainColumnBecomesText turns tickets.s, a column of the domain dstatus over
// the enum status with a default, into text, and removes both types.
func domainColumnBecomesText() styleChange {
	return styleChange{declared: styleDeclaredWith(func(d *schemamodel.Database) {
		d.Tables = append(d.Tables, schemamodel.Table{StructName: "Ticket", Name: "tickets"})
		d.Fields = append(d.Fields,
			schemamodel.Field{StructName: "Ticket", Name: "id", Type: "integer", Primary: true},
			schemamodel.Field{StructName: "Ticket", Name: "s", Type: "text", Nullable: true, Default: "a", DefaultSet: true})
		d.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Desired))
	}), held: styleHeldWith(func(h *catalog.Database) {
		h.Enums = append(h.Enums, catalog.Enum{Name: "status", Schema: "public", Values: []string{"a", "b"}})
		h.Tables = append(h.Tables, styleTable("tickets", styleKey("id"),
			catalog.Column{Name: "s", DataType: "USER-DEFINED", UDTName: "dstatus", IsNullable: "YES", ColumnDefault: new("'a'::status")}))
		h.FeatureObjects = must.Must(schemaext.NewObjects(schemaext.Object{
			Ref: pgtype.Ref(pgtype.DomainKind, "public", "dstatus"), Value: &pgtype.ObservedDomain{BaseType: "status"},
		}))
		h.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Observed))
	})}
}

// matviewIndexRemovedUnderRetype retypes items.sku under the materialized
// view sku_counts and removes its index.
func matviewIndexRemovedUnderRetype() styleChange {
	return styleChange{declared: styleDeclaredWith(func(d *schemamodel.Database) {
		d.Fields[3].Type = "VARCHAR(128)"
		d.MaterializedViews = []schemamodel.MaterializedView{{Name: "sku_counts", Body: "SELECT sku, count(*) AS n FROM items GROUP BY sku"}}
	}), held: styleHeldWith(func(h *catalog.Database) {
		h.MatViews = []catalog.MaterializedView{{Name: "sku_counts", Schema: "public", Body: "SELECT sku, count(*) AS n FROM items GROUP BY sku"}}
		h.Indexes = append(h.Indexes, catalog.Index{Name: "sku_counts_n_idx", TableName: "sku_counts", Schema: "public", Columns: []string{"n"}})
	})}
}

// retypedUnderViewWithRoutine retypes items.sku under item_skus and removes a
// routine that returns its rows.
func retypedUnderViewWithRoutine(routine catalog.Function) styleChange {
	return styleChange{declared: styleDeclaredWith(func(d *schemamodel.Database) {
		d.Fields[3].Type = "VARCHAR(128)"
		d.Views = []schemamodel.View{{Name: "item_skus", Body: "SELECT id, sku FROM items"}}
	}), held: styleHeldWith(func(h *catalog.Database) {
		h.Views = []catalog.View{itemSkusView}
		h.Functions = []catalog.Function{routine}
	})}
}

// viewDroppedWithRoutine swaps the columns of item_skus, which CREATE OR
// REPLACE VIEW refuses, and removes a routine that returns its rows.
func viewDroppedWithRoutine(routine catalog.Function) styleChange {
	return styleChange{declared: styleDeclaredWith(func(d *schemamodel.Database) {
		d.Views = []schemamodel.View{{Name: "item_skus", Body: "SELECT sku, id FROM items"}}
	}), held: styleHeldWith(func(h *catalog.Database) {
		h.Views = []catalog.View{itemSkusView}
		h.Functions = []catalog.Function{routine}
	})}
}

// matviewUnderViewDroppedWithCascade swaps the columns of owner_names, which
// the materialized view owner_counts reads.
func matviewUnderViewDroppedWithCascade() styleChange {
	return styleChange{declared: styleDeclaredWith(func(d *schemamodel.Database) {
		d.Views = []schemamodel.View{{Name: "owner_names", Body: "SELECT name, id FROM owners"}}
		d.MaterializedViews = []schemamodel.MaterializedView{{Name: "owner_counts", Body: "SELECT count(*) AS n FROM owner_names"}}
	}), held: styleHeldWith(func(h *catalog.Database) {
		h.Views = []catalog.View{{Name: "owner_names", Schema: "public", Body: "SELECT id, name FROM owners"}}
		h.MatViews = []catalog.MaterializedView{{Name: "owner_counts", Schema: "public", Body: "SELECT count(*) AS n FROM owner_names"}}
	})}
}

// TestGenerateSchemaDiffSQLStatementsWithOptions_ExactStyleReliance_HappyPath
// pins one row for each reason the exact style keeps CASCADE or a guard, each
// beside a row where the reason is absent.
//
// A removed routine that takes or returns the row type of the one relation the
// plan drops, a removed one, is dropped just ahead of it, so neither drop
// relies on CASCADE. One that another removed view calls, or that names two
// dropped relations, is removed only after every relation, so the relation's
// drop relies on CASCADE and the routine's drop keeps IF EXISTS; one that only
// names the relation in a quoted body holds nothing back. A routine with a
// SQL-standard body that reads a table holds the table's columns. A policy of
// a removed table that reads another removed table puts its own table first,
// two that read each other's tables put both tables in one DROP TABLE, and one
// that reads a removed view, which goes before every table, makes the view's
// drop rely on CASCADE. A domain or a composite type the plan removes holds a
// table back only when it names the table's row type.
// A removed trigger and a generated column leave before the column they read.
// A column that leaves an enum takes its declared default again, so the enum's
// drop finds nothing that uses it; one that becomes an enum drops its default
// around the TYPE clause. An index on a materialized view the rebuild dropped
// keeps IF EXISTS. A routine that returns the rows of a view the rebuild
// drops is dropped before it, so neither needs CASCADE, and so is a
// materialized view over a view the plan cannot replace in place.
func TestGenerateSchemaDiffSQLStatementsWithOptions_ExactStyleReliance_HappyPath(t *testing.T) {
	setof := styleRoutine("list_skus", "", "SETOF item_skus", "SELECT * FROM item_skus")
	rowParameter := styleRoutine("sku_of", "r item_skus", "character varying", "SELECT r.sku")
	quotedBody := styleRoutine("count_skus", "", "bigint", "SELECT count(*) FROM item_skus")
	tests := []struct {
		name   string
		change styleChange
		want   []string
	}{
		{name: "a view removed with a routine that returns its rows", change: removedViewWithRoutine(setof), want: []string{
			`DROP FUNCTION "public"."list_skus"()`,
			`DROP VIEW "public"."item_skus"`,
		}},
		{name: "a view removed with a routine that takes its row", change: removedViewWithRoutine(rowParameter), want: []string{
			`DROP FUNCTION "public"."sku_of"(r item_skus)`,
			`DROP VIEW "public"."item_skus"`,
		}},
		{name: "a view removed with a routine that returns its rows and that another removed view calls",
			change: removedViewWithRoutineCalled(setof),
			want: []string{
				`DROP VIEW "public"."item_skus" CASCADE`,
				`DROP VIEW IF EXISTS "public"."sku_list"`,
				`DROP FUNCTION IF EXISTS "public"."list_skus"()`,
			}},
		{name: "a view removed with a routine that returns its rows, beside a CHECK the plan removes",
			change: removedViewWithRoutineBesideACheck(setof),
			want: []string{
				`ALTER TABLE "public"."owners" DROP CONSTRAINT "owners_name_check"`,
				`DROP FUNCTION "public"."list_skus"()`,
				`DROP VIEW "public"."item_skus"`,
			}},
		{name: "a view removed with a routine whose body reads it and that a removed table's default calls",
			change: removedViewWithRoutineADefaultCalls(),
			want: []string{
				`DROP VIEW "public"."item_skus" CASCADE`,
				`DROP TABLE IF EXISTS "public"."orders"`,
				`DROP FUNCTION IF EXISTS "public"."next_sku"()`,
			}},
		{name: "a view and a table removed with a routine that names both",
			change: removedViewAndTableWithRoutine(styleRoutine("pair", "r item_skus", "SETOF archive", "SELECT * FROM archive")),
			want: []string{
				`DROP VIEW "public"."item_skus" CASCADE`,
				`DROP TABLE IF EXISTS "public"."archive" CASCADE`,
				`DROP FUNCTION IF EXISTS "public"."pair"(r item_skus)`,
			}},
		{name: "a view removed with a routine whose parameter is named after it",
			change: removedViewWithRoutine(styleRoutine("by_sku", "item_skus integer", "integer", "SELECT item_skus")),
			want: []string{
				`DROP VIEW "public"."item_skus"`,
				`DROP FUNCTION "public"."by_sku"(item_skus integer)`,
			}},
		{name: "a view removed with a routine whose result column is named after it",
			change: removedViewWithRoutine(styleRoutine("skus", "", "TABLE(item_skus integer)", "SELECT 1")),
			want: []string{
				`DROP VIEW "public"."item_skus"`,
				`DROP FUNCTION "public"."skus"()`,
			}},
		{name: "a view removed with a routine that takes a row of the same name in another schema",
			change: removedViewWithRoutine(styleRoutine("audit_sku", "r audit.item_skus", "integer", "SELECT 1")),
			want: []string{
				`DROP VIEW "public"."item_skus"`,
				`DROP FUNCTION "public"."audit_sku"(r audit.item_skus)`,
			}},
		{name: "a view removed with a routine that reads it in a quoted body", change: removedViewWithRoutine(quotedBody), want: []string{
			`DROP VIEW "public"."item_skus"`,
			`DROP FUNCTION "public"."count_skus"()`,
		}},
		{name: "a view a column type change rebuilds, with a removed routine that returns its rows",
			change: retypedUnderViewWithRoutine(setof),
			want: []string{
				`DROP FUNCTION "public"."list_skus"()`,
				`DROP VIEW "item_skus"`,
				`ALTER TABLE "items" ALTER COLUMN "sku" TYPE VARCHAR(128)`,
				"CREATE VIEW \"item_skus\" AS\nSELECT id, sku FROM items",
			}},
		{name: "a view the plan cannot replace, with a removed routine that returns its rows",
			change: viewDroppedWithRoutine(setof),
			want: []string{
				`DROP FUNCTION "public"."list_skus"()`,
				`DROP VIEW "item_skus"`,
				"CREATE VIEW \"item_skus\" AS\nSELECT sku, id FROM items",
			}},
		{name: "a table removed with a routine that returns its rows",
			change: removedTableWithRoutine(styleRoutine("list_archive", "", "SETOF archive", "SELECT * FROM archive")),
			want: []string{
				`DROP FUNCTION "public"."list_archive"()`,
				`DROP TABLE "public"."archive"`,
			}},
		{name: "a table removed with a routine whose SQL-standard body reads it",
			change: removedTableWithRoutine(styleRoutine("count_archive", "", "bigint", "RETURN (SELECT count(*) FROM archive)")),
			want: []string{
				`DROP FUNCTION "public"."count_archive"()`,
				`DROP TABLE "public"."archive"`,
			}},
		{name: "a view removed with a routine whose result table has a column of its row type",
			change: removedViewWithRoutine(styleRoutine("sku_rows", "", "TABLE(r item_skus)", "SELECT 1")),
			want: []string{
				`DROP FUNCTION "public"."sku_rows"()`,
				`DROP VIEW "public"."item_skus"`,
			}},
		{name: "a table removed with a routine that does not name it",
			change: removedTableWithRoutine(styleRoutine("one", "", "integer", "SELECT 1")),
			want: []string{
				`DROP TABLE "public"."archive"`,
				`DROP FUNCTION "public"."one"()`,
			}},
		{name: "a column dropped beside a routine whose SQL-standard body reads its table",
			change: droppedColumnWithRoutine(styleRoutine("owner_emails", "", "SETOF character varying", "BEGIN ATOMIC SELECT email FROM owners; END")),
			want: []string{
				`DROP FUNCTION "public"."owner_emails"()`,
				`ALTER TABLE "owners" DROP COLUMN "email"`,
			}},
		{name: "a column dropped beside a routine whose SQL-standard body reads another column",
			change: droppedColumnWithRoutine(styleRoutine("owner_names", "", "SETOF character varying", "BEGIN ATOMIC SELECT name FROM owners; END")),
			want: []string{
				`ALTER TABLE "owners" DROP COLUMN "email"`,
				`DROP FUNCTION "public"."owner_names"()`,
			}},
		{name: "a column dropped beside a routine whose SQL-standard body selects every column",
			change: droppedColumnWithRoutine(styleRoutine("owner_rows", "", "SETOF owners", "BEGIN ATOMIC SELECT * FROM owners; END")),
			want: []string{
				`DROP FUNCTION "public"."owner_rows"()`,
				`ALTER TABLE "owners" DROP COLUMN "email"`,
			}},
		{name: "a column dropped beside a routine whose quoted body reads its table",
			change: droppedColumnWithRoutine(styleRoutine("owner_emails", "", "SETOF character varying", "SELECT email FROM owners")),
			want: []string{
				`ALTER TABLE "owners" DROP COLUMN "email"`,
				`DROP FUNCTION "public"."owner_emails"()`,
			}},
		{name: "a column dropped beside a domain the plan adds", change: droppedColumnBesideDomain(), want: []string{
			`CREATE DOMAIN "public"."positive_int" AS integer`,
			`ALTER TABLE "owners" DROP COLUMN "email"`,
		}},
		// The type owner writes its own drop without a guard or CASCADE in
		// exact style (stokaro/ptah#5000).
		{name: "a column dropped beside a domain the plan removes", change: droppedColumnBesideRemovedDomain(), want: []string{
			`ALTER TABLE "owners" DROP COLUMN "email"`,
			"-- WARNING: Make sure no columns use this domain!\nDROP DOMAIN \"public\".\"positive_int\"",
		}},
		{name: "a column dropped beside a composite type that names its table's row type",
			change: droppedColumnBesideComposite(),
			want: []string{
				`ALTER TABLE "owners" DROP COLUMN "email"`,
				"-- WARNING: Make sure no columns use this composite type!\nDROP TYPE \"public\".\"wrap\"",
			}},
		{name: "a table removed beside a composite type that names another type",
			change: removedTableBesideComposite("text"),
			want: []string{
				`DROP TABLE "public"."archive"`,
				"-- WARNING: Make sure no columns use this composite type!\nDROP TYPE \"public\".\"wrap\"",
			}},
		{name: "a table removed beside a composite type that names its row type",
			change: removedTableBesideComposite("archive"),
			want: []string{
				`DROP TABLE "public"."archive" CASCADE`,
				"-- WARNING: Make sure no columns use this composite type!\nDROP TYPE \"public\".\"wrap\"",
			}},
		{name: "a column dropped under a trigger the declaration removes", change: droppedColumnUnderTrigger(), want: []string{
			`DROP TRIGGER "owners_email_audit" ON "public"."owners"`,
			`ALTER TABLE "owners" DROP COLUMN "email"`,
		}},
		{name: "a column dropped with the generated column computed from it", change: droppedGeneratedPair(), want: []string{
			`ALTER TABLE "owners" DROP COLUMN "email_lower"`,
			`ALTER TABLE "owners" DROP COLUMN "email"`,
		}},
		{name: "a table removed with the table its policy reads", change: removedTablesWithPolicy(), want: []string{
			`DROP POLICY "docs_allowed" ON "docs"`,
			`DROP TABLE "public"."docs"`,
			`DROP TABLE "public"."allowed"`,
		}},
		{name: "a column a policy of a removed table reads", change: droppedColumnUnderRemovedPolicy(), want: []string{
			`DROP POLICY "docs_allowed" ON "docs"`,
			`ALTER TABLE "allowed" DROP COLUMN "who"`,
			`DROP TABLE "public"."docs"`,
		}},
		{name: "two tables removed whose policies read each other", change: removedTablesWithPolicyCycle(), want: []string{
			`DROP POLICY "pa" ON "a"`,
			`DROP POLICY "pb" ON "b"`,
			`DROP TABLE "public"."b", "public"."a"`,
		}},
		{name: "a view removed with a table whose policy reads it", change: removedViewWithPolicy(), want: []string{
			`DROP POLICY "docs_owned" ON "docs"`,
			`DROP VIEW "public"."current_owners"`,
			`DROP TABLE "public"."docs"`,
		}},
		{name: "a column that leaves an enum", change: enumColumnBecomesText(), want: []string{
			`ALTER TABLE "tickets" ALTER COLUMN "s" TYPE text`,
			`ALTER TABLE "tickets" ALTER COLUMN "s" SET DEFAULT 'a'`,
			`DROP TYPE "public"."status"`,
		}},
		{name: "a column that becomes an enum", change: textColumnBecomesEnum(), want: []string{
			`ALTER TABLE "tickets" ALTER COLUMN "s" DROP DEFAULT`,
			`ALTER TABLE "tickets" ALTER COLUMN "s" TYPE status USING "s"::text::status`,
			`ALTER TABLE "tickets" ALTER COLUMN "s" SET DEFAULT 'a'`,
		}},
		{name: "a column that becomes an array of an enum", change: textArrayBecomesEnumArray(), want: []string{
			`ALTER TABLE "tickets" ALTER COLUMN "s" DROP DEFAULT`,
			`ALTER TABLE "tickets" ALTER COLUMN "s" TYPE status[] USING "s"::text[]::status[]`,
			`ALTER TABLE "tickets" ALTER COLUMN "s" SET DEFAULT '{a}'`,
		}},
		{name: "a column that leaves a domain over an enum", change: domainColumnBecomesText(), want: []string{
			`ALTER TABLE "tickets" ALTER COLUMN "s" TYPE text`,
			`ALTER TABLE "tickets" ALTER COLUMN "s" SET DEFAULT 'a'`,
			"-- WARNING: Make sure no columns use this domain!\nDROP DOMAIN \"public\".\"dstatus\"",
			`DROP TYPE IF EXISTS "public"."status"`,
		}},
		{name: "a column retyped between built-in types", change: integerColumnBecomesBigint(), want: []string{
			`ALTER TABLE "tickets" ALTER COLUMN "n" TYPE bigint`,
		}},
		{name: "a column that becomes a domain over an enum", change: textColumnBecomesDomainOverEnum(), want: []string{
			`CREATE DOMAIN "dstatus" AS status`,
			`ALTER TABLE "tickets" ALTER COLUMN "s" DROP DEFAULT`,
			`ALTER TABLE "tickets" ALTER COLUMN "s" TYPE dstatus USING "s"::text::dstatus`,
			`ALTER TABLE "tickets" ALTER COLUMN "s" SET DEFAULT 'a'`,
		}},
		{name: "an index removed from a materialized view the rebuild drops", change: matviewIndexRemovedUnderRetype(), want: []string{
			`DROP MATERIALIZED VIEW "sku_counts"`,
			`ALTER TABLE "items" ALTER COLUMN "sku" TYPE VARCHAR(128)`,
			"CREATE MATERIALIZED VIEW \"sku_counts\" AS\nSELECT sku, count(*) AS n FROM items GROUP BY sku",
			`DROP INDEX IF EXISTS "public"."sku_counts_n_idx"`,
		}},
		{name: "a materialized view over a view the plan cannot replace", change: matviewUnderViewDroppedWithCascade(), want: []string{
			`DROP MATERIALIZED VIEW "owner_counts"`,
			`DROP VIEW "owner_names"`,
			"CREATE VIEW \"owner_names\" AS\nSELECT name, id FROM owners",
			"CREATE MATERIALIZED VIEW \"owner_counts\" AS\nSELECT count(*) AS n FROM owner_names",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			opts := tt.change.opts
			opts.Statements = planext.StatementStyleExact

			got := stylePlan(c, tt.change.declared, tt.change.held, opts)

			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}
