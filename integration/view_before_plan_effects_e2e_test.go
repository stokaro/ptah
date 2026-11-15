//go:build integration

package integration_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A view a plan creates records what the database held before the plan, beside
// the statement's own before, which a create never has. A consumer tells a
// view the plan makes again from a new one by it, also where the drop that took
// the view records no effect (stokaro/ptah#6023). The planner test
// TestDescribe_PostgreSQLPlanRecordsTheViewsItTakes pins a plan whose drop
// records none; this test holds what the record says to the server.

// beforePlanUnit is a statement of a saved plan, or a unit of an apply
// document, read the way a consumer reads it.
type beforePlanUnit struct {
	SQL     string `json:"sql"`
	Effects []struct {
		Object struct {
			Kind   string `json:"kind"`
			Schema string `json:"schema"`
			Name   string `json:"name"`
		} `json:"object"`
		Action     string `json:"action"`
		Properties []struct {
			Name  string `json:"name"`
			After *bool  `json:"after"`
		} `json:"properties"`
		BeforePlan *struct {
			Exists     bool `json:"exists"`
			Properties []struct {
				Name  string `json:"name"`
				Value bool   `json:"value"`
			} `json:"properties"`
		} `json:"before_plan"`
	} `json:"effects"`
}

// beforePlanDocument is the part of a saved plan or an apply document that
// carries effects.
type beforePlanDocument struct {
	Outcome    string           `json:"outcome"`
	Statements []beforePlanUnit `json:"statements"`
	Units      []beforePlanUnit `json:"units"`
}

// recreatedView is what a plan says about one view a CREATE VIEW makes:
// whether the view existed before the plan and the security_invoker it held
// then, and the security_invoker the statement gives it.
type recreatedView struct {
	Name          string
	ExistedBefore bool
	InvokerBefore bool
	InvokerAfter  bool
}

// createdViews reads the effect of each CREATE VIEW of the units, by view.
func createdViews(units []beforePlanUnit) map[string]recreatedView {
	created := make(map[string]recreatedView)
	for _, unit := range units {
		for _, effect := range unit.Effects {
			if effect.Action != "create" || effect.Object.Kind != "view" || effect.BeforePlan == nil {
				continue
			}
			view := recreatedView{Name: effect.Object.Name, ExistedBefore: effect.BeforePlan.Exists}
			for _, property := range effect.BeforePlan.Properties {
				if property.Name == "security_invoker" {
					view.InvokerBefore = property.Value
				}
			}
			for _, property := range effect.Properties {
				if property.Name == "security_invoker" && property.After != nil {
					view.InvokerAfter = *property.After
				}
			}
			created[view.Name] = view
		}
	}
	return created
}

// TestRecreatedViewRecordsWhatItHeldBeforeThePlanE2E plans a lost column on
// two views that hold security_invoker=true, one reading the other, and a new
// view, applies the saved plan, and reads what the plan and the apply document
// say about each view the plan creates. The two it creates again record that
// they existed before the plan, with the security_invoker pg_class.reloptions
// held for them then; the new one records that it did not exist. The value
// each create gives its view is the one the server holds after the plan.
func TestRecreatedViewRecordsWhatItHeldBeforeThePlanE2E(t *testing.T) {
	c := qt.New(t)
	target := newViewSecurityTarget(c, viewSecurityEngine{name: "postgresql", engine: dbtarget.PostgreSQL}, "security_invoker = true")
	execPostgresFamilySQL(c, c.Context(), target.url,
		"CREATE VIEW my_doc_ids WITH (security_invoker = true) AS SELECT id, owner_name FROM my_docs")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(
		"CREATE TABLE docs (id integer PRIMARY KEY, owner_name text NOT NULL);\n"+
			"ALTER TABLE docs ENABLE ROW LEVEL SECURITY;\n"+
			"CREATE POLICY docs_owner ON docs USING ("+viewSecurityPolicy+");\n"+
			"CREATE VIEW my_docs WITH (security_invoker = true) AS "+viewSecurityNarrow+";\n"+
			"CREATE VIEW my_doc_ids WITH (security_invoker = true) AS SELECT id FROM my_docs;\n"+
			"CREATE VIEW my_new_ids WITH (security_invoker = true) AS SELECT id FROM docs;\n"), 0o600), qt.IsNil)
	planPath := filepath.Join(c.TempDir(), "views.plan.json")
	before := heldInvokers(c, target)

	planned := runPtahIn(c, "schema", "plan", "--db-url", target.url, "--schema-file", schema, "--output", planPath)
	saved, err := os.ReadFile(planPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
	applied := runPtahIn(c, "schema", "apply", "--db-url", target.url, "--plan", planPath, "--auto-approve", "--json")

	assertRunExited(c, planned, 0)
	assertRunExited(c, applied, 0)
	var plan, apply beforePlanDocument
	c.Assert(json.Unmarshal(saved, &plan), qt.IsNil, qt.Commentf("%s", saved))
	c.Assert(json.Unmarshal([]byte(applied.Stdout), &apply), qt.IsNil, qt.Commentf("%s", applied.Stdout))
	c.Assert(apply.Outcome, qt.Equals, "applied")
	after := heldInvokers(c, target)
	want := map[string]recreatedView{
		"my_docs":    {Name: "my_docs", ExistedBefore: true, InvokerBefore: before["my_docs"], InvokerAfter: after["my_docs"]},
		"my_doc_ids": {Name: "my_doc_ids", ExistedBefore: true, InvokerBefore: before["my_doc_ids"], InvokerAfter: after["my_doc_ids"]},
		"my_new_ids": {Name: "my_new_ids", ExistedBefore: false, InvokerAfter: after["my_new_ids"]},
	}
	c.Assert(before, qt.DeepEquals, map[string]bool{"my_docs": true, "my_doc_ids": true})
	c.Assert(after, qt.DeepEquals, map[string]bool{"my_docs": true, "my_doc_ids": true, "my_new_ids": true})
	c.Assert(createdViews(plan.Statements), qt.DeepEquals, want, qt.Commentf("%s", saved))
	c.Assert(createdViews(apply.Units), qt.DeepEquals, want, qt.Commentf("%s", applied.Stdout))
}

// heldInvokers reads back the security_invoker pg_class.reloptions holds for
// each of the fixture's views that exists, by name.
func heldInvokers(c *qt.C, target viewSecurityTarget) map[string]bool {
	c.Helper()
	rows, err := target.db.QueryContext(c.Context(), `
		SELECT relname, COALESCE('security_invoker=true' = ANY (reloptions), false)
		FROM pg_class WHERE relname IN ('my_docs', 'my_doc_ids', 'my_new_ids') AND relkind = 'v'`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	held := make(map[string]bool)
	for rows.Next() {
		var (
			name string
			on   bool
		)
		c.Assert(rows.Scan(&name, &on), qt.IsNil)
		held[name] = on
	}
	c.Assert(rows.Err(), qt.IsNil)
	return held
}
