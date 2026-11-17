package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/planext"
)

const (
	ownTotalsToReporting = `ALTER MATERIALIZED VIEW "order_totals" OWNER TO "reporting"`
	ownReportToReporting = `ALTER VIEW "order_report" OWNER TO "reporting"`
	ownReportToAuditors  = `ALTER VIEW "order_report" OWNER TO "Audit Team"`
	ownTotalsToAuditors  = `ALTER MATERIALIZED VIEW "order_totals" OWNER TO "Audit Team"`
	replaceReportWide    = "CREATE OR REPLACE VIEW \"order_report\" AS\nSELECT total, 1 AS one FROM order_totals"
	createReportWide     = "CREATE VIEW \"order_report\" AS\nSELECT total, 1 AS one FROM order_totals"
)

// TestViewRebuild_KeepsTheOwnerItFound_HappyPath pins the owner of the views
// a rebuild drops and creates again. A declared owner is given to the view
// the plan creates, right after it and before its privileges. A view whose
// declaration names no owner gets back the owner the read reported, so the
// rebuild leaves it where it found it rather than with the role that runs the
// plan; it needs no statement when the read reported that role, or no owner.
func TestViewRebuild_KeepsTheOwnerItFound_HappyPath(t *testing.T) {
	tests := []struct {
		name           string
		live, declared rebuildState
		want           []string
	}{
		{
			name: "an undeclared owner another role holds",
			live: rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}, connected: "postgres",
				owners: map[string]string{"order_totals": "reporting", "order_report": "reporting"}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReport}},
			want: []string{
				`DROP VIEW "order_report"`, `DROP MATERIALIZED VIEW "order_totals"`,
				createTotalsChanged, ownTotalsToReporting, createReport, ownReportToReporting,
			},
		},
		{
			name: "an undeclared owner the connected role holds",
			live: rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}, connected: "reporting",
				owners: map[string]string{"order_totals": "reporting", "order_report": "reporting"}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReport}},
			want:     []string{`DROP VIEW "order_report"`, `DROP MATERIALIZED VIEW "order_totals"`, createTotalsChanged, createReport},
		},
		{
			name:     "an owner the read did not report",
			live:     rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}, connected: "postgres"},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReport}},
			want:     []string{`DROP VIEW "order_report"`, `DROP MATERIALIZED VIEW "order_totals"`, createTotalsChanged, createReport},
		},
		{
			name: "a declared owner the view already has",
			live: rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}, connected: "postgres",
				owners: map[string]string{"order_totals": "reporting", "order_report": "reporting"}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReport},
				owners: map[string]string{"order_totals": "reporting", "order_report": "reporting"}},
			want: []string{
				`DROP VIEW "order_report"`, `DROP MATERIALIZED VIEW "order_totals"`,
				createTotalsChanged, ownTotalsToReporting, createReport, ownReportToReporting,
			},
		},
		{
			name: "a declared owner the view does not have",
			live: rebuildState{skuType: liveSKU, objects: []rebuildObject{orderTotals, orderReport}, connected: "postgres",
				owners: map[string]string{"order_totals": "reporting", "order_report": "reporting"}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotalsChanged, orderReport},
				owners: map[string]string{"order_totals": "Audit Team", "order_report": "Audit Team"}},
			want: []string{
				`DROP VIEW "order_report"`, `DROP MATERIALIZED VIEW "order_totals"`,
				createTotalsChanged, ownTotalsToAuditors, createReport, ownReportToAuditors,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := planRebuild(c, planext.StatementStyleExact, test.live, test.declared)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestViewOwner_ChangesInPlace_HappyPath pins the plan of a declared owner
// that differs from the one the read reported, on a view the plan keeps: one
// ALTER ... OWNER TO, which keeps a materialized view's rows. A view whose
// declaration names no owner is unmanaged, whoever holds it, and an owner the
// read did not report is given as declared. A new view gets its owner right
// after it is created, and a view the plan replaces in place keeps its owner
// unless the declaration changes it.
func TestViewOwner_ChangesInPlace_HappyPath(t *testing.T) {
	reporting := map[string]string{"order_totals": "reporting", "order_report": "reporting"}
	auditors := map[string]string{"order_totals": "Audit Team", "order_report": "Audit Team"}
	tests := []struct {
		name           string
		live, declared rebuildState
		want           []string
	}{
		{
			name:     "a changed owner",
			live:     rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotals, orderReport}, owners: reporting},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotals, orderReport}, owners: auditors},
			want:     []string{ownReportToAuditors, ownTotalsToAuditors},
		},
		{
			name:     "the owner the view has",
			live:     rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotals, orderReport}, owners: reporting},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotals, orderReport}, owners: reporting},
			want:     make([]string, 0),
		},
		{
			name:     "no declared owner",
			live:     rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotals, orderReport}, owners: reporting},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotals, orderReport}},
			want:     make([]string, 0),
		},
		{
			name:     "an owner the read did not report",
			live:     rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotals, orderReport}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotals, orderReport}, owners: auditors},
			want:     []string{ownReportToAuditors, ownTotalsToAuditors},
		},
		{
			name:     "a new view",
			live:     rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotals}},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotals, orderReport}, owners: map[string]string{"order_report": "Audit Team"}},
			want:     []string{createReport, ownReportToAuditors},
		},
		{
			name:     "a view replaced in place with a changed owner",
			live:     rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotals, orderReport}, owners: reporting},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotals, orderReportWide}, owners: map[string]string{"order_report": "Audit Team"}},
			want:     []string{replaceReportWide, ownReportToAuditors},
		},
		{
			name:     "a view replaced in place with the owner it has",
			live:     rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotals, orderReport}, owners: reporting},
			declared: rebuildState{skuType: declaredSKU, objects: []rebuildObject{orderTotals, orderReportWide}, owners: reporting},
			want:     []string{replaceReportWide},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := planRebuild(c, planext.StatementStyleExact, test.live, test.declared)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
