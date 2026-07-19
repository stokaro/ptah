package migrate

import (
	"bytes"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/safety"
)

func TestRenderSafetyReportJSON(t *testing.T) {
	c := qt.New(t)

	var out bytes.Buffer
	assessments := []safety.StatementAssessment{{
		Index:     1,
		NodeType:  "sql",
		Statement: "DROP TABLE users",
		Severity:  safety.Destructive,
		Reason:    "drops a table",
	}}

	err := renderSafetyReport(&out, "json", assessments)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Not(qt.Contains), "Safety Assessment")

	var report safety.Report
	c.Assert(json.Unmarshal(out.Bytes(), &report), qt.IsNil)
	c.Assert(report.Highest, qt.Equals, safety.Destructive)
	c.Assert(report.Destructive, qt.IsTrue)
	c.Assert(report.Assessments, qt.DeepEquals, assessments)
}
