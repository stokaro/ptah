package atlasschema

import (
	"context"
	"slices"

	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/migration/safety"
)

// ApplyUnitReport describes one planned SQL or native operation. Native reports
// summarize captured effects and identity; they are not executable artifacts.
type ApplyUnitReport struct {
	SQL    string                    `json:"sql,omitempty"`
	Native *NativeUnitReport         `json:"native,omitempty"`
	Inputs []renderer.InputReference `json:"inputs,omitempty"`
}

// NativeUnitReport describes the owner and effects without publishing opaque
// provider payloads. A missing effect remains unknown in the safety assessment.
type NativeUnitReport struct {
	Kind         schemaext.Kind   `json:"kind"`
	Subject      string           `json:"subject,omitempty"`
	Severity     safety.Severity  `json:"severity"`
	Reason       string           `json:"reason"`
	Access       schemaext.Access `json:"access,omitempty"`
	AccessReason string           `json:"access_reason,omitempty"`
}

// SQLApplyUnitReports preserves statement order in the apply report contract.
func SQLApplyUnitReports(statements []string) []ApplyUnitReport {
	if len(statements) == 0 {
		return nil
	}
	units := make([]ApplyUnitReport, len(statements))
	for i, statement := range statements {
		units[i].SQL = statement
	}
	return units
}

// ReportUnits describes the captured batch without rendering it again. It
// records no native bytes that a reader could mistake for an executable plan.
func (p ApplyRuntimePlan) ReportUnits(ctx context.Context, runtime safety.Runtime) ([]ApplyUnitReport, error) {
	assessments, err := p.Assessments(ctx, runtime)
	if err != nil {
		return nil, err
	}
	units, err := p.Units()
	if err != nil {
		return nil, err
	}
	report := make([]ApplyUnitReport, len(units))
	for i, unit := range units {
		report[i].SQL = unit.SQL
		report[i].Inputs = slices.Clone(unit.Inputs)
		if unit.Native != nil {
			assessment := assessments[i]
			report[i].Native = &NativeUnitReport{Kind: unit.Native.Kind(), Subject: assessment.Subject, Severity: assessment.Severity, Reason: assessment.Reason, Access: assessment.Access, AccessReason: assessment.AccessReason}
		}
	}
	return report, nil
}
