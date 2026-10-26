package safety

import (
	"context"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
)

// AssessUnits judges captured execution units without rendering them again.
// SQL units are judged in one owner batch; native units retain their captured
// lifecycle and access effects. owners holds the planned-node verdict for each
// unit, or a zero assessment when no owner contributed. These verdicts can only
// raise severity. An invalid batch or canceled assessment returns no prefix.
func AssessUnits(ctx context.Context, runtime Runtime, target string, units []renderer.Fragment, owners []StatementAssessment) ([]StatementAssessment, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, err
	}
	if len(units) != len(owners) {
		return nil, fmt.Errorf("%w: unit assessment provenance differs from the batch", renderer.ErrInvalidResult)
	}
	snapshots := make([]renderer.Fragment, len(units))
	var statements []string
	for i, unit := range units {
		cloned, err := unit.Clone()
		if err != nil {
			return nil, err
		}
		if cloned.Annotation != nil {
			return nil, fmt.Errorf("%w: annotation is not an execution unit", renderer.ErrInvalidResult)
		}
		snapshots[i] = cloned
		if cloned.Native == nil {
			statements = append(statements, cloned.SQL)
		}
	}
	judgments, err := AssessSQL(ctx, runtime, target, statements)
	if err != nil {
		return nil, err
	}
	result := make([]StatementAssessment, len(units))
	sqlIndex := 0
	for i, unit := range snapshots {
		var assessment StatementAssessment
		if unit.Native == nil {
			assessment = judgments[sqlIndex]
			sqlIndex++
		} else {
			assessment = assessNode(&ast.ExtensionStatement{Payload: unit.Native})
			assessment.NativeKind = unit.Native.Kind()
		}
		if owners[i] != (StatementAssessment{}) {
			raiseAssessment(&assessment, owners[i])
		}
		assessment.Index = i + 1
		result[i] = assessment
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
