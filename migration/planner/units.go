package planner

import (
	"fmt"

	"ptah.run/core/renderer"
)

// PlannedUnit retains a rendered operation and the input node that owns it.
// Node is -1 for trailing SQL comments. Native data never enters a SQL splitter.
type PlannedUnit struct {
	Fragment renderer.Fragment
	Node     int
}

// Units returns the complete ordered plan with independent native payloads.
// SQL scripts are split with the target grammar; exact execution units retain
// their bytes and boundaries. No prefix is returned when any fragment fails.
func (p RenderedPlan) Units() ([]PlannedUnit, error) {
	if err := p.Result.Validate(len(p.Request.Nodes)); err != nil {
		return nil, err
	}
	if len(p.Result.Diagnostics) != 0 {
		return nil, fmt.Errorf("%w: refused batch has no execution units", renderer.ErrInvalidResult)
	}
	if !p.Result.ExecutableUnits {
		statements, err := p.PlannedStatements()
		if err != nil {
			return nil, err
		}
		result := make([]PlannedUnit, len(statements))
		for i, statement := range statements {
			result[i] = PlannedUnit{Fragment: renderer.Fragment{SQL: statement.SQL}, Node: statement.Node}
		}
		return result, nil
	}
	result := make([]PlannedUnit, 0, len(p.Result.Fragments))
	for i, fragment := range p.Result.Fragments {
		cloned, err := fragment.Clone()
		if err != nil {
			return nil, err
		}
		if cloned.SQL == "" && cloned.Native == nil {
			continue
		}
		result = append(result, PlannedUnit{Fragment: cloned, Node: i})
	}
	return result, nil
}
