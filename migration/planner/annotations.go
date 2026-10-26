package planner

import "ptah.run/core/renderer"

// Annotations returns host prose in execution-unit order. No annotation is an
// executable unit, including a trailing note or a plan that contains only notes.
func (p RenderedPlan) Annotations() ([]renderer.Annotation, error) {
	units, err := p.Units()
	if err != nil {
		return nil, err
	}
	var annotations []renderer.Annotation
	before := 0
	for node, fragment := range p.Result.Fragments {
		if fragment.Annotation == nil {
			continue
		}
		for before < len(units) && units[before].Node < node {
			before++
		}
		annotations = append(annotations, renderer.Annotation{Before: before, Text: *fragment.Annotation})
	}
	return annotations, nil
}
