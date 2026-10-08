package schemaext

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Merge combines source knowledge conservatively. Missing knowledge in either
// source remains uninspected. Conflicting explicit claims or model definitions
// are errors. An accumulator must start with its first source's coverage; the
// zero value describes an unknown source, not an identity for this operation.
func (c Coverage) Merge(other Coverage) (Coverage, error) {
	direction := c.representation
	if direction == "" {
		direction = other.representation
	}
	if direction == "" {
		return Coverage{}, nil
	}
	if other.representation != "" && other.representation != direction {
		return Coverage{}, fmt.Errorf("%w: coverage directions differ", ErrInvalidValue)
	}
	models := maps.Clone(c.kinds)
	if models == nil {
		models = make(map[Kind]KindCoverage)
	}
	for kind, record := range other.kinds {
		if held, found := models[kind]; found && held.Model != record.Model {
			return Coverage{}, fmt.Errorf("%w: conflicting coverage definitions for %q", ErrIncompatibleCodec, kind)
		}
		models[kind] = record
	}
	kinds := make([]KindCoverage, 0, len(models))
	for kind, record := range models {
		knowledge, err := mergeKnowledge(c.kindKnowledge(kind), other.kindKnowledge(kind))
		if err != nil {
			return Coverage{}, err
		}
		record.Knowledge = knowledge
		kinds = append(kinds, record)
	}
	subjects := maps.Clone(c.subjects)
	if subjects == nil {
		subjects = make(map[subjectKey]SubjectCoverage)
	}
	maps.Copy(subjects, other.subjects)
	overrides := make([]SubjectCoverage, 0, len(subjects))
	for _, record := range subjects {
		knowledge, err := mergeKnowledge(c.Lookup(record.Kind, record.Subject), other.Lookup(record.Kind, record.Subject))
		if err != nil {
			return Coverage{}, err
		}
		record.Knowledge = knowledge
		overrides = append(overrides, record)
	}
	return NewCoverage(direction, kinds, overrides)
}

func (c Coverage) kindKnowledge(kind Kind) Knowledge {
	if record, found := c.kinds[kind]; found {
		return record.Knowledge
	}
	return Knowledge{State: Uninspected, Reason: "one source did not describe the feature kind"}
}

func mergeKnowledge(a, b Knowledge) (Knowledge, error) {
	if a.State == Unrepresentable || b.State == Unrepresentable || a.State == Uninspected || b.State == Uninspected {
		state := Uninspected
		if a.State == Unrepresentable || b.State == Unrepresentable {
			state = Unrepresentable
		}
		reasons := []string{a.Reason, b.Reason}
		slices.Sort(reasons)
		reasons = slices.DeleteFunc(slices.Compact(reasons), func(reason string) bool { return reason == "" })
		return Knowledge{State: state, Reason: strings.Join(reasons, "; ")}, nil
	}
	if a.State == Complete {
		return b, nil
	}
	if b.State == Complete || a.State == b.State {
		return a, nil
	}
	return Knowledge{}, fmt.Errorf("%w: conflicting feature coverage claims %q and %q", ErrInvalidValue, a.State, b.State)
}
