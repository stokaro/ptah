package schemaext

import (
	"slices"

	"ptah.run/core/objectidentity"
)

// A merged subject has one scalar claim over all of its retained targets. A
// positive claim on only some of them cannot become authority over the others.
func (c Coverage) lookupForMerge(kind Kind, subject objectidentity.ID, targets []string) Knowledge {
	if record, found := c.subjectRecord(kind, subject); found {
		if record.Knowledge.State == Uninspected || record.Knowledge.State == Unrepresentable || targetScopeCovers(record.Targets, targets) {
			return record.Knowledge
		}
		return Knowledge{State: Uninspected, Reason: "one source's subject claim does not cover every merged target"}
	}
	if record, found := c.kinds[kind]; found {
		return record.Knowledge.lookupForMerge(subject, targets)
	}
	return Knowledge{State: Uninspected, Reason: "this source did not describe the feature kind"}
}

func targetScopeCovers(scope, targets []string) bool {
	if len(scope) == 0 {
		return true
	}
	return len(targets) != 0 && !slices.ContainsFunc(targets, func(target string) bool { return !slices.Contains(scope, target) })
}

func (d KnowledgeDomain) lookupForMerge(subject objectidentity.ID, targets []string) Knowledge {
	if len(d.sources) == 0 {
		return d.Unscoped()
	}
	if len(targets) == 0 {
		targets = []string{""}
	}
	result := Knowledge{State: Complete}
	for _, target := range targets {
		for _, source := range d.sources {
			result = intersectNamespaceKnowledge(result, source.mergeKnowledge(subject, target))
		}
	}
	return result
}

func (s knowledgeSource) mergeKnowledge(subject objectidentity.ID, target string) Knowledge {
	result := Knowledge{State: Complete}
	guaranteed := false
	for _, region := range s.Regions {
		if !region.Scope.matches(subject) {
			continue
		}
		selected := len(region.Scope.Targets) == 0 || target != "" && slices.Contains(region.Scope.Targets, target)
		// A merge has no target resolver. A different spelling could be an alias,
		// so it cannot discard a negative claim. ForTarget resolves that ambiguity.
		if selected || region.Knowledge.State != Complete {
			result = intersectNamespaceKnowledge(result, region.Knowledge)
		}
		guaranteed = guaranteed || selected
	}
	if !guaranteed {
		result = intersectNamespaceKnowledge(result, s.Outside)
	}
	return result
}
