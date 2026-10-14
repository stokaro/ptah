package privilegeplan

import (
	"strings"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/feature/pgdefaultprivilege"
)

func reads(identity pgdefaultprivilege.Identity, semantics identifier.Semantics) []plangraph.Effect {
	builder := objectidentity.NewBuilder(semantics)
	var result []plangraph.Effect
	seen := make(map[objectidentity.Key]bool)
	for _, role := range []string{identity.Grantor, identity.Grantee} {
		switch strings.ToUpper(role) {
		case "PUBLIC", "CURRENT_ROLE", "CURRENT_USER", "SESSION_USER":
			continue
		}
		ref := builder.Role(role)
		if !seen[ref.Key()] {
			result = append(result, plangraph.Effect{Subject: ref, Action: plangraph.Read})
			seen[ref.Key()] = true
		}
	}
	if identity.Schema != "" {
		schema := builder.Role(identity.Schema)
		schema.Kind = objectidentity.KindSchema
		result = append(result, plangraph.Effect{Subject: schema, Action: plangraph.Read})
	}
	return result
}

func addDependencies(step plangraph.Step[featureplan.Operation], common []featureplan.CommonStep, contribution *plangraph.Contribution[featureplan.Operation]) {
	for _, shared := range common {
		for _, effect := range shared.Effects {
			for _, read := range step.Effects {
				if read.Action != plangraph.Read || read.Subject.Key() != effect.Subject.Key() {
					continue
				}
				switch effect.Action {
				case plangraph.Create, plangraph.Alter:
					contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: shared.ID, After: step.ID})
				case plangraph.Drop:
					contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: step.ID, After: shared.ID})
				}
			}
		}
	}
}
