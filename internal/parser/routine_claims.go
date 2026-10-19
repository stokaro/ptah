package parser

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/sqlext"
	"ptah.run/internal/routineext"
)

// claimOwnerRoutineOptions keeps option claims in the document-wide owner
// pass. Options from one routine share a definition number and merge into
// that routine's facets only after the owner validates the claim.
func (p *Parser) claimOwnerRoutineOptions(routine *ast.RoutineNode, options []routineext.OptionSpan, offset int) {
	candidates := p.owners.Candidates(p.dialect, sqlext.RoutineOption)
	p.definitions++
	for i, option := range options {
		for _, candidate := range candidates {
			if !candidate.Matches(option.Words) {
				continue
			}
			p.ownerClaims = append(p.ownerClaims, ownerClaim{
				match: sqlext.Match{Candidate: candidate, Claim: sqlext.Claim{
					Pattern: candidate.Pattern.Name, Site: sqlext.RoutineOption, Subject: routine.Name,
					Text: option.Source, Definition: p.definitions, Offset: offset + option.Start,
				}},
				apply: func(value sqlext.Value) error {
					merged, err := routine.Facets.Merge(value.Facets)
					if err != nil {
						return fmt.Errorf("%s %s: %w", routine.Kind, routine.Name, err)
					}
					routine.Facets = merged
					routine.Options[i].Read = true
					return nil
				},
			})
			break
		}
	}
}
