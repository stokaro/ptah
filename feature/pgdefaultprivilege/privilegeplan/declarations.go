package privilegeplan

import (
	"context"
	"fmt"

	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/pgprivilege"
)

// PlanDeclarations emits authored grants and global built-in revocations. A
// scoped revoke contributes no creation statement: a fresh scope has no added
// rights. Comments remain source documentation and prefix the first statement.
func (Service) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	if err := validateRequest(ctx, request.Target); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	result := featureplan.DeclarationResult{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: pgdefaultprivilege.Owner}
	for index, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return featureplan.DeclarationResult{}, err
		}
		value, ok := object.Value.(*pgdefaultprivilege.Desired)
		if !ok {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: expected default privilege declaration, got %T", schemaext.ErrInvalidValue, object.Value)
		}
		if err := pgdefaultprivilege.ValidateDesired(value); err != nil {
			return featureplan.DeclarationResult{}, err
		}
		identity, err := pgdefaultprivilege.IdentityOf(object.Ref)
		if err != nil {
			return featureplan.DeclarationResult{}, err
		}
		canonical := value.Canonical()
		change := &pgdefaultprivilege.Change{Granted: canonical.Privileges}
		if identity.Schema == "" {
			revoked := canonical.Revoked
			if identity.Grantor == identity.Grantee && pgprivilege.NamesAll(identity.ObjectType, revoked) {
				revoked = []string{"ALL"}
			}
			for _, name := range revoked {
				change.Revoked = append(change.Revoked, pgdefaultprivilege.Grant{Privilege: name})
			}
		}
		start := len(contribution.Steps)
		receipt, err := plan(fmt.Sprintf("declaration/%06d", index), object.Ref, change, request.Identifiers, featureplan.PhaseDefault, request.CommonSteps, &contribution)
		if err != nil {
			return featureplan.DeclarationResult{}, err
		}
		if len(contribution.Steps) > start && canonical.Comment != "" {
			contribution.Steps[start].Payload.Notes = []string{canonical.Comment}
		}
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: object.Ref, Strategy: receipt.Strategy, Steps: receipt.Steps})
	}
	if len(contribution.Steps) != 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	return result, ctx.Err()
}
