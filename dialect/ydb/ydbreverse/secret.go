package ydbreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbsecret"
)

// SecretService reverses secret changes. A created secret is dropped, and a
// dropped one is created again with the value of the default variable for its
// path, since the database never returned the old value and a read names no
// variable. A rotation has no reverse statement: the earlier value was never
// read, so nothing can restore it, and the reversal carries no change and says
// so. Each limitation is reported.
type SecretService struct{}

// ReverseChanges returns one reversal per change, in input order.
func (SecretService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	return reverseStandalone(ctx, request, "secrets", capability.Secrets, reverseSecret)
}

func reverseSecret(record schemaext.ChangeRecord) (schemaext.Reversal, error) {
	if err := ydbsecret.ValidateIdentity(record.Subject); err != nil {
		return schemaext.Reversal{}, err
	}
	change, ok := record.Value.(*ydbdiff.Secret)
	if !ok {
		return schemaext.Reversal{}, fmt.Errorf("%w: reversal requires a secret change", schemaext.ErrInvalidValue)
	}
	if err := change.Validate(); err != nil {
		return schemaext.Reversal{}, err
	}
	path := ydbsecret.Display(record.Subject.Schema.Source, record.Subject.Name.Source)
	projection := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ydbsecret.Kind}
	if change.After != nil {
		projection.Value = change.After.Observed()
	}
	reversal := schemaext.Reversal{ForwardState: []schemaext.ProjectedValue{projection}}
	switch {
	case change.Before == nil:
		reversal.Change = schemaext.ChangeRecord{Subject: record.Subject, Value: &ydbdiff.Secret{Before: change.After.Observed()}}
		reversal.Strategy = "drop the created secret"
	case change.After == nil:
		restored := &ydbsecret.Desired{ValueEnv: ydbsecret.DefaultValueEnv(record.Subject.Schema.Source, record.Subject.Name.Source)}
		reversal.Change = schemaext.ChangeRecord{Subject: record.Subject, Value: &ydbdiff.Secret{After: restored}}
		reversal.Strategy = "create the dropped secret again"
		reversal.Limitations = []string{fmt.Sprintf("the dropped value of secret %s was never read; the rollback takes the value %s holds when it runs",
			path, restored.ValueEnv)}
	default:
		reversal.Change = schemaext.ChangeRecord{Subject: record.Subject}
		reversal.Strategy = "keep the secret and the value it holds"
		reversal.Limitations = []string{fmt.Sprintf("the value secret %s held before the rotation was never read; the rollback keeps the rotated value", path)}
	}
	return reversal, nil
}
