// Package publicationreverse reverses captured PostgreSQL publication changes
// for the owner of package pgpublication and reports what a rollback cannot
// recover. It never reads a database.
package publicationreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpublication"
	"ptah.run/feature/pgpublication/publicationcompare"
)

// Service reconstructs the inverse of publication changes from their
// operands. Its zero value is ready for concurrent use.
//
// The inverse of a creation drops the publication, and the inverse of a drop
// creates it again with the members, options and owner it had. A change of a
// surviving publication goes back member by member and option by option, and
// gives the publication back to the role that owned it where the change gave
// it to another. A rollback that drops a publication and creates it again
// says what its subscribers miss meanwhile.
type Service struct{}

// ReverseChanges preserves input order and returns no partial result on error
// or cancellation. ForwardState is a prediction, never inspection evidence.
func (Service) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reversal requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if platform.NormalizeDialect(request.Target) != platform.Postgres {
		return nil, fmt.Errorf("%w: PostgreSQL publication reversal on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reversed, err := reverse(record)
		if err != nil {
			return nil, err
		}
		result = append(result, reversed)
	}
	return result, ctx.Err()
}

func reverse(record schemaext.ChangeRecord) (schemaext.Reversal, error) {
	change, ok := record.Value.(*pgpublication.Change)
	if !ok {
		return schemaext.Reversal{}, fmt.Errorf("%w: reversal requires a publication change, got %T", schemaext.ErrInvalidValue, record.Value)
	}
	if err := change.Validate(); err != nil {
		return schemaext.Reversal{}, err
	}
	if err := pgpublication.ValidateRef(record.Subject); err != nil {
		return schemaext.Reversal{}, err
	}
	projection := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: pgpublication.PublicationKind}
	switch {
	case change.Before == nil:
		created := change.After.Observed()
		projection.Value = created
		return schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: &pgpublication.Change{Before: created.Copy()}},
			ForwardState: []schemaext.ProjectedValue{projection}, Strategy: "drop the publication the forward plan created"}, nil
	case change.After == nil:
		return schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: &pgpublication.Change{After: restored(change.Before)}},
			ForwardState: []schemaext.ProjectedValue{projection},
			Strategy:     "create the publication again with the members, options and owner it had",
			Limitations:  []string{dropLimitation(pgpublication.Name(record.Subject))}}, nil
	default:
		return reverseSurvivor(record, change, projection), nil
	}
}

// restored is the declaration that puts the observed publication back as it
// was, its owner included.
func restored(observed *pgpublication.ObservedPublication) *pgpublication.DesiredPublication {
	declared := observed.Desired()
	declared.Owner = observed.Owner
	return declared
}

// reverseSurvivor reverses a change of a publication both sides hold.
func reverseSurvivor(record schemaext.ChangeRecord, change *pgpublication.Change, projection schemaext.ProjectedValue) schemaext.Reversal {
	forward := change.After.Observed()
	if forward.Owner == "" {
		forward.Owner = change.Before.Owner
	}
	projection.Value = forward.Copy()
	back := restored(change.Before)
	if change.Owner == "" && !change.Rebuild {
		back.Owner = ""
	}
	reversed, differs := publicationcompare.Difference(forward, back, "")
	result := schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: reversed},
		ForwardState: []schemaext.ProjectedValue{projection},
		Strategy:     "add back the members the forward plan dropped, drop the ones it added, and restore the options and the owner"}
	if change.Rebuild {
		result.Strategy = "drop the publication and create it again in the membership form it had"
		result.Limitations = []string{dropLimitation(pgpublication.Name(record.Subject))}
	}
	if !differs {
		result.Change.Value = nil
	}
	return result
}

// dropLimitation says what a subscriber of a dropped publication loses: a
// subscription that reads it fails while it is gone, and the changes made
// meanwhile are not sent to it once it exists again.
func dropLimitation(publication string) string {
	return fmt.Sprintf("A subscription that reads publication %s fails while the publication is dropped, and the changes made meanwhile "+
		"are not published to it when the publication is created again.", publication)
}
