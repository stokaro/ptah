// Package extensionreverse reverses captured PostgreSQL extension changes for
// the owner of package pgextension and reports what a rollback cannot
// recover. It never reads a database.
package extensionreverse

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
)

// Service reconstructs the inverse of extension changes from their operands.
// Its zero value is ready for concurrent use.
//
// The inverse of an installation drops the extension, and the inverse of a
// drop installs it again at the version and in the schema it had. A change of
// a surviving extension goes back where it can: a move back to the schema it
// left and a comment back to the one it had. A version does not go back:
// PostgreSQL updates an extension in one direction only, and ALTER EXTENSION
// ... UPDATE TO an older version answers "has no update path". The inverse
// says so as a limitation, and an update that changed nothing else has no
// statement to run back at all.
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
	if !platform.IsPostgresFamily(request.Target) {
		return nil, fmt.Errorf("%w: PostgreSQL extension reversal on %q", ptaherr.ErrUnsupportedDialect, request.Target)
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
	change, ok := record.Value.(*pgextension.Change)
	if !ok {
		return schemaext.Reversal{}, fmt.Errorf("%w: reversal requires an extension change, got %T", schemaext.ErrInvalidValue, record.Value)
	}
	if err := change.Validate(); err != nil {
		return schemaext.Reversal{}, err
	}
	if err := pgextension.ValidateRef(record.Subject); err != nil {
		return schemaext.Reversal{}, err
	}
	projection := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: pgextension.ExtensionKind}
	switch {
	case change.Before == nil:
		installed := change.After.Observed()
		projection.Value = installed
		return schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: &pgextension.Change{Before: installed.Copy()}},
			ForwardState: []schemaext.ProjectedValue{projection}, Strategy: "drop the extension the forward plan installed"}, nil
	case change.After == nil:
		return schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: &pgextension.Change{After: change.Before.Desired()}},
			ForwardState: []schemaext.ProjectedValue{projection},
			Strategy:     "install the extension again at the version and in the schema it had"}, nil
	default:
		return reverseSurvivor(record, change, projection), nil
	}
}

// reverseSurvivor reverses a change of an extension both sides hold: the
// schema and the comment go back, and the version stays.
func reverseSurvivor(record schemaext.ChangeRecord, change *pgextension.Change, projection schemaext.ProjectedValue) schemaext.Reversal {
	extension := pgextension.Name(record.Subject)
	forward := change.Before.Copy()
	if change.UpdatesVersion() {
		forward.Version = strings.TrimSpace(change.After.Version)
	}
	if change.Schema != "" {
		forward.Schema = change.Schema
	}
	if change.Comment {
		forward.Comment = new(change.After.Comment)
	}
	projection.Value = forward.Copy()
	reversed := &pgextension.Change{Before: forward, After: &pgextension.DesiredExtension{Schema: change.Before.Schema}}
	if change.Schema != "" {
		reversed.Schema = change.Before.Schema
	}
	if change.Comment {
		reversed.Comment = true
		if change.Before.Comment != nil {
			reversed.After.Comment = *change.Before.Comment
		}
	}
	result := schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: reversed},
		ForwardState: []schemaext.ProjectedValue{projection}, Strategy: survivorStrategy(change)}
	if change.UpdatesVersion() {
		result.Limitations = []string{fmt.Sprintf("PostgreSQL updates an extension in one direction only: after the rollback, %s stays at version %s rather than %s.",
			extension, forward.Version, change.Before.Version)}
	}
	if reversed.Schema == "" && !reversed.Comment {
		result.Change.Value = nil
	}
	return result
}

// survivorStrategy says what the rollback of a surviving extension does.
func survivorStrategy(change *pgextension.Change) string {
	var parts []string
	if change.Schema != "" {
		parts = append(parts, "move the extension back")
	}
	if change.Comment {
		parts = append(parts, "restore its comment")
	}
	if change.UpdatesVersion() {
		parts = append(parts, "keep the version the migration installed")
	}
	return strings.Join(parts, ", ")
}
