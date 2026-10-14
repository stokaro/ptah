// Package privilegereverse restores captured default ACLs without enumerating
// the server's interpretation of ALL. It performs no catalog reads.
package privilegereverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

// Service reconstructs the entire prior ACL delta. A grant of ALL can add only
// part of its rights when some already exist, so revoking ALL alone is not its
// inverse. The captured before value supplies the rights that must remain.
type Service struct{}

// ReverseChanges returns exact restoration changes in input order. An unread
// initial ACL refuses the entire batch; no inverse guesses that it was empty.
func (Service) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: default privilege reversal requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !platform.IsPostgresFamily(request.Target) {
		return nil, fmt.Errorf("%w: default privilege reversal on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		change, ok := record.Value.(*pgdefaultprivilege.Change)
		if !ok {
			return nil, fmt.Errorf("%w: default privilege reversal got %T", schemaext.ErrInvalidValue, record.Value)
		}
		if err := change.Validate(); err != nil {
			return nil, err
		}
		if _, err := pgdefaultprivilege.IdentityOf(record.Subject); err != nil {
			return nil, err
		}
		if change.Before == nil || change.UnknownBefore {
			return nil, fmt.Errorf("%w: default privilege initial ACL is not captured for %s", schemaext.ErrIrreversible, record.Subject)
		}
		result = append(result, schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: &pgdefaultprivilege.Change{Restore: change.Before.Copy()}},
			Strategy: "restore the captured default ACL, including its grant options and built-in revocations"})
	}
	return result, ctx.Err()
}
