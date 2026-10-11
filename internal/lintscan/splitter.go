package lintscan

import (
	"context"
	"fmt"

	"ptah.run/core/lintext"
	"ptah.run/core/ptaherr"
)

// Splitter is the [lintext.Splitter] of one built-in dialect: the scanner in
// the mode [ForDialect] gives it. Its zero value serves no target.
type Splitter struct {
	// Dialect is the canonical target name the splitter serves.
	Dialect string
}

// SplitStatements splits every source of a request for the splitter's
// dialect. Another target wraps ptaherr.ErrUnsupportedDialect, and
// cancellation returns the context's error; either returns no result.
func (s Splitter) SplitStatements(ctx context.Context, request lintext.SplitRequest) (lintext.SplitResult, error) {
	if ctx == nil {
		return lintext.SplitResult{}, fmt.Errorf("%w: splitting requires a context", lintext.ErrInvalid)
	}
	if s.Dialect == "" || request.Target != s.Dialect {
		return lintext.SplitResult{}, fmt.Errorf("%w: the %q statement splitter on %q", ptaherr.ErrUnsupportedDialect, s.Dialect, request.Target)
	}
	mode := ForDialect(s.Dialect)
	result := lintext.SplitResult{Files: make([][]lintext.Statement, 0, len(request.Sources))}
	for _, source := range request.Sources {
		if err := ctx.Err(); err != nil {
			return lintext.SplitResult{}, err
		}
		result.Files = append(result.Files, Split(source.SQL, mode))
	}
	return result, ctx.Err()
}
