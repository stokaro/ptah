package lintext

import (
	"context"
	"fmt"
	"slices"
)

// AnnotateRequest asks an owner for the facts of statements it did not split.
// A lint run that names no target splits with the host's dialect-blind
// scanner, and every owner that registers an [Annotator] judges that split, in
// one call per run, so a rule shared by every dialect still reads the facts
// only an owner recognizes.
type AnnotateRequest struct {
	// Facts are the facts the host asks about. The owner reports no other.
	Facts []Fact `json:"facts"`
	// Sources are the run's files as a [SplitRequest] carries them, and Files
	// the statements the host split each into, in the same order. An owner
	// reads a source with its own lexing to see where it would split it.
	Sources []Source      `json:"sources"`
	Files   [][]Statement `json:"files"`
}

// Annotation is an owner's answer about one statement.
type Annotation struct {
	// Effects are the statement's facts, each one the request asked about.
	Effects []Effect `json:"effects,omitempty"`
	// Declined reports a statement the owner does not judge, because its own
	// lexing would split the text differently. A declined statement carries
	// no effect.
	Declined bool `json:"declined,omitempty"`
}

// Declined names one statement an owner declined to judge.
type Declined struct {
	// Target is the canonical name of the target whose owner declined.
	Target string `json:"target"`
	// File and Statement are the positions of the file in the request and
	// of the statement in its file.
	File      int `json:"file"`
	Statement int `json:"statement"`
}

// AnnotateResult answers an [AnnotateRequest].
type AnnotateResult struct {
	// Files holds one annotation per statement of the request, in request
	// order.
	Files [][]Annotation `json:"files"`
	// Declined lists every declined statement with the target whose owner
	// declined it. A runtime joining several annotators fills it; a single
	// annotator marks [Annotation.Declined] and leaves it empty.
	Declined []Declined `json:"declined,omitempty"`
}

// Annotator judges facts of statements another splitter produced.
//
// An annotation is best-effort. The statements come from a scanner that knows
// no dialect, so a statement the owner's own lexing would split differently --
// where a comment or a string ends, where one statement stops -- is not one
// the owner can judge, and it declines it rather than guess. A host reports a
// declined statement as unjudged; it never reads a declined statement as one
// without the fact.
type Annotator interface {
	// AnnotateStatements returns one annotation per statement of the
	// request, or an error and no result.
	AnnotateStatements(ctx context.Context, request AnnotateRequest) (AnnotateResult, error)
}

// ValidateAnnotateRequest reports a request whose sources and statement lists
// do not pair up one to one, wrapping [ErrInvalid].
func ValidateAnnotateRequest(request AnnotateRequest) error {
	if len(request.Sources) != len(request.Files) {
		return fmt.Errorf("%w: an annotation request holds %d sources and %d statement lists", ErrInvalid,
			len(request.Sources), len(request.Files))
	}
	return nil
}

// ValidateAnnotate reports a result that does not answer request: a file or
// statement count other than the request's, an effect of a fact the request
// did not ask about, a declined statement that carries an effect, a statement
// it does not decline without a kind where the request asks for
// [FactStatementKind], and effects [ValidateSplit] refuses too: an Impact or a
// Reason on a fact other than [FactSafetyEffect], a safety effect whose Impact
// is not Behavioral or Destructive or whose Reason is empty, two safety
// effects or two kinds on one statement, and the kind, primary key and
// capability faults [ValidateSplit] lists. Each failure wraps [ErrInvalid].
func ValidateAnnotate(request AnnotateRequest, result AnnotateResult) error {
	if len(result.Files) != len(request.Files) {
		return fmt.Errorf("%w: an annotation of %d files returned %d", ErrInvalid, len(request.Files), len(result.Files))
	}
	for i, annotations := range result.Files {
		if len(annotations) != len(request.Files[i]) {
			return fmt.Errorf("%w: file %d holds %d statements and %d annotations", ErrInvalid, i, len(request.Files[i]), len(annotations))
		}
		for j, annotation := range annotations {
			if annotation.Declined && len(annotation.Effects) > 0 {
				return fmt.Errorf("%w: statement %d of file %d is declined and carries effects", ErrInvalid, j, i)
			}
			for _, effect := range annotation.Effects {
				if !slices.Contains(request.Facts, effect.Fact) {
					return fmt.Errorf("%w: statement %d of file %d reports %q, which the request did not ask about", ErrInvalid, j, i, effect.Fact)
				}
			}
			asked := request.Facts
			if annotation.Declined {
				asked = nil
			}
			if fault := effectsFault(annotation.Effects, asked); fault != "" {
				return fmt.Errorf("%w: statement %d of file %d %s", ErrInvalid, j, i, fault)
			}
		}
	}
	return nil
}
