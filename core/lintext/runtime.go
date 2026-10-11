package lintext

import "context"

// Runtime is what a lint host asks of the engine runtime it runs with: the
// rules every owner publishes, each owner's split of its dialect, and each
// owner's findings. The bundled runtime answers it for every engine it
// holds, a plugin's included.
type Runtime interface {
	// LintRules returns the rules that judge target, every published rule
	// for an empty target, sorted by code. An unregistered target wraps
	// ptaherr.ErrUnsupportedDialect.
	LintRules(target string) ([]Rule, error)
	// SplitStatements splits a run's sources with the splitter of the
	// request's target. A target without one wraps
	// ptaherr.ErrUnsupportedDialect, and a reply that does not answer the
	// request wraps [ErrInvalid].
	SplitStatements(ctx context.Context, request SplitRequest) (SplitResult, error)
	// AnnotateStatements asks every target that registers an [Annotator]
	// for the facts of statements the host split, one call per annotator,
	// and joins the answers: each statement's effects from every annotator,
	// and every declined statement with its target. A request whose sources
	// and statement lists do not pair up, and a reply that does not answer
	// its request, wrap [ErrInvalid].
	AnnotateStatements(ctx context.Context, request AnnotateRequest) (AnnotateResult, error)
	// CheckMigrations sends the request's codes to the owners that publish
	// them, one call per owner, and joins their findings, sorted by file,
	// statement and code, and their baseline subjects, sorted by file and
	// code. A code no owner publishes, a rule that does not judge the
	// request's target, a reply that does not answer its request, and
	// baseline subjects of a rule that reads no baseline wrap [ErrInvalid].
	CheckMigrations(ctx context.Context, request CheckRequest) (CheckResult, error)
}
