package safety_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/lintext"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/safety"
)

// assessText assesses one statement of target with the bundled runtime's
// owners; an empty target asks every annotator.
func assessText(c *qt.C, target, statement string) safety.StatementAssessment {
	c.Helper()
	assessments, err := safety.AssessSQL(c.Context(), must.Must(builtin.New()), target, []string{statement})
	c.Assert(err, qt.IsNil)
	c.Assert(assessments, qt.HasLen, 1)
	return assessments[0]
}

// unjudged is the owners' part of a fake runtime whose owners judge no
// statement.
type unjudged struct{}

func (unjudged) SplitStatements(_ context.Context, request lintext.SplitRequest) (lintext.SplitResult, error) {
	return lintext.SplitResult{Files: make([][]lintext.Statement, len(request.Sources))}, nil
}

func (unjudged) AnnotateStatements(_ context.Context, request lintext.AnnotateRequest) (lintext.AnnotateResult, error) {
	result := lintext.AnnotateResult{Files: make([][]lintext.Annotation, len(request.Files))}
	for i, statements := range request.Files {
		result.Files[i] = make([]lintext.Annotation, len(statements))
	}
	return result, nil
}

// ledgerSplitter is the splitter of a fake target, ledger, that splits a
// source at its semicolons and judges a statement by its first word: PURGE
// destroys, and ARCHIVE, which the words alone read as the DROP TABLE it
// starts with, only moves the rows aside.
type ledgerSplitter struct {
	calls *int
}

func (s ledgerSplitter) SplitStatements(_ context.Context, request lintext.SplitRequest) (lintext.SplitResult, error) {
	*s.calls++
	result := lintext.SplitResult{Judged: []lintext.Fact{lintext.FactSafetyEffect}}
	for _, source := range request.Sources {
		var statements []lintext.Statement
		start := 0
		for _, piece := range strings.SplitAfter(source.SQL, ";") {
			text := strings.TrimSuffix(piece, ";")
			if strings.TrimSpace(text) != "" {
				statements = append(statements, lintext.Statement{Start: start, End: start + len(text), Line: 1, SQL: text,
					Effects: ledgerEffects(text)})
			}
			start += len(piece)
		}
		result.Files = append(result.Files, statements)
	}
	return result, nil
}

// ledgerEffects are the ledger owner's judgment of a statement.
func ledgerEffects(text string) []lintext.Effect {
	words := strings.Fields(strings.ToUpper(text))
	switch {
	case len(words) > 0 && words[0] == "PURGE":
		return []lintext.Effect{{Fact: lintext.FactSafetyEffect, Impact: schemaext.Destructive, Reason: "PURGE erases the ledger"}}
	case len(words) > 3 && words[3] == "ARCHIVE":
		return []lintext.Effect{{Fact: lintext.FactSafetyEffect, Impact: schemaext.Behavioral, Reason: "ARCHIVE moves the rows aside"}}
	default:
		return nil
	}
}

// ledgerRuntime holds the fake target ledger and the bundled postgres target.
func ledgerRuntime(c *qt.C, calls *int) *engine.Runtime {
	c.Helper()
	return must.Must(engine.New(engine.Provider{ID: "example.org/ledger", Targets: []engine.Target{{
		Name: "ledger", Splitter: ledgerSplitter{calls: calls},
	}}}))
}

// A target's owner judges the statements only it recognizes, in one call for
// them all, and its judgment takes the place of the words: PURGE, which no
// word rule knows, is destructive, and a DROP TABLE ... ARCHIVE the owner
// judges behavioral is a warning, where the words alone read a dropped table.
// A statement the owner does not judge is read by its words. Another target's
// owner does not judge ledger's statements.
func TestAssessSQL_OwnerJudgment(t *testing.T) {
	statements := []string{"PURGE accounts", "DROP TABLE t ARCHIVE", "DROP TABLE t", "SELECT 1"}
	for _, test := range []struct {
		name      string
		target    string
		calls     int
		severity  []safety.Severity
		reason    []string
		wantIndex []int
	}{
		{name: "the owner's target", target: "ledger", calls: 1,
			severity:  []safety.Severity{safety.Destructive, safety.Warning, safety.Destructive, safety.Safe},
			reason:    []string{"PURGE erases the ledger", "ARCHIVE moves the rows aside", "DROP TABLE removes the table and all rows", "does not remove data or tighten constraints"},
			wantIndex: []int{1, 2, 3, 4}},
		{name: "a target whose owner judges nothing", target: "postgres", calls: 0,
			severity:  []safety.Severity{safety.Safe, safety.Destructive, safety.Destructive, safety.Safe},
			reason:    []string{"does not remove data or tighten constraints", "DROP TABLE removes the table and all rows", "DROP TABLE removes the table and all rows", "does not remove data or tighten constraints"},
			wantIndex: []int{1, 2, 3, 4}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			runtime := must.Must(engine.New(
				engine.Provider{ID: "example.org/ledger", Targets: []engine.Target{{Name: "ledger", Splitter: ledgerSplitter{calls: &calls}}}},
				engine.Provider{ID: "example.org/postgres", Targets: []engine.Target{{Name: "postgres", Splitter: unjudged{}}}},
			))

			assessments, err := safety.AssessSQL(c.Context(), runtime, test.target, statements)

			c.Assert(err, qt.IsNil)
			c.Assert(calls, qt.Equals, test.calls)
			var severity []safety.Severity
			var reason []string
			var index []int
			for _, assessment := range assessments {
				severity = append(severity, assessment.Severity)
				reason = append(reason, assessment.Reason)
				index = append(index, assessment.Index)
			}
			c.Assert(severity, qt.DeepEquals, test.severity)
			c.Assert(reason, qt.DeepEquals, test.reason)
			c.Assert(index, qt.DeepEquals, test.wantIndex)
		})
	}
}

// A statement the owner's lexing reads as several is read piece by piece: a
// piece the owner judged by its judgment, and any other by its words, so the
// judgment of one piece does not hide a dropped table beside it. The most
// severe piece decides.
func TestAssessSQL_OwnerJudgesEveryPiece(t *testing.T) {
	for _, test := range []struct {
		statement string
		severity  safety.Severity
		reason    string
	}{
		{statement: "DROP TABLE t ARCHIVE; PURGE accounts", severity: safety.Destructive, reason: "PURGE erases the ledger"},
		{statement: "DROP TABLE t; DROP TABLE u ARCHIVE", severity: safety.Destructive, reason: "DROP TABLE removes the table and all rows"},
		{statement: "SELECT 1; DROP TABLE u ARCHIVE", severity: safety.Warning, reason: "ARCHIVE moves the rows aside"},
	} {
		t.Run(test.statement, func(t *testing.T) {
			c := qt.New(t)
			calls := 0

			assessments, err := safety.AssessSQL(c.Context(), ledgerRuntime(c, &calls), "ledger", []string{test.statement})

			c.Assert(err, qt.IsNil)
			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(assessments[0].Severity, qt.Equals, test.severity)
			c.Assert(assessments[0].Reason, qt.Equals, test.reason)
		})
	}
}

// An owner's split decides how a statement is read only where the owner
// judged a piece of it. A statement it judged nothing of is read whole, as the
// caller wrote it, so a split that cuts a string at its semicolon does not
// invent a dropped table.
func TestAssessSQL_UnjudgedStatementsAreReadWhole(t *testing.T) {
	c := qt.New(t)
	calls := 0

	assessments, err := safety.AssessSQL(c.Context(), ledgerRuntime(c, &calls), "ledger", []string{"UPDATE t SET note = 'a; DROP TABLE b'"})

	c.Assert(err, qt.IsNil)
	c.Assert(assessments, qt.HasLen, 1)
	c.Assert(assessments[0].Severity, qt.Equals, safety.Safe)
}

// unlistedJudgment is a runtime whose split reports a safety effect without
// listing the fact as judged, which a runtime that validates its owners
// refuses.
type unlistedJudgment struct{ unjudged }

func (unlistedJudgment) SplitStatements(_ context.Context, request lintext.SplitRequest) (lintext.SplitResult, error) {
	result := lintext.SplitResult{Judged: []lintext.Fact{lintext.FactStatefulDrop}}
	for _, source := range request.Sources {
		result.Files = append(result.Files, []lintext.Statement{{Start: 0, End: len(source.SQL), Line: 1, SQL: source.SQL,
			Effects: ledgerEffects(source.SQL)}})
	}
	return result, nil
}

// A split that does not list the fact as judged judges no statement, whatever
// effects it carries: a fact not listed was not judged.
func TestAssessSQL_AnUnlistedFactJudgesNothing(t *testing.T) {
	c := qt.New(t)

	assessments, err := safety.AssessSQL(c.Context(), unlistedJudgment{}, "ledger", []string{"PURGE accounts"})

	c.Assert(err, qt.IsNil)
	c.Assert(assessments, qt.HasLen, 1)
	c.Assert(assessments[0].Severity, qt.Equals, safety.Safe)
}

// firstWordAnnotator judges a statement by its first word: one word it judges
// behavioral, another it declines.
type firstWordAnnotator struct {
	judged, declined string
}

func (a firstWordAnnotator) AnnotateStatements(_ context.Context, request lintext.AnnotateRequest) (lintext.AnnotateResult, error) {
	result := lintext.AnnotateResult{Files: make([][]lintext.Annotation, len(request.Files))}
	for i, statements := range request.Files {
		result.Files[i] = make([]lintext.Annotation, len(statements))
		for j, statement := range statements {
			first := strings.ToUpper(strings.Fields(statement.SQL)[0])
			result.Files[i][j].Declined = first == a.declined
			result.Files[i][j].Effects = map[bool][]lintext.Effect{true: {{Fact: lintext.FactSafetyEffect, Impact: schemaext.Behavioral,
				Reason: "reads a pause"}}}[first == a.judged]
		}
	}
	return result, nil
}

// With no target, a decline belongs to the piece the owner declined, not to
// every piece of its statement: here the archive keeps its own judgment and
// the declined purge after it decides.
func TestAssessSQL_ADeclineBelongsToItsPiece(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(engine.Provider{ID: "example.org/alpha", Targets: []engine.Target{{Name: "alpha",
		Annotator: firstWordAnnotator{judged: "ARCHIVE", declined: "PURGE"}}}}))

	assessments, err := safety.AssessSQL(c.Context(), runtime, "", []string{"ARCHIVE t; PURGE accounts"})

	c.Assert(err, qt.IsNil)
	c.Assert(assessments, qt.HasLen, 1)
	c.Assert(assessments[0].Severity, qt.Equals, safety.Destructive)
	c.Assert(assessments[0].Reason, qt.Equals, "the alpha owner reads this statement otherwise and did not judge it; manual review is required")
}

// sameAnnotator judges every statement it is asked about the same way, or
// declines every one.
type sameAnnotator struct {
	effect  lintext.Effect
	decline bool
}

func (a sameAnnotator) AnnotateStatements(_ context.Context, request lintext.AnnotateRequest) (lintext.AnnotateResult, error) {
	result := lintext.AnnotateResult{Files: make([][]lintext.Annotation, len(request.Files))}
	for i, statements := range request.Files {
		result.Files[i] = make([]lintext.Annotation, len(statements))
		for j := range statements {
			result.Files[i][j] = lintext.Annotation{Declined: a.decline}
			if !a.decline && a.effect.Fact != "" {
				result.Files[i][j].Effects = []lintext.Effect{a.effect}
			}
		}
	}
	return result, nil
}

// With no target, every annotator judges the statements. Where two judge one
// differently the most severe impact wins and both reasons are joined in
// target name order, whatever order the providers register them in; a
// statement an owner declined is destructive, because its effect is not
// established.
func TestAssessSQL_AnnotatorsJudgeWithoutATarget(t *testing.T) {
	behavioral := lintext.Effect{Fact: lintext.FactSafetyEffect, Impact: schemaext.Behavioral, Reason: "reads a pause"}
	destructive := lintext.Effect{Fact: lintext.FactSafetyEffect, Impact: schemaext.Destructive, Reason: "reads a purge"}
	for _, test := range []struct {
		name         string
		alpha, beta  sameAnnotator
		wantSeverity safety.Severity
		wantReason   string
	}{
		{name: "owners that disagree", alpha: sameAnnotator{effect: behavioral}, beta: sameAnnotator{effect: destructive},
			wantSeverity: safety.Destructive, wantReason: "reads a pause; reads a purge"},
		{name: "owners that agree", alpha: sameAnnotator{effect: behavioral}, beta: sameAnnotator{effect: behavioral},
			wantSeverity: safety.Warning, wantReason: "reads a pause"},
		{name: "one owner", alpha: sameAnnotator{}, beta: sameAnnotator{effect: behavioral},
			wantSeverity: safety.Warning, wantReason: "reads a pause"},
		{name: "an owner that declines", alpha: sameAnnotator{decline: true}, beta: sameAnnotator{effect: behavioral},
			wantSeverity: safety.Destructive,
			wantReason:   "reads a pause; the alpha owner reads this statement otherwise and did not judge it; manual review is required"},
		{name: "no owner judges it", alpha: sameAnnotator{}, beta: sameAnnotator{},
			wantSeverity: safety.Destructive, wantReason: "DROP TABLE removes the table and all rows"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(engine.New(
				engine.Provider{ID: "example.org/beta", Targets: []engine.Target{{Name: "beta", Annotator: test.beta}}},
				engine.Provider{ID: "example.org/alpha", Targets: []engine.Target{{Name: "alpha", Annotator: test.alpha}}},
			))

			assessments, err := safety.AssessSQL(c.Context(), runtime, "", []string{"DROP TABLE t"})

			c.Assert(err, qt.IsNil)
			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(assessments[0].Severity, qt.Equals, test.wantSeverity)
			c.Assert(assessments[0].Reason, qt.Equals, test.wantReason)
		})
	}
}

// failingOwners fails every judgment it is asked for.
type failingOwners struct{ err error }

func (o failingOwners) SplitStatements(context.Context, lintext.SplitRequest) (lintext.SplitResult, error) {
	return lintext.SplitResult{}, o.err
}

func (o failingOwners) AnnotateStatements(context.Context, lintext.AnnotateRequest) (lintext.AnnotateResult, error) {
	return lintext.AnnotateResult{}, o.err
}

func TestAssessSQL_FailurePath(t *testing.T) {
	failure := errors.New("owner failed")
	invalid := lintext.Effect{Fact: lintext.FactSafetyEffect, Impact: schemaext.Additive, Reason: "adds"}
	for _, test := range []struct {
		name    string
		runtime safety.Runtime
		target  string
		wantIs  error
		wantErr string
	}{
		{name: "no runtime", wantIs: schemaext.ErrInvalidValue, wantErr: `.*a context and selected feature runtime are required`},
		{name: "a target without a splitter", runtime: must.Must(builtin.New()), target: "nowhere",
			wantIs: ptaherr.ErrUnsupportedDialect, wantErr: `.*no statement splitter for target "nowhere"`},
		{name: "a failing splitter", runtime: failingOwners{err: failure}, target: "ledger", wantIs: failure, wantErr: `owner failed`},
		{name: "a failing annotator", runtime: failingOwners{err: failure}, wantIs: failure, wantErr: `owner failed`},
		{name: "a judgment the runtime refuses", runtime: must.Must(engine.New(engine.Provider{ID: "example.org/alpha",
			Targets: []engine.Target{{Name: "alpha", Annotator: sameAnnotator{effect: invalid}}}})),
			wantIs: lintext.ErrInvalid, wantErr: `statement annotator of "example.org/alpha": .*judges impact "additive".*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			assessments, err := safety.AssessSQL(c.Context(), test.runtime, test.target, []string{"DROP TABLE t"})
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(assessments, qt.IsNil)
		})
	}
}

// cancelingOwners cancels the assessment while it judges, and answers.
type cancelingOwners struct {
	unjudged
	cancel context.CancelFunc
}

func (o cancelingOwners) SplitStatements(ctx context.Context, request lintext.SplitRequest) (lintext.SplitResult, error) {
	o.cancel()
	return o.unjudged.SplitStatements(ctx, request)
}

// A context canceled before the owners judge, or while they do, returns no
// assessments.
func TestAssessSQL_Canceled(t *testing.T) {
	for _, test := range []struct {
		name         string
		cancelBefore bool
	}{
		{name: "before the owners judge", cancelBefore: true},
		{name: "while the owners judge"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(c.Context())
			before := map[bool]func(){true: cancel, false: func() {}}
			before[test.cancelBefore]()

			assessments, err := safety.AssessSQL(ctx, cancelingOwners{cancel: cancel}, "ledger", []string{"SELECT 1"})

			c.Assert(err, qt.ErrorIs, context.Canceled)
			c.Assert(assessments, qt.IsNil)
		})
	}
}

// judgingRenderer renders each node as the SQL of a raw SQL node and lets the
// ledger owner judge what it rendered.
type judgingRenderer struct {
	ledgerSplitter
	unjudged
}

func (judgingRenderer) Render(_ context.Context, request renderer.Request) (renderer.Result, error) {
	result := renderer.Result{Complete: true}
	for _, node := range request.Nodes {
		result.Fragments = append(result.Fragments, renderer.Fragment{SQL: node.(*ast.RawSQLNode).SQL})
	}
	return result, nil
}

func (r judgingRenderer) SplitStatements(ctx context.Context, request lintext.SplitRequest) (lintext.SplitResult, error) {
	return r.ledgerSplitter.SplitStatements(ctx, request)
}

// The dialect's owner judges every statement a rendering produced, in one
// call for the whole batch, and its judgment reaches each statement as it
// reaches a statement assessed alone.
func TestAssessRendered_OwnerJudgesTheRenderedStatements(t *testing.T) {
	c := qt.New(t)
	calls := 0
	runtime := judgingRenderer{ledgerSplitter: ledgerSplitter{calls: &calls}}
	nodes := []ast.Node{ast.NewRawSQL("PURGE accounts; SELECT 1;"), ast.NewRawSQL("DROP TABLE t ARCHIVE;")}

	assessments, err := safety.AssessRenderedWithCapabilities(c.Context(), runtime, nodes, "ledger", capability.Capabilities{})

	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	var got []string
	for _, assessment := range assessments {
		got = append(got, string(assessment.Severity)+": "+assessment.Reason)
	}
	c.Assert(got, qt.DeepEquals, []string{
		"destructive: PURGE erases the ledger",
		"safe: does not remove data or tighten constraints",
		"warning: ARCHIVE moves the rows aside",
	})
}
