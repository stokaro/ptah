package safety

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/internal/lintscan"
)

// A statement whose effect only its owner knows is judged by the owner, as a
// [lintext.FactSafetyEffect] of the statement, and the owner's judgment takes
// the place of the reading of its words. The words are what every dialect
// shares; the owner knows that a dropped YDB topic loses its messages and that
// an async replication dropped without CASCADE keeps its tables.

// Runtime is what an assessment of SQL text asks of the engine runtime: the
// owners' judgment of the statements only they recognize. The splitter of a
// named target judges a target's statements, and every annotator judges the
// statements of an assessment that names no target. *ptah.run/engine.Runtime
// implements it.
type Runtime interface {
	lintext.Splitter
	lintext.Annotator
}

// RenderingRuntime is what an assessment of planned nodes asks of the engine
// runtime: the rendering service the nodes render with, and the owners'
// judgment of the statements they render. *ptah.run/engine.Runtime implements
// it.
type RenderingRuntime interface {
	renderer.Service
	Runtime
}

// ownerJudgment is the owners' judgment of one statement.
type ownerJudgment struct {
	severity Severity
	reason   string
}

// statementPiece is one statement as the owners' split reads a statement the
// caller holds, with the owners' judgment of it, nil when no owner judged it.
// A caller's statement is usually one piece; the owner's lexing may read it as
// several.
type statementPiece struct {
	sql      string
	judgment *ownerJudgment
}

// judgeStatements returns, for each statement in input order, the pieces the
// owners' split reads it as, each with the owners' judgment of it, and nil for
// a statement no owner judged a piece of.
//
// With a target, the target's splitter splits and judges every statement,
// each as one source, in one call; a splitter that does not judge the fact
// judges none. With no target, every annotator judges the statements as a
// split that knows no dialect reads them, in one call. Where several owners
// judge one piece, the most severe impact wins and the reasons are joined in
// target name order. A piece an owner declined to judge is Destructive,
// because its effect is not established: the owner's lexing reads the text
// otherwise, and under that reading it is a statement the owner judges. A
// decline's reason follows the judgments, since an annotation does not say
// which owner made which judgment.
func judgeStatements(ctx context.Context, runtime Runtime, target string, statements []string) ([][]statementPiece, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, err
	}
	sources := make([]lintext.Source, 0, len(statements))
	for i, statement := range statements {
		sources = append(sources, lintext.Source{Name: fmt.Sprintf("statement %d", i+1), SQL: statement})
	}
	judged := make([][]statementPiece, len(statements))
	if len(statements) == 0 {
		return judged, nil
	}
	if target != "" {
		result, err := runtime.SplitStatements(ctx, lintext.SplitRequest{Target: target, Sources: sources})
		if err != nil {
			return nil, err
		}
		if !slices.Contains(result.Judged, lintext.FactSafetyEffect) {
			return judged, nil
		}
		for i, split := range result.Files {
			pieces := make([]statementPiece, 0, len(split))
			for _, piece := range split {
				pieces = append(pieces, statementPiece{sql: piece.SQL, judgment: combineJudgments(piece.Effects, nil)})
			}
			judged[i] = keepJudged(pieces)
		}
		return judged, nil
	}
	files := make([][]lintext.Statement, 0, len(sources))
	for _, source := range sources {
		files = append(files, lintscan.Split(source.SQL, lintscan.ForDialect("")))
	}
	result, err := runtime.AnnotateStatements(ctx, lintext.AnnotateRequest{
		Facts: []lintext.Fact{lintext.FactSafetyEffect}, Sources: sources, Files: files,
	})
	if err != nil {
		return nil, err
	}
	for i, annotations := range result.Files {
		pieces := make([]statementPiece, 0, len(annotations))
		for j, annotation := range annotations {
			var declined []string
			for _, decline := range result.Declined {
				if decline.File == i && decline.Statement == j && !slices.Contains(declined, decline.Target) {
					declined = append(declined, decline.Target)
				}
			}
			pieces = append(pieces, statementPiece{sql: files[i][j].SQL, judgment: combineJudgments(annotation.Effects, declined)})
		}
		judged[i] = keepJudged(pieces)
	}
	return judged, nil
}

// keepJudged is pieces when an owner judged one of them, and nil otherwise.
func keepJudged(pieces []statementPiece) []statementPiece {
	if slices.ContainsFunc(pieces, func(piece statementPiece) bool { return piece.judgment != nil }) {
		return pieces
	}
	return nil
}

// combineJudgments is the judgment of one statement from the owners' safety
// effects, in target name order, and the targets whose owners declined it:
// the most severe impact, with every distinct reason joined in that order, or
// nil when no owner judged the statement and none declined it.
func combineJudgments(effects []lintext.Effect, declined []string) *ownerJudgment {
	var judgment *ownerJudgment
	var reasons []string
	raise := func(severity Severity, reason string) {
		if judgment == nil {
			judgment = &ownerJudgment{severity: severity}
		}
		if severityRank(severity) > severityRank(judgment.severity) {
			judgment.severity = severity
		}
		if !slices.Contains(reasons, reason) {
			reasons = append(reasons, reason)
		}
	}
	for _, effect := range effects {
		if effect.Fact == lintext.FactSafetyEffect {
			raise(classifyFeatureEffect(schemaext.Effect{Impact: effect.Impact, Reason: effect.Reason}))
		}
	}
	for _, target := range declined {
		raise(Destructive, fmt.Sprintf("the %s owner reads this statement otherwise and did not judge it; manual review is required", target))
	}
	if judgment != nil {
		judgment.reason = strings.Join(reasons, "; ")
	}
	return judgment
}
