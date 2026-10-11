// Package ydblint holds what the YDB owner contributes to migration lint: the
// YD family of YDB hazards, and the splitter that reads a YDB migration as
// YQL and reports the facts the rules every dialect shares read.
//
// Rules publishes the family as data and Service checks it, once per lint
// run. Splitter splits a run's files and reports each statement's effects, as
// ptah.run/core/lintext describes: a drop that loses state, YQL's unique
// index, and the steps of a table rebuild. Annotator judges the first of
// those over the split of a run that names no dialect. The bundled runtime
// ptah.run/engine/builtin registers all four on the ydb target, and
// ptah.run/migration/lint applies its policy to the findings: suppression,
// severity overrides, excluded paths and the gate.
//
// # The YD family
//
// The YD family is the YDB statements the server refuses, or applies with an
// effect the statement does not state. Each rule reads YQL through
// internal/yqlddl, which `ptah sql lint` reads it through too, and each trap
// was measured on YDB 26.2.1.14 and 25.1.4.7 with the answers quoted at the
// rule.
//
// Every rule here describes whether a statement can run or what it does when
// it runs, which the server decides the same way in either direction, so each
// one reads the down half of a migration as well.
//
// YD104, YD105, YD106 and YD118 need to know what the table looks like before
// the statement: whether an index uses a column, which column the TTL reads,
// what its minimum partition count and its partition size are, which views
// read a table. They read that state from the directory itself, whether or not
// the run has a dev database: the up migrations before the analyzed version, in
// version order, then the statements of the file before the one analyzed. A
// table created outside the directory is unknown to it, and an unknown table
// reports nothing. The splitter reads a rebuild's old table from the same
// history.
//
// A run that names no dialect sends every rule, YD included, over the host's
// dialect-blind split, which does not read YQL, against a target that answers
// no capability. A YD rule says nothing there ([ydbRun]): judging another
// dialect's SQL by what YDB refuses would report hazards that are not there,
// and YD103 would take DD101's place on every NOT NULL column.
package ydblint

import (
	"cmp"
	"context"
	"slices"

	"ptah.run/core/lintext"
	"ptah.run/core/platform"
	"ptah.run/internal/lintkit"
)

// Rules returns the rules the YDB owner publishes, in code order. Each call
// returns a fresh slice.
func Rules() []lintext.Rule {
	return rules(nil).Published()
}

// Service checks the YDB owner's rules. Its zero value is ready to use.
type Service struct{}

// CheckMigrations judges the request's files under the codes it enables. A
// code this owner does not publish wraps lintext.ErrInvalid, and cancellation
// returns the context's error; either returns no result.
func (Service) CheckMigrations(ctx context.Context, request lintext.CheckRequest) (lintext.CheckResult, error) {
	var h history
	if request.Target.Name == platform.YDB {
		h = newHistory(request.Files)
	}
	return rules(h).CheckMigrations(ctx, request)
}

// rules are the owner's rules and the Go functions behind them, in code
// order, reading the schema history h.
func rules(h history) lintkit.Rules {
	all := lintkit.Rules(ydbRules(h))
	slices.SortFunc(all, func(a, b lintkit.Rule) int { return cmp.Compare(a.Code, b.Code) })
	return all
}
