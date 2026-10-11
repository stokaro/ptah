package ydblint

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/lintscan"
)

// judged are the facts [Splitter] reports for every statement it returns.
var judged = []lintext.Fact{
	lintext.FactStatefulDrop, lintext.FactUniqueIndex, lintext.FactRebuildDrop, lintext.FactRebuildRename,
	lintext.FactSafetyEffect,
}

// annotated are the facts [Annotator] judges, in the order it reports them.
var annotated = []lintext.Fact{lintext.FactStatefulDrop, lintext.FactSafetyEffect}

// Splitter splits YDB migrations as YQL and reports each statement's effects:
// a drop that loses state ([lintext.FactStatefulDrop]), YQL's unique index
// ([lintext.FactUniqueIndex]), the steps of a table rebuild
// ([lintext.FactRebuildDrop], [lintext.FactRebuildRename]), whose old table's
// columns it reads from the directory's own history (see the package
// documentation), and its judgment of a statement whose effect only this owner
// knows ([lintext.FactSafetyEffect]). Its zero value is ready to use.
type Splitter struct{}

// SplitStatements splits every source of a request on the ydb target, in one
// call, and judges every fact it reports. Another target wraps
// ptaherr.ErrUnsupportedDialect, and cancellation returns the context's
// error; either returns no result.
func (Splitter) SplitStatements(ctx context.Context, request lintext.SplitRequest) (lintext.SplitResult, error) {
	if ctx == nil {
		return lintext.SplitResult{}, fmt.Errorf("%w: splitting requires a context", lintext.ErrInvalid)
	}
	if request.Target != platform.YDB {
		return lintext.SplitResult{}, fmt.Errorf("%w: the %q statement splitter on %q", ptaherr.ErrUnsupportedDialect, platform.YDB, request.Target)
	}
	mode := lintscan.ForDialect(platform.YDB)
	files := make([]lintext.File, 0, len(request.Sources))
	for _, source := range request.Sources {
		if err := ctx.Err(); err != nil {
			return lintext.SplitResult{}, err
		}
		files = append(files, lintext.File{
			Name: source.Name, Version: source.Version, Up: source.Up, Down: source.Down, Repeatable: source.Repeatable,
			Statements: lintscan.Split(source.SQL, mode),
		})
	}
	h := newHistory(files)
	result := lintext.SplitResult{Files: make([][]lintext.Statement, 0, len(files)), Judged: slices.Clone(judged)}
	for i := range files {
		statements := files[i].Statements
		steps := lintkit.Rebuilds(statements, h.columns(files[i].Name, statements))
		for j := range statements {
			stmt := &statements[j]
			if effect, dropped := statefulDrop(stmt); dropped {
				stmt.Effects = append(stmt.Effects, effect)
			}
			stmt.Effects = append(stmt.Effects, uniqueIndexes(stmt)...)
			if effect, judged := safetyEffect(stmt); judged {
				stmt.Effects = append(stmt.Effects, effect)
			}
			if steps.Drops[j] {
				stmt.Effects = append(stmt.Effects, lintext.Effect{Fact: lintext.FactRebuildDrop})
			}
			if steps.Renames[j] {
				stmt.Effects = append(stmt.Effects, lintext.Effect{Fact: lintext.FactRebuildRename})
			}
		}
		result.Files = append(result.Files, statements)
	}
	return result, ctx.Err()
}

// Annotator judges a drop that loses state ([lintext.FactStatefulDrop]) and
// a statement whose effect only this owner knows ([lintext.FactSafetyEffect])
// over the split of a run that names no dialect, so DS107 reports such a drop
// there as it does on a YDB run, and a safety assessment with no target reads
// the owner's judgment. It splits each source again as YQL and judges a
// statement YQL splits the same way, from its YQL reading. It declines one
// whose two readings disagree about a fact it was asked: YQL splits it
// differently where either reading holds one, or splits it the same way and
// reads it otherwise. The dialect-blind scanner ends a string or a comment
// where YQL does not, and which reading the author meant is not this owner's
// to guess. It judges nothing else. Its zero value is ready to use.
type Annotator struct{}

// AnnotateStatements judges every statement of the request for the facts it
// asks about that the annotator judges. A request whose sources and statement
// lists do not pair up wraps lintext.ErrInvalid, and cancellation returns the
// context's error; either returns no result.
func (Annotator) AnnotateStatements(ctx context.Context, request lintext.AnnotateRequest) (lintext.AnnotateResult, error) {
	if ctx == nil {
		return lintext.AnnotateResult{}, fmt.Errorf("%w: annotating requires a context", lintext.ErrInvalid)
	}
	if err := lintext.ValidateAnnotateRequest(request); err != nil {
		return lintext.AnnotateResult{}, err
	}
	asked := slices.DeleteFunc(slices.Clone(annotated), func(fact lintext.Fact) bool { return !slices.Contains(request.Facts, fact) })
	result := lintext.AnnotateResult{Files: make([][]lintext.Annotation, len(request.Files))}
	for i, statements := range request.Files {
		if err := ctx.Err(); err != nil {
			return lintext.AnnotateResult{}, err
		}
		result.Files[i] = make([]lintext.Annotation, len(statements))
		if !mayHold(request.Sources[i].SQL, asked) {
			continue
		}
		yql := lintscan.Split(request.Sources[i].SQL, lintscan.ForDialect(platform.YDB))
		for j := range statements {
			result.Files[i][j] = annotate(&statements[j], yql, asked)
		}
	}
	return result, ctx.Err()
}

// mayHold reports whether a source may hold a statement with one of the asked
// facts. Every such statement starts with, or holds, one of the words a fact
// lists, so a source that holds none of them in any letter case holds no such
// statement however it is lexed, which spares reading it again.
func mayHold(sql string, asked []lintext.Fact) bool {
	upper := strings.ToUpper(sql)
	for _, fact := range asked {
		words := []string{"DROP"}
		if fact == lintext.FactSafetyEffect {
			words = append(words, "ALTER", "REPLACE")
		}
		if slices.ContainsFunc(words, func(word string) bool { return strings.Contains(upper, word) }) {
			return true
		}
	}
	return false
}

// annotate judges one statement the dialect-blind scanner split, against the
// statements YQL splits its source into.
func annotate(stmt *lintext.Statement, yql []lintext.Statement, asked []lintext.Fact) lintext.Annotation {
	var overlapping []lintext.Statement
	for _, piece := range yql {
		if piece.Start < stmt.End && stmt.Start < piece.End {
			overlapping = append(overlapping, piece)
		}
	}
	blind := read(stmt, asked)
	if len(overlapping) == 1 && overlapping[0].Start == stmt.Start && overlapping[0].End == stmt.End {
		own := read(&overlapping[0], asked)
		if !slices.EqualFunc(blind, own, func(a, b lintext.Effect) bool { return a.Fact == b.Fact && a.Impact == b.Impact }) {
			return lintext.Annotation{Declined: true}
		}
		return lintext.Annotation{Effects: own}
	}
	if len(blind) == 0 && !slices.ContainsFunc(overlapping, func(piece lintext.Statement) bool { return len(read(&piece, asked)) > 0 }) {
		return lintext.Annotation{}
	}
	return lintext.Annotation{Declined: true}
}

// read returns the statement's effects of the asked facts, in their order.
func read(stmt *lintext.Statement, asked []lintext.Fact) []lintext.Effect {
	var effects []lintext.Effect
	for _, fact := range asked {
		reader := statefulDrop
		if fact == lintext.FactSafetyEffect {
			reader = safetyEffect
		}
		if effect, found := reader(stmt); found {
			effects = append(effects, effect)
		}
	}
	return effects
}

// statefulDrop reads a drop of a YDB object whose state a new object of the
// same name does not get back, and returns its effect.
//
// A topic holds every message written to it and every consumer's position in
// it. Dropping a transfer stops it and drops the topic consumer YDB created
// for it, with its position; an async replication dropped with CASCADE drops
// its replica tables, and one dropped without it is YD115's question. A
// coordination node is dropped through Ptah's own statement (see
// dialect/ydb/ydbcoordination). Measured on 26.2.1.14 and 25.1.4.7: DropNode
// succeeds while a session holds a semaphore on the node, the holder's session
// and lease end about five seconds later, and a node created again under the
// same path holds none of the persistent semaphores the dropped one held. Its
// rate limiter resources go with it. A streaming query keeps its checkpoint.
func statefulDrop(stmt *lintext.Statement) (lintext.Effect, bool) {
	w := stmt.Words
	if len(w) < 2 || w[0] != "DROP" {
		return lintext.Effect{}, false
	}
	var kind []string
	switch {
	case w[1] == "TOPIC" || w[1] == "TRANSFER":
		kind = w[1:2]
	case w[1] == "ASYNC" && len(w) > 2 && w[2] == "REPLICATION" && w[len(w)-1] == "CASCADE":
		kind = w[1:3]
	case w[1] == "COORDINATION" && len(w) > 2 && w[2] == "NODE", w[1] == "STREAMING" && len(w) > 2 && w[2] == "QUERY":
		kind = w[1:3]
	default:
		return lintext.Effect{}, false
	}
	object, _ := lintkit.TableRefAt(w, stmt.SourceWords, lintkit.SkipIfExists(w, 1+len(kind)))
	return lintext.Effect{Fact: lintext.FactStatefulDrop, Object: object.Name, Kind: strings.Join(kind, " ")}, true
}

// uniqueIndexes reads YQL's unique index, which only YQL writes with UNIQUE
// after the index name: ALTER TABLE t ADD INDEX name [GLOBAL] UNIQUE [SYNC]
// [USING kind] ON (columns), at each clause of an ALTER TABLE.
//
// YDB words a failed build its own way: measured on 26.2.1.14 with
// EnableAddUniqueIndex on, ADD INDEX ... GLOBAL UNIQUE SYNC over two rows
// holding the same value answers `Duplicate key found: (v=a)`.
func uniqueIndexes(stmt *lintext.Statement) []lintext.Effect {
	w := stmt.Words
	if !lintkit.IsAlterTable(w) {
		return nil
	}
	table := lintkit.AlterTableReference(w, stmt.SourceWords)
	var effects []lintext.Effect
	for _, i := range lintkit.ClauseStarts(w) {
		end := lintkit.ClauseEnd(w, i)
		if i+2 >= end || w[i] != "ADD" || w[i+1] != "INDEX" || !lintkit.IdentLike(w[i+2]) {
			continue
		}
		if columns, ok := uniqueKey(w[:end], stmt.SourceWords, i+3); ok {
			effects = append(effects, lintext.Effect{
				Fact: lintext.FactUniqueIndex, Object: lintkit.SourceWordAt(w, stmt.SourceWords, i+2), Table: table.Name,
				Columns: columns, Clause: "ADD INDEX", Failure: "YDB: Duplicate key found",
			})
		}
	}
	return effects
}

// uniqueKey reads the rest of an ADD INDEX clause from j: whether it is
// UNIQUE, and the columns its ON names.
func uniqueKey(w, sourceWords []string, j int) ([]string, bool) {
	unique := false
	for ; j < len(w); j++ {
		switch w[j] {
		case "UNIQUE":
			unique = true
		case "ON":
			columns, _, ok := lintkit.KeyColumnList(w, sourceWords, j+1)
			return columns, ok && unique
		}
	}
	return nil, false
}
