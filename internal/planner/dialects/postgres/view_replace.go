package postgres

import (
	"go.5x5.cz/ptah/internal/viewprojection"
)

// viewReplaceVerdict is what the mechanical test can say about replacing one
// view body with another.
//
// Three answers are needed rather than two, because "PostgreSQL will refuse
// this" and "this parser cannot tell" select different plans. A refusal is a
// fact about the change and holds in both directions. Not being able to tell is
// a fact about the parser, and the caller resolves it from the direction it is
// planning: see viewReplaceKeepsDependents.
type viewReplaceVerdict int

const (
	// viewReplaceUndecidable means at least one of the two bodies has a shape
	// this parser does not model, so nothing is known about the change: a WITH
	// prefix, a top-level set operation, a parenthesized query, a star
	// projection whose columns are not spelled out, a select item whose output
	// column name cannot be derived, or an empty previous body.
	viewReplaceUndecidable viewReplaceVerdict = iota

	// viewReplaceAppendsColumns means both bodies parsed, they read the same
	// relations, and the new select list is the old one with columns appended
	// to the end. That is the one shape PostgreSQL accepts for an in-place
	// replace.
	viewReplaceAppendsColumns

	// viewReplaceMovesColumns means both bodies parsed and the new select list
	// is NOT the old one with columns appended -- a column was dropped, renamed
	// or given a different type-determining expression -- or the two read
	// different relations, which fixes the types of the shared items and cannot
	// be checked from the select list alone. PostgreSQL refuses the replace for
	// the first group; for the second it may or may not, and answering "do not
	// replace" is the only appliable choice.
	viewReplaceMovesColumns
)

// viewReplaceLegality classifies a change from previousBody to nextBody.
//
// PostgreSQL accepts CREATE OR REPLACE VIEW only when the new query produces the
// old column list with columns appended to the end -- the same names, the same
// types, in the same order. Measured on PostgreSQL 17.10 against a view over
// (id bigint, email text, age integer):
//
//	append a trailing column   accepted
//	drop the appended column   ERROR: cannot drop columns from view
//	rename a column            ERROR: cannot change name of view column "id" to "uid"
//	change a column type       ERROR: cannot change data type of view column "id"
//	change only the predicate  accepted
//
// Column names are read off the select list, which is the only place a view's
// projection is written down. Two items are the same column when their output
// names match and either both are plain references to the same column, or both
// are the same expression text. That comparison sees through the two spellings
// the same view legitimately has -- "SELECT id FROM t" as authored and
// "SELECT t.id FROM t" as pg_get_viewdef reads it back -- while a cast, a rename
// or a different expression changes it.
//
// A select item's TYPE, though, is fixed by the relation it reads, and the
// select list does not say what that relation is. Comparing the items alone
// answered "appends only" for a swapped relation, which PostgreSQL then refused:
//
//	CREATE VIEW v AS SELECT id FROM b;            -- b.id is text
//	CREATE OR REPLACE VIEW v AS SELECT id FROM a; -- a.id is bigint
//	ERROR:  cannot change data type of view column "id" from text to bigint
//
// So the FROM/JOIN text is part of the comparison: any change to the relations,
// including one that merely spells them differently, answers
// viewReplaceMovesColumns. The clauses that do not decide a type -- WHERE,
// GROUP BY, HAVING, ORDER BY and the rest -- are excluded, so a predicate-only
// edit still answers viewReplaceAppendsColumns and keeps the cheap replace.
//
// "Spelled the same way" is decided by PostgreSQL's identifier rules rather
// than by letter case alone -- see [viewprojection.Parse], which folds case
// everywhere except inside quoted identifiers, because "Foo" and "foo" are two
// relations.
//
// The residual assumption is narrow: the shared items read the same relations,
// spelled the same way, and those relations' columns were not retyped by the
// same migration. PostgreSQL refuses to alter a column type a view depends on,
// so reaching that last state requires dropping the view first, which is a
// different plan than this one.
func viewReplaceLegality(previousBody, nextBody string) viewReplaceVerdict {
	previous, previousFrom, ok := viewprojection.Parse(previousBody)
	if !ok {
		return viewReplaceUndecidable
	}
	next, nextFrom, ok := viewprojection.Parse(nextBody)
	if !ok {
		return viewReplaceUndecidable
	}
	if previousFrom != nextFrom {
		return viewReplaceMovesColumns
	}
	if len(next) < len(previous) {
		return viewReplaceMovesColumns
	}
	for i, item := range previous {
		if item != next[i] {
			return viewReplaceMovesColumns
		}
	}
	return viewReplaceAppendsColumns
}
