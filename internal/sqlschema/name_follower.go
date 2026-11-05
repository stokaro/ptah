package sqlschema

import (
	"maps"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// NameCollision is a name a statement writes that a server stores as a name
// its namespace already holds under another spelling. Two spellings stored as
// one means the server cut at least one of them: both to the same first
// bytes, or one to the bytes the other wrote.
type NameCollision struct {
	// Name is the later declaration: what it names, where, as written and as
	// stored.
	Name StoredName
	// HeldBy is the kind of the object that holds the stored name, and HeldAs
	// the name as its declaration wrote it, or as the server derived it.
	HeldBy string
	HeldAs string
	// Refused reports that the server refuses the later statement. Otherwise
	// the statement is guarded: IF NOT EXISTS makes the server skip it, and OR
	// REPLACE applies it to the object the server finds, so the server keeps
	// one of the two objects the SQL names.
	Refused bool
}

// StatementNames is what one statement does to the names a server keeps.
type StatementNames struct {
	// Declares reports a statement of a kind that writes names of its own,
	// such as a CREATE or an ALTER TABLE. A drop only forgets names.
	Declares bool
	// Cut are the names the statement declares that the server cuts, in the
	// order it writes them. A name that collides is reported in Collisions
	// instead.
	Cut []StoredName
	// Collisions are the names the statement writes that the server stores
	// as a name already in use, in the order it writes them.
	Collisions []NameCollision
}

// NameFollower follows, statement by statement, the names SQL declares on a
// server that cuts long names, through the namespaces PostgreSQL keeps (see
// the reader's analysis for what was measured on 15.19). It is what a linter
// reads: each statement's answer is its own, so a finding can be placed in
// the statement that caused it.
//
// A statement that collides is followed up to the colliding name and no
// further, so a later statement is judged as though the names it did declare
// stand. The server refuses such a statement, so a file that collides once
// may report less after it than the server would.
type NameFollower struct {
	names *storedNames
}

// NewNameFollower returns a follower for statements of dialect. It returns
// false for a dialect whose descriptor does not say its server cuts long
// names, where no name is cut and none collides by cutting.
func NewNameFollower(dialect string) (*NameFollower, bool) {
	if !cutsNames(dialect) {
		return nil, false
	}
	return &NameFollower{names: newStoredNames(dialect)}, true
}

// Follow records the names statement declares, drops or renames, and returns
// what it did to the names the server keeps.
func (f *NameFollower) Follow(statement ast.Node) StatementNames {
	cut, collided := len(f.names.cut), len(f.names.collided)
	// The only error following returns is a collision, which collided already
	// holds in the form a linter reports.
	declares, _ := f.names.follow(statement)
	return StatementNames{
		Declares:   declares,
		Cut:        since(f.names.cut, cut),
		Collisions: since(f.names.collided, collided),
	}
}

// since is a copy of the entries of list from index on, nil when there are
// none.
func since[T any](list []T, index int) []T {
	if index >= len(list) {
		return nil
	}
	return slices.Clone(list[index:])
}

// Clone returns a follower that starts from the names f holds and follows
// statements on its own, such as the statements of a rollback that runs from
// the state a sequence of migrations leaves.
func (f *NameFollower) Clone() *NameFollower {
	return &NameFollower{names: f.names.clone()}
}

// clone copies the namespaces and what was reported, so following on the
// copy changes nothing here. An entry's columns are shared: they are replaced,
// never written in place.
func (s *storedNames) clone() *storedNames {
	copied := *s
	copied.spaces = make(map[string]map[string]storedEntry, len(s.spaces))
	for key, names := range s.spaces {
		copied.spaces[key] = maps.Clone(names)
	}
	copied.cut = slices.Clone(s.cut)
	copied.collided = slices.Clone(s.collided)
	copied.skipped = maps.Clone(s.skipped)
	copied.derived = nil
	return &copied
}

// LocateName returns the byte offset in sql of the first identifier dialect
// reads as written, the folded form [StoredName.Written] carries: a bare word
// folded as the server folds one, or a quoted identifier. It returns false
// when sql writes no such identifier.
//
// A name is found in the statement's tokens rather than in its text, so a
// string literal or a comment that spells the name is not taken for it.
func LocateName(dialect, sql, written string) (int, bool) {
	tokens := lexer.NewLexerWithOptions(sql, dialectlexer.Options(dialect))
	for token := tokens.NextToken(); token.Type != lexer.TokenEOF; token = tokens.NextToken() {
		identifier := token.Type == lexer.TokenIdentifier ||
			(token.Type == lexer.TokenString && isQuotedSQLIdentifierPart(token.Value))
		if identifier && foldedIdentifierPart(dialect, token.Value) == written {
			return token.Start, true
		}
	}
	return 0, false
}
