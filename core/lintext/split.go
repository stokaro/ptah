package lintext

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
)

// Source is one migration file's executable SQL as a host hands it to a
// splitter.
type Source struct {
	// Name identifies the file, as [File.Name] does in a check request. It
	// is not interpreted.
	Name string `json:"name"`
	// SQL is the text to split.
	SQL string `json:"sql"`
	// Version, Up, Down and Repeatable place the file in the directory: its
	// migration version, zero when its name carries none, which half of a
	// migration it is, and whether it is a repeatable migration rather than
	// an ordered version. A fact that depends on what earlier files did, such
	// as a table rebuild that leaves a column out, reads them.
	Version    int64 `json:"version,omitempty"`
	Up         bool  `json:"up,omitempty"`
	Down       bool  `json:"down,omitempty"`
	Repeatable bool  `json:"repeatable,omitempty"`
}

// Statement is one statement of a source, in the forms rules read. Every
// form is the splitter's, because each depends on the dialect: where a
// statement ends, what is a comment, how a name is quoted.
type Statement struct {
	// Start and End are the statement's byte range in the source's SQL, from
	// its first token to its last, the terminating semicolon excluded unless
	// the dialect makes it part of the statement.
	Start int `json:"start"`
	End   int `json:"end"`
	// Line is the 1-based line of the statement's first token.
	Line int `json:"line"`
	// SQL is the statement text as written, SQL[Start:End] of the source.
	SQL string `json:"sql"`
	// Canonical is the display form: comments removed, every whitespace run
	// collapsed to one space, everything but string literals and quoted
	// identifiers uppercased.
	Canonical string `json:"canonical"`
	// Words is the token sequence rules scan: comments and whitespace
	// dropped, bare words uppercased, punctuation as entries of its own,
	// string literals and quoted identifiers verbatim as single entries.
	Words []string `json:"words"`
	// SourceWords is Words before the uppercasing, as the author spelled
	// each token.
	SourceWords []string `json:"source_words"`
	// Routine is the routine the statement defines, nil when it defines none
	// or the splitter cannot parse one.
	Routine *Routine `json:"routine,omitempty"`
	// Comments are the comments directly above the statement, in order:
	// those after the line the previous statement ends on, with no blank
	// line between them and the statement. A host reads its suppression
	// directives from them.
	Comments []string `json:"comments,omitempty"`
	// Effects are what the statement does, as facts a rule reads instead of
	// recognizing the dialect's spelling of them. A splitter reports the
	// facts it lists in [SplitResult.Judged].
	Effects []Effect `json:"effects,omitempty"`
}

// Fact names one kind of thing a statement does that a rule shared by every
// dialect needs and only the dialect's owner can recognize.
type Fact string

const (
	// FactStatefulDrop is a drop of an object whose state a new object of the
	// same name does not get back: the messages of a topic and the positions
	// of its consumers, the semaphores of a coordination node.
	FactStatefulDrop Fact = "stateful-drop"
	// FactUniqueIndex is a unique index built over the rows a table already
	// holds, which fails on the first duplicate it meets, in a spelling only
	// the owner's dialect uses. A host reads CREATE UNIQUE INDEX and ADD
	// UNIQUE itself.
	FactUniqueIndex Fact = "unique-index"
	// FactRebuildDrop is a DROP TABLE of a table whose rows a table rebuild
	// copied, and FactRebuildRename a rename that moves the replaced table
	// aside or its copy into place: steps of a rebuild, not a drop or a rename
	// of their own. Whether a copy keeps every column depends on what the old
	// table holds, which an owner may know where a host does not, so a host
	// that matches rebuilds itself takes the owner's answer where a split
	// judged them. A split judges both or neither.
	FactRebuildDrop   Fact = "rebuild-drop"
	FactRebuildRename Fact = "rebuild-rename"
	// FactSafetyEffect is the owner's judgment of what a statement only it
	// recognizes does to stored data or to running work, which a safety
	// assessment takes in place of its own reading of the words: a dropped
	// topic loses its messages, and an async replication dropped without
	// CASCADE leaves its replica tables read-only. The effect carries the
	// judgment in [Effect.Impact] and [Effect.Reason]. An owner reports one
	// effect of this fact for a statement, or none, and none means the
	// statement is not one the owner judges, not that it is harmless.
	FactSafetyEffect Fact = "safety-effect"
)

// Effect is one fact about what a statement does.
type Effect struct {
	// Fact is what the statement does.
	Fact Fact `json:"fact"`
	// Object is what the statement acts on, as written: the dropped object or
	// the unique index. A rebuild step names none.
	Object string `json:"object,omitempty"`
	// Kind names what Object is in the dialect's words, such as TOPIC or
	// COORDINATION NODE, empty where the fact implies it.
	Kind string `json:"kind,omitempty"`
	// Table and Columns are the table a unique index is built on and its key
	// columns, as written.
	Table   string   `json:"table,omitempty"`
	Columns []string `json:"columns,omitempty"`
	// Clause is the clause head a message names a unique index build by,
	// such as ADD INDEX.
	Clause string `json:"clause,omitempty"`
	// Failure is the engine's own words for the error the statement fails
	// with, for a message, such as "YDB: Duplicate key found".
	Failure string `json:"failure,omitempty"`
	// Impact and Reason are the judgment a [FactSafetyEffect] carries, and
	// no other fact carries them. Impact is [schemaext.Behavioral] or
	// [schemaext.Destructive]: an owner does not vouch that a statement is
	// additive, because the assessment takes the owner's judgment in place
	// of its own. Reason says why, in the words a report shows.
	Impact schemaext.Impact `json:"impact,omitempty"`
	Reason string           `json:"reason,omitempty"`
}

// Routine is what a rule may read of the routine a statement defines.
type Routine struct {
	// BodyWords is the body tokenized the way [Statement.Words] is.
	BodyWords []string `json:"body_words"`
}

// SplitRequest asks a target's owner to split a lint run's sources, all of
// them in one call.
type SplitRequest struct {
	// Target is the canonical name of the target the run names.
	Target string `json:"target"`
	// Sources are the run's files, in the order the run reads them.
	Sources []Source `json:"sources"`
}

// SplitResult holds each source's statements, in request order.
type SplitResult struct {
	// Files holds one statement list per source, in source order.
	Files [][]Statement `json:"files"`
	// Judged are the facts the splitter judged for every statement it
	// returned: a statement without an effect of one of them does not have
	// it. A fact not listed was not judged.
	Judged []Fact `json:"judged,omitempty"`
}

// Splitter splits migration SQL for one dialect. A host calls it once per
// lint run with every file of the run.
type Splitter interface {
	// SplitStatements returns the statements of every source, or an error
	// and no result. A target the splitter does not serve wraps
	// ptaherr.ErrUnsupportedDialect.
	SplitStatements(ctx context.Context, request SplitRequest) (SplitResult, error)
}

// ValidateSplit reports a result that does not answer request: a statement
// count per source other than one list per source, a statement whose span
// falls outside its source, overlaps the one before it, disagrees with its
// text, or has no line, a judged fact this package does not define, one of
// the two rebuild facts judged without the other, an effect of a fact the
// split does not list as judged, and effects [ValidateAnnotate] refuses too: an
// Impact or a Reason on a fact other than [FactSafetyEffect], a safety effect
// whose Impact is not Behavioral or Destructive or whose Reason is empty, and
// two safety effects on one statement. Each failure wraps [ErrInvalid].
func ValidateSplit(request SplitRequest, result SplitResult) error {
	if len(result.Files) != len(request.Sources) {
		return fmt.Errorf("%w: a split of %d sources returned %d", ErrInvalid, len(request.Sources), len(result.Files))
	}
	for _, fact := range result.Judged {
		if !slices.Contains(facts, fact) {
			return fmt.Errorf("%w: a split judges unknown fact %q", ErrInvalid, fact)
		}
	}
	if slices.Contains(result.Judged, FactRebuildDrop) != slices.Contains(result.Judged, FactRebuildRename) {
		return fmt.Errorf("%w: a split judges %q and %q together or neither", ErrInvalid, FactRebuildDrop, FactRebuildRename)
	}
	for i, statements := range result.Files {
		source := request.Sources[i]
		end := 0
		for j, statement := range statements {
			if statement.Start < end || statement.End < statement.Start || statement.End > len(source.SQL) ||
				source.SQL[statement.Start:statement.End] != statement.SQL || statement.Line < 1 {
				return fmt.Errorf("%w: statement %d of %s does not match its source", ErrInvalid, j, source.Name)
			}
			for _, effect := range statement.Effects {
				if !slices.Contains(result.Judged, effect.Fact) {
					return fmt.Errorf("%w: statement %d of %s reports %q, which the split does not judge", ErrInvalid, j, source.Name, effect.Fact)
				}
			}
			if fault := effectsFault(statement.Effects); fault != "" {
				return fmt.Errorf("%w: statement %d of %s %s", ErrInvalid, j, source.Name, fault)
			}
			end = statement.End
		}
	}
	return nil
}

// facts are the facts this package defines.
var facts = []Fact{FactStatefulDrop, FactUniqueIndex, FactRebuildDrop, FactRebuildRename, FactSafetyEffect}

// effectsFault says what is wrong with one statement's effects, as the rest of
// a sentence that names the statement, and is empty when nothing is. A split
// and an annotation are held to it alike, so an owner's judgment reads the same
// whichever of them carried it.
func effectsFault(effects []Effect) string {
	judged := 0
	for _, effect := range effects {
		if effect.Fact != FactSafetyEffect {
			if effect.Impact != "" || effect.Reason != "" {
				return fmt.Sprintf("carries an impact or a reason on %q, which only %q carries", effect.Fact, FactSafetyEffect)
			}
			continue
		}
		judged++
		switch {
		case effect.Impact != schemaext.Behavioral && effect.Impact != schemaext.Destructive:
			return fmt.Sprintf("judges impact %q, which is not %q or %q", effect.Impact, schemaext.Behavioral, schemaext.Destructive)
		case strings.TrimSpace(effect.Reason) == "":
			return fmt.Sprintf("judges impact %q without a reason", effect.Impact)
		case judged > 1:
			return fmt.Sprintf("reports %q twice", FactSafetyEffect)
		}
	}
	return ""
}
