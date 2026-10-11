// Package pgtype reads PostgreSQL column types out of migration statements
// and decides what PostgreSQL does when a column changes from one to
// another: nothing, a catalog edit, an index rebuild or a table rewrite.
//
// It is PostgreSQL knowledge, so it belongs to the PostgreSQL owner
// (ptah.run/dialect/postgres/pglint), whose PG301 and PG301P rules read it.
// It sits here, shared, for one reason: the common DS103 rule of
// ptah.run/migration/lint keeps a PostgreSQL arm that reads it to tell an
// unchanged type from a changed one, and the host must not import an owner.
// That arm is on the stokaro/ptah#4140 list of engine arms that move behind
// the owner seam, and this package moves into the owner with it. Until then
// it is debt, not design.
package pgtype

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/lintscan"
)

// Type is one column type as PostgreSQL's catalog holds it: the canonical
// name format_type prints, the type modifier, and whether it is an array.
type Type struct {
	// Base is format_type's spelling: integer, character varying, timestamp
	// without time zone, ... For a type this package does not know, Base is
	// the name as written and Known is false.
	Base  string
	Known bool
	// Mods is the type modifier: a length, (precision, scale), or a
	// fractional-seconds precision. nil is "no modifier".
	Mods []int
	// Array reports an array of Base.
	Array bool
}

// baseSynonyms folds the spellings of one catalog type.
var baseSynonyms = map[string]string{
	"INT": "integer", "INTEGER": "integer", "INT4": "integer", "SERIAL": "integer", "SERIAL4": "integer",
	"BIGINT": "bigint", "INT8": "bigint", "BIGSERIAL": "bigint", "SERIAL8": "bigint",
	"SMALLINT": "smallint", "INT2": "smallint", "SMALLSERIAL": "smallint", "SERIAL2": "smallint",
	"REAL": "real", "FLOAT4": "real",
	"FLOAT8": "double precision", "DOUBLE": "double precision",
	"NUMERIC": "numeric", "DECIMAL": "numeric", "DEC": "numeric",
	"BOOL": "boolean", "BOOLEAN": "boolean",
	"VARCHAR": "character varying", "CHARACTER": "character", "CHAR": "character", "BPCHAR": "character",
	"TEXT": "text", "BYTEA": "bytea", "UUID": "uuid", "JSON": "json", "JSONB": "jsonb", "XML": "xml",
	"DATE": "date", "MONEY": "money", "INET": "inet", "CIDR": "cidr", "MACADDR": "macaddr", "MACADDR8": "macaddr8",
	"TIMESTAMP": "timestamp without time zone", "TIMESTAMPTZ": "timestamp with time zone",
	"TIME": "time without time zone", "TIMETZ": "time with time zone",
	"INTERVAL": "interval",
	"BIT":      "bit", "VARBIT": "bit varying",
	"OID": "oid", "NAME": "name",
}

// precisionBases are the families whose modifier is a fractional-seconds
// precision that defaults to 6.
var precisionBases = map[string]bool{
	"timestamp without time zone": true, "timestamp with time zone": true,
	"time without time zone": true, "time with time zone": true,
	"interval": true,
}

// ParseAt reads a type from statement words starting at words[i] and
// the COLLATE and USING clauses that may follow it, up to end.
func ParseAt(words []string, i, end int) (Type, bool) {
	var t Type
	j := i
	if j >= end {
		return t, false
	}
	j = t.parsePGBase(words, j, end)
	if t.Base == "" {
		return t, false
	}
	mods, next, ok := lintkit.ParseTypeParams(words[:end], j)
	if !ok {
		return t, false
	}
	t.Mods = mods
	j = t.parsePGZone(words, next, end)
	t.parsePGArray(words, j, end)
	// What follows is read elsewhere or by nobody: the PostgreSQL owner
	// judges the COLLATE clause against the column's current collation and
	// indexes, and a USING expression decides for itself, so it is not
	// claimed either way.
	return t.finish(), true
}

// parsePGBase reads the type name, folding the two-word spellings, and
// returns the index after it.
func (t *Type) parsePGBase(words []string, j, end int) int {
	word := words[j]
	switch {
	case word == "DOUBLE" && j+1 < end && words[j+1] == "PRECISION":
		t.Base, t.Known = "double precision", true
		return j + 2
	case word == "CHARACTER" && j+1 < end && words[j+1] == "VARYING":
		t.Base, t.Known = "character varying", true
		return j + 2
	case word == "BIT" && j+1 < end && words[j+1] == "VARYING":
		t.Base, t.Known = "bit varying", true
		return j + 2
	case word == "FLOAT":
		t.Base, t.Known = "float", true
		return j + 1
	}
	if base, ok := baseSynonyms[word]; ok {
		t.Base, t.Known = base, true
		return j + 1
	}
	if !lintkit.IdentLike(word) {
		return j
	}
	// A qualified or quoted name: kept as written, compared by equality only.
	parts := []string{strings.Trim(word, `"`)}
	k := j + 1
	for k+1 < end && words[k] == "." && lintkit.IdentLike(words[k+1]) {
		parts = append(parts, strings.Trim(words[k+1], `"`))
		k += 2
	}
	t.Base = strings.ToLower(strings.Join(parts, "."))
	return k
}

// parsePGZone reads WITH TIME ZONE or WITHOUT TIME ZONE after a timestamp
// or time.
func (t *Type) parsePGZone(words []string, j, end int) int {
	if j+2 >= end || (words[j] != "WITH" && words[j] != "WITHOUT") || words[j+1] != "TIME" || words[j+2] != "ZONE" {
		return j
	}
	family, _, _ := strings.Cut(t.Base, " ")
	if family != "timestamp" && family != "time" {
		return j
	}
	t.Base = family + " " + strings.ToLower(words[j]) + " time zone"
	return j + 3
}

// parsePGArray reads [] or ARRAY after the type.
func (t *Type) parsePGArray(words []string, j, end int) {
	if j < end && (words[j] == "[" || words[j] == "ARRAY") {
		t.Array = true
	}
}

// finish applies the defaults the catalog applies.
func (t Type) finish() Type {
	switch {
	case t.Base == "float":
		t.Base = "double precision"
		if len(t.Mods) == 1 && t.Mods[0] <= 24 {
			t.Base = "real"
		}
		t.Mods = nil
	case t.Base == "character" || t.Base == "bit":
		if len(t.Mods) == 0 {
			t.Mods = []int{1}
		}
	case precisionBases[t.Base]:
		if len(t.Mods) == 0 {
			t.Mods = []int{6}
		}
	case t.Base == "numeric" && len(t.Mods) == 1:
		t.Mods = []int{t.Mods[0], 0}
	}
	return t
}

// ParseSpelling reads the type as the report spells it from the
// catalog -- `character varying(20)`, `timestamp(3) without time zone`,
// `integer[]` -- through the same tokenizer and parser as the statement.
func ParseSpelling(spelling string) (Type, bool) {
	spelling = strings.TrimSpace(spelling)
	if spelling == "" {
		return Type{}, false
	}
	words := lintscan.Words(spelling, lintscan.ForDialect("postgres"))
	return ParseAt(words, 0, len(words))
}

// Spell renders the canonical form for a message, the way format_type would.
func (t Type) Spell() string {
	var b strings.Builder
	family, zone := t.Base, ""
	if i := strings.Index(t.Base, " with"); i > 0 {
		family, zone = t.Base[:i], t.Base[i+1:]
	}
	b.WriteString(family)
	if len(t.Mods) > 0 && (!precisionBases[t.Base] || t.Mods[0] != 6) {
		b.WriteString("(")
		for i, m := range t.Mods {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(strconv.Itoa(m))
		}
		b.WriteString(")")
	}
	if zone != "" {
		b.WriteString(" " + zone)
	}
	if t.Array {
		b.WriteString("[]")
	}
	return b.String()
}

// SameAs reports whether two types are the same catalog type: base, array
// and every modifier.
func (t Type) SameAs(o Type) bool {
	if t.Base != o.Base || t.Array != o.Array || len(t.Mods) != len(o.Mods) {
		return false
	}
	for i := range t.Mods {
		if t.Mods[i] != o.Mods[i] {
			return false
		}
	}
	return true
}

// Outcome is what PostgreSQL does with one type change. Only the two
// rewrites produce a finding; the other three are told apart for the
// message a future rule might want, and are silent alike today.
type Outcome int

// The outcomes of one type change.
const (
	// Unchanged is no change of the catalog type.
	Unchanged Outcome = iota
	// InPlace is a change PostgreSQL applies as a catalog edit.
	InPlace
	// Unknown is a change between types this package does not know.
	Unknown
	// Rewrite rewrites the table and rebuilds its indexes.
	Rewrite
	// RewriteUnlessUTC is the timestamp / timestamptz conversion, which
	// rewrites unless the session TimeZone is UTC and rebuilds the column's
	// indexes either way.
	RewriteUnlessUTC
	// IndexRebuild is a collation change on an indexed column: the heap
	// is kept and every index on the column is rebuilt.
	IndexRebuild
)

// Transition is the outcome of one type change and the words that explain
// it.
type Transition struct {
	Outcome Outcome
	// Why is the clause a message gives as the reason.
	Why string
	// Abort names the error a stored value the new type cannot hold raises,
	// where one was measured.
	Abort string
	// Indexes names the indexes an IndexRebuild rebuilds.
	Indexes []string
}

// Compare decides the outcome for one column.
func Compare(old, updated Type) Transition {
	if !old.Known || !updated.Known {
		if old.Base == updated.Base && old.Array == updated.Array {
			return Transition{Outcome: Unchanged}
		}
		return Transition{Outcome: Unknown}
	}
	return compareKnown(old, updated)
}

// compareKnown decides the outcome between two types this package knows.
func compareKnown(old, updated Type) Transition {
	change := fmt.Sprintf("changes the type from %s to %s", old.Spell(), updated.Spell())
	switch {
	case old.SameAs(updated):
		return Transition{Outcome: Unchanged}
	case old.Array || updated.Array:
		return Transition{Outcome: Rewrite, Why: change + ", and PostgreSQL rewrites an array column for any change of its element type"}
	case old.Base != updated.Base:
		return compareBases(old, updated, change)
	}
	switch old.Base {
	case "character varying":
		return compareVarchar(old, updated)
	case "numeric":
		return compareNumeric(old, updated, change)
	case "bit varying":
		if len(old.Mods) == 1 && (len(updated.Mods) == 0 || updated.Mods[0] >= old.Mods[0]) {
			return Transition{Outcome: InPlace, Why: "widens a bit varying"}
		}
		return Transition{Outcome: Rewrite, Why: change, Abort: "bit string too long for type"}
	case "bit":
		return Transition{Outcome: Rewrite, Why: change, Abort: "bit string length does not match type"}
	}
	if precisionBases[old.Base] {
		if updated.Mods[0] >= old.Mods[0] {
			return Transition{Outcome: InPlace, Why: "lengthens a fractional-seconds precision"}
		}
		return Transition{Outcome: Rewrite, Why: change + ", which rounds every stored value to the shorter precision"}
	}
	return Transition{Outcome: Rewrite, Why: change}
}

// compareBases judges a change between two catalog types.
func compareBases(old, updated Type, change string) Transition {
	switch {
	case old.Base == "character varying" && updated.Base == "text":
		return Transition{Outcome: InPlace, Why: "drops a varchar's limit"}
	case old.Base == "text" && updated.Base == "character varying" && len(updated.Mods) == 0:
		return Transition{Outcome: InPlace, Why: "renames text to an unlimited varchar"}
	case old.Base == "text" && updated.Base == "character varying":
		return Transition{Outcome: Rewrite, Why: change + ", which checks every value against the new limit", Abort: "value too long for type"}
	case isTimestampFamily(old.Base) && isTimestampFamily(updated.Base):
		return Transition{Outcome: RewriteUnlessUTC, Why: change}
	}
	abort := ""
	if strings.HasSuffix(updated.Base, "int") {
		abort = updated.Base + " out of range"
	}
	return Transition{Outcome: Rewrite, Why: change, Abort: abort}
}

// isTimestampFamily reports timestamp with or without time zone.
func isTimestampFamily(base string) bool {
	return strings.HasPrefix(base, "timestamp ")
}

// compareVarchar judges a change of limit on a character varying column.
func compareVarchar(old, updated Type) Transition {
	change := fmt.Sprintf("changes the type from %s to %s", old.Spell(), updated.Spell())
	switch {
	case len(updated.Mods) == 0:
		return Transition{Outcome: InPlace, Why: "drops a varchar's limit"}
	case len(old.Mods) == 0:
		return Transition{Outcome: Rewrite, Why: change + ", which checks every value against the new limit", Abort: "value too long for type"}
	case updated.Mods[0] >= old.Mods[0]:
		return Transition{Outcome: InPlace, Why: "widens a varchar"}
	default:
		return Transition{Outcome: Rewrite, Why: change + ", which checks every value against the shorter limit", Abort: "value too long for type"}
	}
}

// compareNumeric judges a change of precision or scale.
func compareNumeric(old, updated Type, change string) Transition {
	switch {
	case len(updated.Mods) == 0:
		return Transition{Outcome: InPlace, Why: "drops a numeric's precision"}
	case len(old.Mods) == 0:
		return Transition{Outcome: Rewrite, Why: change + ", which checks and rounds every value to the new precision and scale", Abort: "numeric field overflow"}
	case updated.Mods[1] == old.Mods[1] && updated.Mods[0] >= old.Mods[0]:
		return Transition{Outcome: InPlace, Why: "raises a numeric's precision at the same scale"}
	case updated.Mods[1] != old.Mods[1]:
		return Transition{Outcome: Rewrite, Why: change + ", and a change of scale rounds every stored value", Abort: "numeric field overflow"}
	default:
		return Transition{Outcome: Rewrite, Why: change + ", which checks every value against the lower precision", Abort: "numeric field overflow"}
	}
}

// ChangeSite is one ALTER [COLUMN] name [SET DATA] TYPE clause.
type ChangeSite struct {
	// Statement is the position of the statement in its file.
	Statement int
	// Table is the altered table and Column the column as written.
	Table  lintkit.TableReference
	Column string
	// TypeStart and TypeEnd bound the new type's words in the statement.
	TypeStart int
	TypeEnd   int
}

// ChangeSites finds every ALTER COLUMN ... TYPE clause of a file's
// statements, in file order.
func ChangeSites(statements []lintext.Statement) []ChangeSite {
	var sites []ChangeSite
	for index := range statements {
		stmt := &statements[index]
		w := stmt.Words
		if !lintkit.IsAlterTable(w) {
			continue
		}
		table := lintkit.AlterTableReference(w, stmt.SourceWords)
		for _, i := range lintkit.ClauseStarts(w) {
			if i >= len(w) || w[i] != "ALTER" {
				continue
			}
			j := i + 1
			if j < len(w) && w[j] == "COLUMN" {
				j++
			}
			j = lintkit.SkipIfExists(w, j)
			if j >= len(w) || !lintkit.IdentLike(w[j]) {
				continue
			}
			column := lintkit.SourceWordAt(w, stmt.SourceWords, j)
			k := j + 1
			if k+1 < len(w) && w[k] == "SET" && w[k+1] == "DATA" {
				k += 2
			}
			if k >= len(w) || w[k] != "TYPE" {
				continue
			}
			sites = append(sites, ChangeSite{
				Statement: index,
				Table:     table,
				Column:    column,
				TypeStart: k + 1,
				TypeEnd:   lintkit.ClauseEnd(w, i),
			})
		}
	}
	return sites
}
