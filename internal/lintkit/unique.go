package lintkit

import (
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/core/platform"
)

// UniqueIndexSite is one unique index or constraint a statement builds.
type UniqueIndexSite struct {
	// Statement is the position of the statement in its file.
	Statement int
	// Name is the index or constraint name as written, empty when the
	// server picks one.
	Name string
	// Table is the indexed table and Columns its key columns, as written.
	Table   TableReference
	Columns []string
	// Concurrently and NullsNotDistinct are the PostgreSQL modifiers that
	// change what a failure leaves behind and what counts as a duplicate.
	Concurrently     bool
	NullsNotDistinct bool
	// Spelled is the clause head for the message: CREATE UNIQUE INDEX,
	// ADD UNIQUE, or YQL's ADD INDEX.
	Spelled string
	// YQL records YQL's spelling, whose failure YDB words its own way.
	YQL bool
}

// UniqueIndexSites finds every unique index a file's statements build, in
// file order. dialect is the run's target, which decides whether YQL's own
// unique index spelling is read.
func UniqueIndexSites(statements []lintext.Statement, dialect string) []UniqueIndexSite {
	var sites []UniqueIndexSite
	for index := range statements {
		stmt := &statements[index]
		w := stmt.Words
		switch {
		case IsCreateIndex(w) && len(w) > 1 && w[1] == "UNIQUE":
			if site, ok := CreateUniqueIndexSite(w, stmt.SourceWords); ok {
				site.Statement = index
				sites = append(sites, site)
			}
		case IsAlterTable(w):
			table := AlterTableReference(w, stmt.SourceWords)
			for _, i := range ClauseStarts(w) {
				site, ok := AddUniqueSite(w, stmt.SourceWords, i)
				if !ok && dialect == platform.YDB {
					site, ok = YQLUniqueIndexSite(w, stmt.SourceWords, i)
				}
				if !ok {
					continue
				}
				site.Statement = index
				site.Table = table
				sites = append(sites, site)
			}
		}
	}
	return sites
}

// CreateUniqueIndexSite reads CREATE UNIQUE INDEX [CONCURRENTLY] [IF NOT
// EXISTS] [name] ON [ONLY] table [USING method] (columns) [NULLS NOT DISTINCT].
func CreateUniqueIndexSite(w, sourceWords []string) (UniqueIndexSite, bool) {
	site := UniqueIndexSite{Spelled: "CREATE UNIQUE INDEX"}
	j := 3
	if j < len(w) && w[j] == "CONCURRENTLY" {
		site.Concurrently = true
		j++
	}
	j = SkipIfNotExists(w, j)
	if j < len(w) && w[j] != "ON" && IdentLike(w[j]) {
		site.Name = SourceWordAt(w, sourceWords, j)
		j++
	}
	if j >= len(w) || w[j] != "ON" {
		return site, false
	}
	j++
	if j < len(w) && w[j] == "ONLY" {
		j++
	}
	table, next := TableRefAt(w, sourceWords, j)
	if table.Normalized == "" {
		return site, false
	}
	site.Table = table
	j = next
	if j+1 < len(w) && w[j] == "USING" {
		j += 2
	}
	columns, next, ok := KeyColumnList(w, sourceWords, j)
	if !ok {
		return site, false
	}
	site.Columns = columns
	site.NullsNotDistinct = HasWordSeq(w[next:], "NULLS", "NOT", "DISTINCT")
	return site, true
}

// AddUniqueSite reads ADD [CONSTRAINT name] UNIQUE [INDEX|KEY] [name]
// [USING method] (columns) at a clause start.
func AddUniqueSite(w, sourceWords []string, i int) (UniqueIndexSite, bool) {
	end := ClauseEnd(w, i)
	site := UniqueIndexSite{Spelled: "ADD UNIQUE"}
	j := i
	if j >= end || w[j] != "ADD" {
		return site, false
	}
	j++
	if j+1 < end && w[j] == "CONSTRAINT" && IdentLike(w[j+1]) {
		site.Name = SourceWordAt(w, sourceWords, j+1)
		j += 2
	}
	if j >= end || w[j] != "UNIQUE" {
		return site, false
	}
	j++
	if j < end && (w[j] == "INDEX" || w[j] == "KEY") {
		j++
	}
	name, j := UniqueNameAndMethod(w[:end], sourceWords, j)
	if name != "" {
		site.Name = name
	}
	columns, next, ok := KeyColumnList(w[:end], sourceWords, j)
	if !ok {
		return site, false
	}
	site.Columns = columns
	site.NullsNotDistinct = HasWordSeq(w[next:end], "NULLS", "NOT", "DISTINCT")
	return site, true
}

// YQLUniqueIndexSite reads YQL's ADD INDEX name [GLOBAL] UNIQUE [SYNC] [USING
// kind] ON (columns) at a clause start. YQL is the only dialect that writes
// UNIQUE after the index name, and it is read only on a YDB run, where the
// statement is lexed as YQL.
func YQLUniqueIndexSite(w, sourceWords []string, i int) (UniqueIndexSite, bool) {
	end := ClauseEnd(w, i)
	site := UniqueIndexSite{Spelled: "ADD INDEX", YQL: true}
	if i+2 >= end || w[i] != "ADD" || w[i+1] != "INDEX" || !IdentLike(w[i+2]) {
		return site, false
	}
	site.Name = SourceWordAt(w, sourceWords, i+2)
	unique := false
	for j := i + 3; j < end; j++ {
		switch w[j] {
		case "UNIQUE":
			unique = true
		case "ON":
			columns, _, ok := KeyColumnList(w[:end], sourceWords, j+1)
			site.Columns = columns
			return site, ok && unique
		}
	}
	return site, false
}

// UniqueNameAndMethod reads the optional index name and USING method between
// UNIQUE [INDEX|KEY] and the column list. The PostgreSQL USING INDEX form
// attaches an index that already exists: it names no column list, so the
// caller's column-list requirement is what keeps it from being a site.
func UniqueNameAndMethod(w, sourceWords []string, j int) (name string, next int) {
	if j < len(w) && w[j] != "(" && w[j] != "USING" && IdentLike(w[j]) {
		name = SourceWordAt(w, sourceWords, j)
		j++
	}
	if j+1 < len(w) && w[j] == "USING" {
		j += 2
	}
	return name, j
}

// KeyColumnList reads ( a, b (10), c DESC, (expr) ) and returns the column
// names it names, in order, and the index after the closing parenthesis.
// A key part that is an expression contributes no name.
func KeyColumnList(w, sourceWords []string, j int) ([]string, int, bool) {
	if j >= len(w) || w[j] != "(" {
		return nil, j, false
	}
	var columns []string
	depth := 0
	partStart := true
	for k := j; k < len(w); k++ {
		switch w[k] {
		case "(":
			depth++
			partStart = depth == 1
			continue
		case ")":
			depth--
			if depth == 0 {
				return columns, k + 1, len(columns) > 0
			}
		case ",":
			if depth == 1 {
				partStart = true
			}
		default:
			if depth == 1 && partStart && IdentLike(w[k]) {
				columns = append(columns, SourceWordAt(w, sourceWords, k))
			}
			partStart = false
		}
	}
	return nil, j, false
}

// LastComponent is the part of a dotted name after its last dot.
func LastComponent(word string) string {
	return word[strings.LastIndex(word, ".")+1:]
}

// Label names the site in a message: its clause head and, where the author
// gave one, its name.
func (s UniqueIndexSite) Label() string {
	if s.Name != "" {
		return s.Spelled + " " + s.Name
	}
	return s.Spelled
}
