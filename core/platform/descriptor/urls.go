package descriptor

import (
	"fmt"
	"slices"
)

// URLs states how a database URL of an engine is read and compared before
// anything is connected. internal/atlasurl reads it. The zero value is an
// engine whose URL net/url reads, that names a server, whose port the URL
// states, and that is compared as the dialect it names.
type URLs struct {
	// DefaultPort is the TCP port a connection reaches when its URL names
	// none, such as 5432. Two URLs of the engine that differ only by naming
	// that port and by naming none address one server. Zero states none, and
	// an address without a port is compared as written: YDB's driver chooses
	// its port by the URL's scheme rather than by the dialect.
	DefaultPort uint16 `json:"default_port,omitempty"`
	// ComparedAs names the dialect a URL of this dialect is compared as, for
	// an engine whose URL cannot say which of two servers it reaches.
	// MariaDB's mariadb:// and maria:// open the driver MySQL's mysql://
	// opens, and only the server's version banner tells the two servers
	// apart, so MariaDB states "mysql": a dev URL of either scheme may name a
	// target of the other, and two such URLs may address one database. Empty
	// compares the dialect as itself. The named dialect is a dialect of the
	// registry that compares as itself.
	ComparedAs string `json:"compared_as,omitempty"`
	// Grammar is the grammar a URL of the engine may be written in beyond
	// the one net/url reads: [URLGrammarGoSQLDriver] or empty for none.
	Grammar URLGrammar `json:"grammar,omitempty"`
	// FileSchemes are the URL schemes whose URL names a database file rather
	// than a server, such as SQLite's "sqlite" and "file". Opening one creates
	// the file when it does not exist, so it has no database name to replace.
	// A dialect that states any keeps its databases in files: two of its URLs
	// address one database when they name one file, and a URL of its scheme
	// with no "//" names a path. A scheme belongs to one dialect of a
	// registry, and uses the alphabet of Name.
	FileSchemes []string `json:"file_schemes,omitempty"`
}

// URLGrammar is a grammar a database URL may be written in beyond the one
// net/url reads.
type URLGrammar string

const (
	// URLGrammarGoSQLDriver is the grammar of go-sql-driver/mysql. After its
	// scheme a URL may carry the driver's own data source name,
	// user:pass@tcp(host:port)/db or user:pass@unix(/path)/db, which net/url
	// refuses, and a scheme ending in +unix names a socket by its path and
	// the database in a `database` query parameter.
	URLGrammarGoSQLDriver URLGrammar = "go-sql-driver"
)

// URLGrammars returns every declared URL grammar.
func URLGrammars() []URLGrammar {
	return []URLGrammar{URLGrammarGoSQLDriver}
}

// validateURLs checks the fields of [URLs] one descriptor can check alone.
// The registry checks ComparedAs and that no two dialects claim one file
// scheme.
func (d Dialect) validateURLs() error {
	if grammar := d.URLs.Grammar; grammar != "" && !slices.Contains(URLGrammars(), grammar) {
		return fmt.Errorf("%w: dialect %q: URL grammar %q is not one of %q", ErrInvalidDescriptor, d.Name, grammar, URLGrammars())
	}
	if d.URLs.ComparedAs != "" && !validSpelling(d.URLs.ComparedAs) {
		return fmt.Errorf("%w: dialect %q: compared as %q, which is not spelled as a dialect name", ErrInvalidDescriptor, d.Name, d.URLs.ComparedAs)
	}
	return validNames(d.Name, "file scheme", d.URLs.FileSchemes)
}

// validNames checks a list of names of one kind a descriptor states: each in
// the alphabet of Name, and none twice.
func validNames(dialect, kind string, names []string) error {
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if !validSpelling(name) {
			return fmt.Errorf("%w: dialect %q: %s %q must be lowercase ASCII letters, digits, '-', '_' or '+', beginning with a letter",
				ErrInvalidDescriptor, dialect, kind, name)
		}
		if seen[name] {
			return fmt.Errorf("%w: dialect %q states %s %q twice", ErrInvalidDescriptor, dialect, kind, name)
		}
		seen[name] = true
	}
	return nil
}
