package sqlite

import (
	"fmt"
	"sort"
	"strings"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/sqlitemodule"
)

// tableKind is SQLite's own answer to "what kind of table is this", as
// reported by `PRAGMA table_list`.
//
// The three kinds have to be told apart before anything is emitted for them,
// because only one of them is an object an operator created:
//
//   - tableKindOrdinary is a table a CREATE TABLE statement made.
//   - tableKindVirtual is a table a CREATE VIRTUAL TABLE statement made. Its
//     shape is owned by a module, not by a column list, so describing it with
//     CREATE TABLE emits a statement that never created it and that, replayed,
//     produces a different database: a plain table named `docs` is not a
//     full-text index, and `MATCH` against it fails.
//   - tableKindShadow is a table a module maintains for a virtual table of its
//     own accord. It is implementation detail. An operator who applies a
//     description containing one creates a table SQLite would have created
//     itself, which then collides when the virtual table is created.
//
// See stokaro/ptah#1028.
type tableKind int

const (
	tableKindOrdinary tableKind = iota
	tableKindVirtual
	tableKindShadow
)

// readTableKinds asks SQLite which of its tables are virtual and which are the
// shadow tables a module maintains.
//
// The classification is taken from the catalog rather than from the name,
// because a shadow table's name carries no reliable mark. `PRAGMA table_list`
// reports a table as `shadow` only when its name is `X_Y`, `X` names a virtual
// table, and the module behind `X` claims `Y` as one of its own shadow names.
// A user table called `docs_data` sitting beside an FTS5 table called `docs`
// is therefore reported as an ordinary table, while the `docs_data` that FTS5
// itself maintains is reported as a shadow -- a distinction no suffix rule can
// make. Measured on both fixtures in
// TestReadSchemaSeparatesShadowTablesFromUserTablesOfTheSameShape.
//
// The `virtual` classification does not need the module to be present: SQLite
// records the table as a virtual table when the schema is parsed, before any
// module is resolved. The `shadow` classification does need it, because only
// the module can say which suffixes it owns. A database using a module this
// build does not register therefore keeps its virtual table recognized as
// virtual, while that module's shadow tables are reported as ordinary tables.
//
// Which case a given database is in is not guessed at. The reader asks
// [sqlitemodule.RegisteredOn] on the same connection and records every virtual
// table whose module is missing, so the description says out loud that it could
// not classify part of this database instead of presenting the module's storage
// as user tables. See [readUnregisteredVirtualTables] and docs/sqlite.md.
func (r *Reader) readTableKinds() (map[string]tableKind, error) {
	const query = `SELECT name, type FROM pragma_table_list WHERE schema = ?`
	rows, err := r.db.Query(query, r.schema)
	if err != nil {
		return nil, fmt.Errorf("sqlite: read table kinds: %w", err)
	}
	defer rows.Close()

	kinds := make(map[string]tableKind)
	for rows.Next() {
		var name, kind string
		if err := rows.Scan(&name, &kind); err != nil {
			return nil, fmt.Errorf("sqlite: scan table kind: %w", err)
		}
		kinds[name] = tableKindFromCatalog(kind)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate table kinds: %w", err)
	}
	return kinds, nil
}

// readUnregisteredVirtualTables names the virtual tables in this database whose
// module the connection cannot load, in a stable order.
//
// Those are exactly the tables whose shadow tables SQLite could not mark, so
// the list is also the reader's statement about which ordinary-looking tables
// in the same description may not be ordinary at all. It is asked of the same
// connection the rest of the read uses, because an answer from anywhere else
// could disagree with the `PRAGMA table_list` kinds it explains.
//
// An empty result is the ordinary case and claims something specific: every
// module this database uses is loaded, so every shadow table was marked and the
// description contains no module-owned storage.
func (r *Reader) readUnregisteredVirtualTables(
	virtualTables map[string]virtualTableSpec,
) ([]types.DBVirtualTable, error) {
	if len(virtualTables) == 0 {
		// No virtual table means no module to resolve. Skipping the query here
		// is not only an optimization: it keeps a reader pointed at a build or
		// a connection that cannot answer `PRAGMA module_list` working exactly
		// as before for every database that never needed the answer.
		return nil, nil
	}
	registered, err := sqlitemodule.RegisteredOn(r.db)
	if err != nil {
		return nil, err
	}
	var unregistered []types.DBVirtualTable
	for name, spec := range virtualTables {
		if registered.Registers(spec.Module) {
			continue
		}
		unregistered = append(unregistered, types.DBVirtualTable{
			Schema: r.outputSchema(),
			Name:   name,
			Module: spec.Module,
		})
	}
	sort.Slice(unregistered, func(i, j int) bool {
		return unregistered[i].Name < unregistered[j].Name
	})
	return unregistered, nil
}

func tableKindFromCatalog(kind string) tableKind {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "virtual":
		return tableKindVirtual
	case "shadow":
		return tableKindShadow
	default:
		return tableKindOrdinary
	}
}

// virtualTableSpec is the module declaration of a virtual table: everything
// after USING in the statement that created it.
type virtualTableSpec struct {
	// Module is the module name, unquoted. `fts5`, `rtree`, `geopoly`, or any
	// module a build registers, including one Ptah has never heard of.
	Module string
	// Arguments is the text between the module's parentheses, verbatim.
	// Module arguments are not SQL: FTS5 reads `tokenize = 'porter unicode61'`
	// and column names that may themselves be quoted and contain commas, and
	// only the module can interpret them. Reproducing them byte for byte is
	// the only way to recreate the same object.
	//
	// An empty Arguments covers both `USING dbstat` and `USING dbstat()`.
	// SQLite accepts and stores either, and creates the same object from
	// both, so the difference carries nothing to preserve.
	Arguments string
}

// parseVirtualTableDDL reads the module declaration out of a
// CREATE VIRTUAL TABLE statement.
//
// It reports false for anything that is not one, including an ordinary CREATE
// TABLE, so it doubles as a second, module-independent virtual-table signal
// beside PRAGMA table_list.
func parseVirtualTableDDL(ddl string) (virtualTableSpec, bool) {
	s := &ddlScanner{text: ddl}
	for _, keyword := range []string{"CREATE", "VIRTUAL", "TABLE"} {
		if !s.keyword(keyword) {
			return virtualTableSpec{}, false
		}
	}
	// SQLite reconstructs the statement it stores for a virtual table, so a
	// guard and a schema qualifier are not expected here. They are accepted
	// anyway: the catalog of a database Ptah did not create is not Ptah's to
	// assume.
	s.optionalKeywords("IF", "NOT", "EXISTS")
	if _, ok := s.identifier(); !ok {
		return virtualTableSpec{}, false
	}
	if s.punctuation('.') {
		if _, ok := s.identifier(); !ok {
			return virtualTableSpec{}, false
		}
	}
	if !s.keyword("USING") {
		return virtualTableSpec{}, false
	}
	module, ok := s.identifier()
	if !ok || module == "" {
		return virtualTableSpec{}, false
	}
	arguments, ok := s.parenthesizedGroup()
	if !ok {
		return virtualTableSpec{}, false
	}
	return virtualTableSpec{Module: module, Arguments: arguments}, true
}

// ddlScanner walks a SQLite DDL statement one token at a time, respecting
// SQLite's four identifier quotings, its string literals, and both comment
// forms. A scan that ignored them would stop at the first comma inside
// `"col,two"` and split a module argument list in the wrong place.
type ddlScanner struct {
	text string
	pos  int
}

func (s *ddlScanner) skipTrivia() {
	for s.pos < len(s.text) {
		switch {
		case isSpace(s.text[s.pos]):
			s.pos++
		case strings.HasPrefix(s.text[s.pos:], "--"):
			end := strings.IndexByte(s.text[s.pos:], '\n')
			if end < 0 {
				s.pos = len(s.text)
				return
			}
			s.pos += end + 1
		case strings.HasPrefix(s.text[s.pos:], "/*"):
			end := strings.Index(s.text[s.pos+2:], "*/")
			if end < 0 {
				s.pos = len(s.text)
				return
			}
			s.pos += end + 4
		default:
			return
		}
	}
}

// keyword consumes one unquoted word and reports whether it matched.
func (s *ddlScanner) keyword(want string) bool {
	start := s.pos
	s.skipTrivia()
	wordStart := s.pos
	for s.pos < len(s.text) && isDDLIdentifierByte(s.text[s.pos]) {
		s.pos++
	}
	if strings.EqualFold(s.text[wordStart:s.pos], want) {
		return true
	}
	s.pos = start
	return false
}

func (s *ddlScanner) optionalKeywords(words ...string) {
	start := s.pos
	for _, word := range words {
		if !s.keyword(word) {
			s.pos = start
			return
		}
	}
}

func (s *ddlScanner) punctuation(want byte) bool {
	start := s.pos
	s.skipTrivia()
	if s.pos < len(s.text) && s.text[s.pos] == want {
		s.pos++
		return true
	}
	s.pos = start
	return false
}

// identifier consumes one identifier and returns it unquoted.
func (s *ddlScanner) identifier() (string, bool) {
	s.skipTrivia()
	if s.pos >= len(s.text) {
		return "", false
	}
	switch s.text[s.pos] {
	case '"', '`', '\'':
		return s.quotedIdentifier(s.text[s.pos], s.text[s.pos])
	case '[':
		return s.quotedIdentifier('[', ']')
	}
	start := s.pos
	for s.pos < len(s.text) && isDDLIdentifierByte(s.text[s.pos]) {
		s.pos++
	}
	if s.pos == start {
		return "", false
	}
	return s.text[start:s.pos], true
}

// quotedIdentifier consumes a quoted identifier. SQLite doubles the closing
// quote to escape it inside `"`, “ ` “ and `'`; `[` ... `]` has no escape.
func (s *ddlScanner) quotedIdentifier(open, closing byte) (string, bool) {
	s.pos++ // the opening quote
	var out strings.Builder
	for s.pos < len(s.text) {
		char := s.text[s.pos]
		if char != closing {
			out.WriteByte(char)
			s.pos++
			continue
		}
		if open != '[' && s.pos+1 < len(s.text) && s.text[s.pos+1] == closing {
			out.WriteByte(closing)
			s.pos += 2
			continue
		}
		s.pos++
		return out.String(), true
	}
	return "", false
}

// parenthesizedGroup returns the raw text inside the balanced parentheses that
// follow, or reports that there were none. Quoted runs inside the group are
// skipped whole, so a parenthesis or comma inside a module argument does not
// end it.
func (s *ddlScanner) parenthesizedGroup() (group string, ok bool) {
	start := s.pos
	if !s.punctuation('(') {
		s.pos = start
		s.skipTrivia()
		// A module declaration with no argument list is complete here; a
		// trailing semicolon is the only thing SQLite may still add.
		if s.pos < len(s.text) && s.text[s.pos] != ';' {
			return "", false
		}
		return "", true
	}
	contentStart := s.pos
	depth := 1
	for s.pos < len(s.text) {
		if strings.HasPrefix(s.text[s.pos:], "--") || strings.HasPrefix(s.text[s.pos:], "/*") {
			before := s.pos
			s.skipTrivia()
			if s.pos > before {
				continue
			}
		}
		switch char := s.text[s.pos]; char {
		case '\'', '"', '`':
			if _, closed := s.quotedIdentifier(char, char); !closed {
				return "", false
			}
		case '[':
			if _, closed := s.quotedIdentifier('[', ']'); !closed {
				return "", false
			}
		case '(':
			depth++
			s.pos++
		case ')':
			depth--
			if depth == 0 {
				return s.text[contentStart:s.pos], true
			}
			s.pos++
		default:
			s.pos++
		}
	}
	return "", false
}

func isSpace(char byte) bool {
	return char == ' ' || char == '\t' || char == '\n' || char == '\r' || char == '\f' || char == '\v'
}

func isDDLIdentifierByte(char byte) bool {
	switch {
	case char >= 'a' && char <= 'z',
		char >= 'A' && char <= 'Z',
		char >= '0' && char <= '9',
		char == '_',
		char == '$',
		char >= 0x80:
		return true
	}
	return false
}
