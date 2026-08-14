// Package sqlitemodule answers one question, in one place: which SQLite
// virtual-table modules does the build actually register?
//
// It exists because that answer decides what SQLite is able to tell Ptah about
// a database, and because getting it from anywhere else means getting it twice.
//
// `PRAGMA table_list` classifies every table as `table`, `virtual` or `shadow`,
// and the three kinds are what the SQLite reader is built on. Only two of them
// survive a missing module. SQLite records a table as `virtual` while parsing
// the schema, before any module is resolved, so that classification holds for a
// module the build has never heard of. But `shadow` is the module's own answer:
// a table is shadow only when its name is `X_Y`, `X` names a virtual table, and
// the module behind `X` claims `Y` as one of its own. With the module absent
// nobody can be asked, and the module's private storage is reported as ordinary
// user tables.
//
// Measured on the driver Ptah links, modernc.org/sqlite v1.56.0, with the two
// halves of the same fixture built by a system SQLite that has every module:
//
//	fts5 (registered)      docs virtual, docs_data/_idx/_content/_docsize/
//	                       _config all shadow
//	fts4 (not registered)  docs virtual, docs_content/_segdir/_segments/
//	                       _docsize/_stat all reported as `table`
//
// Those five ordinary-looking tables are an FTS4 index. A comparison that reads
// them as user tables the desired state does not declare plans `DROP TABLE` for
// each one, and running it leaves `docs` in place over no storage: `MATCH`
// stops answering and reports `SQL logic error`. That is measured end to end in
// stokaro/ptah#1028, and it is why this package's answer is consulted at read
// time rather than assumed.
//
// The question is asked of a connection rather than answered from a hard-coded
// list, because a list is a claim about a build that a dependency bump can
// falsify in silence. [RegisteredOn] asks whichever database is in hand -- which
// is also the only way to see a module a runtime extension added to that one
// connection -- and [Registered] asks the build itself, for callers holding no
// connection.
package sqlitemodule

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"sync"

	_ "modernc.org/sqlite" // SQLite database/sql driver, for Registered
)

// Querier is the read-only slice of a database handle this package needs. It
// is declared here, on the consumer side, so asking the question needs no more
// authority than running one query.
type Querier interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

// Set is the modules one build registers.
//
// The zero Set registers nothing, which is the direction that fails closed: a
// caller that forgot to fill it refuses to classify anything rather than
// quietly declaring every module present.
type Set struct {
	folded map[string]struct{}
	names  []string
}

// Registers reports whether the build can load module.
//
// The comparison folds ASCII case because SQLite does, and only ASCII, matching
// [go.5x5.cz/ptah/core/platform/identifier]'s rule for the same reason. This is
// not a detail that can be skipped: measured on a real database,
// `CREATE VIRTUAL TABLE t USING FTS5(a)` succeeds and creates a genuine FTS5
// index with all five shadow tables, `sqlite_schema` records the module
// verbatim as `FTS5`, and `PRAGMA module_list` reports it as `fts5`. Comparing
// those two spellings byte for byte would report a registered module as absent
// and refuse a comparison that has nothing wrong with it.
func (s Set) Registers(module string) bool {
	_, ok := s.folded[foldASCII(module)]
	return ok
}

// Names lists the registered modules in a stable order, for a diagnostic that
// has to tell an operator what this build can and cannot do.
func (s Set) Names() []string {
	return slices.Clone(s.names)
}

// String renders the set the way a diagnostic prints it.
func (s Set) String() string {
	return strings.Join(s.names, ", ")
}

// pragmaFunctionPrefix marks the modules SQLite registers for its own pragma
// table-valued functions rather than for anything a schema can use.
//
// They have to be dropped, and the reason is worth stating because the obvious
// spelling of this query gets it wrong. `PRAGMA module_list` and
// `SELECT name FROM pragma_module_list` are not the same question: asking
// through the table-valued function registers that function as an eponymous
// module, which then appears in its own answer. Measured on three fresh
// connections to modernc.org/sqlite v1.56.0:
//
//	PRAGMA module_list                       7 rows, the real set
//	SELECT name FROM pragma_module_list      8 rows, incl. pragma_module_list
//	the TVF form, then PRAGMA module_list    8 rows -- the extra row persists
//
// The last line is why the prefix filter is here rather than a note to use the
// pragma form. The SQLite reader already queries `pragma_table_list` on the
// connection it later asks this question of, so by then that function is a
// registered module too, and the pragma form alone would report it. Neither
// entry names anything a `CREATE VIRTUAL TABLE` can use, and printing them in a
// diagnostic that tells an operator what this build supports would be a
// straightforward lie.
const pragmaFunctionPrefix = "pragma_"

// RegisteredOn asks one database which modules it can load.
//
// Callers holding a connection should prefer this over [Registered]: it is the
// same connection the rest of the read runs on, so its answer cannot disagree
// with the `PRAGMA table_list` classifications it is used to interpret.
func RegisteredOn(q Querier) (Set, error) {
	// The pragma form, not the table-valued function, so a connection that has
	// touched neither reports the true set. See [pragmaFunctionPrefix] for the
	// measurement, and for why the filter below is needed anyway.
	rows, err := q.Query("PRAGMA module_list")
	if err != nil {
		return Set{}, fmt.Errorf("sqlite: read registered modules: %w", err)
	}
	defer rows.Close()

	set := Set{folded: make(map[string]struct{})}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return Set{}, fmt.Errorf("sqlite: scan registered module: %w", err)
		}
		folded := foldASCII(name)
		if strings.HasPrefix(folded, pragmaFunctionPrefix) {
			continue
		}
		// A module registered under two spellings is one module. Fold for the
		// membership test, but report the spelling SQLite gave.
		if _, seen := set.folded[folded]; seen {
			continue
		}
		set.folded[folded] = struct{}{}
		set.names = append(set.names, name)
	}
	if err := rows.Err(); err != nil {
		return Set{}, fmt.Errorf("sqlite: iterate registered modules: %w", err)
	}
	slices.Sort(set.names)
	return set, nil
}

// registered caches the build's own answer. The set is a property of the
// linked amalgamation, so it cannot change while the process runs, and the
// in-memory database it is read from is opened once and closed immediately.
var registered = sync.OnceValues(func() (Set, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return Set{}, fmt.Errorf("sqlite: open module probe: %w", err)
	}
	defer db.Close()
	return RegisteredOn(db)
})

// Registered reports the modules this build registers, for callers that have
// no database in hand -- the comparison validator reaching a desired state that
// was read somewhere else, above all.
//
// The answer describes the binary, not any one database, so it is computed once
// and reused. An error here means the linked driver could not open an in-memory
// database, which is a condition under which nothing else works either; it is
// returned rather than swallowed so that no caller mistakes it for "this build
// registers no modules" and refuses everything.
func Registered() (Set, error) {
	return registered()
}

// foldASCII lowercases the ASCII letters and leaves every other byte alone.
//
// [strings.ToLower] is deliberately not used: it folds Unicode, which SQLite
// does not, and a fold the engine does not perform invents matches the engine
// will not honor. The same reasoning is spelled out on
// [go.5x5.cz/ptah/internal/sqlitevirtual]'s table identity, where it was
// measured on a live database.
func foldASCII(value string) string {
	folded := []byte(value)
	for i := range folded {
		if folded[i] >= 'A' && folded[i] <= 'Z' {
			folded[i] += 'a' - 'A'
		}
	}
	return string(folded)
}
