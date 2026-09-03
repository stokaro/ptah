package toschema

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
)

// ErrUnnamedIndex is the class of an inline index left without a name that no
// rule here can supply one for.
//
// Refused rather than guessed at. The one shape that reaches it is an index
// whose first element is an expression: MySQL calls that `functional_index`
// and MariaDB rejects the syntax outright, so there is no name the two engines
// would agree on.
//
// Ptah's own parser refuses `KEY ((a + 1))` before this pass sees it -- named
// or unnamed, measured -- so nothing reaches this branch through a SQL source
// today. Naming it for MySQL is stokaro/ptah#2758, and it belongs with the
// parser change that makes such an index representable rather than here, where
// it would be a rule with no caller.
var ErrUnnamedIndex = errors.New("the index has no name and none can be derived")

// ErrIndexNameTooLong is the class of a derived name the engine will not accept.
//
// MariaDB appends its suffix without truncating and refuses the result:
// measured on 11.8.9, a 63-character column with two unnamed indexes is
// `ERROR 1280 (42000): Incorrect index name`, while 62 characters is accepted.
// MySQL never reaches this -- it truncates instead -- so a source Ptah refuses
// here is one MariaDB refuses too, reported at conversion rather than at
// execution (stokaro/ptah#2759).
var ErrIndexNameTooLong = errors.New("the derived index name is longer than the engine accepts")

// ErrReservedIndexName is the class of an index an author named PRIMARY.
//
// Both engines answer `ERROR 1280 (42000): Incorrect index name 'PRIMARY'`
// -- measured on MySQL 9.7.2 and MariaDB 11.8.9. It is not a duplicate: the
// name is refused whether or not the table has a primary key.
var ErrReservedIndexName = errors.New("PRIMARY is reserved and cannot name a secondary index")

// ErrDuplicateIndexName is the class of two indexes on one table claiming a
// single name.
//
// MySQL and MariaDB both answer `ERROR 1061 (42000): Duplicate key name`, so
// accepting it models a table neither server can create. It was previously
// silent in a worse way than an error: schemamodel.Finalize deduplicates
// indexes on {table, name}, so the second declaration was discarded without a
// word and the schema converged as though it had never been written.
var ErrDuplicateIndexName = errors.New("two indexes on one table claim the same name")

// indexNames is the per-table index-name namespace, with the engine's own
// notion of when two names are one.
//
// Case-insensitively, because both engines are: measured on MySQL 9.7.2 and
// MariaDB 11.8.9, `KEY Foo (a), KEY foo (b)` is `ERROR 1061 (42000): Duplicate
// key name 'foo'`. A Go map keyed on the raw string called those two distinct
// and accepted a table neither server can create (stokaro/ptah#2757).
//
// The FOLDED name decides identity and the original decides what is written:
// measured, a table with a column `Foo` and an index explicitly named `foo`
// yields `foo` and `Foo_2`, so the derived name keeps its column's spelling.
type indexNames map[string]struct{}

func (n indexNames) taken(name string) bool {
	_, found := n[strings.ToLower(name)]
	return found
}

func (n indexNames) claim(name string) {
	n[strings.ToLower(name)] = struct{}{}
}

// engineIndexNaming is what one engine does when it has to name an index.
//
// Two engines, two answers, and every field here was measured rather than read
// off documentation. A family-wide rule is what produced the defect this
// replaces: the two agree on the namespace and disagree on the length.
type engineIndexNaming struct {
	// baseBytes is how much of the base name survives before a `_N` suffix is
	// appended, zero where the engine does not truncate.
	//
	// BYTES rather than characters, and 61 rather than 64 - len(suffix):
	// measured on MySQL 9.7.2, a 64-character column yields a 63-character
	// `_2` and a 64-character `_10`, so the base is cut to 61 whatever the
	// suffix costs.
	baseBytes int
	// maxBytes is the longest index name the engine accepts.
	maxBytes int
	// functionalBase is the name the engine gives an index whose first key
	// part is an expression, and the empty string where the engine has no such
	// index at all.
	//
	// Measured on MySQL 8.4: `KEY ((a + 1))` becomes `functional_index`, and
	// further unnamed ones become `functional_index_2` and `functional_index_3`
	// -- the same `_N` walk every other derived name takes, so it is a base
	// here rather than a rule of its own. MariaDB 11.8 answers `ERROR 1064` to
	// the syntax, which is why its entry is empty (stokaro/ptah#2758).
	functionalBase string
	// descendingCovers is whether a descending key can back a foreign key.
	//
	// MariaDB reuses one and MySQL does not, so the same document gives the two
	// engines different index names. coverage.covers carries the measurement.
	descendingCovers bool
}

// mysqlNaming and mariaDBNaming are the two measured answers.
var (
	mysqlNaming   = engineIndexNaming{baseBytes: 61, maxBytes: 64, functionalBase: "functional_index"}
	mariaDBNaming = engineIndexNaming{maxBytes: 64, descendingCovers: true}
)

// nameMySQLInlineIndexes gives every unnamed inline index and unique constraint
// of one table the name a MySQL-family server would assign it.
//
// MySQL and MariaDB name an index after its first key part's column, appending
// _2, _3 and so on when that name is taken. Measured identically on MySQL
// 8.4.11 and 26.7.0 and on MariaDB 11.8.9 and 12.3.3, including the cases that
// look like they might differ: the prefix in `KEY (email(10))` and the
// direction in `KEY (a DESC)` are both outside the name, a quoted column keeps
// its spaces, and a column-level UNIQUE claims its column's name before any
// index gets to.
//
// The name has to be decided HERE, on the desired model, rather than invented
// when the SQL is written. A live reader takes index names from the catalog, so
// it reports what the server chose; a desired model still holding the empty
// string is refused by the comparator outright -- "requires a name and owning
// table" -- and, for the unique form, produces a plan that drops the named
// constraint and adds a nameless one on every run. It has to be decided before
// schemamodel.Finalize as well, which deduplicates indexes on {table, name}:
// two unnamed indexes share the key {table, ""}, and the second one is dropped
// in silence.
//
// Order is Ptah's own emission order rather than the document's, and that is a
// deliberate divergence with a reason. The server assigns names in declaration
// order, but ast.CreateTableNode keeps constraints and indexes in two separate
// slices, so `KEY (a), UNIQUE KEY (a)` and `UNIQUE KEY (a), KEY (a)` parse to
// the identical tree -- measured -- and the document's interleaving is not
// recoverable here. What makes that safe is that Ptah renders the name it
// derived: a unique constraint carries `CONSTRAINT <name> UNIQUE (...)` and an
// index is a `CREATE INDEX <name>`, so a server applying Ptah's output never
// auto-names anything and the database matches this model exactly. A database
// somebody built by hand from the other spelling differs by two names, which
// Ptah reports and converges on the next apply rather than fighting forever.
func nameMySQLInlineIndexes(
	database *schemamodel.Database, table schemamodel.Table,
	fieldsStart int, order []namedElement, sourcePlatform string,
) error {
	naming, ok := namingFor(sourcePlatform)
	if !ok {
		return nil
	}
	claimed := make(indexNames)
	// PRIMARY is reserved rather than derived, and UNCONDITIONALLY: measured on
	// MySQL 9.7.2 and MariaDB 11.8.9, a table whose only key is
	// ``KEY (`PRIMARY`)`` over a column called `PRIMARY` gets `PRIMARY_2`,
	// with no primary key anywhere. Seeding this only when a primary key
	// existed derived `PRIMARY` for that table (stokaro/ptah#2757).
	//
	// It is taken by the name rather than by any column of it: `a INT PRIMARY
	// KEY, KEY (a)` still leaves the index called `a`.
	claimed.claim("PRIMARY")
	for _, field := range database.Fields[fieldsStart:] {
		// A column-level UNIQUE is an index named after its column, and it is
		// created before any table-level element, so it claims the bare name.
		if field.Unique {
			claimed.claim(field.Name)
		}
	}
	// Coverage is a property of the whole table body and order is not.
	// Measured on MySQL 26.7 and MariaDB 12.3, all three of these leave the
	// foreign key with no backing index of its own, so its name stays free:
	//
	//	CONSTRAINT fk1 FOREIGN KEY (a) ..., KEY (a)   the key comes AFTER
	//	a INT NOT NULL PRIMARY KEY, ... FOREIGN KEY (a)   a column-level key
	//	a INT UNIQUE, ... FOREIGN KEY (a)                 a column-level unique
	//
	// Deciding it from what precedes the constraint refused all three, which
	// are documents both engines accept.
	covered := coversOf(database, table, fieldsStart, order)
	// Names, though, are allocated in the order the document declared them:
	// that is the order the server allocates in, and the two disagree on a
	// document that is valid one way round and refused the other
	// (stokaro/ptah#2773).
	for _, element := range order {
		if element.isIndex() {
			if err := claimIndexName(
				claimed, &database.Indexes[element.index], table, naming); err != nil {
				return err
			}
			continue
		}
		if err := claimConstraintName(
			claimed, &database.Constraints[element.constraint], table, naming, covered); err != nil {
			return err
		}
	}
	return nil
}

// coversOf is every access path the table body declares, in any position.
//
// A foreign key reuses whichever of them leads with its columns, so all of them
// have to be here: the table's primary key, a column's own primary key or
// unique, every inline index, and every unique constraint.
func coversOf(
	database *schemamodel.Database, table schemamodel.Table,
	fieldsStart int, order []namedElement,
) coverage {
	covered := coverage{ascending(table.PrimaryKey)}
	for _, field := range database.Fields[fieldsStart:] {
		if field.Primary || field.Unique {
			covered = append(covered, ascending([]string{field.Name}))
		}
	}
	for _, element := range order {
		if element.isIndex() {
			covered = append(covered, indexCandidate(database.Indexes[element.index]))
			continue
		}
		if constraint := database.Constraints[element.constraint]; constraint.Type == "UNIQUE" {
			covered = append(covered, ascending(constraint.Columns))
		}
	}
	return covered
}

// ascending is a candidate whose every part is in the default direction.
//
// A primary key, a column's own key and a UNIQUE constraint have no per-part
// direction to lose: the model carries their columns and nothing else.
func ascending(columns []string) candidateKey {
	return candidateKey{columns: columns, descending: make([]bool, len(columns))}
}

// indexCandidate reads an inline index as the engine would consider it.
//
// Parts before Fields, because Parts is where the direction lives -- Fields is
// the column list with `a DESC` already flattened to `a`, so a candidate built
// from it claims to cover what MySQL will not reuse.
func indexCandidate(index schemamodel.Index) candidateKey {
	if len(index.Parts) == 0 {
		return ascending(index.Fields)
	}
	candidate := candidateKey{
		columns:    make([]string, 0, len(index.Parts)),
		descending: make([]bool, 0, len(index.Parts)),
	}
	for _, part := range index.Parts {
		candidate.columns = append(candidate.columns, part.Name)
		candidate.descending = append(candidate.descending, part.Desc)
	}
	return candidate
}

// namedElement is one table-body element the naming pass walks, as a POSITION
// in the model rather than a pointer into it.
//
// A pointer would be the obvious spelling and it is wrong: these are collected
// while the same slices are still being appended to, and an append that grows
// past the capacity moves the backing array. Measured -- with two inline
// indexes on one table, the first one's name was written through a pointer into
// the abandoned array and the index reached the renderer nameless.
//
// Exactly one position is set; the other is absent.
type namedElement struct {
	// constraint is the element's position in Database.Constraints, when it was
	// a table-level constraint.
	constraint int
	// index is the element's position in Database.Indexes, when it was an
	// inline index.
	index int
}

// noPosition marks the half of a namedElement that is not set.
const noPosition = -1

// isIndex reports whether this element is an inline index.
func (e namedElement) isIndex() bool { return e.index != noPosition }

// coverage is the key prefixes a table already has an access path for, in the
// order they were declared.
type coverage []candidateKey

// candidateKey is one access path the table body declares, as the engine sees
// it: the columns in order, and whether the part at each position is DESC.
//
// Per position rather than one flag for the key, because coverage asks about
// the LEADING columns only: `KEY (a, b DESC)` backs a foreign key on `a` even
// where the descending part would disqualify a wider one.
type candidateKey struct {
	columns    []string
	descending []bool
}

// covers reports whether some key already declared can serve as a foreign key's
// backing index.
//
// A foreign key needs an index whose leading columns are the key's columns, in
// order. MySQL and MariaDB create one when nothing satisfies that and reuse
// whatever does, which is why the same foreign key sometimes takes a name in
// the index namespace and sometimes takes none.
//
// Two things decide it that a column-name comparison alone gets wrong, both
// measured on live MySQL 8.4.11 and MariaDB 11.8.9 (stokaro/ptah#2769).
//
// Column identity FOLDS CASE, on both engines:
//
//	CREATE TABLE c (`B` INT, z INT, KEY k (`B`),
//	                CONSTRAINT z FOREIGN KEY (`b`) REFERENCES p(id), KEY (z));
//
// leaves `k` and `z` on both -- `k(B)` backs the key on `b`, so the constraint
// consumes no name and the unnamed index keeps `z`. Comparing the spellings
// reserved `z`, derived `z_2`, and described a table neither server builds.
//
// DIRECTION is engine-specific, and it is the half that cannot be shared:
//
//	CREATE TABLE c (a INT, KEY k (a DESC),
//	                CONSTRAINT f FOREIGN KEY (a) REFERENCES p(id));
//
// MySQL keeps `k` descending and adds an ascending `f`; MariaDB reuses `k` and
// adds nothing. So a descending candidate covers on MariaDB and does not on
// MySQL. Treating it one way for both either invents a MariaDB suffix or misses
// the name MySQL is about to take -- and the second is the duplicate-key error
// this issue opened on, because Ptah then emits `CREATE INDEX b` for a name the
// foreign key's own backing index already holds.
func (c coverage) covers(columns []string, naming engineIndexNaming) bool {
	if len(columns) == 0 {
		return false
	}
	for _, key := range c {
		if len(key.columns) < len(columns) {
			continue
		}
		if !slices.EqualFunc(key.columns[:len(columns)], columns, strings.EqualFold) {
			continue
		}
		if naming.descendingCovers || !slices.Contains(key.descending[:len(columns)], true) {
			return true
		}
	}
	return false
}

// claimConstraintName names one table-level constraint.
//
// Only the ones that occupy the index namespace. A UNIQUE constraint is an
// index on these engines and always takes a name; a CHECK is not and never
// does.
//
// A FOREIGN KEY is the one that depends on what came before it. It needs an
// index whose leading columns are its own, so it reuses one already declared
// and otherwise gets a backing index named after the constraint -- a name in
// the same namespace every other key draws from. Measured on MySQL 26.7 and
// MariaDB 12.3: `CONSTRAINT b FOREIGN KEY (a) ..., KEY (b)` builds `b` and
// `b_2`, while `FOREIGN KEY (a) ..., KEY (a)` builds a single `a`, because
// there the index covers the key.
//
// An earlier version of this comment said a foreign key "reuses a covering
// index rather than adding a name" with no condition, generalized from the
// second measurement alone. It is the first one this file has to get right.
func claimConstraintName(
	claimed indexNames, constraint *schemamodel.Constraint, table schemamodel.Table,
	naming engineIndexNaming, covered coverage,
) error {
	if constraint.Type == "FOREIGN KEY" {
		if constraint.Name == "" || covered.covers(constraint.Columns, naming) {
			return nil
		}
		return claimExplicit(claimed, constraint.Name, table)
	}
	if constraint.Type != "UNIQUE" {
		return nil
	}
	if constraint.Name != "" {
		return claimExplicit(claimed, constraint.Name, table)
	}
	if len(constraint.Columns) == 0 {
		return fmt.Errorf("%w: a unique constraint on %s names no column",
			ErrUnnamedIndex, table.Name)
	}
	name, err := derive(claimed, constraint.Columns[0], table, naming)
	if err != nil {
		return err
	}
	constraint.Name = name
	return nil
}

// claimIndexName names one inline index.
func claimIndexName(
	claimed indexNames, index *schemamodel.Index, table schemamodel.Table,
	naming engineIndexNaming,
) error {
	if index.Name != "" {
		return claimExplicit(claimed, index.Name, table)
	}
	candidate := firstIndexColumn(*index)
	if candidate == "" {
		candidate = naming.functionalBase
	}
	if candidate == "" {
		return fmt.Errorf(
			"%w: the index on %s starts with an expression, which this engine "+
				"has no functional index for",
			ErrUnnamedIndex, table.Name)
	}
	name, err := derive(claimed, candidate, table, naming)
	if err != nil {
		return err
	}
	index.Name = name
	return nil
}

// firstIndexColumn is the column an unnamed index takes its name from.
//
// Parts before Fields, because Parts is where a prefix length, a direction and
// an expression live: an expression part carries no column and is the one input
// that has no name to derive.
func firstIndexColumn(index schemamodel.Index) string {
	if len(index.Parts) > 0 {
		return index.Parts[0].Name
	}
	if len(index.Fields) > 0 {
		return index.Fields[0]
	}
	return ""
}

// claimExplicit records a name the author wrote, refusing a second claim on it.
func claimExplicit(claimed indexNames, name string, table schemamodel.Table) error {
	// PRIMARY is refused rather than reported as a duplicate, because it is one
	// even on a table with no other index: both engines answer
	// `ERROR 1280 (42000): Incorrect index name 'PRIMARY'`, and calling that a
	// collision would send the reader looking for the other index.
	if strings.EqualFold(name, "PRIMARY") {
		return fmt.Errorf("%w: %s", ErrReservedIndexName, table.Name)
	}
	if claimed.taken(name) {
		return fmt.Errorf("%w: %s on %s", ErrDuplicateIndexName, name, table.Name)
	}
	claimed.claim(name)
	return nil
}

// derive is the server's own rule: the column name, then _2, _3 and so on.
//
// The counter walks upward from 2 and skips a name already claimed rather than
// counting how many claims share the base, which is what the servers do:
// measured, `KEY a_2 (b), KEY (a), KEY (a)` yields a_2, a and a_3.
//
// The unsuffixed candidate is never truncated: it is the column's own name, and
// a column the engine accepted is short enough to name an index.
func derive(
	claimed indexNames, column string, table schemamodel.Table, naming engineIndexNaming,
) (string, error) {
	candidate := column
	for suffix := 2; ; suffix++ {
		if !claimed.taken(candidate) {
			if len(candidate) > naming.maxBytes {
				return "", fmt.Errorf("%w: %s on %s is %d bytes and the limit is %d",
					ErrIndexNameTooLong, candidate, table.Name, len(candidate), naming.maxBytes)
			}
			claimed.claim(candidate)
			return candidate, nil
		}
		candidate = truncateBase(column, naming) + "_" + strconv.Itoa(suffix)
	}
}

// truncateBase cuts the base name down to what the engine leaves room for.
//
// MySQL truncates to 61 BYTES and does it whatever the suffix costs -- measured
// on 9.7.2, a 64-character column yields a 63-character `_2` and a
// 64-character `_10`. MariaDB does not truncate at all and refuses the result
// instead, which is what maxBytes then reports.
//
// Truncation that splits a multibyte character is the engine's behavior too,
// and the engine then rejects its own name: measured, a 32-character `ä` column
// produces `ERROR 1280` naming a string cut mid-character. Ptah cuts on a rune
// boundary instead, so the name it derives is one the engine can accept -- the
// alternative is reproducing a defect for the sake of matching it.
func truncateBase(column string, naming engineIndexNaming) string {
	if naming.baseBytes <= 0 || len(column) <= naming.baseBytes {
		return column
	}
	cut := naming.baseBytes
	for cut > 0 && !utf8.RuneStart(column[cut]) {
		cut--
	}
	return column[:cut]
}

// namingFor answers which engine's rules apply, and whether any do.
func namingFor(dialect string) (engineIndexNaming, bool) {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL:
		return mysqlNaming, true
	case platform.MariaDB:
		return mariaDBNaming, true
	default:
		return engineIndexNaming{}, false
	}
}
