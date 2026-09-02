package toschema

import (
	"errors"
	"fmt"
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
}

// mysqlNaming and mariaDBNaming are the two measured answers.
var (
	mysqlNaming   = engineIndexNaming{baseBytes: 61, maxBytes: 64, functionalBase: "functional_index"}
	mariaDBNaming = engineIndexNaming{maxBytes: 64}
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
	fieldsStart, constraintsStart, indexesStart int, sourcePlatform string,
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
	for i := constraintsStart; i < len(database.Constraints); i++ {
		if err := claimConstraintName(claimed, &database.Constraints[i], table, naming); err != nil {
			return err
		}
	}
	for i := indexesStart; i < len(database.Indexes); i++ {
		if err := claimIndexName(claimed, &database.Indexes[i], table, naming); err != nil {
			return err
		}
	}
	return nil
}

// claimConstraintName names one table-level constraint.
//
// Only the ones that occupy the index namespace: a UNIQUE constraint is an
// index on these engines, while a CHECK is not and a FOREIGN KEY reuses a
// covering index rather than adding a name -- measured, an FK declared beside
// `KEY (a)` on the same column produces one index called `a` and not two.
func claimConstraintName(
	claimed indexNames, constraint *schemamodel.Constraint, table schemamodel.Table,
	naming engineIndexNaming,
) error {
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
