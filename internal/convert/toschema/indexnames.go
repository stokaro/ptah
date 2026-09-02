package toschema

import (
	"errors"
	"fmt"
	"strconv"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
)

// ErrUnnamedIndex is the class of an inline index left without a name that no
// rule here can supply one for.
//
// Refused rather than guessed at. The one shape that reaches it is an index
// whose first element is an expression: MySQL 8.4 calls that `functional_index`
// and MariaDB 11.8 rejects the syntax outright, so there is no name the two
// engines would agree on and any invention would be a name the server never
// assigns.
var ErrUnnamedIndex = errors.New("the index has no name and none can be derived")

// ErrDuplicateIndexName is the class of two indexes on one table claiming a
// single name.
//
// MySQL and MariaDB both answer `ERROR 1061 (42000): Duplicate key name`, so
// accepting it models a table neither server can create. It was previously
// silent in a worse way than an error: schemamodel.Finalize deduplicates
// indexes on {table, name}, so the second declaration was discarded without a
// word and the schema converged as though it had never been written.
var ErrDuplicateIndexName = errors.New("two indexes on one table claim the same name")

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
	if !isMySQLFamily(sourcePlatform) {
		return nil
	}
	claimed := make(map[string]struct{})
	// PRIMARY is the one name a server reserves rather than derives, and it is
	// taken by the key's existence rather than by any column of it: measured,
	// `a INT PRIMARY KEY, KEY (a)` leaves the index called `a`.
	if len(table.PrimaryKey) > 0 {
		claimed["PRIMARY"] = struct{}{}
	}
	for _, field := range database.Fields[fieldsStart:] {
		if field.Primary {
			claimed["PRIMARY"] = struct{}{}
		}
		// A column-level UNIQUE is an index named after its column, and it is
		// created before any table-level element, so it claims the bare name.
		if field.Unique {
			claimed[field.Name] = struct{}{}
		}
	}
	for i := constraintsStart; i < len(database.Constraints); i++ {
		if err := claimConstraintName(claimed, &database.Constraints[i], table); err != nil {
			return err
		}
	}
	for i := indexesStart; i < len(database.Indexes); i++ {
		if err := claimIndexName(claimed, &database.Indexes[i], table); err != nil {
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
	claimed map[string]struct{}, constraint *schemamodel.Constraint, table schemamodel.Table,
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
	constraint.Name = derive(claimed, constraint.Columns[0])
	return nil
}

// claimIndexName names one inline index.
func claimIndexName(
	claimed map[string]struct{}, index *schemamodel.Index, table schemamodel.Table,
) error {
	if index.Name != "" {
		return claimExplicit(claimed, index.Name, table)
	}
	candidate := firstIndexColumn(*index)
	if candidate == "" {
		return fmt.Errorf(
			"%w: the index on %s starts with an expression, which MySQL names "+
				"functional_index and MariaDB refuses outright",
			ErrUnnamedIndex, table.Name)
	}
	index.Name = derive(claimed, candidate)
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
func claimExplicit(claimed map[string]struct{}, name string, table schemamodel.Table) error {
	if _, taken := claimed[name]; taken {
		return fmt.Errorf("%w: %s on %s", ErrDuplicateIndexName, name, table.Name)
	}
	claimed[name] = struct{}{}
	return nil
}

// derive is the server's own rule: the column name, then _2, _3 and so on.
//
// The counter walks upward from 2 and skips a name already claimed rather than
// counting how many claims share the base, which is what the servers do:
// measured, `KEY a_2 (b), KEY (a), KEY (a)` yields a_2, a and a_3.
func derive(claimed map[string]struct{}, column string) string {
	candidate := column
	for suffix := 2; ; suffix++ {
		if _, taken := claimed[candidate]; !taken {
			claimed[candidate] = struct{}{}
			return candidate
		}
		candidate = column + "_" + strconv.Itoa(suffix)
	}
}

// isMySQLFamily reports whether a source dialect names its indexes the way this
// file describes.
//
// Deliberately not a predicate in core/platform: PostgreSQL derives a different
// name for the same declaration -- users_a_key rather than a -- so this is a
// statement about one naming rule and not about a family of engines that share
// a catalog.
func isMySQLFamily(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}
