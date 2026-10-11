package lintext

// Baseline is the schema state migration versions start from, which a host
// reads from a dev database for the rules that ask for it. The package
// documentation says when a host asks and what it replays. Every row names
// the version whose starting state it describes, so one value carries every
// version a run read.
type Baseline struct {
	// Columns are the columns of each version's starting state.
	Columns []BaselineColumn `json:"columns,omitempty"`
	// Indexes are the indexes of the same states, from the same read.
	Indexes []BaselineIndex `json:"indexes,omitempty"`
	// Hypertables are the TimescaleDB hypertables of the same states.
	Hypertables []BaselineHypertable `json:"hypertables,omitempty"`
	// Dependents are the views and routines that read each column of the
	// same states.
	Dependents []BaselineDependent `json:"dependents,omitempty"`
}

// BaselineSubject names the statements of one file that one rule would judge
// further with the schema state the file's version starts from. An owner
// lists it in [CheckResult.BaselineSubjects] for a rule whose input reads a
// baseline.
type BaselineSubject struct {
	// Code is the rule that asks, one of the request's codes.
	Code string `json:"code"`
	// File is the position of the file in the request.
	File int `json:"file"`
	// Statements are the positions of the statements in that file, at least
	// one.
	Statements []int `json:"statements"`
}

// BaselineColumn is one column of the schema state a migration version starts
// from, as read from the dev database the run validates against.
//
// Reading SQL text is enough to see that `ALTER TABLE users RENAME COLUMN id TO
// oid` retires the name `id`. It is not enough to see what the new name
// introduces: the retired column's type, nullability and default live in an
// earlier migration file or in the base schema, never in the rename statement.
// A host that replays the directory on a dev database can read them there and
// hand them over, and only then can the add side of a rename be reported.
//
// A host reads the state only for the versions a rule lists in
// [CheckResult.BaselineSubjects], so a directory with no renames costs no
// introspection at all.
type BaselineColumn struct {
	// Version is the migration version whose starting state this column belongs
	// to: the state read BEFORE that version is applied, not after.
	Version int64 `json:"version"`
	// Schema is the schema of the owning table, as the server spells it.
	Schema string `json:"schema,omitempty"`
	// Table is the owning table, as the server spells it.
	Table string `json:"table"`
	// Name is the column name, as the server spells it.
	Name string `json:"name"`
	// DataType is the column type spelled the way the compatibility surface
	// prints it. Empty when the caller could not establish that spelling, which
	// leaves the diagnostic reported under Ptah's own wording rather than
	// inventing an Atlas sentence for a type nobody measured.
	DataType string `json:"data_type,omitempty"`
	// ColumnType is the type exactly as the server spells it, member lists
	// included: MySQL's information_schema COLUMN_TYPE, `enum('a','b')` or
	// `set('x','y')`. It is separate from DataType because DataType is a
	// normalized spelling for one family of diagnostics and is empty for
	// everything that spelling does not cover, which included every ENUM and
	// SET column; the rules that compare member lists read this field and
	// find nothing when it is empty (stokaro/ptah#2942).
	ColumnType string `json:"column_type,omitempty"`
	// Charset is the column's character set as the server names it, empty
	// for a column whose type carries none. TableCharset is the default
	// character set of the owning table, which is what a MODIFY that names
	// no character set gives the column (stokaro/ptah#2942).
	Charset      string `json:"charset,omitempty"`
	TableCharset string `json:"table_charset,omitempty"`
	// Collation is the column's collation as the server names it: MySQL's
	// COLLATION_NAME, which every character column carries, or the collation
	// a PostgreSQL column was declared with, empty for one that takes the
	// database default. TableCollation is the owning table's default
	// collation, which is what a MODIFY that names no COLLATE gives a column
	// of the table's character set. A statement that names a collation is
	// judged against these: a restated collation is a no-op on every engine
	// and a changed one costs an index rebuild or a copy (stokaro/ptah#2957).
	Collation      string `json:"collation,omitempty"`
	TableCollation string `json:"table_collation,omitempty"`
	// TablePartitioned reports that the owning table is a PostgreSQL
	// declaratively partitioned table (relkind 'p'): a parent, or a partition
	// that is partitioned itself. Nothing in the migration text that indexes it
	// can say so, and the answer decides which index builds PostgreSQL
	// accepts: it refuses CREATE INDEX CONCURRENTLY on one.
	TablePartitioned bool `json:"table_partitioned,omitempty"`
	// NotNull reports whether the column rejects NULL.
	NotNull bool `json:"not_null,omitempty"`
	// HasDefault reports whether the column carries a DEFAULT expression.
	HasDefault bool `json:"has_default,omitempty"`
}

// BaselineDependent is one object that reads a column in the schema state a
// version starts from.
//
// It is the answer to "what breaks if this column goes", carried into the
// linter rather than derived there: resolving it needs the view and routine
// bodies of the replayed schema, and the linter reads migration text.
//
// Kind names what does the reading -- a view, a materialized view, a function
// or a procedure -- because the operator's next move differs by kind and a bare
// name does not say which.
type BaselineDependent struct {
	// Version is the migration version whose starting state this belongs to:
	// the state read BEFORE that version is applied.
	Version int64 `json:"version"`
	// Schema and Table are the owning table, as the server spells them.
	Schema string `json:"schema,omitempty"`
	Table  string `json:"table"`
	// Column is the column that is read.
	Column string `json:"column"`
	// Dependent is the object that reads it.
	Dependent string `json:"dependent"`
	// Kind is what the dependent is.
	Kind string `json:"kind"`
}

// BaselineIndex is one index of the schema state a migration version starts
// from, read from the dev database beside [BaselineColumn].
//
// It settles the questions the columns alone leave open: whether a collation
// or character-set change on a column takes a copy or an index rebuild,
// whether a unique index that replaces a dropped one covers the same columns,
// which columns a primary key attached USING INDEX sets NOT NULL, and whether
// a unique index already proves the rows unique (stokaro/ptah#2957).
type BaselineIndex struct {
	// Version is the migration version whose starting state this index
	// belongs to: the state read BEFORE that version is applied.
	Version int64 `json:"version"`
	// Schema and Table are the indexed table, as the server spells them.
	Schema string `json:"schema,omitempty"`
	Table  string `json:"table"`
	// Name is the index name, as the server spells it.
	Name string `json:"name"`
	// Parts are the key parts in key order.
	Parts []BaselineIndexPart `json:"parts,omitempty"`
	// Unique reports a unique index or constraint, and Primary the primary
	// key, which is unique as well.
	Unique  bool `json:"unique,omitempty"`
	Primary bool `json:"primary,omitempty"`
	// Partial reports an index with a WHERE clause, which covers some rows
	// and so proves nothing about the others.
	Partial bool `json:"partial,omitempty"`
	// Incomplete reports a key part the reader could not name, so Parts
	// lists fewer parts than the key has.
	Incomplete bool `json:"incomplete,omitempty"`
}

// BaselineIndexPart is one key part of a [BaselineIndex].
type BaselineIndexPart struct {
	// Column is the indexed column, empty for a part that is an expression.
	Column string `json:"column,omitempty"`
	// Prefix is the number of leading characters the part indexes, MySQL's
	// `KEY (notes(20))`; zero indexes the whole value.
	Prefix int `json:"prefix,omitempty"`
}

// BaselineHypertable is one TimescaleDB hypertable of the schema state a
// migration version starts from, read from the dev database beside
// [BaselineColumn].
//
// Nothing in PostgreSQL's own catalogs says a table is a hypertable: it
// reads back as an ordinary table, and only the extension's catalog knows. The
// difference decides what an index build on it may be told to do. Measured on
// TimescaleDB 2.30.1 over PostgreSQL 18.6, a hypertable refuses `CREATE INDEX
// CONCURRENTLY` outright (`hypertables do not support concurrent index
// creation`), so the remedy that fits an ordinary table is a statement the
// engine rejects here.
type BaselineHypertable struct {
	// Version is the migration version whose starting state this hypertable
	// belongs to: the state read BEFORE that version is applied.
	Version int64 `json:"version"`
	// Schema and Table name the hypertable, as the server spells them.
	Schema string `json:"schema,omitempty"`
	Table  string `json:"table"`
}
