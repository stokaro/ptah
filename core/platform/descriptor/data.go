package descriptor

// Literals states how an engine spells a value written into a statement as a
// literal, as migration/datadiff writes the rows of a data migration. The zero
// value is the standard spelling: TRUE and FALSE, a backslash read as itself,
// a moment as quoted text without its offset, and no binary literal, so a
// binary value is refused.
type Literals struct {
	// NumericBool writes a boolean as 1 or 0 rather than as TRUE or FALSE.
	NumericBool bool `json:"numeric_bool,omitempty"`
	// BackslashEscapes reports whether a string literal reads a backslash as
	// the start of an escape, so a backslash in a value is doubled.
	BackslashEscapes bool `json:"backslash_escapes,omitempty"`
	// BinaryPrefix and BinarySuffix surround the hexadecimal digits of a
	// binary value, as `X'` and `'` do. An empty BinaryPrefix means the
	// engine has no binary literal the renderer knows, and a binary value is
	// refused rather than guessed at.
	BinaryPrefix string `json:"binary_prefix,omitempty"`
	// BinarySuffix follows the hexadecimal digits; see BinaryPrefix.
	BinarySuffix string `json:"binary_suffix,omitempty"`
	// TimestampPrefix is written in front of the quoted text of a moment,
	// such as "TIMESTAMP ", for an engine that reads bare text through a
	// session format.
	TimestampPrefix string `json:"timestamp_prefix,omitempty"`
	// TimestampOffset writes a moment with its numeric UTC offset, for an
	// engine whose timestamp literal accepts one and keeps the instant.
	TimestampOffset bool `json:"timestamp_offset,omitempty"`
	// ByOwner reports that the engine's owner writes the literals whose form
	// the column's declared type decides, through the dataext.Literals
	// service its runtime provides. A renderer given no such service refuses
	// the engine rather than write those literals from the value alone.
	ByOwner bool `json:"by_owner,omitempty"`
}

// ManagedData states how an engine's managed rows are compared and changed.
type ManagedData struct {
	// AlterTableUpdate writes a row change as `ALTER TABLE ... UPDATE`, the
	// engine's mutation, rather than as an UPDATE statement.
	AlterTableUpdate bool `json:"alter_table_update,omitempty"`
	// IdentityRefusesInsert is which identity columns refuse an explicit value
	// in an INSERT, so that re-inserting a deleted row's value on rollback
	// would fail. Empty means none does.
	IdentityRefusesInsert IdentityInsert `json:"identity_refuses_insert,omitempty"`
	// TypedRows reports that the engine's owner types the declared and live
	// rows before they are compared, and the rows a checkpoint bootstraps
	// before they are written, through the dataext.Rows service its runtime
	// provides. A caller given no such service refuses the engine.
	TypedRows bool `json:"typed_rows,omitempty"`
}

// IdentityInsert names the identity columns that refuse an explicit value.
type IdentityInsert string

const (
	// IdentityInsertAutoIncrement is every column the reader reports as
	// auto-increment.
	IdentityInsertAutoIncrement IdentityInsert = "auto-increment"
	// IdentityInsertGeneratedAlways is every column generated ALWAYS as an
	// identity. A BY DEFAULT identity takes an explicit value.
	IdentityInsertGeneratedAlways IdentityInsert = "generated-always"
)

// Seeds states how migration/seeder runs seed files and records them in its
// tracker table, schema_seeds.
type Seeds struct {
	// Tracker is the statement that creates the tracker table when it is
	// absent. It runs on every seed run, so it succeeds against a table that
	// exists. Empty takes the portable CREATE TABLE IF NOT EXISTS.
	Tracker string `json:"tracker,omitempty"`
	// Transactional runs each seed file in one transaction with the row that
	// records it. Without it the statements run one by one, and an
	// idempotent run is refused.
	Transactional bool `json:"transactional,omitempty"`
	// TablesQuery lists the base tables of the connection's schema, taking no
	// parameter. Empty takes the portable information_schema query, bound to
	// the connection's schema.
	TablesQuery string `json:"tables_query,omitempty"`
	// ByOwner reports that the engine runs each seed file and lists the
	// tables through its own dataext.Seeds service, which a connection to the
	// engine provides (dbschema.DatabaseConnection.Seeds). A seed run on a
	// connection without one refuses the engine.
	ByOwner bool `json:"by_owner,omitempty"`
}
