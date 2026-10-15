package descriptor

// Preparation states how internal/schemaprep prepares a declared schema for an
// engine before it is rendered or compared: how an enum column is built,
// which declared types the engine writes as others, and how the names of
// foreign keys a declaration leaves unnamed are chosen and compared; how an
// unnamed CHECK is named is the dialect's CheckNaming and ConstraintNaming.
// The zero value keeps an enum a standalone type, writes every type as
// declared, and scopes a foreign key name to its table with exact comparison.
type Preparation struct {
	// Enums is how the engine holds an enum column. Empty keeps the enum a
	// standalone type that columns name, as CREATE TYPE ... AS ENUM does.
	Enums EnumStyle `json:"enums,omitempty"`
	// EnumCheckType is the column type an [EnumCheck] column is built as,
	// such as "TEXT".
	EnumCheckType string `json:"enum_check_type,omitempty"`
	// EnumCheckQuotesColumn names the column inside an [EnumCheck] column's
	// CHECK the way the dialect refers to an identifier, rather than bare.
	EnumCheckQuotesColumn bool `json:"enum_check_quotes_column,omitempty"`
	// SerialAutoIncrement writes SERIAL, BIGSERIAL and SMALLSERIAL as INT,
	// BIGINT and SMALLINT with the engine's own auto-increment clause, for an
	// engine that has no serial type.
	SerialAutoIncrement bool `json:"serial_auto_increment,omitempty"`
	// TypeSubstitutions maps a declared type, matched exactly, to the type the
	// engine writes in its place.
	TypeSubstitutions map[string]string `json:"type_substitutions,omitempty"`
	// ForeignKeyNamespace is where a foreign key name has to be unique: the
	// whole database, the table's schema, or, empty, the table alone.
	ForeignKeyNamespace Namespace `json:"foreign_key_namespace,omitempty"`
	// ForeignKeyNamesFoldCase reports that two foreign key names differing
	// only in letter case collide.
	ForeignKeyNamesFoldCase bool `json:"foreign_key_names_fold_case,omitempty"`
	// FoldsIndexConstraints reports that the server keeps one index for equal
	// UNIQUE and PRIMARY KEY constraints of a table, under the name of the one
	// it keeps; see internal/schemaprep.FoldedIndexConstraints.
	FoldsIndexConstraints bool `json:"folds_index_constraints,omitempty"`
	// PostgresBuiltInTypes reports that a bare type name PostgreSQL's catalog
	// or grammar defines names that built-in type, so a user type of the same
	// name is not assumed.
	PostgresBuiltInTypes bool `json:"postgres_built_in_types,omitempty"`
}

// EnumStyle is how an engine holds an enum column.
type EnumStyle string

const (
	// EnumColumnType builds the column as the engine's ENUM column type,
	// enum('a','b').
	EnumColumnType EnumStyle = "enum-column-type"
	// EnumNumbered builds the column as an Enum8, or an Enum16 past 127
	// values, numbering the values from 1 in declaration order.
	EnumNumbered EnumStyle = "enum-numbered"
	// EnumCheck builds the column as text with a CHECK that it holds one of
	// the values.
	EnumCheck EnumStyle = "check"
)

// Namespace is where a name has to be unique.
type Namespace string

const (
	// NamespaceDatabase is the whole database.
	NamespaceDatabase Namespace = "database"
	// NamespaceSchema is one schema.
	NamespaceSchema Namespace = "schema"
)
