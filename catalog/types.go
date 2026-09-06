// Package catalog defines the database-agnostic model of an introspected live
// schema (Database and its tables, columns, indexes, and constraints) shared by
// the dbschema readers and writers and consumed by schema diffing.
//
// The name says which side of the comparison this is. A schema Ptah reads from
// a server is the CURRENT state; what an authoring source declares is the
// DESIRED one, and that lives in core/goschema. Under the old name the two
// were both `types`, one of them nested under `dbschema/`, and a reader had to
// know which import a file had picked to tell them apart -- while the type
// named `Database` here is a catalog and the type named `DBSchema` was not a
// schema (stokaro/ptah#2246 section 2.1).
//
// The DB prefix went with it. It repeated the package for every type, so
// `types.DBTable` said "table" twice and `catalog.Table` says it once.
package catalog

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/normalize"
	"ptah.run/internal/tableref"
)

// Database is the complete description one schema read produced: the CURRENT
// side of a schema comparison, where the desired side is core/goschema. The
// dbschema readers fill it from a live server; any other [SchemaReader]
// implementation, stokaro/ptah-testkit's fakes included, constructs it
// directly.
//
// The Schema field the object types here carry follows one convention: a
// reader blanks it for an object in the read's default schema and fills it
// for an object outside that schema. Empty therefore does not mean
// "no schema" -- it means the connection's default, which
// ServerInfo.IdentifierSemantics.DefaultSchema names and from which the
// qualified spelling is reconstructed. It is a convention rather than an
// invariant: a reader is free to fill the default schema instead of blanking
// it, so a consumer tolerates both spellings rather than relying on the
// blank. Both sides of a comparison then have to build their object keys the
// same way, through the QualifiedName methods, which delegate to
// [QualifyTableName]. Keying the two sides differently is how a synced table
// turns into a phantom CREATE and DROP pair (stokaro/ptah#1244,
// stokaro/ptah#1991).
//
// NotDescribed records what the read deliberately did not look at, which is a
// different fact from an object being absent; see that field.
type Database struct {
	Schemas     []Schema           `json:"schemas"`
	Tables      []Table            `json:"tables"`
	Enums       []Enum             `json:"enums"`
	Indexes     []Index            `json:"indexes"`
	Constraints []Constraint       `json:"constraints"`
	Extensions  []Extension        `json:"extensions"` // PostgreSQL extensions
	Functions   []Function         `json:"functions"`  // PostgreSQL custom functions
	Sequences   []Sequence         `json:"sequences"`  // PostgreSQL standalone sequences
	Domains     []Domain           `json:"domains"`    // PostgreSQL domain types
	Composites  []CompositeType    `json:"composites"` // PostgreSQL composite types
	Ranges      []Range            `json:"ranges"`     // PostgreSQL range types
	Views       []View             `json:"views"`      // Database views
	MatViews    []MaterializedView `json:"matviews"`   // Database materialized views
	Synonyms    []Synonym          `json:"synonyms"`   // SQL Server synonyms
	// ExtendedProperties are the SQL Server extended properties this
	// description covers: schema-, table- and column-scoped ones. See
	// [ExtendedProperty] for what is deliberately not in it.
	ExtendedProperties []ExtendedProperty `json:"extended_properties,omitempty"`

	// ContinuousAggregates are the TimescaleDB continuous aggregates this read
	// found. See [ContinuousAggregate] for why they are not views.
	ContinuousAggregates []ContinuousAggregate `json:"continuous_aggregates,omitempty"`

	// Hypertables are the TimescaleDB hypertables among the tables above.
	//
	// They are recorded BESIDE the tables rather than instead of them, because
	// a hypertable is an ordinary PostgreSQL table as far as this description
	// goes: pg_class reports relkind 'r', the columns are the columns, and
	// every statement Ptah renders for it is correct. What is missing is that
	// it is partitioned, and no declaration syntax can say so yet -- so this
	// list exists to be REPORTED rather than compared, and its consumer is the
	// note that tells an operator the description they are reading is not the
	// whole truth about these tables (stokaro/ptah#1026).
	Hypertables []Hypertable `json:"hypertables,omitempty"`
	Triggers    []Trigger    `json:"triggers"`     // Database triggers
	RLSPolicies []RLSPolicy  `json:"rls_policies"` // PostgreSQL RLS policies
	Roles       []Role       `json:"roles"`        // PostgreSQL roles
	Grants      []Grant      `json:"grants"`       // PostgreSQL privilege grants

	// ObjectOwners are the owners of the objects this read covers, one row per
	// object, on the engines that have an owner to report.
	//
	// Like RoleMemberships it is read for ANALYSIS rather than for planning:
	// Ptah renders no OWNER TO and no CREATE SCHEMA ... AUTHORIZATION, and
	// treating ownership as part of the description is what made an inspect
	// describe the connecting superuser and plan a CREATE ROLE for it
	// (stokaro/ptah#1950).
	ObjectOwners []ObjectOwner `json:"object_owners,omitempty"`

	// RoleMemberships are the role-in-role edges the server holds between the
	// roles Ptah manages: who inherits whose privileges.
	//
	// It is read for ANALYSIS rather than for planning. A membership is a
	// cluster-wide fact on the PostgreSQL family, and Ptah neither renders nor
	// diffs one today, so this list describes the server rather than the
	// desired state -- which is why it is omitted when empty and why nothing
	// in the comparator reads it (stokaro/ptah#1950).
	RoleMemberships []RoleMembership `json:"role_memberships,omitempty"`

	// RolesOutOfScope lists roles that exist on the server but that this
	// description deliberately does not define, because nothing in the
	// schemas being read refers to them.
	//
	// PostgreSQL roles belong to the cluster rather than to one database, so
	// "the reader did not describe this role" and "this role does not exist"
	// are different facts, and a comparator that cannot tell them apart plans
	// CREATE ROLE for a role that is already there. Roles and RolesOutOfScope
	// partition the roles Ptah manages, so their union answers the
	// comparator's question -- does this role exist -- whatever scoping rule
	// decided the description. See stokaro/ptah#1267 and stokaro/ptah#1276.
	//
	// The partition is over managed roles, not over the whole catalog. A
	// PostgreSQL reader leaves the reserved pg_ roles and the bootstrap
	// superuser out of both lists, because Ptah manages neither in either
	// direction, so the union is not every role the server has and must not be
	// described as if it were. A desired schema that names one of them is
	// refused before anything is compared or planned, naming the role and the
	// rule, rather than compared against nothing. See stokaro/ptah#1312.
	//
	// This field is not part of the description: it carries role names from
	// outside the inspected scope, so it is never serialized and never
	// rendered. Only Roles reaches output.
	RolesOutOfScope []Role `json:"-"`

	// NotDescribed records what this read did not look at, so a comparator can
	// tell an object the database does not have from one the reader was never
	// asked about. The zero value claims the read covered everything; see
	// [ptah.run/core/coverage].
	NotDescribed coverage.Set `json:"not_described,omitzero"`

	// UnregisteredVirtualTables lists the SQLite virtual tables this read found
	// whose module the reading build does not register.
	//
	// It is the description's own statement that part of this database could
	// not be classified. SQLite reports a table as `shadow` only when the
	// module that owns it is loaded and claims the name, so with the module
	// absent a virtual table is still recognized as virtual while its private
	// storage arrives as ordinary user tables. Tables therefore holds objects
	// that are not user tables, and no field on any of them says so -- the fact
	// belongs to the read, not to a row.
	//
	// A comparator that ignores this plans DROP TABLE for a module's storage.
	// Measured on stokaro/ptah#1028: against an fts4 database this build cannot
	// load, `ptah schema apply` planned and executed five drops at exit 0 and
	// `MATCH` went from returning a row to `SQL logic error`.
	//
	// It is separate from Tables on purpose, because narrowing survives it. The
	// tables at risk are not the virtual table an operator would exclude, so a
	// selection that removes the virtual table leaves every dangerous row in
	// place; a signal carried on the excluded row would vanish exactly when it
	// matters. See [ptah.run/internal/sqlitevirtual].
	//
	// It is never serialized: the same database read by a build that registers
	// the module yields an empty list, so this describes the reader rather than
	// the database, and a description that carried it would not be portable.
	UnregisteredVirtualTables []VirtualTable `json:"-"`
}

// VirtualTable identifies one SQLite virtual table and the module that owns
// it, for the lists that talk about virtual tables rather than describe them.
type VirtualTable struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Module string `json:"module"`
}

// Schema represents a database schema/namespace.
type Schema struct {
	Name    string `json:"name"`
	Comment string `json:"comment,omitempty"`
	Charset string `json:"charset,omitempty"`
	Collate string `json:"collate,omitempty"`
}

// Table represents a database table
//
// EstimatedRows and RowStatsUnknown are a pair, and reading EstimatedRows
// alone is a mistake: PostgreSQL spells "nobody has ever counted this table"
// as pg_class.reltuples = -1, which floors to the same 0 a genuinely empty
// table reports. RowStatsUnknown carries that distinction so a caller choosing
// between a blocking and a non-blocking index build can fail safe instead of
// reading an absent statistic as an empty table. Readers that never collect
// row statistics leave both fields zero, which claims nothing.
//
// Partitioned marks a PostgreSQL declaratively partitioned parent (relkind
// 'p'). information_schema.tables reports it as an ordinary BASE TABLE, so
// Type cannot carry it, and the distinction decides which statements are legal:
// PostgreSQL rejects CREATE INDEX CONCURRENTLY and DROP INDEX CONCURRENTLY on
// a partitioned relation with SQLSTATE 0A000.
type Table struct {
	Name            string   `json:"name"`
	Schema          string   `json:"schema,omitempty"`
	Type            string   `json:"type"` // TABLE, VIEW, etc.
	Comment         string   `json:"comment"`
	Charset         string   `json:"charset,omitempty"` // MySQL/MariaDB default character set for columns declared without one
	Collate         string   `json:"collate,omitempty"` // MySQL/MariaDB default collation
	Columns         []Column `json:"columns"`
	EstimatedRows   int64    `json:"estimated_rows,omitempty"`    // Best-effort planner estimate from database statistics
	RowStatsUnknown bool     `json:"row_stats_unknown,omitempty"` // The database reports no usable row statistics; EstimatedRows is not a row count
	Partitioned     bool     `json:"partitioned,omitempty"`       // PostgreSQL declaratively partitioned parent (pg_class.relkind = 'p')
	RLSEnabled      bool     `json:"rls_enabled"`                 // Whether RLS is enabled on this table (PostgreSQL)
	Strict          bool     `json:"strict,omitempty"`            // SQLite STRICT table option
	WithoutRowID    bool     `json:"without_rowid,omitempty"`     // SQLite WITHOUT ROWID table option
	// ClickHouseSortingKey is the ORDER BY a MergeTree table sorts by, when
	// that is not simply its primary-key columns.
	//
	// The two are usually the same and the renderer derives the ORDER BY from
	// the primary key, so this stays empty. They come apart when a table
	// declares `PRIMARY KEY (a) ORDER BY (a, b)`: the primary key alone would
	// describe a table that sorts differently from the one being read, and
	// applying that description would create one (stokaro/ptah#1603).
	ClickHouseSortingKey string `json:"clickhouse_sorting_key,omitempty"`
	// ClickHouseOrderBy is the ORDER BY exactly as system.tables reports it,
	// including when it equals the primary key.
	//
	// ClickHouseSortingKey above deliberately omits that case, because the
	// renderer can derive the clause from the primary key -- but it derives the
	// COLUMNS, not their order, and a table sorted `(day, id)` came back sorted
	// `(id, day)`. That is a different physical layout: every range scan the
	// original order served becomes a full scan (stokaro/ptah#2198).
	ClickHouseOrderBy string `json:"clickhouse_order_by,omitempty"`
	// ClickHouseEngine is the table engine with its parameters, as
	// system.tables spells it -- "ReplacingMergeTree(ver)", not the bare family
	// name the `engine` column reports.
	//
	// A description without it takes the renderer's default of MergeTree, so a
	// ReplacingMergeTree read and replayed loses the deduplicating merge the
	// table exists for (stokaro/ptah#2198).
	ClickHouseEngine string `json:"clickhouse_engine,omitempty"`
	// ClickHousePartitionKey and ClickHouseSamplingKey are the PARTITION BY and
	// SAMPLE BY expressions, empty for a table that declares neither.
	ClickHousePartitionKey string `json:"clickhouse_partition_key,omitempty"`
	ClickHouseSamplingKey  string `json:"clickhouse_sampling_key,omitempty"`
	// ClickHousePrimaryKey is the PRIMARY KEY a MergeTree table declares when
	// it is not the whole sorting key, and empty when the two agree.
	//
	// A primary key that is a prefix of the ORDER BY is how a MergeTree table is
	// tuned: the sparse index holds one mark per granule of the prefix, and the
	// rest of the sorting key only orders rows within it. Measured on ClickHouse
	// 26.7.5.10, `PRIMARY KEY (id) ORDER BY (id, s)` read and replayed came back
	// as `PRIMARY KEY (id, s)` -- a wider index over the same data, which is a
	// different table and says nothing about it (stokaro/ptah#2198).
	ClickHousePrimaryKey string `json:"clickhouse_primary_key,omitempty"`
	// ClickHouseTTL is the table's TTL expression, empty for a table with none.
	// It is the retention rule: a table replayed without it keeps rows it was
	// configured to delete.
	ClickHouseTTL string `json:"clickhouse_ttl,omitempty"`
	// ClickHouseSettings is the SETTINGS clause body.
	//
	// The server resolves one whether or not the author wrote it -- every
	// MergeTree table reports at least index_granularity -- so this is what the
	// table HAS rather than what was declared.
	ClickHouseSettings string `json:"clickhouse_settings,omitempty"`
	// RowTTL is the CockroachDB row-level TTL this table carries, nil for a
	// table with none and for every target without capability.RowLevelTTL.
	//
	// omitzero keeps every other dialect's serialization byte-identical, which
	// matters because a description is compared, fingerprinted and replayed
	// (stokaro/ptah#1027).
	RowTTL *ast.RowTTLSpec `json:"row_ttl,omitzero"`
	// RowDeletionPolicy is the row deletion policy this table carries, nil for
	// a table with none (stokaro/ptah#2236).
	RowDeletionPolicy *ast.RowDeletionPolicySpec `json:"row_deletion_policy,omitzero"`
	// VirtualModule is the SQLite module that owns this table, from the USING
	// clause of the CREATE VIRTUAL TABLE statement that created it -- `fts5`,
	// `rtree`, `geopoly`, or any other module a build registers. It is empty
	// for an ordinary table, and a non-empty value means the table cannot be
	// described by CREATE TABLE at all: it has no column list of its own, and
	// a plain table of the same name is a different object. See
	// stokaro/ptah#1028.
	VirtualModule string `json:"virtual_module,omitempty"`
	// VirtualArguments is the text between the module's parentheses, verbatim.
	// Module arguments are not SQL -- only the module interprets them -- so
	// they are carried unparsed and reproduced byte for byte.
	VirtualArguments string `json:"virtual_arguments,omitempty"`
}

// RawType is the type spelling a comparator holds a desired schema against.
//
// FormattedType comes first because it is the only field that survives an array
// or a domain, and because the desired side reads the same field -- see
// goSchemaFieldType in internal/convert/dbschematogo. The reader fills it from
// the server's own format_type for exactly those two shapes and leaves it empty
// for every other column.
//
// With ColumnType and UDTName first the two sides read different fields for the
// same column, and the comparator reported a change between a database and
// ITSELF. Measured on PostgreSQL 17, `ptah-compat schema diff` with --from and
// --to naming one database, seven phantom rows:
//
//	arrays.a_bit          type: _bit    -> bit(8)[]
//	arrays.a_char         type: _bpchar -> character(5)[]
//	arrays.a_cube         type: _cube   -> cube[]
//	arrays.a_enum         type: _status -> status[]
//	arrays.a_varchar      type: varchar -> character varying(100)[]
//	arrays.a_varchar_dim  type: varchar -> character varying(100)[]
//	scalars.c_tags        type: text    -> tags
//
// Every one of them proposed an ALTER COLUMN ... TYPE to the type the column
// already had. None survive this (stokaro/ptah#1138).
//
// The width lives in a field of its own, which is the other half of the answer:
// a PostgreSQL read reports `code varchar(50)` as DataType "character varying"
// with CharacterMaxLength 50, so a caller reading DataType alone holds a type
// with no width and reports an ALTER for every varchar column that never
// changed (stokaro/ptah#1662).
//
// It is a method on the column rather than a helper beside one comparator
// because both comparators have to ask the same question and get the same
// answer.
//
// What this string may then be USED for is not uniform, and the difference is
// the whole of #1138's comparator half. An array's spelling is a type. A
// domain's spelling is the identifier its author chose, and a caller comparing
// types keeps it away from type normalization for that reason.
func (c Column) RawType() string {
	rawType := strings.TrimSpace(c.FormattedType)
	if rawType == "" {
		rawType = strings.TrimSpace(c.ColumnType)
	}
	if rawType == "" && c.UDTName != "" {
		rawType = strings.TrimSpace(c.UDTName)
	}
	if rawType == "" {
		rawType = strings.TrimSpace(c.DataType)
	}
	if strings.Contains(rawType, "(") {
		return rawType
	}
	return withTypeSize(rawType, c)
}

// withTypeSize appends the width or precision the catalog keeps in a field of
// its own, for the families that have one.
//
// The bit families are here for a reason worth stating: without them
// `ptah schema inspect` wrote a `bit(4)` column as `bit`, and replaying that
// document into a fresh database produced `bit(1)` -- three bits of every value
// gone, measured on PostgreSQL 17.11. A `bit varying(8)` column came back
// unlimited, and applying the document to the SOURCE database removed the
// declared width from the live column. PostgreSQL keeps both widths in the same
// `character_maximum_length` the varchar family uses, and the reader already
// carries it (stokaro/ptah#2034).
func withTypeSize(rawType string, column Column) string {
	// The fold is normalize.Type's, not a second list of spellings: `character
	// varying` and `varchar` are one type and only that package says so.
	switch normalize.Type(rawType) {
	case "varchar", "char", "bit", "bit varying":
		if column.CharacterMaxLength != nil {
			return fmt.Sprintf("%s(%d)", rawType, *column.CharacterMaxLength)
		}
	case "decimal":
		if column.NumericPrecision == nil {
			return rawType
		}
		if column.NumericScale != nil {
			return fmt.Sprintf("%s(%d,%d)", rawType, *column.NumericPrecision, *column.NumericScale)
		}
		return fmt.Sprintf("%s(%d)", rawType, *column.NumericPrecision)
	}
	return rawType
}

// QualifiedName returns schema.table when Schema is set, or Name otherwise.
func (t Table) QualifiedName() string {
	return QualifyTableName(t.Schema, t.Name)
}

// QualifyTableName returns an unambiguous schema-qualified table reference:
// the string the QualifiedName methods in this package delegate to, and the
// key both sides of a schema comparison must build object names through.
//
// The format is deterministic: the same pair always yields the same string,
// and the two parts stay distinguishable in it. An empty schema yields the
// table part alone; otherwise the two are joined with a dot, and a part that
// would otherwise be misread -- one carrying a dot, or a quoting character of
// its own -- is quoted, so a table named "tenant.data" with no schema and a
// table named "data" in schema "tenant" produce different keys.
//
// The escaping is this package's own, not any one dialect's quoting rules.
// Build keys through this function rather than joining the parts by hand, and
// compare them rather than parsing them back apart.
func QualifyTableName(schema, table string) string {
	return tableref.Canonical(schema, table)
}

// Column represents a database column.
//
// GeneratedKind / GeneratedExpression are populated by readers for dialects
// that expose generated column metadata. Schema comparison matches these fields
// when the goschema-side model also carries generated column metadata.
//
// DomainName and FormattedType are a pair. DataType reports a domain column's
// BASE type, so it alone cannot say that the declared type was a domain:
// DomainName carries that fact and FormattedType carries the spelling the
// server uses for it, schema-qualified where the search path needs that.
// Everything that answers "what type is this column declared as" -- the
// desired-state conversion, the Atlas-compatible JSON inspect output, and the
// comparator's database side -- must consult DomainName rather than infer the
// answer from DataType, which reports the base type and drops the domain's
// constraints with it. See stokaro/ptah#1242.
type Column struct {
	Name               string  `json:"name"`
	DataType           string  `json:"data_type"`
	UDTName            string  `json:"udt_name"`                 // For PostgreSQL enum types
	FormattedType      string  `json:"formatted_type,omitempty"` // Server's own spelling, where the catalog cannot express it
	ColumnType         string  `json:"column_type"`              // For MySQL ENUM syntax
	IsNullable         string  `json:"is_nullable"`              // YES/NO
	ColumnDefault      *string `json:"column_default"`           // Can be NULL
	CharacterMaxLength *int    `json:"character_max_length"`     // For VARCHAR, etc.
	Charset            string  `json:"charset,omitempty"`        // MySQL/MariaDB column character set
	Collate            string  `json:"collate,omitempty"`        // MySQL/MariaDB column collation; on PostgreSQL the declared collation, empty for the database default
	// Comment is the column's own comment, where the target has one and the
	// reader reads it. It is empty for a column nobody commented, which is why
	// it is omitted from the encoding rather than written as "".
	Comment string `json:"comment,omitempty"`
	// NotNullConstraintName is the name the target keeps for this column's NOT
	// NULL, where it keeps one as an addressable catalog object.
	//
	// PostgreSQL 18 records one row per NOT NULL in pg_constraint with
	// contype 'n', keyed to the column through conkey. PostgreSQL 17 stores
	// nothing, so this is empty there and the read is not even attempted --
	// see the capability gate on the reader's projection.
	//
	// PostgreSQL 18 names EVERY NOT NULL and provides no catalog flag
	// separating an author-supplied name from a generated one, so a faithful
	// read returns all of them. Suppressing names that look generated would
	// silently lose a supplied name that happens to use the same pattern
	// (stokaro/ptah#2161).
	NotNullConstraintName string `json:"not_null_constraint_name,omitempty"`
	NumericPrecision      *int   `json:"numeric_precision"` // For DECIMAL, etc.
	NumericScale          *int   `json:"numeric_scale"`     // For DECIMAL, etc.
	// DatetimePrecision is the fractional-seconds precision of a timestamp,
	// time, or interval column, as information_schema.datetime_precision
	// reports it: the declared (p), or the type's default when none was
	// declared. PostgreSQL rewrites a column whose precision shrinks and not
	// one whose precision grows, which is why the lint baseline needs it.
	DatetimePrecision *int `json:"datetime_precision,omitempty"`
	OrdinalPosition   int  `json:"ordinal_position"`
	IsAutoIncrement   bool `json:"is_auto_increment"` // Derived field
	IsPrimaryKey      bool `json:"is_primary_key"`    // Derived field
	IsUnique          bool `json:"is_unique"`         // Derived field

	// TypeIsDeclaredText records that DataType must be written back as it
	// stands, because canonicalizing it would describe a different column.
	//
	// The name records where the fact was first found. SQLite keeps the
	// declaration and derives an affinity from it at use time, so `VARCHAR(80)`
	// comes back as `VARCHAR(80)` and means TEXT -- the text an author wrote,
	// stored verbatim. A description that rewrote it to `TEXT` replayed as a
	// different table, and the comparison planned a rebuild that changed
	// nothing (stokaro/ptah#2040).
	//
	// The contract is broader than that origin, and two more engines need it
	// for a reason of their own: their catalog answers with a NATIVE type whose
	// name Ptah's portable mapping also uses for something else.
	//
	//   - SQL Server: `varchar` is one byte per character. The portable mapping
	//     turns a declared VARCHAR into NVARCHAR, which is two, so a native
	//     varchar re-rendered bare comes back Unicode (stokaro/ptah#2147).
	//   - ClickHouse: `DateTime` is second precision and four bytes wide. The
	//     portable mapping turns a declared DATETIME into DateTime64(3), which
	//     is millisecond precision and eight (stokaro/ptah#2142).
	//
	// PostgreSQL, MySQL, MariaDB and Oracle do not set it: their catalogs
	// answer with types whose names the portable mapping leaves alone, so there
	// is nothing for the flag to protect.
	//
	// A renderer reads it to decide whether it may canonicalize the spelling.
	// It must not.
	TypeIsDeclaredText bool `json:"type_is_declared_text,omitempty"`

	// DomainName names the domain a column is declared with, empty for every
	// column whose declared type is not a domain (PostgreSQL only today).
	//
	// It is a separate fact rather than something read back out of
	// FormattedType, because FormattedType is filled for arrays as well and the
	// two shapes want opposite answers: an array's spelling is a TYPE, which may
	// be compared and normalized like any other, while a domain's spelling is an
	// IDENTIFIER its author chose and must only ever be compared by identity.
	// Nothing but the catalog can tell them apart -- a domain over an array is
	// reported with data_type "ARRAY" exactly like a plain array column
	// (stokaro/ptah#1138).
	DomainName string `json:"domain_name,omitempty"`

	// DomainSchema names the schema holding that domain, empty for every column
	// whose declared type is not a domain.
	//
	// A domain's identity is (schema, name), and the name alone is not it:
	// public.status and other.status are two different types with different
	// CHECK constraints over possibly different base types. A comparator handed
	// only "status" for both calls a column that must be converted unchanged,
	// while the plan still drops the domain the desired schema no longer
	// declares -- so DROP DOMAIN ... CASCADE takes the column and its data with
	// nothing having converted it first (stokaro/ptah#1138).
	//
	// It is recorded raw, exactly as information_schema.columns.domain_schema
	// reports it, rather than blanked for the schema being read: the domain a
	// column is declared with may live in a schema the read never visits, and
	// blanking it there erases the very distinction this field exists for.
	// FormattedType cannot stand in, because the server writes a qualifier
	// there only when the search path forces one.
	DomainSchema string `json:"domain_schema,omitempty"`

	// GeneratedExpression holds the generated-column expression. Nil for plain
	// columns.
	GeneratedExpression *string `json:"generated_expression,omitempty"`
	// GeneratedKind names the generated-column kind, for example STORED,
	// VIRTUAL, MATERIALIZED, ALIAS, or EPHEMERAL. Empty for plain columns.
	GeneratedKind string `json:"generated_kind,omitempty"`

	// UpdateExpression is the expression a MySQL-family column reassigns itself
	// to on every UPDATE -- `ON UPDATE CURRENT_TIMESTAMP` and its parameterized
	// forms. Empty for a column that has none, and for every other engine.
	//
	// It is read from `information_schema.COLUMNS.EXTRA`, which reports it
	// plainly: measured on MySQL 8.4, a column declared `DATETIME ON UPDATE
	// CURRENT_TIMESTAMP` comes back with EXTRA `on update CURRENT_TIMESTAMP`.
	// It is on the description because a render that omits it builds a column
	// that silently stops maintaining itself, which nothing downstream can
	// notice (stokaro/ptah#1215).
	UpdateExpression string `json:"update_expression,omitempty"`

	// IdentityGeneration records an identity column's generation mode, mirroring
	// the goschema-side field: "ALWAYS" (GENERATED ALWAYS AS IDENTITY, which
	// rejects explicit inserts), "BY_DEFAULT" (GENERATED BY DEFAULT AS IDENTITY,
	// which accepts them), or "" for non-identity columns. Populated by readers
	// for dialects that expose identity metadata (currently PostgreSQL via
	// pg_attribute.attidentity).
	IdentityGeneration string `json:"identity_generation,omitempty"`

	// IdentityStart and IdentityIncrement are the first value an identity
	// column issues and the step between values, as the catalog spells them.
	//
	// They are strings rather than numbers because that is what the model they
	// feed uses, and because the value is not always an int64: SQL Server
	// allows DECIMAL(38,0) IDENTITY, whose seed does not fit one. Carrying the
	// digits keeps a reader from having to choose between rounding and
	// refusing.
	//
	// Empty for a non-identity column, and for an identity column whose reader
	// does not report them.
	IdentityStart     string `json:"identity_start,omitempty"`
	IdentityIncrement string `json:"identity_increment,omitempty"`
}

// Enum represents a database enum type (PostgreSQL)
type Enum struct {
	Name string `json:"name"`
	// Schema owns the enum. Readers blank it for the connection's own schema,
	// exactly as they do for tables, views and domains, so a filter or a
	// comparison reconstructs the qualified spelling from the connection's
	// default. Without it a schema-qualified `--exclude app.color` matched
	// nothing and silently kept the enum (stokaro/ptah#933).
	Schema string   `json:"schema,omitempty"`
	Values []string `json:"values"`
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
func (e Enum) QualifiedName() string { return QualifyTableName(e.Schema, e.Name) }

// Domain represents a PostgreSQL domain type read from the database.
type Domain struct {
	Name     string `json:"name"`
	Schema   string `json:"schema,omitempty"`
	BaseType string `json:"base_type"`
	NotNull  bool   `json:"not_null"`
	Default  string `json:"default,omitempty"`
	Check    string `json:"check,omitempty"`
	// CheckConstraints names each CHECK the catalog holds for this domain,
	// alongside the expression the server stores for it. Check above is the
	// same expressions joined with AND, which is what a renderer needs and
	// what a comparison of the whole domain reads; a constraint cannot be
	// altered by that joined form, because ALTER DOMAIN ... DROP CONSTRAINT
	// takes a name.
	//
	// It is a reader-only execution fact, like Function.IdentityArguments:
	// a serialized schema description declares what a domain must enforce,
	// not what the server happened to name the constraint enforcing it.
	CheckConstraints []DomainCheck `json:"-"`
}

// DomainCheck is one named CHECK constraint of a domain, as the catalog
// holds it. Expression is the server's own rewritten form -- PostgreSQL
// reparses and prints a CHECK rather than storing the text it was given -- so
// it compares equal only to another expression that made the same round trip.
type DomainCheck struct {
	Name       string `json:"name"`
	Expression string `json:"expression"`
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
func (d Domain) QualifiedName() string { return QualifyTableName(d.Schema, d.Name) }

// CompositeField is a single field of a composite type read from the database.
type CompositeField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// CompositeType represents a PostgreSQL composite type read from the database.
type CompositeType struct {
	Name   string           `json:"name"`
	Schema string           `json:"schema,omitempty"`
	Fields []CompositeField `json:"fields"`
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
func (c CompositeType) QualifiedName() string { return QualifyTableName(c.Schema, c.Name) }

// Range represents a PostgreSQL range type read from the database.
//
// Everything after Subtype exists so a change to an EXISTING range type can be
// detected. While the reader returned only the name and subtype, the comparator
// had nothing to compare and reported a changed range as converged
// (stokaro/ptah#931 item 2).
type Range struct {
	Name    string `json:"name"`
	Schema  string `json:"schema,omitempty"`
	Subtype string `json:"subtype"`
	// SubtypeOpClass is the operator class backing the subtype's ordering
	// (pg_range.rngsubopc). Always populated by the catalog, including when the
	// author never named one, so the comparator only consults it when the
	// desired schema declares one.
	SubtypeOpClass string `json:"subtype_opclass,omitempty"`
	// Collation is the subtype collation (pg_range.rngcollation), empty for
	// non-collatable subtypes.
	Collation string `json:"collation,omitempty"`
	// Canonical is the canonicalization function (pg_range.rngcanonical), empty
	// when the range has none.
	Canonical string `json:"canonical,omitempty"`
	// SubtypeDiff is the subtype difference function (pg_range.rngsubdiff),
	// empty when the range has none.
	SubtypeDiff string `json:"subtype_diff,omitempty"`
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
func (r Range) QualifiedName() string { return QualifyTableName(r.Schema, r.Name) }

// IndexPart represents one ordered key column in an introspected index.
type IndexPart struct {
	Name string `json:"name"`
	// Expr is a raw indexed expression, such as lower(name). It is mutually
	// exclusive with Name: an expression is not an identifier, and a reader
	// that reports one in Name makes the renderer quote it into a column
	// reference that does not exist.
	Expr string `json:"expr,omitempty"`
	// Operator is the operator class this key was built with, for example
	// text_pattern_ops, together with its parameters when it has any, for
	// example tsvector_ops(siglen=64). Empty means the key uses its type's
	// default class with no parameters, which is the only case where omitting
	// it from emitted DDL reproduces the index. See #1242.
	//
	// A parameterised class is carried even when it is the type's default,
	// because the class name is the only place its parameters can hang.
	Operator string `json:"operator,omitempty"`
	Desc     bool   `json:"desc,omitempty"`
	// NullsOrder is an explicit NULLS ordering for this key: "FIRST", "LAST",
	// or empty when the key uses the direction's default. PostgreSQL defaults
	// to NULLS LAST for ASC and NULLS FIRST for DESC, so only the deviating
	// spelling has to be carried.
	NullsOrder string `json:"nulls_order,omitempty"`
	// Prefix is the number of leading characters this key indexes, which MySQL
	// spells `INDEX (notes(20))`. Empty means the key covers the whole column.
	//
	// It is REQUIRED for a BLOB or TEXT column there -- MySQL refuses an index
	// on one without a length -- so a key that loses it produces a description
	// that cannot be applied (stokaro/ptah#2112).
	Prefix string `json:"prefix,omitempty"`
}

// Index NULLS ordering spellings for IndexPart.NullsOrder.
const (
	NullsOrderFirst = "FIRST"
	NullsOrderLast  = "LAST"
)

// Index represents a database index.
//
// Most fields are dialect-neutral. The Type/Expression/Granularity trio is
// populated only by the ClickHouse reader for data-skipping indexes; other
// readers leave them at their zero values so the diff layer does not start
// emitting spurious type/granularity changes for PostgreSQL or MySQL
// indexes.
type Index struct {
	Name      string   `json:"name"`
	TableName string   `json:"table_name"`
	Schema    string   `json:"schema,omitempty"`
	Columns   []string `json:"columns"`
	// Parts preserves key order and direction when the database exposes it.
	// Empty means the reader supplied only the legacy ascending Columns form.
	Parts      []IndexPart `json:"parts,omitempty"`
	IsUnique   bool        `json:"is_unique"`
	IsPrimary  bool        `json:"is_primary"`
	Definition string      `json:"definition"` // Full index definition
	// KeyPartsIncomplete reports that the catalog described a key part this
	// reader cannot name, so Columns (and Parts) list fewer parts than the key
	// actually has. The MySQL reader sets it for a functional key part --
	// `KEY idx ((b + 1))` -- whose information_schema.STATISTICS row carries a
	// NULL COLUMN_NAME and an EXPRESSION column that MariaDB does not have.
	//
	// A comparison must not read the key columns of such an index as the whole
	// key: it would plan a rebuild on every run for a key that never changed.
	// Everything else about the index is reported normally.
	KeyPartsIncomplete bool `json:"key_parts_incomplete,omitempty"`
	// Condition is the WHERE clause for partial indexes when the dialect
	// exposes one structurally.
	Condition string `json:"condition,omitempty"`
	// Comment is the index's own object comment, as PostgreSQL keeps it in
	// pg_description for the index relation and reports through
	// obj_description(indexrelid, 'pg_class').
	//
	// It is the index's, not the table's: COMMENT ON INDEX and COMMENT ON TABLE
	// address different objects, and an index has a slot for one on every
	// surface below this model -- schemamodel.Index.Comment, the Atlas-compatible
	// HCL reader's `comment` attribute, and the HCL writer's. Before #1242 this
	// field did not exist, so a comment the server reported was dropped between
	// the catalog and the model and every surface below it saw an index that
	// had none. Empty means the object carries no comment.
	Comment string `json:"comment,omitempty"`
	// NullsDistinct carries PostgreSQL UNIQUE INDEX NULLS [NOT] DISTINCT
	// state. Nil means the clause was not present in the definition.
	NullsDistinct *bool `json:"nulls_distinct,omitempty"`

	// Method is the index access method as the server spells it -- btree,
	// gin, gist, brin, hash. It is deliberately not Type: Type is the
	// ClickHouse data-skipping-index type below, a different concept that
	// happens to share a slot in the annotation surface, and overloading one
	// field with both would make a ClickHouse "bloom_filter" and a PostgreSQL
	// "gin" indistinguishable at this layer. Empty means the reader did not
	// report an access method.
	//
	// A dropped access method is not always a quiet degradation: an index on
	// a type with no btree operator class, such as point, does not replay at
	// all without it. See #1242.
	Method string `json:"method,omitempty"`
	// IncludeColumns carries PostgreSQL INCLUDE payload columns, the
	// non-key columns stored in the index for index-only scans.
	IncludeColumns []string `json:"include_columns,omitempty"`
	// StorageParams carries the index's WITH (...) storage parameters, keyed by
	// parameter name with the value as the server spells it.
	//
	// A reader records only the parameters the rest of the model can carry back
	// out again. A parameter recorded here but dropped by one of the surfaces
	// the model passes through would make every such index differ from its own
	// inspected document forever, and the rebuild that difference plans would
	// drop the parameter it was meant to protect. See #1242 and
	// docs/conformance.md for the ones a PostgreSQL reader deliberately omits.
	StorageParams map[string]string `json:"storage_params,omitempty"`

	// RequiresExtensions names the extensions this index cannot be built
	// without, as the catalog resolved them rather than as the DDL spells them.
	//
	// An index key names an operator class only when that class is not the
	// default for its type on its access method, so an index whose default
	// class comes from an extension refers to that extension with no token at
	// all. Measured on PostgreSQL 17.10: with btree_gin installed,
	// CREATE INDEX t_gin ON t USING gin (n int4_ops) over an integer column is
	// stored, and rendered back, as CREATE INDEX t_gin ON public.t USING gin
	// (n) -- and replaying that on a database without btree_gin fails with
	// `data type integer has no default operator class for access method
	// "gin"`. pg_index.indclass holds the class each key actually resolved to,
	// so the reader answers the question exactly (stokaro/ptah#1286).
	//
	// The index's access method is read the same way, from pg_class.relam. An
	// access method the catalog owns, `gin` or `gist`, belongs to no extension
	// and pins nothing; one an extension supplies, such as bloom's `bloom`,
	// resolves to that extension.
	//
	// The edge is recorded whether or not the DDL prints the class. It does print
	// a non-default one -- pg_trgm's gin_trgm_ops arrives as
	// CREATE INDEX w_trgm ON public.w USING gin (txt gin_trgm_ops) -- and that
	// index resolves to pg_trgm here as well. This field answers "which
	// extension owns what this index resolved to", not "which dependency the DDL
	// omitted".
	//
	// Empty means the reader found no such edge, which is the ordinary case:
	// core supplies the default operator class for every core type its access
	// methods index. Readers with no catalog to ask leave it unset.
	RequiresExtensions []string `json:"requires_extensions,omitempty"`

	// Type is the ClickHouse data-skipping-index type. One of
	// "minmax" / "set(N)" / "bloom_filter" / "bloom_filter(p)" /
	// "tokenbf_v1(...)" / "ngrambf_v1(...)" etc. Empty on non-ClickHouse
	// readers.
	Type string `json:"type,omitempty"`
	// Expression is the full ClickHouse skipping-index expression
	// (column reference, function call, tuple, etc.). The reader also writes
	// the expression into Columns[0] for back-compat with the existing diff
	// layer; Expression is the canonical field for richer diffing once
	// that's wired up. Empty on non-ClickHouse readers.
	Expression string `json:"expression,omitempty"`
	// Granularity is the GRANULARITY value the index was declared with.
	// Non-zero only on ClickHouse skipping indexes.
	Granularity int `json:"granularity,omitempty"`

	// PartitionAttached reports that this index is a partition's copy of an
	// index on its partitioned parent, attached to that parent index rather
	// than standing on its own.
	//
	// PostgreSQL creates one such index per partition whenever an index is
	// created on a partitioned parent, names it itself
	// (events_2026_tenant_idx), and refuses to drop it alone:
	// DROP INDEX "events_2026_tenant_idx" answers `cannot drop index
	// events_2026_tenant_idx because index idx_events_tenant requires it`
	// (SQLSTATE 2BP01). It is the index equivalent of a constraint's backing
	// index -- a real catalog row that no standalone statement addresses --
	// and a comparison that reads it as an ordinary index plans a DROP the
	// server refuses.
	//
	// The parent index is not marked: it is the object the DDL names, and on
	// PostgreSQL it is relkind 'I' while a partition's copy is relkind 'i'.
	// Only readers that can ask the catalog (pg_inherits over index
	// relations) set this; everything else leaves it false.
	PartitionAttached bool `json:"partition_attached,omitempty"`
}

// QualifiedTableName returns schema.table when Schema is set, or TableName otherwise.
func (i Index) QualifiedTableName() string {
	return QualifyTableName(i.Schema, i.TableName)
}

// Constraint represents one database constraint -- a PRIMARY KEY, FOREIGN
// KEY, UNIQUE, CHECK, or EXCLUDE row, as Type spells it.
//
// The column lists exist in two spellings. ColumnName and ForeignColumn are
// the legacy single-column fields; ColumnNames and ForeignColumns are the
// multi-column slices, and a reader may have populated either. Consumers must
// read through [Constraint.ColumnNamesOrDefault] and
// [Constraint.ForeignColumnsOrDefault], which merge the two; a fake reader
// should fill the slices. Code reading one spelling directly works against
// every fixture that happens to use that spelling and silently misses the
// other.
type Constraint struct {
	Name           string   `json:"name"`
	TableName      string   `json:"table_name"`
	Schema         string   `json:"schema,omitempty"`
	Type           string   `json:"type"` // PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK, EXCLUDE
	ColumnName     string   `json:"column_name"`
	ColumnNames    []string `json:"column_names,omitempty"`
	ForeignTable   *string  `json:"foreign_table"` // For foreign keys
	ForeignSchema  string   `json:"foreign_schema,omitempty"`
	ForeignColumn  *string  `json:"foreign_column"` // For foreign keys
	ForeignColumns []string `json:"foreign_columns,omitempty"`
	DeleteRule     *string  `json:"delete_rule"` // CASCADE, RESTRICT, etc.
	UpdateRule     *string  `json:"update_rule"` // CASCADE, RESTRICT, etc.
	// Deferrable reports a foreign key whose check may be postponed to the end
	// of a transaction, read from pg_constraint.condeferrable.
	Deferrable bool `json:"deferrable,omitempty"`
	// Initially is the default timing of a deferrable check -- "deferred" or
	// "immediate" -- read from pg_constraint.condeferred. It is empty for a
	// constraint that is not deferrable, so a schema that never asked for one
	// reads back exactly as it did before (stokaro/ptah#1624).
	Initially   string  `json:"initially,omitempty"`
	CheckClause *string `json:"check_clause"` // For CHECK constraints
	// NullsDistinct carries PostgreSQL UNIQUE NULLS [NOT] DISTINCT state.
	// Nil means the clause was not present in the definition.
	NullsDistinct *bool `json:"nulls_distinct,omitempty"`
	// IncludeColumns carries PostgreSQL INCLUDE columns for covering UNIQUE
	// and PRIMARY KEY constraints.
	IncludeColumns []string `json:"include_columns,omitempty"`
	// EXCLUDE constraint specific fields (PostgreSQL only)
	UsingMethod     *string `json:"using_method"`     // Index method: gist, btree, etc.
	ExcludeElements *string `json:"exclude_elements"` // Elements with operators: "room_id WITH =, during WITH &&"
	WhereCondition  *string `json:"where_condition"`  // Optional WHERE clause for EXCLUDE constraints

	// RequiresExtensions names the extensions the index backing this constraint
	// cannot be built without, read from that index exactly as
	// [Index.RequiresExtensions] is.
	//
	// It is a separate field rather than a lookup because the backing index is
	// not part of the entity model: a constraint owns its index, and the
	// converter drops the index row so the constraint is rendered once. The
	// dependency is as hard to read off the text here as on a plain index, and
	// for the same reason. Measured on PostgreSQL 17.10,
	// `EXCLUDE USING gist (room WITH =, during WITH &&)` over an integer column
	// needs btree_gist and pg_get_constraintdef prints no token of it, while
	// `EXCLUDE USING gist (txt gist_trgm_ops WITH =)` needs pg_trgm and prints
	// the class, because a class is printed exactly when it is not the default
	// (stokaro/ptah#1286).
	RequiresExtensions []string `json:"requires_extensions,omitempty"`
}

// QualifiedTableName returns schema.table when Schema is set, or TableName otherwise.
func (c Constraint) QualifiedTableName() string {
	return QualifyTableName(c.Schema, c.TableName)
}

// QualifiedForeignTableName returns schema.table for a foreign key target.
func (c Constraint) QualifiedForeignTableName() string {
	if c.ForeignTable == nil {
		return ""
	}
	return QualifyTableName(c.ForeignSchema, *c.ForeignTable)
}

// ColumnNamesOrDefault returns all local constraint columns, falling back to
// the legacy single-column field for callers that have not populated slices.
func (c Constraint) ColumnNamesOrDefault() []string {
	if len(c.ColumnNames) > 0 {
		return c.ColumnNames
	}
	if c.ColumnName != "" {
		return []string{c.ColumnName}
	}
	return nil
}

// ForeignColumnsOrDefault returns all referenced FK columns, falling back to
// the legacy single-column field for older readers and test fixtures.
func (c Constraint) ForeignColumnsOrDefault() []string {
	if len(c.ForeignColumns) > 0 {
		return c.ForeignColumns
	}
	if c.ForeignColumn != nil && *c.ForeignColumn != "" {
		return []string{*c.ForeignColumn}
	}
	return nil
}

// Extension represents a PostgreSQL extension installed in the database
type Extension struct {
	Name             string  `json:"name"`              // Extension name (pg_trgm, postgis, etc.)
	Version          string  `json:"version"`           // Installed version
	Schema           string  `json:"schema"`            // Schema where extension is installed
	Relocatable      bool    `json:"relocatable"`       // Whether extension can be moved between schemas
	Comment          *string `json:"comment"`           // Extension comment/description
	DefaultVersion   *string `json:"default_version"`   // Default version available
	InstalledVersion *string `json:"installed_version"` // Currently installed version (may differ from default)

	// Provides lists the catalog names this extension supplies -- its types,
	// functions, relations, operator classes and operator families -- read from
	// pg_depend. It answers "what would stop resolving if this extension were
	// not here", which is a different question from the extension's own name and
	// usually a disjoint set of words: `isn` supplies the type `isbn`, and
	// `pgcrypto` supplies the function `gen_salt`.
	//
	// Names pg_catalog also supplies are excluded, because a document using one
	// resolves it with the extension dropped. This matters because contrib
	// extensions mostly supply overloads of core functions: unfiltered, `citext`
	// contributes `max`, `min`, `strpos`, `replace` and `split_part`, and
	// `pgcrypto` contributes `gen_random_uuid`, which core has supplied since
	// PostgreSQL 13.
	//
	// A function name pg_get_keywords() reports as a SQL keyword is excluded too,
	// because a name read out of SQL text carries no position: `hstore` supplies
	// three functions named `delete`, and `DELETE FROM audit` in a plpgsql body
	// is indistinguishable from a call to `delete(h, 'k')`. That exclusion is
	// conditional on the redundancy that makes it free -- the same extension
	// must also contribute to this list a type appearing in that function's
	// signature, so a genuine call spells the type and the type entry answers
	// for it. An extension supplying `merge(text, text)` and no type has its
	// only evidence in the name, and the name is kept.
	//
	// Empty means not measured rather than "supplies nothing" -- every extension
	// supplies something, and readers that do not consult a catalog (Go
	// annotations, YAML) leave this unset.
	Provides []string `json:"provides,omitempty"`
}

// Sequence represents a standalone PostgreSQL sequence read from the database.
//
// Only user-declared, standalone sequences appear here. Implicit sequences that
// back SERIAL / BIGSERIAL / identity columns (those OWNED BY a column via an
// internal/auto dependency) are deliberately excluded so that declaring a plain
// SERIAL column does not surface as a spurious standalone sequence.
type Sequence struct {
	Name      string `json:"name"`                // Sequence name
	Schema    string `json:"schema,omitempty"`    // Schema containing the sequence
	DataType  string `json:"data_type,omitempty"` // Underlying integer type (e.g. "bigint")
	Start     *int64 `json:"start,omitempty"`     // START WITH value
	Increment *int64 `json:"increment,omitempty"` // INCREMENT BY value
	MinValue  *int64 `json:"min_value,omitempty"` // MINVALUE bound
	MaxValue  *int64 `json:"max_value,omitempty"` // MAXVALUE bound
	Cache     *int64 `json:"cache,omitempty"`     // CACHE size
	Cycle     bool   `json:"cycle"`               // Whether the sequence uses CYCLE
	OwnedBy   string `json:"owned_by,omitempty"`  // Owning table.column, if any
	Comment   string `json:"comment,omitempty"`   // Sequence comment/description
}

// QualifiedName returns schema.sequence when Schema is set, or Name otherwise.
func (s Sequence) QualifiedName() string {
	return QualifyTableName(s.Schema, s.Name)
}

// ServerInfo is the identity of one connected server as
// dbschema.ConnectToDatabase resolved it: which product answered, which
// version, which schema unqualified names mean there, and the capability set
// and identifier semantics that follow from those answers.
type ServerInfo struct {
	// Dialect is a canonical dialect name from core/platform, the set
	// platform.NormalizeDialect resolves to. Where several products share one
	// wire protocol it names the product the connection actually reached
	// rather than the URL's scheme, so a caller must not assume the two agree:
	// a postgres:// connection can report cockroachdb, yugabytedb or spanner,
	// and a mysql:// connection to MariaDB reports mariadb.
	Dialect string `json:"dialect"`
	Version string `json:"version"`
	// Schema is the schema this connection resolves unqualified names
	// against, and the value a blank Schema field means throughout a
	// [Database] this connection read. It is the connected server's own notion
	// of a current schema, so its spelling is per-dialect and is not "public"
	// everywhere: it is a database name on the engines whose schemas are
	// databases, main on SQLite, the connected user on Oracle. Where the
	// dialect has a selectable schema, a URL naming one selects it here.
	Schema string `json:"schema"`

	// URL is the database connection URL the connection was opened from, with
	// whatever credentials it carried. Callers that reconnect to the same
	// target read it -- a dev-database URL, a second session for an online DDL
	// tool, a realm-scope decision that depends on the URL's path.
	//
	// It is excluded from JSON because every other field here is tagged, and a
	// struct whose tags invite marshalling must not carry one field that turns
	// a marshal into a credential disclosure: `json.Marshal(conn.Info())` is
	// the obvious thing to do with a ServerInfo, and it must not write the
	// database password into whatever the caller does with the bytes. Marshal
	// [ServerInfo.RedactedURL] instead -- it names the same target with the
	// secrets removed (stokaro/ptah#2246).
	URL string `json:"-"`

	// RedactedURL is URL with every secret it carries replaced, suitable for a
	// log line, an error message or a serialized report. It occupies the `url`
	// JSON name so a consumer that marshals ServerInfo still learns which target
	// the description came from.
	RedactedURL string `json:"url"`

	Capabilities        capability.Capabilities `json:"capabilities"`         // resolved from Dialect + Version for live connections
	IdentifierSemantics identifier.Semantics    `json:"identifier_semantics"` // catalog identifier metadata and static rules

	// CapabilityNote says what a non-version-specific resolution actually
	// planned, and is empty when the version selected a measured release line.
	//
	// It carries the same sentence the typed --server-version path already
	// prints, from the same producer, because a live connection asks the same
	// question: this server is newer than anything measured, or it fell in a
	// gap, or its dialect has no ladder to spend a version on. Reporting it
	// only for the typed path is how planning against an unmodeled server reads
	// as planning against a modeled one (stokaro/ptah#916).
	CapabilityNote string `json:"capability_note,omitempty"`
}

// SchemaReader reads a live database schema, in the two forms this package
// offers every other database call in: a context-free one and a Context one.
//
// ReadSchemaContext governs every catalog query the read issues with the
// provided context: canceling it, or letting its deadline pass, makes the read
// return promptly with an error rather than running the remaining queries.
//
// The context is part of the contract rather than a convenience. A schema read
// is dozens of round trips against a server that may be slow or unreachable,
// and it is what the migration generator's own documented context contract
// bottoms out in, so a caller with no way to say "stop" has no way to bound
// either one (stokaro/ptah#2246).
//
// ReadSchema is that same read under context.Background(), and is what a caller
// with no context to hand writes. It is the pairing database/sql itself uses
// and that dbschema.DatabaseConnection already follows -- Exec beside
// ExecContext, Query beside QueryContext, QueryRow beside QueryRowContext -- so
// a reader implements both and the choice is the caller's. Prefer
// ReadSchemaContext: only it can be stopped.
//
// Both are part of the interface rather than one being a helper beside it,
// because an implementation of SchemaReader can live outside this module.
// stokaro/ptah-testkit is one such consumer: a separate module that builds
// against the last published release, so the read it issues has to be spelled
// a way that release carries.
type SchemaReader interface {
	ReadSchema() (*Database, error)
	ReadSchemaContext(ctx context.Context) (*Database, error)
}

// SchemaExecutor executes SQL statements produced by schema operations.
//
// ExecuteSQL accepts a context and an optional slice of arguments that are
// bound as native driver parameters, mirroring database/sql's ExecContext.
// Use placeholders (`?` or the dialect-native form such as `$1`/`$2` for
// PostgreSQL) instead of interpolating values into the SQL string; this
// prevents the SQL injection class of bugs that the no-args signature used
// to invite (see issue #130). Identifiers (table/column names) cannot be
// parameterized — route them through a validated escape helper instead.
//
// IsDryRun reports whether the executor is in dry-run mode. In that mode no
// statement reaches the server: ExecuteSQL reports success without executing
// anything. The mode is selected on a writer with [SchemaWriter.SetDryRun];
// IsDryRun is what lets a layer that wraps or replaces an executor carry the
// mode onto the replacement rather than silently losing it.
type SchemaExecutor interface {
	ExecuteSQL(ctx context.Context, sql string, args ...any) error
	IsDryRun() bool
}

// SchemaWriter writes schemas to databases.
type SchemaWriter interface {
	SchemaExecutor
	// DropAllTables removes supported user objects from the writer's configured
	// schema or database. Implementations must refuse cleanup when dependencies
	// or database-global ownership prevent them from confining the destructive
	// effect to that scope. The context governs object discovery and DDL.
	// Implementations may use a short, bounded cleanup context after
	// cancellation to restore connection-local settings before returning.
	// Under dry-run mode it drops nothing and performs no destructive work.
	DropAllTables(ctx context.Context) error
	// BeginTransaction starts a database transaction and returns a
	// transaction-scoped executor for it. Under dry-run mode no server
	// transaction is begun: the returned SchemaTransaction is itself a
	// dry-run executor whose Commit and Rollback succeed without touching
	// the server.
	BeginTransaction(ctx context.Context) (SchemaTransaction, error)
	// SetDryRun turns dry-run mode on or off for this writer and for every
	// transaction it begins afterward. In dry-run mode every mutating call
	// reports success without sending anything to the server -- see the
	// contract on [SchemaExecutor]. Set the mode before beginning a
	// transaction: whether a transaction already in flight observes a later
	// change is the dialect's business, and must not be relied on either way.
	SetDryRun(dryRun bool)
}

// SchemaTransaction executes schema changes inside a database transaction.
//
// It deliberately owns transaction lifecycle instead of storing the active
// transaction on the parent SchemaWriter. That keeps dialect writers reentrant:
// concurrent callers may begin independent transactions without racing over
// shared writer state.
type SchemaTransaction interface {
	SchemaExecutor
	Commit() error
	Rollback() error
}

// Function represents a custom function read from the database.
type Function struct {
	Name string `json:"name"` // Function name
	// Kind separates a function from a procedure. Empty means function, which
	// is what every description written before procedures existed meant.
	Kind string `json:"kind,omitempty"`
	// Schema owns the function. Readers blank it for the connection's own
	// schema, the same convention tables, views and domains follow, so a
	// filter reconstructs the qualified spelling from the connection's
	// default. Without it a schema-qualified `--exclude app.fn_app` matched
	// nothing and silently kept the function (stokaro/ptah#933).
	Schema     string `json:"schema,omitempty"`
	Parameters string `json:"parameters"` // Full declaration parameters (e.g., "tenant_id_param TEXT DEFAULT 'public'")
	// IdentityArguments is PostgreSQL's canonical input-argument type list for
	// identifying an overload in ALTER or DROP FUNCTION. It deliberately stays
	// separate from Parameters: declaration parameters may contain names,
	// defaults, and OUT-only arguments that are not part of a function's drop
	// identity. Nil means a reader did not capture the identity; a non-nil empty
	// string is the valid identity of a zero-input or OUT-only function. This
	// reader-only execution fact is not part of serialized schema descriptions.
	IdentityArguments *string `json:"-"`
	// Definer is the MySQL-family account that owns the routine. It is a
	// reader-only execution fact: replacing a foreign SQL SECURITY DEFINER
	// routine without preserving this account silently changes the principal
	// under which its body runs.
	Definer string `json:"-"`
	// CurrentAccount is the MySQL-family CURRENT_USER() value for the connection
	// that read this routine. Together with Definer it lets database-aware
	// comparison distinguish a same-owner replacement from a principal change.
	CurrentAccount string `json:"-"`
	Returns        string `json:"returns"`    // Return type (e.g., "VOID", "TEXT")
	Language       string `json:"language"`   // Function language (e.g., "plpgsql", "sql")
	Security       string `json:"security"`   // Security context (e.g., "DEFINER", "INVOKER")
	Volatility     string `json:"volatility"` // Function volatility (e.g., "STABLE", "IMMUTABLE", "VOLATILE")
	// Settings are the routine's own configuration settings as the catalog
	// reports them, each `name=value`. PostgreSQL keeps them in
	// pg_proc.proconfig; a dialect without such a facility leaves this empty.
	Settings []string `json:"settings,omitempty"`
	Body     string   `json:"body"`    // Function body/implementation
	Comment  string   `json:"comment"` // Function comment/description
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
func (f Function) QualifiedName() string { return QualifyTableName(f.Schema, f.Name) }

// View represents a database view read from the database.
type View struct {
	Name        string `json:"name"`         // View name
	Schema      string `json:"schema"`       // Schema where the view is defined
	Body        string `json:"body"`         // SELECT query used as the view definition
	CheckOption string `json:"check_option"` // NONE, LOCAL, CASCADED, or dialect equivalent
	Comment     string `json:"comment"`      // View comment/description
	// Attributes carries the view's own WITH clause -- SQL Server's
	// SCHEMABINDING and VIEW_METADATA -- uppercased, in the order the server
	// wrote them.
	//
	// They belong to the view rather than to its body: SCHEMABINDING binds it
	// to the tables it names, so they cannot be altered under it, and an
	// indexed view requires it. The reader cuts the header off to get at the
	// body, and nothing kept this, so a replayed view came back unbound with
	// no diagnostic anywhere (stokaro/ptah#2125).
	Attributes []string `json:"attributes,omitempty"`
}

// QualifiedName returns schema.view when Schema is set, or Name otherwise.
func (v View) QualifiedName() string {
	return QualifyTableName(v.Schema, v.Name)
}

// Synonym represents a SQL Server synonym read from the database.
//
// A synonym is an alias, and the thing it aliases may not be in this database.
// SQL Server records the target as a name of one to four parts, and the shape
// of that name is the whole question a dependency ordering has to answer: a
// two-part target names an object here, and a three- or four-part target names
// one in another database or behind a linked server.
//
// The raw catalog value is kept alongside the parsed parts, because it is what
// the server will resolve and what has to be written back unchanged. The parts
// exist so that ordering can tell local from remote without re-parsing, and
// External is derived from them rather than stored, so the two cannot disagree.
type Synonym struct {
	Name   string `json:"name"`   // Synonym name (the alias)
	Schema string `json:"schema"` // Schema the alias lives in
	// Target is base_object_name exactly as the catalog records it, including
	// the server's own bracket quoting.
	Target string `json:"target"`
	// TargetServer, TargetDatabase, TargetSchema and TargetObject are the
	// parsed parts of Target. Absent leading parts are empty.
	TargetServer   string `json:"target_server,omitempty"`
	TargetDatabase string `json:"target_database,omitempty"`
	TargetSchema   string `json:"target_schema,omitempty"`
	TargetObject   string `json:"target_object"`
	Comment        string `json:"comment,omitempty"` // Synonym comment/description
}

// ContinuousAggregate is one TimescaleDB continuous aggregate.
//
// To PostgreSQL it is a view: pg_class reports relkind 'v', and a reader that
// asks only PostgreSQL describes it as one. That is wrong in both directions,
// and both were measured on TimescaleDB 2.29.2 / PostgreSQL 17.11.
//
// A plan that drops it emits DROP VIEW, and the server answers
// `cannot drop continuous aggregate using DROP VIEW`, hinting at DROP
// MATERIALIZED VIEW. So the plan cannot apply, and the next run reports the
// same pending change.
//
// A plan that creates it emits CREATE VIEW with the body pg_get_viewdef
// answers, which is not the body anybody wrote: TimescaleDB rewrites the
// definition to select from the materialization hypertable, so the emitted
// view names a relation in a schema the extension owns.
//
// Definition is therefore the catalog's own view_definition -- the SELECT as
// it was written -- and not pg_get_viewdef's.
type ContinuousAggregate struct {
	Schema string `json:"schema"` // Schema holding the aggregate
	Name   string `json:"name"`   // Aggregate name, which is also the view name

	// HypertableSchema and HypertableName name the hypertable the aggregate
	// materializes from, which is the object it depends on.
	HypertableSchema string `json:"hypertable_schema"`
	HypertableName   string `json:"hypertable_name"`

	// MaterializedOnly reports whether the aggregate reads only materialized
	// data, rather than combining it with the raw rows since the last refresh.
	MaterializedOnly bool `json:"materialized_only"`

	// Definition is the SELECT the aggregate was declared with.
	Definition string `json:"definition"`
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
func (a ContinuousAggregate) QualifiedName() string {
	return QualifyTableName(a.Schema, a.Name)
}

// Hypertable is a TimescaleDB hypertable, as the extension's own catalog
// describes it.
//
// It carries the primary dimension and nothing else about the partitioning,
// which is a scope decision rather than an oversight: this description is read
// to be NAMED, not to be replayed, and the column a hypertable is partitioned
// on is what makes the note concrete enough to act on. Representing the full
// dimension set is what a declaration syntax would need, and that is the slice
// after this one.
type Hypertable struct {
	Schema string `json:"schema"` // Schema holding the hypertable
	Name   string `json:"name"`   // Table name, which is an ordinary table name

	// PrimaryDimension is the column the hypertable partitions on first, and
	// PrimaryDimensionType is that column's type as the catalog spells it.
	PrimaryDimension     string `json:"primary_dimension"`
	PrimaryDimensionType string `json:"primary_dimension_type"`

	// ChunkInterval is the width of one chunk on the primary dimension, in the
	// server's own spelling -- `7 days`, `1 day`. It is empty for a dimension
	// the catalog reports no time interval for, which is every hash dimension
	// and an integer range one.
	//
	// The server's spelling is what a declaration has to carry: a value
	// converted to compare would differ from the catalog on every run.
	ChunkInterval string `json:"chunk_interval,omitempty"`

	// Dimensions counts the partitioning dimensions, so a note can say that a
	// table has more than the one it names.
	Dimensions int `json:"dimensions"`
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
func (h Hypertable) QualifiedName() string {
	return QualifyTableName(h.Schema, h.Name)
}

// ExtendedProperty is one SQL Server extended property read from
// sys.extended_properties.
//
// SQL Server hangs a property off a three-level address, and this type carries
// it. No level at all is a DATABASE-scoped property (class 0); Schema alone is
// a schema-scoped one (class 3); Schema and Table together are object-scoped,
// and adding Column addresses a column of it (class 1, minor_id 0 or the
// column's id).
//
// A database-scoped property is in no schema, so a read narrowed to one still
// carries it -- the rule an extension already follows, where placement is not
// ownership. Dropping it from a narrowed description would plan
// sp_dropextendedproperty for a property the declaration still names.
//
// MS_Description is not read here, because Ptah already models it: the
// reader turns it into the object's Comment, and reporting it twice would let
// the comment comparator and this one plan the same change from two places.
// The declaration side refuses it by name for the same reason, and says to use
// the comment instead.
type ExtendedProperty struct {
	Name   string `json:"name"`             // Property name, as sys.extended_properties records it
	Schema string `json:"schema"`           // Schema owning the addressed object, or the addressed schema
	Table  string `json:"table,omitempty"`  // Table the property is on; empty for a schema-scoped property
	Column string `json:"column,omitempty"` // Column the property is on; requires Table
	Value  string `json:"value"`            // The value, when ValueType names one this description can carry

	// ValueType is the sql_variant base type SQL_VARIANT_PROPERTY reports:
	// nvarchar, int, date, and so on.
	ValueType string `json:"value_type"`

	// ValueNotRepresentable marks a property whose value is stored under a base
	// type Ptah cannot write back.
	//
	// sp_addextendedproperty takes a sql_variant, so a property may hold an int
	// or a date as well as a string -- measured on SQL Server 2022, @value=42
	// stores base type `int` and a DATE stores `date`. The renderer writes an
	// N'' literal, so re-emitting either of those would change its type, and
	// CONVERT(NVARCHAR, value) on the date answers `Jan  2 2026`, which is a
	// locale-dependent rendering rather than the value.
	//
	// The row is reported rather than dropped so that a description says the
	// property is there, and the comparator declines it in both directions:
	// nothing is planned to add it, and nothing is planned to take it away.
	// Ptah leaves it exactly as it found it.
	ValueNotRepresentable bool `json:"value_not_representable,omitempty"`
}

// QualifiedOwner names the object the property is attached to, as
// schema.table.column, omitting the levels that are absent.
//
// A property with no levels is the database's own, and says so rather than
// rendering as an empty string: it appears in diagnostics beside properties
// that do name an object, and "" beside "app.docs" reads as a bug.
func (p ExtendedProperty) QualifiedOwner() string {
	if p.Schema == "" {
		return "(database)"
	}
	parts := []string{p.Schema}
	if p.Table != "" {
		parts = append(parts, p.Table)
	}
	if p.Column != "" {
		parts = append(parts, p.Column)
	}
	return strings.Join(parts, ".")
}

// QualifiedName returns schema.synonym when Schema is set, or Name otherwise.
func (s Synonym) QualifiedName() string {
	return QualifyTableName(s.Schema, s.Name)
}

// DeclaredTarget is the synonym's target in the spelling a declaration uses:
// one to four dot-separated parts, unquoted.
//
// It is rebuilt from the parsed parts rather than read off Target, which is
// base_object_name exactly as the catalog records it, brackets included.
// Copying that form into a document writes `[other].[dbo].[gauge]`, which
// renders again as a name with brackets inside it (stokaro/ptah#2001).
//
// Absent leading parts are empty, so joining what is present gives the
// one-to-four part name without inventing a level. A row Ptah could not parse
// still names something, and the raw form is better than nothing: it is what
// the server holds.
//
// The rule lives here because both the conversion that writes a document and
// the comparison that carries a removal need the same answer, and a second
// copy of it answers differently the first time either one learns something
// (stokaro/ptah#2315).
func (s Synonym) DeclaredTarget() string {
	parts := make([]string, 0, 4)
	for _, part := range []string{s.TargetServer, s.TargetDatabase, s.TargetSchema, s.TargetObject} {
		if strings.TrimSpace(part) == "" {
			continue
		}
		parts = append(parts, part)
	}
	if len(parts) == 0 {
		return s.Target
	}
	return strings.Join(parts, ".")
}

// IsExternal reports whether the target lives outside this database.
//
// Ptah manages the alias in both cases and never the target: a synonym is a
// pointer, and creating one does not require the object it points at to exist.
// What External changes is dependency ordering -- a local target is an object
// the same plan may create or drop, and a remote one is not something this
// plan can order against at all.
func (s Synonym) IsExternal() bool {
	return s.TargetServer != "" || s.TargetDatabase != ""
}

// TargetQualifiedName returns the local schema.object the target names, or the
// empty string when the target is external.
//
// It is the join key a dependency ordering uses, and returning nothing for an
// external target is deliberate: an ordering that matched on the object name
// alone would make a synonym for another database's `orders` table depend on
// the local table of the same name, which is a dependency that does not exist.
func (s Synonym) TargetQualifiedName() string {
	if s.IsExternal() || s.TargetObject == "" {
		return ""
	}
	return QualifyTableName(s.TargetSchema, s.TargetObject)
}

// MaterializedView represents a PostgreSQL materialized view read from the database.
type MaterializedView struct {
	Name    string `json:"name"`    // Materialized view name
	Schema  string `json:"schema"`  // Schema where the materialized view is defined
	Body    string `json:"body"`    // SELECT query used as the materialized view definition
	Comment string `json:"comment"` // Materialized view comment/description

	// Refresh is the ClickHouse refresh schedule read back from the server,
	// nil for a view that has none.
	//
	// It is read from create_table_query, which is the only place the schedule
	// survives: system.tables.as_select is byte-identical for a plain view and
	// a refreshable one (stokaro/ptah#1802).
	// Tagged like every other field here, and like the two ast specs Table
	// embeds: this is a serialized document, and an untagged field puts a Go
	// identifier into it. It rendered `"Refresh":{"Mode":...}` among lowercase
	// keys, which no reader following the document's own convention could
	// reach (stokaro/ptah#2760).
	Refresh *ast.MatViewRefreshSpec `json:"refresh,omitzero"`
}

// QualifiedName returns schema.materialized_view when Schema is set, or Name otherwise.
func (v MaterializedView) QualifiedName() string {
	return QualifyTableName(v.Schema, v.Name)
}

// Trigger represents a database trigger read from the database.
type Trigger struct {
	Name    string `json:"name"`    // Trigger name
	Schema  string `json:"schema"`  // Schema where the trigger is defined
	Table   string `json:"table"`   // Target table
	Timing  string `json:"timing"`  // BEFORE, AFTER, or INSTEAD OF
	Event   string `json:"event"`   // INSERT, UPDATE, DELETE, or TRUNCATE
	ForEach string `json:"for"`     // ROW or STATEMENT
	Body    string `json:"body"`    // Trigger body
	Comment string `json:"comment"` // Trigger comment/description

	// ExecuteFunction is the name of the function the trigger runs, as the
	// catalog reports it.
	//
	// PostgreSQL has no inline trigger body: `CREATE TRIGGER ... EXECUTE
	// FUNCTION f()` always names a function, and Ptah writes one per trigger
	// when the declaration carries a body. Reading only that function's source
	// and discarding its NAME made every trigger look like one Ptah owns, so a
	// function shared by several triggers was described once per trigger and
	// replayed as that many copies (stokaro/ptah#2210).
	ExecuteFunction string `json:"execute_function,omitempty"`
}

// QualifiedTable returns schema.table when Schema is set, or Table otherwise.
func (t Trigger) QualifiedTable() string {
	return QualifyTableName(t.Schema, t.Table)
}

// RLSPolicy represents a PostgreSQL RLS policy read from the database
type RLSPolicy struct {
	Name                string `json:"name"`                  // Policy name
	Table               string `json:"table"`                 // Target table name
	PolicyFor           string `json:"policy_for"`            // Operations policy applies to (e.g., "ALL", "SELECT")
	ToRoles             string `json:"to_roles"`              // Target roles (e.g., "app_user", "PUBLIC")
	UsingExpression     string `json:"using_expression"`      // USING clause expression
	WithCheckExpression string `json:"with_check_expression"` // WITH CHECK clause expression
	Comment             string `json:"comment"`               // Policy comment/description
}

// Role represents a PostgreSQL role read from the database
type Role struct {
	Name          string            `json:"name"`           // Role name
	Login         bool              `json:"login"`          // Whether role can login
	Superuser     bool              `json:"superuser"`      // Whether role is superuser
	CreateDB      bool              `json:"create_db"`      // Whether role can create databases
	CreateRole    bool              `json:"create_role"`    // Whether role can create other roles
	Inherit       bool              `json:"inherit"`        // Whether role inherits privileges
	Replication   bool              `json:"replication"`    // Whether role can initiate replication
	PasswordState RolePasswordState `json:"password_state"` // What the reader established about password presence
	Comment       string            `json:"comment"`        // Role comment/description
}

// ObjectOwner is the owner of one schema object.
//
// Kind is the object kind in Ptah's own vocabulary -- table, view,
// materialized view, sequence, schema -- rather than the catalog's letter, so a
// consumer need not know pg_class.relkind to read it.
type ObjectOwner struct {
	// Kind is the object kind.
	Kind string `json:"kind"`
	// Schema is the schema the object lives in, empty for a schema itself.
	Schema string `json:"schema,omitempty"`
	// Name is the object's name.
	Name string `json:"name"`
	// Owner is the role that owns it.
	Owner string `json:"owner"`
	// OwnerCanLogin reports whether that role can log in, read from the
	// catalog beside the owner rather than inferred from the described roles:
	// the owner of everything on a default PostgreSQL database is the
	// bootstrap superuser, which Ptah deliberately does not describe, and an
	// analysis that had to look it up in the role list would go quiet exactly
	// where the question matters.
	OwnerCanLogin bool `json:"owner_can_login"`
}

// RoleMembership is one role-in-role edge: Member holds everything Role
// grants, subject to Role's inheritance setting.
//
// The two names are read from the same catalog the roles come from, and both
// are roles Ptah manages -- reserved system roles are excluded on both sides,
// for the same reason they are excluded from Roles.
type RoleMembership struct {
	// Role is the role whose privileges are granted.
	Role string `json:"role"`
	// Member is the role that receives them.
	Member string `json:"member"`
	// AdminOption reports whether Member may grant Role onward.
	AdminOption bool `json:"admin_option"`
}

// Grant represents a privilege grant read from the database.
type Grant struct {
	Role       string `json:"role"`                 // Role receiving the privilege
	Privilege  string `json:"privilege"`            // Granted privilege, e.g. SELECT or USAGE
	ObjectType string `json:"object_type"`          // TABLE, SCHEMA, or SEQUENCE
	Schema     string `json:"schema,omitempty"`     // Schema containing the target object
	ObjectName string `json:"object_name"`          // Target table or schema name
	WithOption bool   `json:"with_option"`          // Whether the grant has WITH GRANT OPTION
	GrantedBy  string `json:"granted_by,omitempty"` // Grantor role

	// IsPartialRevoke marks a row that SUBTRACTS from a broader grant rather
	// than adding one. Only ClickHouse produces it, and only the ClickHouse
	// reader sets it: `GRANT SELECT ON db.* TO r` followed by
	// `REVOKE SELECT ON db.t FROM r` leaves two rows in system.grants, the
	// second with is_partial_revoke = 1, and SHOW GRANTS prints both lines.
	//
	// Ptah does not manage that shape — a grant minus exceptions has no
	// declaration to compare against — so nothing plans one. The field exists
	// so the reader reports the row instead of dropping it, which is what lets
	// a live validation refuse a managed role whose effective privileges are
	// quietly narrower than its grant row alone would say.
	//
	// omitempty keeps every PostgreSQL serialization byte-identical.
	IsPartialRevoke bool `json:"is_partial_revoke,omitempty"`
}

// QualifiedTarget returns schema.object for table grants and the schema name
// itself for schema grants.
func (g Grant) QualifiedTarget() string {
	if strings.EqualFold(g.ObjectType, "SCHEMA") {
		return g.ObjectName
	}
	return QualifyTableName(g.Schema, g.ObjectName)
}
