package goschema

import (
	"fmt"
	"hash/fnv"
	"strings"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/internal/tableref"
)

// Database represents the complete database schema derived from Go struct annotations.
//
// This struct aggregates all database schema information discovered during the recursive
// parsing process. It includes all entity types, their relationships, and dependency
// information needed for proper migration generation.
//
// The result is processed to:
//   - Remove duplicates that may occur when entities are defined in multiple files
//   - Build dependency graphs based on foreign key relationships
//   - Sort tables in topological order to ensure proper creation sequence
//
// Fields:
//   - Schemas: All explicit database schema/namespace directives
//   - Tables: All table directives found in the project
//   - Fields: All field definitions with their database mappings
//   - Indexes: All index definitions for database optimization
//   - Enums: Global enum definitions that can be referenced by fields
//   - EmbeddedFields: Fields from embedded structs with their relation modes
//   - Dependencies: Dependency graph mapping table names to their dependencies
type Database struct {
	Schemas                    []Schema
	Tables                     []Table
	Fields                     []Field
	Indexes                    []Index
	Constraints                []Constraint // Table-level constraints (EXCLUDE, CHECK, etc.)
	Enums                      []Enum
	EmbeddedFields             []EmbeddedField
	Extensions                 []Extension                    // PostgreSQL extensions (pg_trgm, postgis, etc.)
	Functions                  []Function                     // PostgreSQL custom functions
	Sequences                  []Sequence                     // PostgreSQL standalone sequences (CREATE SEQUENCE)
	Domains                    []Domain                       // PostgreSQL domain types (CREATE DOMAIN)
	CompositeTypes             []CompositeType                // PostgreSQL composite types (CREATE TYPE ... AS (...))
	Ranges                     []Range                        // PostgreSQL range types (CREATE TYPE ... AS RANGE (...))
	Views                      []View                         // Database views
	MaterializedViews          []MaterializedView             // Database materialized views
	Triggers                   []Trigger                      // Database triggers
	RLSPolicies                []RLSPolicy                    // PostgreSQL Row-Level Security policies
	RLSEnabledTables           []RLSEnabledTable              // Tables with RLS enabled
	Roles                      []Role                         // PostgreSQL roles
	Grants                     []Grant                        // PostgreSQL privilege grants
	ManagedData                []ManagedData                  // Declarative reference/seed row data for tables
	Dependencies               map[string][]string            // table -> list of tables it depends on
	FunctionDependencies       map[string][]string            // function -> list of functions it depends on
	SelfReferencingForeignKeys map[string][]SelfReferencingFK // table -> list of self-referencing foreign keys

	// EmbeddedSources retains source-only helper declarations needed to
	// materialize embedded columns again after a schema is merged or finalized.
	EmbeddedSources EmbeddedSources

	// NotDescribed records what this description does not claim to describe, so
	// a comparator can tell an object the description says is gone from one it
	// was never asked about. The zero value claims everything, which is what a
	// hand-authored schema file is; see [go.5x5.cz/ptah/core/coverage].
	//
	// `omitzero` is load-bearing rather than cosmetic. This struct's JSON
	// encoding IS the desired-state fingerprint (see
	// [go.5x5.cz/ptah/internal/atlasschema.SchemaFingerprint] and the plan
	// file's `to` attribute), so a field that serialized unconditionally would
	// change the fingerprint of every schema anyone has already planned against
	// -- the one thing "adding coverage changes no existing plan" promises it
	// does not do. A description that declares no limits must encode exactly as
	// it did before this field existed.
	NotDescribed coverage.Set `json:",omitzero"`
}

// Schema represents a database schema/namespace.
type Schema struct {
	Name    string // Schema name, e.g. "public"
	Comment string // Optional schema comment/description
	Charset string // Optional default character set (MySQL/MariaDB)
	Collate string // Optional default collation (MySQL/MariaDB)
}

// EmbeddedField represents an embedded field in a Go struct that should be handled specially
// during schema generation. Embedded fields allow for composition and reuse of common field
// patterns across multiple tables.
//
// The EmbeddedField supports four different modes of handling:
//   - "inline": Injects the embedded struct's fields directly as separate columns
//   - "json": Serializes the entire embedded struct into a single JSON/JSONB column
//   - "relation": Creates a foreign key relationship to another table
//   - "skip": Completely ignores the embedded field during schema generation
//
// Usage in Go structs:
//
//	type User struct {
//	    ID int64
//	    //ptah:embedded mode="inline"
//	    Timestamps  // Results in: created_at, updated_at columns
//
//	    //ptah:embedded mode="json" name="metadata" type="JSONB"
//	    Meta UserMeta  // Results in: metadata JSONB column
//
//	    //ptah:embedded mode="relation" field="company_id" ref="companies(id)"
//	    Company Company  // Results in: company_id INTEGER + FK constraint
//	}
type EmbeddedField struct {
	StructName       string                       // The struct that contains this embedded field
	Mode             string                       // inline, json, relation, skip
	Prefix           string                       // For inline mode - prefix for field names
	Name             string                       // For json mode - column name
	Type             string                       // For json/relation mode - generated column type
	Nullable         bool                         // Whether the field can be null
	Field            string                       // For relation mode - foreign key field name
	Ref              string                       // For relation mode - reference table(column)
	OnDelete         string                       // For relation mode - ON DELETE action
	OnUpdate         string                       // For relation mode - ON UPDATE action
	Comment          string                       // Comment for the field/column
	EmbeddedTypeName string                       // The name of the embedded type (e.g., "Timestamps")
	Overrides        map[string]map[string]string // Platform-specific overrides
}

// EmbeddedSources preserves helper declarations separately from materialized
// database columns so Finalize and Merge remain idempotent.
type EmbeddedSources struct {
	Fields      []Field
	Definitions []EmbeddedField
}

// Field represents a database column/field definition parsed from Go struct field annotations.
// This is the core building block for table schema generation, containing all the metadata
// needed to generate appropriate CREATE TABLE column definitions for different database platforms.
//
// Field is created by parsing //ptah:schema:field annotations from Go struct fields:
//
//	type Product struct {
//	    //ptah:schema:field name="id" type="SERIAL" primary="true"
//	    ID int64
//
//	    //ptah:schema:field name="name" type="VARCHAR(255)" not_null="true" unique="true"
//	    Name string
//
//	    //ptah:schema:field name="price" type="DECIMAL(10,2)" check="price > 0" default="0.00"
//	    Price float64
//
//	    //ptah:schema:field name="status" type="ENUM" enum="active,inactive" default="active"
//	    Status string
//
//	    //ptah:schema:field name="category_id" type="INTEGER" foreign="categories(id)"
//	    CategoryID int64
//	}
//
// The Field supports platform-specific overrides through the Overrides field:
//
//	//ptah:schema:field name="id" type="SERIAL" platform.mysql.type="INT AUTO_INCREMENT"
//	ID int64
type Field struct {
	StructName string // Name of the Go struct this field belongs to
	FieldName  string // Name of the Go struct field
	Name       string // Database column name
	// APIName is the name this column carries in an exported API schema, when
	// that is meant to differ from the column name. Empty leaves the exporters
	// deriving it from Name as they always have.
	//
	// The two identities are separate on purpose: a storage rename should not
	// rename a published field, and a storage-shaped name should not have to
	// become the public one — `billing_amount_minor` can be exported as
	// `amount` and stay itself in the database (stokaro/ptah#905).
	APIName string
	Type    string // Database column type (e.g., "VARCHAR(255)", "INTEGER")
	// TypeRawSQL records that Type was written with Atlas HCL's sql() raw
	// expression -- `type = sql("USER_DEFINED")` -- rather than as a type the
	// grammar names. Type still holds the reduced SQL text so rendered DDL is
	// valid, but a writer that emits Atlas HCL must put the call back: the
	// pinned Atlas community binary v1.3.0 refuses the bare identifier
	// (`There is no type named "USER_DEFINED"`) and accepts only the call.
	TypeRawSQL bool
	Nullable   bool // Whether the column allows NULL values
	Primary    bool // Whether this is a primary key column
	AutoInc    bool // Whether this column auto-increments
	// IdentityGeneration stores PostgreSQL identity generation mode: ALWAYS or BY_DEFAULT.
	IdentityGeneration string
	// IdentityStart stores the optional PostgreSQL identity START WITH value.
	IdentityStart string
	// IdentityIncrement stores the optional PostgreSQL identity INCREMENT BY value.
	IdentityIncrement string
	// IdentityOptions stores raw PostgreSQL identity sequence options for SQL round-trips.
	IdentityOptions string
	Unique          bool     // Whether this column has a unique constraint
	UniqueExpr      string   // Custom unique constraint expression
	Default         string   // Default value for the column
	DefaultSet      bool     // Whether Default is set, including an empty string literal
	DefaultExpr     string   // Default expression (e.g., "NOW()", "UUID()", "CURRENT_TIMESTAMP", "1", "true")
	Foreign         string   // Foreign key reference (e.g., "users(id)")
	ForeignKeyName  string   // Custom foreign key constraint name
	OnDelete        string   // Foreign key ON DELETE action (CASCADE, SET NULL, RESTRICT, NO ACTION)
	OnUpdate        string   // Foreign key ON UPDATE action (CASCADE, SET NULL, RESTRICT, NO ACTION)
	Enum            []string // Enum values for ENUM type fields
	Check           string   // Check constraint expression
	CheckName       string   // Optional constraint name for the column-level CHECK; defaults to "<table>_<column>_check"
	// GeneratedExpression stores the raw SQL expression for generated columns.
	GeneratedExpression string
	// GeneratedKind stores the generated column kind, such as VIRTUAL or STORED.
	GeneratedKind string
	// UpdateExpression stores MySQL/MariaDB ON UPDATE expressions such as CURRENT_TIMESTAMP(6).
	UpdateExpression string
	// Charset stores the column character set for MySQL-compatible dialects.
	Charset string
	// Collate stores the column collation for MySQL-compatible dialects.
	Collate   string
	Comment   string                       // Column comment
	Overrides map[string]map[string]string // Platform-specific overrides (e.g., platform.mysql.type)

	// GeneratedFromEmbedded distinguishes materialized embedded columns from
	// source declarations so Finalize can rebuild them after schema mutation.
	GeneratedFromEmbedded bool
}

// IndexPart represents one column or expression inside an index definition.
type IndexPart struct {
	Name     string // Column name
	Expr     string // Raw index expression
	Operator string // PostgreSQL operator class for this part
	Prefix   string // MySQL index prefix length
	Desc     bool   // Whether this part is ordered DESC
	// NullsOrder is an explicit NULLS ordering for this part: "FIRST",
	// "LAST", or empty for the direction's default (NULLS LAST for ASC,
	// NULLS FIRST for DESC on PostgreSQL).
	NullsOrder string
}

// Index NULLS ordering spellings for IndexPart.NullsOrder, matching
// [go.5x5.cz/ptah/core/ast.NullsOrderFirst] and
// [go.5x5.cz/ptah/dbschema/types.NullsOrderFirst] so the value survives every
// hop between the three index-part shapes unchanged.
const (
	NullsOrderFirst = "FIRST"
	NullsOrderLast  = "LAST"
)

// Index represents a database index definition parsed from Go struct annotations.
// Indexes are used to improve query performance and enforce uniqueness constraints
// on one or more columns.
//
// Index is created by parsing //ptah:schema:index annotations:
//
//	type User struct {
//	    //ptah:schema:field name="id" type="SERIAL" primary="true"
//	    ID int64
//
//	    //ptah:schema:field name="email" type="VARCHAR(255)" not_null="true"
//	    Email string
//
//	    //ptah:schema:field name="status" type="VARCHAR(50)"
//	    Status string
//
//	    // Single column index
//	    //ptah:schema:index name="idx_users_email" fields="email" unique="true"
//	    _ int
//
//	    // Multi-column index
//	    //ptah:schema:index name="idx_users_email_status" fields="email,status"
//	    _ int
//
//	    // PostgreSQL GIN index for JSONB fields
//	    //ptah:schema:index name="idx_users_tags" fields="tags" type="GIN"
//	    _ int
//
//	    // Partial index with WHERE condition
//	    //ptah:schema:index name="idx_active_users" fields="status" condition="deleted_at IS NULL"
//	    _ int
//
//	    // Trigram similarity index
//	    //ptah:schema:index name="idx_users_name_trgm" fields="name" type="GIN" ops="gin_trgm_ops"
//	    _ int
//
//	    // Cross-table index targeting specific table
//	    //ptah:schema:index name="idx_products_name" fields="name" table="products"
//	    _ int
//	}
//
// # ClickHouse data-skipping indexes
//
// On ClickHouse, the `type=` and `granularity=` keys configure a
// data-skipping index. `type=` accepts any spelling ClickHouse understands —
// `minmax`, `set(N)`, `bloom_filter`, `bloom_filter(p)`, `tokenbf_v1(...)`,
// `ngrambf_v1(...)`, etc. `granularity=` is the number of marks per index
// block; omitting it falls back to ClickHouse's documented default (8192).
// Both keys are silently ignored by non-ClickHouse renderers.
//
//	type Event struct {
//	    //ptah:schema:field name="payload" type="String"
//	    Payload string
//
//	    //ptah:schema:index name="idx_e_payload" fields="payload" type="bloom_filter(0.01)" granularity="64"
//	    _ int
//	}
type Index struct {
	StructName string   // Name of the Go struct this index belongs to
	Name       string   // Index name (e.g., "idx_users_email")
	Fields     []string // Column names included in the index
	// Parts carries structured index elements for dialect-specific metadata,
	// such as DESC ordering and expression indexes. Fields remains the legacy
	// column/expression list for compatibility.
	Parts   []IndexPart
	Unique  bool   // Whether this is a unique index
	Comment string // Index comment/description
	// NullsDistinct carries PostgreSQL UNIQUE INDEX NULLS [NOT] DISTINCT
	// state. Nil means the clause was not specified.
	NullsDistinct *bool

	// Type carries the dialect-specific index type. For PostgreSQL this is
	// GIN/GIST/BTREE/HASH; for ClickHouse data-skipping indexes it is
	// "minmax"/"set(N)"/"bloom_filter(p)"/"tokenbf_v1(...)"/etc.
	Type string
	// Parser carries a MySQL FULLTEXT parser name, for example ngram.
	Parser string
	// Condition is the WHERE clause for partial or filtered indexes.
	Condition string
	// Operator is the operator class (PostgreSQL only, e.g. "gin_trgm_ops").
	Operator string
	// IncludeColumns carries INCLUDE payload columns for PostgreSQL,
	// YugabyteDB, and Spanner PostgreSQL-dialect covering indexes.
	IncludeColumns []string
	// StorageParams carries PostgreSQL index storage parameters rendered as
	// WITH (key='value'), for example pages_per_range for BRIN indexes.
	StorageParams map[string]string
	// TableName is the cross-table association (overrides StructName-based
	// resolution when set).
	TableName string

	// Granularity is the ClickHouse data-skipping-index GRANULARITY value.
	// Zero means "use the dialect default" (8192 for ClickHouse, which is
	// what the renderer falls back to when this field is unset). Ignored by
	// all non-ClickHouse renderers.
	Granularity int

	// RequiresExtensions names the extensions this index cannot be built
	// without, as the catalog resolved them rather than as the index's own DDL
	// spells them. It is filled in when the schema was read from a live
	// PostgreSQL catalog.
	//
	// An operator class is printed only when it is not the default for its key's
	// type on the index's access method, so an index resting on a default class
	// an extension supplies spells no token of that extension: `USING gin (n)`
	// over an integer column needs btree_gin, and `isbn` at least says a word
	// [Extension.Provides] can match. The catalog is asked instead of the text
	// (stokaro/ptah#1286).
	//
	// A printed class such as pg_trgm's `gin_trgm_ops` is recorded here too. The
	// field is the catalog's answer, not "the part the DDL left out", so a reader
	// of it must not conclude that the document names nothing behind the edge.
	//
	// Empty means not measured or nothing to report. Annotation and YAML sources
	// have no catalog to ask and leave it unset.
	RequiresExtensions []string
}

// Constraint represents a table-level constraint definition parsed from Go struct annotations.
// Constraints are used to enforce data integrity rules at the table level, such as EXCLUDE
// constraints for preventing overlapping data, CHECK constraints for data validation, etc.
//
// Constraint is created by parsing //ptah:schema:constraint annotations:
//
//	type Booking struct {
//	    //ptah:schema:constraint name="no_overlapping_bookings" type="EXCLUDE" using="gist" elements="room_id WITH =, during WITH &&"
//	    RoomID int64
//	    During string // TSRANGE type
//
//	    //ptah:schema:constraint name="one_active_session_per_user" type="EXCLUDE" using="gist" elements="user_id WITH =" condition="is_active = true"
//	    UserID   int64
//	    IsActive bool
//	}
//
// The Constraint supports different constraint types:
//   - EXCLUDE: PostgreSQL EXCLUDE constraints for preventing conflicts
//   - CHECK: Table-level CHECK constraints for data validation
//   - UNIQUE: Table-level UNIQUE constraints spanning multiple columns
//   - PRIMARY KEY: Composite primary key constraints
//   - FOREIGN KEY: Table-level foreign key constraints
type Constraint struct {
	StructName string // Name of the Go struct this constraint belongs to
	Name       string // Constraint name (e.g., "no_overlapping_bookings")
	Type       string // Constraint type: EXCLUDE, CHECK, UNIQUE, PRIMARY KEY, FOREIGN KEY
	Table      string // Table name (if different from struct name)

	// EXCLUDE constraint specific fields
	UsingMethod     string // Index method for EXCLUDE constraints (e.g., "gist", "btree")
	ExcludeElements string // Elements specification (e.g., "room_id WITH =, during WITH &&")
	WhereCondition  string // Optional WHERE clause for EXCLUDE constraints

	// CHECK constraint specific fields
	CheckExpression string // Check expression for CHECK constraints

	// UNIQUE/PRIMARY KEY constraint specific fields
	Columns []string // Column names for UNIQUE/PRIMARY KEY constraints
	// IncludeColumns carries PostgreSQL INCLUDE columns for covering UNIQUE
	// constraints.
	IncludeColumns []string
	// NullsDistinct carries PostgreSQL UNIQUE NULLS [NOT] DISTINCT state.
	// Nil means the clause was not specified.
	NullsDistinct *bool

	// FOREIGN KEY constraint specific fields
	ForeignTable   string   // Referenced table name
	ForeignColumn  string   // Referenced column name for single-column foreign keys
	ForeignColumns []string // Referenced column names for composite foreign keys
	OnDelete       string   // ON DELETE action
	OnUpdate       string   // ON UPDATE action

	// RequiresExtensions names the extensions the index backing this constraint
	// cannot be built without, as the catalog resolved them rather than as
	// ExcludeElements spells them. It is filled in for EXCLUDE constraints read
	// from a live PostgreSQL catalog, where an element prints its operator and
	// prints its operator class only when that class is not the default:
	// `EXCLUDE USING gist (room WITH =, during WITH &&)` over an integer column
	// needs btree_gist and says so nowhere, while
	// `EXCLUDE USING gist (txt gist_trgm_ops WITH =)` needs pg_trgm and does name
	// the class (stokaro/ptah#1286). See [Index.RequiresExtensions].
	RequiresExtensions []string

	Comment string // Constraint comment/description
}

// ForeignColumnsOrDefault returns the referenced column list for FOREIGN KEY
// constraints, falling back to ForeignColumn for legacy single-column callers.
func (c Constraint) ForeignColumnsOrDefault() []string {
	if len(c.ForeignColumns) > 0 {
		return c.ForeignColumns
	}
	if c.ForeignColumn != "" {
		return []string{c.ForeignColumn}
	}
	return nil
}

// Extension represents a PostgreSQL extension definition parsed from Go struct annotations.
// Extensions enable additional functionality in PostgreSQL databases.
//
// Extension is created by parsing //ptah:schema:extension annotations:
//
//	// Enable trigram similarity search
//	//ptah:schema:extension name="pg_trgm" if_not_exists="true"
//	type DatabaseExtensions struct{}
//
//	// Enable PostGIS for geographic data
//	//ptah:schema:extension name="postgis" schema="extensions" version="3.0" if_not_exists="true"
//	type GeoExtensions struct{}
type Extension struct {
	Name        string // Extension name (pg_trgm, postgis, etc.)
	Schema      string // PostgreSQL installation schema (optional; empty selects the connection default)
	IfNotExists bool   // Whether to use IF NOT EXISTS clause
	Version     string // Specific version requirement (optional)
	Comment     string // Extension comment/description

	// Provides lists the catalog names this extension supplies -- types,
	// functions, relations, operator classes and operator families. It is filled
	// in when the schema was read from a live PostgreSQL catalog and answers
	// "what stops resolving without this extension", which is almost never the
	// extension's own name: `isn` supplies the type `isbn`.
	//
	// Names the extension supplies that pg_catalog also supplies are excluded,
	// because those keep resolving with the extension dropped. Contrib
	// extensions mostly supply overloads of core functions, so the unfiltered
	// list is full of ordinary words -- `citext` supplies `max`, `strpos` and
	// `replace` -- and treating those as evidence would tie the extension to
	// every schema that happens to use them.
	//
	// A function name that is a SQL keyword is excluded for the same reason, but
	// only when this list also carries a type of the same extension that appears
	// in that function's signature: `hstore` supplies three functions named
	// `delete`, and a caller reading identifiers out of SQL text cannot tell
	// `DELETE FROM audit` from `delete(h, 'k')`, while a genuine call needs an
	// `hstore` value and the type entry is what keeps the extension here. Where
	// no such type entry exists -- an extension supplying `merge(text, text)`
	// and no type at all -- the name is the only evidence there is and it stays.
	//
	// Empty means not measured. Annotation and YAML sources have no catalog to
	// ask and leave it unset.
	Provides []string

	// Dialects scopes this declaration to the named target dialects. See
	// [ScopeToDialect] for what an empty scope means and why the JSON tag is
	// load-bearing.
	Dialects []string `json:",omitempty"`
}

// Table represents a database table configuration parsed from Go struct annotations.
// This defines the overall table properties and metadata that will be used to generate
// CREATE TABLE statements.
//
// Table is created by parsing //ptah:schema:table annotations:
//
//	//ptah:schema:table name="users" comment="User accounts table"
//	type User struct {
//	    //ptah:schema:field name="id" type="SERIAL" primary="true"
//	    ID int64
//
//	    //ptah:schema:field name="email" type="VARCHAR(255)" not_null="true"
//	    Email string
//	}
//
// Platform-specific configurations can be specified using overrides:
//
//	//ptah:schema:table name="products" platform.mysql.engine="InnoDB" platform.mysql.comment="Product catalog"
//	type Product struct {
//	    // ... fields
//	}
//
// Composite primary keys can be defined using the primary_key attribute:
//
//	//ptah:schema:table name="user_roles" primary_key="user_id,role_id"
//	type UserRole struct {
//	    //ptah:schema:field name="user_id" type="INTEGER" foreign="users(id)"
//	    UserID int64
//
//	    //ptah:schema:field name="role_id" type="INTEGER" foreign="roles(id)"
//	    RoleID int64
//	}
type Table struct {
	StructName string // Name of the Go struct this table represents
	Name       string // Database table name
	// APIName is the name this table carries in an exported API schema, when
	// that differs from the table name. Empty leaves the exporters deriving it
	// from Name as they always have.
	//
	// It is the table-level half of the same separation the field carries: a
	// storage rename should not rename a published type, so an established
	// `Invoice` can keep its name while the table underneath becomes
	// `billing_invoices` (stokaro/ptah#905).
	APIName       string
	Schema        string // Optional database schema/namespace (PostgreSQL-style)
	Engine        string // Storage engine (MySQL/MariaDB specific, e.g., "InnoDB")
	AutoIncrement string // Initial AUTO_INCREMENT value (MySQL/MariaDB specific)
	Charset       string // Table default character set (MySQL/MariaDB specific)
	Collate       string // Table default collation (MySQL/MariaDB specific)
	Strict        bool   // SQLite STRICT table option
	WithoutRowID  bool   // SQLite WITHOUT ROWID table option
	// VirtualModule is the SQLite module that owns this table, from the USING
	// clause of its CREATE VIRTUAL TABLE statement. The SQLite reader sets it,
	// and so does the native SQL parser -- a `.sql` schema file may declare
	// one, which is what makes `ptah db read` output readable back. Go
	// annotations, HCL and YAML still have no syntax for it, so it is empty
	// on a schema parsed from those. A non-empty value makes the table render
	// as CREATE VIRTUAL TABLE rather than CREATE TABLE. See stokaro/ptah#1028.
	VirtualModule string
	// VirtualArguments is the text between the module's parentheses, verbatim.
	VirtualArguments string
	Comment          string   // Table comment/description
	PrimaryKey       []string // Composite primary key column names
	// PrimaryKeyParts carries dialect-specific metadata for composite primary
	// key elements, such as MySQL prefix lengths and DESC ordering.
	PrimaryKeyParts []PrimaryKeyPart
	// PrimaryKeyInclude carries PostgreSQL INCLUDE columns for table-level
	// primary keys.
	PrimaryKeyInclude []string
	Checks            []string                     // Table-level check constraints
	Partition         *PartitionSpec               // PostgreSQL table partitioning metadata
	CustomSQL         string                       // Custom SQL to append to CREATE TABLE
	Overrides         map[string]map[string]string // Platform-specific overrides
}

// PrimaryKeyPart represents one column reference inside a table primary key.
type PrimaryKeyPart struct {
	Name   string // Column name
	Prefix string // MySQL index prefix length
	Desc   bool   // Whether the column is ordered DESC
}

// PartitionSpec represents PostgreSQL table partitioning metadata.
type PartitionSpec struct {
	Type  string          // Partitioning method, such as RANGE, LIST, or HASH
	Parts []PartitionPart // Partition key columns or expressions
}

// PartitionPart represents one partition key column or expression.
type PartitionPart struct {
	Name string // Column name
	Expr string // Raw partition expression
}

// QualifiedName returns the schema-qualified database table name when a schema
// is configured, or the plain table name otherwise.
func (t Table) QualifiedName() string {
	return QualifyTableName(t.Schema, t.Name)
}

// QualifyTableName returns an unambiguous schema-qualified table reference.
// Components containing identifier delimiters are encoded with SQL-standard
// double quotes so a literal dot cannot be confused with schema qualification.
// Renderers remain responsible for converting that canonical reference to the
// target dialect's quote style.
func QualifyTableName(schema, table string) string {
	return tableref.Canonical(schema, table)
}

// Enum represents a global enumeration type definition that can be shared across
// multiple tables and fields. Global enums are automatically generated when ENUM type
// fields are defined in struct annotations.
//
// What makes an enum "global":
// Global enums are database-level type definitions (particularly in PostgreSQL) that can be
// referenced by multiple tables and columns. Unlike inline enum constraints, global enums:
//   - Are created once as a database type (CREATE TYPE ... AS ENUM in PostgreSQL)
//   - Can be reused across multiple tables and columns
//   - Provide better type safety and consistency
//   - Allow for easier maintenance when enum values need to be modified
//
// How global enums are created:
// When you define a field with type="ENUM" and enum values, Ptah automatically generates
// a global enum with a standardized name pattern: "enum_{struct_name}_{field_name}":
//
//	type User struct {
//	    //ptah:schema:field name="status" type="ENUM" enum="active,inactive,suspended" default="active"
//	    Status string  // Creates global enum: "enum_user_status"
//	}
//
//	type Post struct {
//	    //ptah:schema:field name="status" type="ENUM" enum="draft,published,archived" default="draft"
//	    Status string  // Creates global enum: "enum_post_status"
//	}
//
// Database platform differences:
//   - PostgreSQL: Creates actual ENUM types (CREATE TYPE enum_user_status AS ENUM ('active', 'inactive'))
//   - MySQL/MariaDB: Uses ENUM column type with values (status ENUM('active', 'inactive'))
//   - SQLite: Uses CHECK constraints with IN clauses (status TEXT CHECK (status IN ('active', 'inactive')))
//
// Example of generated SQL:
//
//	PostgreSQL:
//	  CREATE TYPE enum_user_status AS ENUM ('active', 'inactive', 'suspended');
//	  CREATE TABLE users (status enum_user_status DEFAULT 'active');
//
//	MySQL:
//	  CREATE TABLE users (status ENUM('active', 'inactive', 'suspended') DEFAULT 'active');
type Enum struct {
	Name string // The generated enum type name (e.g., "enum_user_status")
	// Schema owns the enum, empty for the connection's or document's default
	// schema. It is a field rather than a qualifier folded into Name because
	// Name is what a column's declared type is matched against, and a domain,
	// composite and range each keep the two apart for the same reason.
	//
	// An enum is a TYPE, so its identity is (schema, name) exactly as a
	// domain's is: public.mood and extra.mood are two types with different
	// value sets. Without this field an enum read out of a non-default schema
	// was described as belonging to the connected one, and applying that
	// description built the type in the wrong schema and typed the column that
	// uses it against the wrong type (stokaro/ptah#1276).
	Schema string
	Values []string // The allowed enum values (e.g., ["active", "inactive", "suspended"])
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
//
// Name is returned VERBATIM when Schema is empty, rather than through
// QualifyTableName, because Name is not always a bare identifier here. A SQL
// schema file loaded through internal/convert/toschema parks the qualifier in
// Name -- `public.e1` -- and an enum may legitimately be named with a literal
// dot, which QualifyTableName canonicalizes by quoting. Running either through
// it changes the identifier: `public.e1` becomes the single quoted name
// "public.e1", and the already-quoted "tenant.data" gains a second layer.
func (e Enum) QualifiedName() string {
	if strings.TrimSpace(e.Schema) == "" {
		return e.Name
	}
	return QualifyTableName(e.Schema, e.Name)
}

// Domain represents a PostgreSQL domain type parsed from Go annotations.
//
// A domain is a base type constrained with optional NOT NULL, DEFAULT, and CHECK
// clauses. Domains are defined using //ptah:schema:domain annotations:
//
//	//ptah:schema:domain name="email" type="TEXT" check="VALUE ~ '^[^@]+@[^@]+$'"
//	type EmailDomain struct{}
type Domain struct {
	StructName  string // Name of the Go struct this domain is associated with
	Name        string // Domain name (e.g., "email")
	Schema      string // Optional schema/namespace (PostgreSQL-style)
	BaseType    string // Underlying base data type (e.g., "TEXT", "VARCHAR(255)")
	NotNull     bool   // Whether the domain is NOT NULL
	Default     string // Optional literal DEFAULT value
	DefaultExpr string // Optional DEFAULT expression (function call)
	Check       string // Optional CHECK constraint expression (uses VALUE)
	Comment     string // Optional comment for documentation

	// Dialects scopes this declaration to the named target dialects. See
	// [ScopeToDialect].
	Dialects []string `json:",omitempty"`
}

// Canonicalize normalizes domain attributes for downstream consumers.
func (d *Domain) Canonicalize() {
	d.Name = strings.TrimSpace(d.Name)
	d.Schema = strings.TrimSpace(d.Schema)
	d.BaseType = strings.TrimSpace(d.BaseType)
	d.Check = strings.TrimSpace(d.Check)
	d.Comment = strings.TrimSpace(d.Comment)
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
func (d Domain) QualifiedName() string {
	return QualifyTableName(d.Schema, d.Name)
}

// CompositeTypeField is a single named field of a composite type.
type CompositeTypeField struct {
	Name string // Field name
	Type string // Field data type
}

// CompositeType represents a PostgreSQL composite type parsed from Go annotations.
//
// Composite types are defined using //ptah:schema:composite annotations with
// a comma-separated fields list of "name:type" pairs:
//
//	//ptah:schema:composite name="address" fields="street:TEXT,city:TEXT,zip:VARCHAR(10)"
//	type AddressType struct{}
type CompositeType struct {
	StructName string               // Name of the Go struct this type is associated with
	Name       string               // Composite type name (e.g., "address")
	Schema     string               // Optional schema/namespace (PostgreSQL-style)
	Fields     []CompositeTypeField // Ordered fields of the composite type
	Comment    string               // Optional comment for documentation

	// Dialects scopes this declaration to the named target dialects. See
	// [ScopeToDialect].
	Dialects []string `json:",omitempty"`
}

// Canonicalize normalizes composite-type attributes for downstream consumers.
func (c *CompositeType) Canonicalize() {
	c.Name = strings.TrimSpace(c.Name)
	c.Schema = strings.TrimSpace(c.Schema)
	c.Comment = strings.TrimSpace(c.Comment)
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
func (c CompositeType) QualifiedName() string {
	return QualifyTableName(c.Schema, c.Name)
}

// Range represents a PostgreSQL range type parsed from Go annotations.
//
// Range types are defined using //ptah:schema:range annotations:
//
//	//ptah:schema:range name="floatrange" subtype="float8" subtype_diff="float8mi"
//	type FloatRange struct{}
type Range struct {
	StructName     string // Name of the Go struct this type is associated with
	Name           string // Range type name (e.g., "floatrange")
	Schema         string // Optional schema/namespace (PostgreSQL-style)
	Subtype        string // Required element subtype (e.g., "float8")
	SubtypeOpClass string // Optional operator class for the subtype
	Collation      string // Optional collation for the subtype
	Canonical      string // Optional canonicalization function
	SubtypeDiff    string // Optional subtype difference function
	Comment        string // Optional comment for documentation

	// Dialects scopes this declaration to the named target dialects. See
	// [ScopeToDialect].
	Dialects []string `json:",omitempty"`
}

// Canonicalize normalizes range-type attributes for downstream consumers.
func (r *Range) Canonicalize() {
	r.Name = strings.TrimSpace(r.Name)
	r.Schema = strings.TrimSpace(r.Schema)
	r.Subtype = strings.TrimSpace(r.Subtype)
	r.Comment = strings.TrimSpace(r.Comment)
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
func (r Range) QualifiedName() string {
	return QualifyTableName(r.Schema, r.Name)
}

// Function represents a PostgreSQL custom function definition parsed from Go struct annotations.
//
// Functions are defined using //ptah:schema:function annotations and are used to create
// custom PostgreSQL functions that can be referenced by RLS policies, triggers, or application code.
//
// Function is created by parsing //ptah:schema:function annotations:
//
//	//ptah:schema:function name="set_tenant_context" params="tenant_id_param TEXT" returns="VOID" language="plpgsql" security="DEFINER" body="BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, false); END;"
//	type User struct {
//	    // ... fields
//	}
//
// The function definition supports various PostgreSQL function attributes:
//   - Parameters: Function parameter definitions (e.g., "tenant_id_param TEXT, user_id INTEGER")
//   - Returns: Return type specification (e.g., "VOID", "TEXT", "INTEGER")
//   - Language: Function language (e.g., "plpgsql", "sql")
//   - Security: Security context (e.g., "DEFINER", "INVOKER")
//   - Volatility: Function volatility (e.g., "STABLE", "IMMUTABLE", "VOLATILE")
//   - Body: Function implementation code
//
// Example generated SQL:
//
//	CREATE OR REPLACE FUNCTION set_tenant_context(tenant_id_param TEXT)
//	RETURNS VOID AS $$
//	BEGIN
//	    PERFORM set_config('app.current_tenant_id', tenant_id_param, false);
//	END;
//	$$ LANGUAGE plpgsql SECURITY DEFINER;
type Function struct {
	StructName string // Name of the Go struct this function is associated with
	Name       string // Function name (e.g., "set_tenant_context")
	Parameters string // Function parameters (e.g., "tenant_id_param TEXT")
	Returns    string // Return type (e.g., "VOID", "TEXT")
	Language   string // Function language (e.g., "plpgsql", "sql")
	Security   string // Security context (e.g., "DEFINER", "INVOKER")
	Volatility string // Function volatility (e.g., "STABLE", "IMMUTABLE", "VOLATILE")
	Body       string // Function body/implementation
	Comment    string // Optional comment for documentation

	// Dialects scopes this declaration to the named target dialects. See
	// [ScopeToDialect].
	Dialects []string `json:",omitempty"`
}

// Sequence represents a standalone PostgreSQL sequence object parsed from Go
// annotations.
//
// A sequence is a distinct schema object created with CREATE SEQUENCE and
// controlled with ALTER SEQUENCE / DROP SEQUENCE. It is separate from the
// implicit, table-owned sequences that back SERIAL / BIGSERIAL / SMALLSERIAL
// columns: those are created and dropped automatically with their owning
// column and are never emitted as standalone objects.
//
// Sequence is created by parsing //ptah:schema:sequence annotations:
//
//	//ptah:schema:sequence name="order_number_seq" start="1000" increment="1" cache="20"
//	type OrderNumberSeq struct{}
//
// Supported attributes mirror the PostgreSQL CREATE SEQUENCE options:
//   - AsType: the underlying integer type (e.g. "bigint", "integer", "smallint")
//   - Start: the START WITH value
//   - Increment: the INCREMENT BY value (must be non-zero)
//   - MinValue / MaxValue: the MINVALUE / MAXVALUE bounds
//   - Cache: the CACHE size
//   - Cycle: whether the sequence wraps around (CYCLE vs NO CYCLE)
//   - OwnedBy: an optional "table.column" association (OWNED BY)
//
// Example generated SQL:
//
//	CREATE SEQUENCE order_number_seq AS bigint START WITH 1000 INCREMENT BY 1 CACHE 20;
type Sequence struct {
	StructName  string // Name of the Go struct this sequence is associated with
	Name        string // Sequence name (e.g., "order_number_seq")
	Schema      string // Optional schema/namespace (PostgreSQL-style)
	AsType      string // Optional underlying integer type (e.g., "bigint")
	Start       *int64 // Optional START WITH value
	Increment   *int64 // Optional INCREMENT BY value (must be non-zero)
	MinValue    *int64 // Optional MINVALUE bound
	MaxValue    *int64 // Optional MAXVALUE bound
	Cache       *int64 // Optional CACHE size
	Cycle       bool   // Whether the sequence uses CYCLE (default NO CYCLE)
	OwnedBy     string // Optional "table.column" association (OWNED BY)
	IfNotExists bool   // Whether to use IF NOT EXISTS clause
	Comment     string // Optional comment for documentation

	// Dialects scopes this declaration to the named target dialects. See
	// [ScopeToDialect].
	Dialects []string `json:",omitempty"`
}

// sequenceTypeAliases maps accepted spellings of a sequence's underlying
// integer type to the canonical form that PostgreSQL's format_type reports, so
// an annotation using an alias (e.g. int8) does not churn against an
// introspected sequence.
var sequenceTypeAliases = map[string]string{
	"smallint": "smallint",
	"int2":     "smallint",
	"integer":  "integer",
	"int":      "integer",
	"int4":     "integer",
	"bigint":   "bigint",
	"int8":     "bigint",
}

// IsValidSequenceType reports whether asType is an empty (unspecified) or
// recognized sequence integer type. It backs the parser's fail-fast validation
// that keeps unvalidated text out of the rendered AS clause.
func IsValidSequenceType(asType string) bool {
	normalized := strings.ToLower(strings.TrimSpace(asType))
	if normalized == "" {
		return true
	}
	_, ok := sequenceTypeAliases[normalized]
	return ok
}

// Canonicalize normalizes sequence attributes so every downstream consumer
// (planner, renderer, comparator) sees the same values regardless of how the
// annotation was typed. Recognized integer-type aliases are mapped to the
// canonical form; unrecognized types are left as-is for the parser to reject.
func (s *Sequence) Canonicalize() {
	s.Name = strings.TrimSpace(s.Name)
	s.Schema = strings.TrimSpace(s.Schema)
	s.AsType = strings.ToLower(strings.TrimSpace(s.AsType))
	if canonical, ok := sequenceTypeAliases[s.AsType]; ok {
		s.AsType = canonical
	}
	s.OwnedBy = strings.TrimSpace(s.OwnedBy)
	s.Comment = strings.TrimSpace(s.Comment)
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise. It
// is the stable identity used to match a declared sequence against an
// introspected one.
func (s Sequence) QualifiedName() string {
	return QualifyTableName(s.Schema, s.Name)
}

// View represents a database view definition parsed from Go annotations.
//
// View is created by parsing //ptah:schema:view annotations:
//
//	//ptah:schema:view name="active_users" body="SELECT * FROM users WHERE deleted_at IS NULL" with_check="false"
//	type User struct{}
type View struct {
	StructName string // Name of the Go struct this view is associated with
	Name       string // View name
	Body       string // SELECT query used as the view body
	WithCheck  bool   // Whether to add WITH CHECK OPTION where supported
	Comment    string // Optional comment for documentation

	// Dialects scopes this declaration to the named target dialects. See
	// [ScopeToDialect].
	Dialects []string `json:",omitempty"`
}

// MaterializedView represents a database materialized view definition parsed
// from Go annotations.
//
// MaterializedView is created by parsing //ptah:schema:matview annotations:
//
//	//ptah:schema:matview name="user_stats" body="SELECT user_id, COUNT(*) FROM users GROUP BY user_id" refresh_strategy="manual"
//	type UserStats struct{}
type MaterializedView struct {
	StructName      string // Name of the Go struct this materialized view is associated with
	Name            string // Materialized view name
	Body            string // SELECT query used as the materialized view body
	RefreshStrategy string // Ptah refresh workflow; manual emits no separate refresh operation
	Comment         string // Optional comment for documentation

	// Dialects scopes this declaration to the named target dialects. See
	// [ScopeToDialect].
	Dialects []string `json:",omitempty"`
}

// Canonicalize fills in materialized-view defaults used by the planner and
// comparator.
func (v *MaterializedView) Canonicalize() {
	v.RefreshStrategy = strings.ToLower(strings.TrimSpace(v.RefreshStrategy))
	if v.RefreshStrategy == "" {
		v.RefreshStrategy = "manual"
	}
}

// Trigger represents a database trigger definition parsed from Go annotations.
//
// Trigger is created by parsing //ptah:schema:trigger annotations:
//
//	//ptah:schema:trigger name="set_updated_at" table="users" timing="BEFORE" event="UPDATE" for="ROW" body="NEW.updated_at = NOW(); RETURN NEW;"
//	type User struct{}
type Trigger struct {
	StructName string // Name of the Go struct this trigger is associated with
	Name       string // Trigger name
	Table      string // Target table
	Timing     string // BEFORE, AFTER, or INSTEAD OF
	Event      string // INSERT, UPDATE, DELETE, or TRUNCATE
	ForEach    string // ROW or STATEMENT
	Body       string // Trigger body
	Comment    string // Optional comment for documentation

	// ExecuteFunction names an already-existing function the trigger executes
	// instead of a body Ptah owns. It is set when a SQL schema file spells
	// EXECUTE FUNCTION with a name that is not this trigger's own
	// FunctionName(), so reading such SQL back does not silently rebind the
	// trigger to a Ptah-generated function. Body and ExecuteFunction are
	// alternatives: when ExecuteFunction is set, Ptah does not render a
	// function definition for the trigger.
	ExecuteFunction string

	// Dialects scopes this declaration to the named target dialects. See
	// [ScopeToDialect].
	Dialects []string `json:",omitempty"`
}

// Canonicalize fills in trigger defaults and case-folds attributes reported in
// canonical uppercase by database catalogs.
func (t *Trigger) Canonicalize() {
	t.Timing = strings.ToUpper(strings.TrimSpace(t.Timing))
	t.Event = strings.ToUpper(strings.TrimSpace(t.Event))
	t.ForEach = strings.ToUpper(strings.TrimSpace(t.ForEach))
	t.ExecuteFunction = strings.TrimSpace(t.ExecuteFunction)
	if t.ForEach == "" {
		t.ForEach = "ROW"
	}
}

// FunctionName returns the deterministic PostgreSQL trigger function name used
// for this trigger. PostgreSQL stores executable trigger code in a function, so
// Ptah manages that linked function as part of the trigger definition.
func (t Trigger) FunctionName() string {
	name := "ptah_trigger_" + sanitizeTriggerFunctionPart(t.Table) + "_" + sanitizeTriggerFunctionPart(t.Name)
	if len(name) <= maxPostgreSQLIdentifierLength {
		return name
	}

	hash := fnv.New32a()
	_, _ = hash.Write([]byte(t.Table))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(t.Name))
	suffix := fmt.Sprintf("_%08x", hash.Sum32())
	return name[:maxPostgreSQLIdentifierLength-len(suffix)] + suffix
}

const maxPostgreSQLIdentifierLength = 63

func sanitizeTriggerFunctionPart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var builder strings.Builder
	builder.Grow(len(value))
	lastUnderscore := false
	for i := range len(value) {
		character := value[i]
		if isIdentifierPart(character) {
			builder.WriteByte(character)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			builder.WriteByte('_')
			lastUnderscore = true
		}
	}

	result := strings.Trim(builder.String(), "_")
	if result == "" {
		return "object"
	}
	if result[0] >= '0' && result[0] <= '9' {
		return "_" + result
	}
	return result
}

func isIdentifierPart(character byte) bool {
	return character >= 'a' && character <= 'z' ||
		character >= '0' && character <= '9' ||
		character == '_'
}

// Canonicalize fills in PostgreSQL's implicit defaults and case-folds the
// attributes that pg_proc/pg_language always report in canonical form. Apply
// this immediately after constructing or mutating a Function so every
// downstream consumer — parser, planner, renderer, comparator — sees the same
// values.
//
//   - Language: empty → "plpgsql"; otherwise lowercased. pg_language.lanname
//     is stored lowercase, and the postgres renderer omits the LANGUAGE
//     clause if this field is empty, which the server rejects with
//     "ERROR: no language specified". Defaulting to plpgsql is what
//     `CREATE FUNCTION` would assume in handwritten SQL too.
//   - Security: empty → "INVOKER"; otherwise uppercased. pg_proc surfaces
//     this as either "DEFINER" or "INVOKER".
//   - Volatility: empty → "VOLATILE"; otherwise uppercased. pg_proc surfaces
//     this as "IMMUTABLE", "STABLE", or "VOLATILE".
//
// The DB-side read path (internal/dbschema/postgres/reader.go) returns canonical case
// by construction, so it does not need to call this. The motivating callers
// are the annotation parser (which sees raw user-typed text) and any
// programmatic constructor — test fixtures, downstream API consumers — that
// builds Function values without going through the parser.
func (f *Function) Canonicalize() {
	f.Language = strings.ToLower(f.Language)
	if f.Language == "" {
		f.Language = "plpgsql"
	}
	f.Security = strings.ToUpper(f.Security)
	if f.Security == "" {
		f.Security = "INVOKER"
	}
	f.Volatility = strings.ToUpper(f.Volatility)
	if f.Volatility == "" {
		f.Volatility = "VOLATILE"
	}
	// Returns and Parameters: PostgreSQL stores types in canonical lowercase
	// (`pg_get_function_result`, `pg_get_function_arguments`) and lowercases
	// unquoted parameter names too. Mirror that on the Go side so an
	// annotation written as `returns="VOID"` or `params="x TEXT"` doesn't
	// false-diff on every run against pg_proc.
	f.Returns = strings.ToLower(f.Returns)
	f.Parameters = strings.ToLower(f.Parameters)
}

// RLSPolicy represents a PostgreSQL Row-Level Security policy definition parsed from Go struct annotations.
//
// RLS policies are defined using //ptah:schema:rls:policy annotations and provide database-level
// tenant isolation by automatically filtering rows based on specified conditions.
//
// RLSPolicy is created by parsing //ptah:schema:rls:policy annotations:
//
//	//ptah:schema:rls:policy name="user_tenant_isolation" table="users" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()"
//	type User struct {
//	    //ptah:schema:field name="tenant_id" type="TEXT" not_null="true"
//	    TenantID string
//	    // ... other fields
//	}
//
// The policy definition supports various PostgreSQL RLS policy attributes:
//   - Name: Policy name for identification
//   - Table: Target table name the policy applies to
//   - PolicyFor: Operations the policy applies to (e.g., "ALL", "SELECT", "INSERT", "UPDATE", "DELETE")
//   - ToRoles: Database roles the policy applies to (e.g., "app_user", "PUBLIC")
//   - UsingExpression: USING clause expression for row filtering
//   - WithCheckExpression: WITH CHECK clause expression for INSERT/UPDATE validation
//
// Example generated SQL:
//
//	CREATE POLICY user_tenant_isolation ON users
//	    FOR ALL
//	    TO inventario_app
//	    USING (tenant_id = get_current_tenant_id());
type RLSPolicy struct {
	StructName          string // Name of the Go struct this policy is associated with
	Name                string // Policy name (e.g., "user_tenant_isolation")
	Table               string // Target table name (e.g., "users")
	PolicyFor           string // Operations policy applies to (e.g., "ALL", "SELECT")
	ToRoles             string // Target roles (e.g., "inventario_app", "PUBLIC")
	UsingExpression     string // USING clause expression for row filtering
	WithCheckExpression string // WITH CHECK clause expression (optional)
	Comment             string // Optional comment for documentation

	// Dialects scopes this declaration to the named target dialects. See
	// [ScopeToDialect].
	Dialects []string `json:",omitempty"`
}

// RLSEnabledTable represents a table that has Row-Level Security enabled.
//
// RLS must be enabled on a table before policies can be applied to it.
// This is done using //ptah:schema:rls:enable annotations.
//
// RLSEnabledTable is created by parsing //ptah:schema:rls:enable annotations:
//
//	//ptah:schema:rls:enable table="users"
//	type User struct {
//	    // ... fields
//	}
//
// Example generated SQL:
//
//	ALTER TABLE users ENABLE ROW LEVEL SECURITY;
type RLSEnabledTable struct {
	StructName string // Name of the Go struct this RLS enablement is associated with
	Table      string // Table name to enable RLS on (e.g., "users")
	Comment    string // Optional comment for documentation

	// Dialects scopes this declaration to the named target dialects. See
	// [ScopeToDialect].
	Dialects []string `json:",omitempty"`
}

// Role represents a PostgreSQL role definition parsed from Go struct annotations.
//
// Roles are defined using //ptah:schema:role annotations and are used to create
// PostgreSQL database roles that can be referenced by RLS policies, granted permissions,
// or used for authentication and authorization.
//
// Role is created by parsing //ptah:schema:role annotations:
//
//	//ptah:schema:role name="app_user" login="true" password="encrypted_password" comment="Application user role"
//	//ptah:schema:role name="admin_user" login="true" superuser="true" comment="Administrator role"
//	//ptah:schema:role name="readonly_user" login="true" comment="Read-only user role"
//	type UserRoles struct {
//	    // Dummy struct to hold role annotations
//	}
//
// The role definition supports various PostgreSQL role attributes:
//   - Name: Role name (e.g., "app_user")
//   - Login: Whether role can login (default: false)
//   - Password: Encrypted password (optional)
//   - Superuser: Whether role is superuser (default: false)
//   - CreateDB: Whether role can create databases (default: false)
//   - CreateRole: Whether role can create other roles (default: false)
//   - Inherit: Whether role inherits privileges (default: true)
//   - Replication: Whether role can initiate replication (default: false)
//   - Comment: Optional comment for documentation
//
// Example generated SQL:
//
//	-- Application user role
//	CREATE ROLE app_user WITH LOGIN PASSWORD 'encrypted_password';
//
//	-- Administrator role
//	CREATE ROLE admin_user WITH LOGIN SUPERUSER;
//
//	-- Read-only user role
//	CREATE ROLE readonly_user WITH LOGIN;
type Role struct {
	StructName  string // Name of the Go struct this role is associated with
	Name        string // Role name (e.g., "app_user")
	Login       bool   // Whether role can login (default: false)
	Password    string // Encrypted password (optional)
	Superuser   bool   // Whether role is superuser (default: false)
	CreateDB    bool   // Whether role can create databases (default: false)
	CreateRole  bool   // Whether role can create other roles (default: false)
	Inherit     bool   // Whether role inherits privileges (default: true)
	Replication bool   // Whether role can initiate replication (default: false)
	Comment     string // Optional comment for documentation

	// Dialects scopes this declaration to the named target dialects. See
	// [ScopeToDialect].
	Dialects []string `json:",omitempty"`
}

// Grant represents a PostgreSQL privilege grant parsed from Go annotations.
//
// Grants are defined using //ptah:schema:grant annotations and are used to
// manage access-control privileges for roles that Ptah manages as first-class
// schema objects.
//
// Example:
//
//	//ptah:schema:grant role="app_user" privilege="USAGE" on_schema="public"
//	//ptah:schema:grant role="app_user" privilege="SELECT,INSERT" on_table="users"
//	type AccessControl struct{}
type Grant struct {
	StructName string   // Name of the Go struct this grant is associated with
	Role       string   // Role receiving the privilege
	Privileges []string // Privileges to grant, e.g. SELECT, INSERT, USAGE
	OnTable    string   // Target table, mutually exclusive with OnSchema/OnSequence
	OnSchema   string   // Target schema, mutually exclusive with OnTable/OnSequence
	OnSequence string   // Target sequence, mutually exclusive with OnTable/OnSchema
	WithOption bool     // Whether the grant includes WITH GRANT OPTION
	GrantedBy  string   // Grantor reported by database introspection, if available
	Comment    string   // Optional comment for documentation

	// Dialects scopes this declaration to the named target dialects. See
	// [ScopeToDialect].
	Dialects []string `json:",omitempty"`
}

// Canonicalize fills in normalized privilege and object names used by renderers
// and comparators.
func (g *Grant) Canonicalize() {
	seen := make(map[string]bool)
	privileges := make([]string, 0, len(g.Privileges))
	for _, privilege := range g.Privileges {
		trimmed := strings.TrimSpace(privilege)
		if trimmed == "" {
			continue
		}
		normalized := strings.ToUpper(trimmed)
		if !seen[normalized] {
			seen[normalized] = true
			privileges = append(privileges, normalized)
		}
	}
	g.Privileges = privileges
	g.Role = strings.TrimSpace(g.Role)
	g.OnTable = strings.TrimSpace(g.OnTable)
	g.OnSchema = strings.TrimSpace(g.OnSchema)
	g.OnSequence = strings.TrimSpace(g.OnSequence)
}

// ManagedData declares a set of reference/seed rows that Ptah manages as
// desired-state data for a target table. It is parsed from
// //ptah:schema:data annotations.
//
// Unlike other schema objects, the row values are not embedded in the Go source:
// the annotation points to an external YAML row-data file. The Go annotation
// parser reads comment text and cannot evaluate Go value expressions, and
// reference data naturally lives in a data file rather than in code, so the file
// reference is the source of truth for the rows themselves.
//
// ManagedData is created by parsing //ptah:schema:data annotations:
//
//	//ptah:schema:data table="countries" key="code" file="countries.yaml"
//	type Country struct {
//	    //ptah:schema:field name="code" type="VARCHAR(2)" primary="true"
//	    Code string
//
//	    //ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
//	    Name string
//	}
//
// Composite keys are declared as a comma-separated list, e.g. key="tenant_id,code".
//
// The referenced file is a top-level YAML list of row maps, resolved relative to
// the directory of the Go source file that carries the annotation. Use
// LoadManagedRows to read it:
//
//   - code: US
//     name: United States
//   - code: CZ
//     name: Czechia
//
// The key column(s) form each row's logical identity and are consumed by the
// (later) data-diff phase to match a desired row against an existing one.
type ManagedData struct {
	StructName string   // Name of the Go struct this data annotation is associated with
	Table      string   // Target table the rows belong to
	Schema     string   // Database schema the table belongs to; empty targets the connection's default schema
	Keys       []string // Key column(s) forming each row's logical identity (parsed from the comma-separated "key" attribute)
	File       string   // Path to the YAML row-data file, verbatim, relative to SourceDir
	// SourceDir identifies the directory of the Go source file that carried the
	// annotation. ParseDir and ParseDirRaw store an absolute directory so each
	// entry retains its root after composite merging. ParseFS and ParseSource
	// retain a filesystem-relative directory because they have no host root.
	SourceDir string
}

// SelfReferencingFK represents a field-level self-referencing foreign key that
// migration planners emit after creating its table.
//
// Self-referencing foreign keys occur when a table has a foreign key that references
// its own primary key, such as a "parent_id" field in a hierarchical structure.
// Ptah tracks these references separately because PostgreSQL-family and MySQL-
// family planners use a table-first, foreign-key-second creation strategy.
// SQL permits a self reference inside CREATE TABLE, and SQLite renderers use
// that inline form because SQLite cannot add a foreign key with ALTER TABLE.
// Table-level foreign keys remain in Database.Constraints so composite column
// lists are never collapsed into this single-field representation.
//
// Example:
//
//	type User struct {
//	    ID       int64  `db:"id"`
//	    ParentID *int64 `db:"parent_id" foreign:"users(id)"`
//	    Name     string `db:"name"`
//	}
//
// This would generate:
//
//	CREATE TABLE users (
//	    id SERIAL PRIMARY KEY,
//	    parent_id INTEGER,
//	    name VARCHAR(255)
//	);
//
//	ALTER TABLE users ADD CONSTRAINT fk_users_parent
//	    FOREIGN KEY (parent_id) REFERENCES users(id);
type SelfReferencingFK struct {
	FieldName      string // Name of the field (e.g., "parent_id")
	Foreign        string // Foreign key reference (e.g., "users(id)")
	ForeignKeyName string // Name of the foreign key constraint (e.g., "fk_users_parent")
	OnDelete       string // ON DELETE action (CASCADE, SET NULL, RESTRICT, NO ACTION)
	OnUpdate       string // ON UPDATE action (CASCADE, SET NULL, RESTRICT, NO ACTION)
}

func normalizeIdentityGeneration(value string) string {
	switch strings.ToUpper(strings.ReplaceAll(value, " ", "_")) {
	case "ALWAYS":
		return "ALWAYS"
	case "BY_DEFAULT":
		return "BY_DEFAULT"
	default:
		return ""
	}
}
