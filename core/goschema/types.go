// Package goschematypes defines the core data structures used throughout the Ptah schema migration system.
// These types represent the intermediate representation of database schema elements parsed from
// Go struct annotations and used for generating database-specific migration SQL.
package goschema

import (
	"fmt"
	"hash/fnv"
	"strings"
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
	Views                      []View                         // Database views
	MaterializedViews          []MaterializedView             // Database materialized views
	Triggers                   []Trigger                      // Database triggers
	RLSPolicies                []RLSPolicy                    // PostgreSQL Row-Level Security policies
	RLSEnabledTables           []RLSEnabledTable              // Tables with RLS enabled
	Roles                      []Role                         // PostgreSQL roles
	Grants                     []Grant                        // PostgreSQL privilege grants
	Dependencies               map[string][]string            // table -> list of tables it depends on
	FunctionDependencies       map[string][]string            // function -> list of functions it depends on
	SelfReferencingForeignKeys map[string][]SelfReferencingFK // table -> list of self-referencing foreign keys
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
//	    //migrator:embedded mode="inline"
//	    Timestamps  // Results in: created_at, updated_at columns
//
//	    //migrator:embedded mode="json" name="metadata" type="JSONB"
//	    Meta UserMeta  // Results in: metadata JSONB column
//
//	    //migrator:embedded mode="relation" field="company_id" ref="companies(id)"
//	    Company Company  // Results in: company_id INTEGER + FK constraint
//	}
type EmbeddedField struct {
	StructName       string                       // The struct that contains this embedded field
	Mode             string                       // inline, json, relation, skip
	Prefix           string                       // For inline mode - prefix for field names
	Name             string                       // For json mode - column name
	Type             string                       // For json mode - column type (JSON/JSONB)
	Nullable         bool                         // Whether the field can be null
	Index            bool                         // Whether to create an index
	Field            string                       // For relation mode - foreign key field name
	Ref              string                       // For relation mode - reference table(column)
	OnDelete         string                       // For relation mode - ON DELETE action
	OnUpdate         string                       // For relation mode - ON UPDATE action
	Comment          string                       // Comment for the field/column
	EmbeddedTypeName string                       // The name of the embedded type (e.g., "Timestamps")
	Overrides        map[string]map[string]string // Platform-specific overrides
}

// Field represents a database column/field definition parsed from Go struct field annotations.
// This is the core building block for table schema generation, containing all the metadata
// needed to generate appropriate CREATE TABLE column definitions for different database platforms.
//
// Field is created by parsing //migrator:schema:field annotations from Go struct fields:
//
//	type Product struct {
//	    //migrator:schema:field name="id" type="SERIAL" primary="true"
//	    ID int64
//
//	    //migrator:schema:field name="name" type="VARCHAR(255)" not_null="true" unique="true"
//	    Name string
//
//	    //migrator:schema:field name="price" type="DECIMAL(10,2)" check="price > 0" default="0.00"
//	    Price float64
//
//	    //migrator:schema:field name="status" type="ENUM" enum="active,inactive" default="active"
//	    Status string
//
//	    //migrator:schema:field name="category_id" type="INTEGER" foreign="categories(id)"
//	    CategoryID int64
//	}
//
// The Field supports platform-specific overrides through the Overrides field:
//
//	//migrator:schema:field name="id" type="SERIAL" platform.mysql.type="INT AUTO_INCREMENT"
//	ID int64
type Field struct {
	StructName string // Name of the Go struct this field belongs to
	FieldName  string // Name of the Go struct field
	Name       string // Database column name
	Type       string // Database column type (e.g., "VARCHAR(255)", "INTEGER")
	Nullable   bool   // Whether the column allows NULL values
	Primary    bool   // Whether this is a primary key column
	AutoInc    bool   // Whether this column auto-increments
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
}

// IndexPart represents one column or expression inside an index definition.
type IndexPart struct {
	Name   string // Column name
	Expr   string // Raw index expression
	Prefix string // MySQL index prefix length
	Desc   bool   // Whether this part is ordered DESC
}

// Index represents a database index definition parsed from Go struct annotations.
// Indexes are used to improve query performance and enforce uniqueness constraints
// on one or more columns.
//
// Index is created by parsing //migrator:schema:index annotations:
//
//	type User struct {
//	    //migrator:schema:field name="id" type="SERIAL" primary="true"
//	    ID int64
//
//	    //migrator:schema:field name="email" type="VARCHAR(255)" not_null="true"
//	    Email string
//
//	    //migrator:schema:field name="status" type="VARCHAR(50)"
//	    Status string
//
//	    // Single column index
//	    //migrator:schema:index name="idx_users_email" fields="email" unique="true"
//	    _ int
//
//	    // Multi-column index
//	    //migrator:schema:index name="idx_users_email_status" fields="email,status"
//	    _ int
//
//	    // PostgreSQL GIN index for JSONB fields
//	    //migrator:schema:index name="idx_users_tags" fields="tags" type="GIN"
//	    _ int
//
//	    // Partial index with WHERE condition
//	    //migrator:schema:index name="idx_active_users" fields="status" condition="deleted_at IS NULL"
//	    _ int
//
//	    // Trigram similarity index
//	    //migrator:schema:index name="idx_users_name_trgm" fields="name" type="GIN" ops="gin_trgm_ops"
//	    _ int
//
//	    // Cross-table index targeting specific table
//	    //migrator:schema:index name="idx_products_name" fields="name" table="products"
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
//	    //migrator:schema:field name="payload" type="String"
//	    Payload string
//
//	    //migrator:schema:index name="idx_e_payload" fields="payload" type="bloom_filter(0.01)" granularity="64"
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

	// Type carries the dialect-specific index type. For PostgreSQL this is
	// GIN/GIST/BTREE/HASH; for ClickHouse data-skipping indexes it is
	// "minmax"/"set(N)"/"bloom_filter(p)"/"tokenbf_v1(...)"/etc.
	Type string
	// Parser carries a MySQL FULLTEXT parser name, for example ngram.
	Parser string
	// Condition is the WHERE clause for partial indexes (PostgreSQL only).
	Condition string
	// Operator is the operator class (PostgreSQL only, e.g. "gin_trgm_ops").
	Operator string
	// TableName is the cross-table association (overrides StructName-based
	// resolution when set).
	TableName string

	// Granularity is the ClickHouse data-skipping-index GRANULARITY value.
	// Zero means "use the dialect default" (8192 for ClickHouse, which is
	// what the renderer falls back to when this field is unset). Ignored by
	// all non-ClickHouse renderers.
	Granularity int
}

// Constraint represents a table-level constraint definition parsed from Go struct annotations.
// Constraints are used to enforce data integrity rules at the table level, such as EXCLUDE
// constraints for preventing overlapping data, CHECK constraints for data validation, etc.
//
// Constraint is created by parsing //migrator:schema:constraint annotations:
//
//	type Booking struct {
//	    //migrator:schema:constraint name="no_overlapping_bookings" type="EXCLUDE" using="gist" elements="room_id WITH =, during WITH &&"
//	    RoomID int64
//	    During string // TSRANGE type
//
//	    //migrator:schema:constraint name="one_active_session_per_user" type="EXCLUDE" using="gist" elements="user_id WITH =" condition="is_active = true"
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

	// FOREIGN KEY constraint specific fields
	ForeignTable   string   // Referenced table name
	ForeignColumn  string   // Referenced column name for single-column foreign keys
	ForeignColumns []string // Referenced column names for composite foreign keys
	OnDelete       string   // ON DELETE action
	OnUpdate       string   // ON UPDATE action

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
// Extension is created by parsing //migrator:schema:extension annotations:
//
//	// Enable trigram similarity search
//	//migrator:schema:extension name="pg_trgm" if_not_exists="true"
//	type DatabaseExtensions struct{}
//
//	// Enable PostGIS for geographic data
//	//migrator:schema:extension name="postgis" version="3.0" if_not_exists="true"
//	type GeoExtensions struct{}
type Extension struct {
	Name        string // Extension name (pg_trgm, postgis, etc.)
	IfNotExists bool   // Whether to use IF NOT EXISTS clause
	Version     string // Specific version requirement (optional)
	Comment     string // Extension comment/description
}

// Table represents a database table configuration parsed from Go struct annotations.
// This defines the overall table properties and metadata that will be used to generate
// CREATE TABLE statements.
//
// Table is created by parsing //migrator:schema:table annotations:
//
//	//migrator:schema:table name="users" comment="User accounts table"
//	type User struct {
//	    //migrator:schema:field name="id" type="SERIAL" primary="true"
//	    ID int64
//
//	    //migrator:schema:field name="email" type="VARCHAR(255)" not_null="true"
//	    Email string
//	}
//
// Platform-specific configurations can be specified using overrides:
//
//	//migrator:schema:table name="products" platform.mysql.engine="InnoDB" platform.mysql.comment="Product catalog"
//	type Product struct {
//	    // ... fields
//	}
//
// Composite primary keys can be defined using the primary_key attribute:
//
//	//migrator:schema:table name="user_roles" primary_key="user_id,role_id"
//	type UserRole struct {
//	    //migrator:schema:field name="user_id" type="INTEGER" foreign="users(id)"
//	    UserID int64
//
//	    //migrator:schema:field name="role_id" type="INTEGER" foreign="roles(id)"
//	    RoleID int64
//	}
type Table struct {
	StructName    string   // Name of the Go struct this table represents
	Name          string   // Database table name
	Schema        string   // Optional database schema/namespace (PostgreSQL-style)
	Engine        string   // Storage engine (MySQL/MariaDB specific, e.g., "InnoDB")
	AutoIncrement string   // Initial AUTO_INCREMENT value (MySQL/MariaDB specific)
	Charset       string   // Table default character set (MySQL/MariaDB specific)
	Collate       string   // Table default collation (MySQL/MariaDB specific)
	Strict        bool     // SQLite STRICT table option
	WithoutRowID  bool     // SQLite WITHOUT ROWID table option
	Comment       string   // Table comment/description
	PrimaryKey    []string // Composite primary key column names
	// PrimaryKeyParts carries dialect-specific metadata for composite primary
	// key elements, such as MySQL prefix lengths and DESC ordering.
	PrimaryKeyParts []PrimaryKeyPart
	Checks          []string                     // Table-level check constraints
	CustomSQL       string                       // Custom SQL to append to CREATE TABLE
	Overrides       map[string]map[string]string // Platform-specific overrides
}

// PrimaryKeyPart represents one column reference inside a table primary key.
type PrimaryKeyPart struct {
	Name   string // Column name
	Prefix string // MySQL index prefix length
	Desc   bool   // Whether the column is ordered DESC
}

// QualifiedName returns the schema-qualified database table name when a schema
// is configured, or the plain table name otherwise.
func (t Table) QualifiedName() string {
	return QualifyTableName(t.Schema, t.Name)
}

// QualifyTableName joins schema and table without quoting. Renderers remain
// responsible for dialect-specific identifier escaping.
func QualifyTableName(schema, table string) string {
	schema = strings.TrimSpace(schema)
	table = strings.TrimSpace(table)
	if schema == "" {
		return table
	}
	return schema + "." + table
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
//	    //migrator:schema:field name="status" type="ENUM" enum="active,inactive,suspended" default="active"
//	    Status string  // Creates global enum: "enum_user_status"
//	}
//
//	type Post struct {
//	    //migrator:schema:field name="status" type="ENUM" enum="draft,published,archived" default="draft"
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
	Name   string   // The generated enum type name (e.g., "enum_user_status")
	Values []string // The allowed enum values (e.g., ["active", "inactive", "suspended"])
}

// Function represents a PostgreSQL custom function definition parsed from Go struct annotations.
//
// Functions are defined using //migrator:schema:function annotations and are used to create
// custom PostgreSQL functions that can be referenced by RLS policies, triggers, or application code.
//
// Function is created by parsing //migrator:schema:function annotations:
//
//	//migrator:schema:function name="set_tenant_context" params="tenant_id_param TEXT" returns="VOID" language="plpgsql" security="DEFINER" body="BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, false); END;"
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
}

// View represents a database view definition parsed from Go annotations.
//
// View is created by parsing //migrator:schema:view annotations:
//
//	//migrator:schema:view name="active_users" body="SELECT * FROM users WHERE deleted_at IS NULL" with_check="false"
//	type User struct{}
type View struct {
	StructName string // Name of the Go struct this view is associated with
	Name       string // View name
	Body       string // SELECT query used as the view body
	WithCheck  bool   // Whether to add WITH CHECK OPTION where supported
	Comment    string // Optional comment for documentation
}

// MaterializedView represents a database materialized view definition parsed
// from Go annotations.
//
// MaterializedView is created by parsing //migrator:schema:matview annotations:
//
//	//migrator:schema:matview name="user_stats" body="SELECT user_id, COUNT(*) FROM users GROUP BY user_id" refresh_strategy="manual"
//	type UserStats struct{}
type MaterializedView struct {
	StructName      string // Name of the Go struct this materialized view is associated with
	Name            string // Materialized view name
	Body            string // SELECT query used as the materialized view body
	RefreshStrategy string // manual, concurrently, or future scheduled variants
	Comment         string // Optional comment for documentation
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
// Trigger is created by parsing //migrator:schema:trigger annotations:
//
//	//migrator:schema:trigger name="set_updated_at" table="users" timing="BEFORE" event="UPDATE" for="ROW" body="NEW.updated_at = NOW(); RETURN NEW;"
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
}

// Canonicalize fills in trigger defaults and case-folds attributes reported in
// canonical uppercase by database catalogs.
func (t *Trigger) Canonicalize() {
	t.Timing = strings.ToUpper(strings.TrimSpace(t.Timing))
	t.Event = strings.ToUpper(strings.TrimSpace(t.Event))
	t.ForEach = strings.ToUpper(strings.TrimSpace(t.ForEach))
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
// The DB-side read path (dbschema/postgres/reader.go) returns canonical case
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
// RLS policies are defined using //migrator:schema:rls:policy annotations and provide database-level
// tenant isolation by automatically filtering rows based on specified conditions.
//
// RLSPolicy is created by parsing //migrator:schema:rls:policy annotations:
//
//	//migrator:schema:rls:policy name="user_tenant_isolation" table="users" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()"
//	type User struct {
//	    //migrator:schema:field name="tenant_id" type="TEXT" not_null="true"
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
}

// RLSEnabledTable represents a table that has Row-Level Security enabled.
//
// RLS must be enabled on a table before policies can be applied to it.
// This is done using //migrator:schema:rls:enable annotations.
//
// RLSEnabledTable is created by parsing //migrator:schema:rls:enable annotations:
//
//	//migrator:schema:rls:enable table="users"
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
}

// Role represents a PostgreSQL role definition parsed from Go struct annotations.
//
// Roles are defined using //migrator:schema:role annotations and are used to create
// PostgreSQL database roles that can be referenced by RLS policies, granted permissions,
// or used for authentication and authorization.
//
// Role is created by parsing //migrator:schema:role annotations:
//
//	//migrator:schema:role name="app_user" login="true" password="encrypted_password" comment="Application user role"
//	//migrator:schema:role name="admin_user" login="true" superuser="true" comment="Administrator role"
//	//migrator:schema:role name="readonly_user" login="true" comment="Read-only user role"
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
}

// Grant represents a PostgreSQL privilege grant parsed from Go annotations.
//
// Grants are defined using //migrator:schema:grant annotations and are used to
// manage access-control privileges for roles that Ptah manages as first-class
// schema objects.
//
// Example:
//
//	//migrator:schema:grant role="app_user" privilege="USAGE" on_schema="public"
//	//migrator:schema:grant role="app_user" privilege="SELECT,INSERT" on_table="users"
//	type AccessControl struct{}
type Grant struct {
	StructName string   // Name of the Go struct this grant is associated with
	Role       string   // Role receiving the privilege
	Privileges []string // Privileges to grant, e.g. SELECT, INSERT, USAGE
	OnTable    string   // Target table, mutually exclusive with OnSchema
	OnSchema   string   // Target schema, mutually exclusive with OnTable
	WithOption bool     // Whether the grant includes WITH GRANT OPTION
	GrantedBy  string   // Grantor reported by database introspection, if available
	Comment    string   // Optional comment for documentation
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
}

// SelfReferencingFK represents a self-referencing foreign key that needs to be
// handled separately from regular foreign keys to avoid circular dependencies.
//
// Self-referencing foreign keys occur when a table has a foreign key that references
// its own primary key, such as a "parent_id" field in a hierarchical structure.
// These cannot be created as part of the initial CREATE TABLE statement because
// the table doesn't exist yet when the constraint is being defined.
//
// Instead, these foreign keys are tracked separately and added as ALTER TABLE
// ADD CONSTRAINT statements after the table has been created.
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
