package types

import (
	"context"
	"strings"

	"github.com/stokaro/ptah/core/platform/capability"
)

// DBSchema represents the complete schema read from a database
type DBSchema struct {
	Tables      []DBTable      `json:"tables"`
	Enums       []DBEnum       `json:"enums"`
	Indexes     []DBIndex      `json:"indexes"`
	Constraints []DBConstraint `json:"constraints"`
	Extensions  []DBExtension  `json:"extensions"`   // PostgreSQL extensions
	Functions   []DBFunction   `json:"functions"`    // PostgreSQL custom functions
	Views       []DBView       `json:"views"`        // Database views
	MatViews    []DBMatView    `json:"matviews"`     // Database materialized views
	Triggers    []DBTrigger    `json:"triggers"`     // Database triggers
	RLSPolicies []DBRLSPolicy  `json:"rls_policies"` // PostgreSQL RLS policies
	Roles       []DBRole       `json:"roles"`        // PostgreSQL roles
	Grants      []DBGrant      `json:"grants"`       // PostgreSQL privilege grants
}

// DBTable represents a database table
type DBTable struct {
	Name       string     `json:"name"`
	Schema     string     `json:"schema,omitempty"`
	Type       string     `json:"type"` // TABLE, VIEW, etc.
	Comment    string     `json:"comment"`
	Columns    []DBColumn `json:"columns"`
	RLSEnabled bool       `json:"rls_enabled"` // Whether RLS is enabled on this table (PostgreSQL)
}

// QualifiedName returns schema.table when Schema is set, or Name otherwise.
func (t DBTable) QualifiedName() string {
	return QualifyTableName(t.Schema, t.Name)
}

// QualifyTableName joins schema and table without quoting. Dialect renderers
// remain responsible for escaping identifiers.
func QualifyTableName(schema, table string) string {
	schema = strings.TrimSpace(schema)
	table = strings.TrimSpace(table)
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// DBColumn represents a database column.
//
// GeneratedKind / GeneratedExpression are currently populated by the ClickHouse
// reader for columns declared with non-DEFAULT default kinds (MATERIALIZED,
// ALIAS, EPHEMERAL). Schema comparison can match these fields when the
// goschema-side model also carries generated column metadata.
type DBColumn struct {
	Name               string  `json:"name"`
	DataType           string  `json:"data_type"`
	UDTName            string  `json:"udt_name"`             // For PostgreSQL enum types
	ColumnType         string  `json:"column_type"`          // For MySQL ENUM syntax
	IsNullable         string  `json:"is_nullable"`          // YES/NO
	ColumnDefault      *string `json:"column_default"`       // Can be NULL
	CharacterMaxLength *int    `json:"character_max_length"` // For VARCHAR, etc.
	Charset            string  `json:"charset,omitempty"`    // MySQL/MariaDB column character set
	Collate            string  `json:"collate,omitempty"`    // MySQL/MariaDB column collation
	NumericPrecision   *int    `json:"numeric_precision"`    // For DECIMAL, etc.
	NumericScale       *int    `json:"numeric_scale"`        // For DECIMAL, etc.
	OrdinalPosition    int     `json:"ordinal_position"`
	IsAutoIncrement    bool    `json:"is_auto_increment"` // Derived field
	IsPrimaryKey       bool    `json:"is_primary_key"`    // Derived field
	IsUnique           bool    `json:"is_unique"`         // Derived field

	// GeneratedExpression holds the MATERIALIZED / ALIAS / EPHEMERAL
	// expression for ClickHouse columns. Nil for plain columns. Other
	// dialects always leave this nil.
	GeneratedExpression *string `json:"generated_expression,omitempty"`
	// GeneratedKind names the ClickHouse default-kind: "MATERIALIZED",
	// "ALIAS" or "EPHEMERAL". Empty for plain columns. Other dialects always
	// leave this empty.
	GeneratedKind string `json:"generated_kind,omitempty"`
}

// DBEnum represents a database enum type (PostgreSQL)
type DBEnum struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}

// DBIndex represents a database index.
//
// Most fields are dialect-neutral. The Type/Expression/Granularity trio is
// populated only by the ClickHouse reader for data-skipping indexes; other
// readers leave them at their zero values so the diff layer does not start
// emitting spurious type/granularity changes for PostgreSQL or MySQL
// indexes.
type DBIndex struct {
	Name       string   `json:"name"`
	TableName  string   `json:"table_name"`
	Schema     string   `json:"schema,omitempty"`
	Columns    []string `json:"columns"`
	IsUnique   bool     `json:"is_unique"`
	IsPrimary  bool     `json:"is_primary"`
	Definition string   `json:"definition"` // Full index definition

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
}

// QualifiedTableName returns schema.table when Schema is set, or TableName otherwise.
func (i DBIndex) QualifiedTableName() string {
	return QualifyTableName(i.Schema, i.TableName)
}

// DBConstraint represents a database constraint
type DBConstraint struct {
	Name          string  `json:"name"`
	TableName     string  `json:"table_name"`
	Schema        string  `json:"schema,omitempty"`
	Type          string  `json:"type"` // PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK, EXCLUDE
	ColumnName    string  `json:"column_name"`
	ForeignTable  *string `json:"foreign_table"` // For foreign keys
	ForeignSchema string  `json:"foreign_schema,omitempty"`
	ForeignColumn *string `json:"foreign_column"` // For foreign keys
	DeleteRule    *string `json:"delete_rule"`    // CASCADE, RESTRICT, etc.
	UpdateRule    *string `json:"update_rule"`    // CASCADE, RESTRICT, etc.
	CheckClause   *string `json:"check_clause"`   // For CHECK constraints
	// EXCLUDE constraint specific fields (PostgreSQL only)
	UsingMethod     *string `json:"using_method"`     // Index method: gist, btree, etc.
	ExcludeElements *string `json:"exclude_elements"` // Elements with operators: "room_id WITH =, during WITH &&"
	WhereCondition  *string `json:"where_condition"`  // Optional WHERE clause for EXCLUDE constraints
}

// QualifiedTableName returns schema.table when Schema is set, or TableName otherwise.
func (c DBConstraint) QualifiedTableName() string {
	return QualifyTableName(c.Schema, c.TableName)
}

// QualifiedForeignTableName returns schema.table for a foreign key target.
func (c DBConstraint) QualifiedForeignTableName() string {
	if c.ForeignTable == nil {
		return ""
	}
	return QualifyTableName(c.ForeignSchema, *c.ForeignTable)
}

// DBExtension represents a PostgreSQL extension installed in the database
type DBExtension struct {
	Name             string  `json:"name"`              // Extension name (pg_trgm, postgis, etc.)
	Version          string  `json:"version"`           // Installed version
	Schema           string  `json:"schema"`            // Schema where extension is installed
	Relocatable      bool    `json:"relocatable"`       // Whether extension can be moved between schemas
	Comment          *string `json:"comment"`           // Extension comment/description
	DefaultVersion   *string `json:"default_version"`   // Default version available
	InstalledVersion *string `json:"installed_version"` // Currently installed version (may differ from default)
}

// DBInfo contains connection and metadata information
type DBInfo struct {
	Dialect      string                  `json:"dialect"` // postgres, mysql, mariadb
	Version      string                  `json:"version"`
	Schema       string                  `json:"schema"`       // public, database name, etc.
	URL          string                  `json:"url"`          // database connection URL (for reference)
	Capabilities capability.Capabilities `json:"capabilities"` // resolved from Dialect + Version for live connections
}

// SchemaReader interface for reading database schemas
type SchemaReader interface {
	ReadSchema() (*DBSchema, error)
}

// SchemaWriter interface for writing schemas to databases.
//
// ExecuteSQL accepts a context and an optional slice of arguments that are
// bound as native driver parameters, mirroring database/sql's ExecContext.
// Use placeholders (`?` or the dialect-native form such as `$1`/`$2` for
// PostgreSQL) instead of interpolating values into the SQL string; this
// prevents the SQL injection class of bugs that the no-args signature used
// to invite (see issue #130). Identifiers (table/column names) cannot be
// parameterized — route them through a validated escape helper instead.
type SchemaWriter interface {
	DropAllTables() error
	ExecuteSQL(ctx context.Context, sql string, args ...any) error
	BeginTransaction() error
	CommitTransaction() error
	RollbackTransaction() error
	SetDryRun(dryRun bool)
	IsDryRun() bool
}

// DBFunction represents a PostgreSQL custom function read from the database
type DBFunction struct {
	Name       string `json:"name"`       // Function name
	Parameters string `json:"parameters"` // Function parameters (e.g., "tenant_id_param TEXT")
	Returns    string `json:"returns"`    // Return type (e.g., "VOID", "TEXT")
	Language   string `json:"language"`   // Function language (e.g., "plpgsql", "sql")
	Security   string `json:"security"`   // Security context (e.g., "DEFINER", "INVOKER")
	Volatility string `json:"volatility"` // Function volatility (e.g., "STABLE", "IMMUTABLE", "VOLATILE")
	Body       string `json:"body"`       // Function body/implementation
	Comment    string `json:"comment"`    // Function comment/description
}

// DBView represents a database view read from the database.
type DBView struct {
	Name        string `json:"name"`         // View name
	Schema      string `json:"schema"`       // Schema where the view is defined
	Body        string `json:"body"`         // SELECT query used as the view definition
	CheckOption string `json:"check_option"` // NONE, LOCAL, CASCADED, or dialect equivalent
	Comment     string `json:"comment"`      // View comment/description
}

// QualifiedName returns schema.view when Schema is set, or Name otherwise.
func (v DBView) QualifiedName() string {
	return QualifyTableName(v.Schema, v.Name)
}

// DBMatView represents a PostgreSQL materialized view read from the database.
type DBMatView struct {
	Name            string `json:"name"`             // Materialized view name
	Schema          string `json:"schema"`           // Schema where the materialized view is defined
	Body            string `json:"body"`             // SELECT query used as the materialized view definition
	RefreshStrategy string `json:"refresh_strategy"` // Ptah-managed refresh policy; database introspection defaults to manual
	Comment         string `json:"comment"`          // Materialized view comment/description
}

// QualifiedName returns schema.materialized_view when Schema is set, or Name otherwise.
func (v DBMatView) QualifiedName() string {
	return QualifyTableName(v.Schema, v.Name)
}

// DBTrigger represents a database trigger read from the database.
type DBTrigger struct {
	Name    string `json:"name"`    // Trigger name
	Schema  string `json:"schema"`  // Schema where the trigger is defined
	Table   string `json:"table"`   // Target table
	Timing  string `json:"timing"`  // BEFORE, AFTER, or INSTEAD OF
	Event   string `json:"event"`   // INSERT, UPDATE, DELETE, or TRUNCATE
	ForEach string `json:"for"`     // ROW or STATEMENT
	Body    string `json:"body"`    // Trigger body
	Comment string `json:"comment"` // Trigger comment/description
}

// QualifiedTable returns schema.table when Schema is set, or Table otherwise.
func (t DBTrigger) QualifiedTable() string {
	return QualifyTableName(t.Schema, t.Table)
}

// DBRLSPolicy represents a PostgreSQL RLS policy read from the database
type DBRLSPolicy struct {
	Name                string `json:"name"`                  // Policy name
	Table               string `json:"table"`                 // Target table name
	PolicyFor           string `json:"policy_for"`            // Operations policy applies to (e.g., "ALL", "SELECT")
	ToRoles             string `json:"to_roles"`              // Target roles (e.g., "app_user", "PUBLIC")
	UsingExpression     string `json:"using_expression"`      // USING clause expression
	WithCheckExpression string `json:"with_check_expression"` // WITH CHECK clause expression
	Comment             string `json:"comment"`               // Policy comment/description
}

// DBRole represents a PostgreSQL role read from the database
type DBRole struct {
	Name        string `json:"name"`         // Role name
	Login       bool   `json:"login"`        // Whether role can login
	Superuser   bool   `json:"superuser"`    // Whether role is superuser
	CreateDB    bool   `json:"create_db"`    // Whether role can create databases
	CreateRole  bool   `json:"create_role"`  // Whether role can create other roles
	Inherit     bool   `json:"inherit"`      // Whether role inherits privileges
	Replication bool   `json:"replication"`  // Whether role can initiate replication
	HasPassword bool   `json:"has_password"` // Whether role has a password set
	Comment     string `json:"comment"`      // Role comment/description
}

// DBGrant represents a PostgreSQL privilege grant read from the database.
type DBGrant struct {
	Role       string `json:"role"`                 // Role receiving the privilege
	Privilege  string `json:"privilege"`            // Granted privilege, e.g. SELECT or USAGE
	ObjectType string `json:"object_type"`          // TABLE or SCHEMA
	Schema     string `json:"schema,omitempty"`     // Schema containing the target object
	ObjectName string `json:"object_name"`          // Target table or schema name
	WithOption bool   `json:"with_option"`          // Whether the grant has WITH GRANT OPTION
	GrantedBy  string `json:"granted_by,omitempty"` // Grantor role
}

// QualifiedTarget returns schema.object for table grants and the schema name
// itself for schema grants.
func (g DBGrant) QualifiedTarget() string {
	if strings.EqualFold(g.ObjectType, "SCHEMA") {
		return g.ObjectName
	}
	return QualifyTableName(g.Schema, g.ObjectName)
}
