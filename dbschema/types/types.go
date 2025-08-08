package types

// DBSchema represents the complete schema read from a database
type DBSchema struct {
	Tables      []DBTable      `json:"tables"`
	Enums       []DBEnum       `json:"enums"`
	Indexes     []DBIndex      `json:"indexes"`
	Constraints []DBConstraint `json:"constraints"`
	Extensions  []DBExtension  `json:"extensions"`  // PostgreSQL extensions
	Functions   []DBFunction   `json:"functions"`   // PostgreSQL custom functions
	RLSPolicies []DBRLSPolicy  `json:"rls_policies"` // PostgreSQL RLS policies
}

// DBTable represents a database table
type DBTable struct {
	Name       string     `json:"name"`
	Type       string     `json:"type"` // TABLE, VIEW, etc.
	Comment    string     `json:"comment"`
	Columns    []DBColumn `json:"columns"`
	RLSEnabled bool       `json:"rls_enabled"` // Whether RLS is enabled on this table (PostgreSQL)
}

// DBColumn represents a database column
type DBColumn struct {
	Name               string  `json:"name"`
	DataType           string  `json:"data_type"`
	UDTName            string  `json:"udt_name"`             // For PostgreSQL enum types
	ColumnType         string  `json:"column_type"`          // For MySQL ENUM syntax
	IsNullable         string  `json:"is_nullable"`          // YES/NO
	ColumnDefault      *string `json:"column_default"`       // Can be NULL
	CharacterMaxLength *int    `json:"character_max_length"` // For VARCHAR, etc.
	NumericPrecision   *int    `json:"numeric_precision"`    // For DECIMAL, etc.
	NumericScale       *int    `json:"numeric_scale"`        // For DECIMAL, etc.
	OrdinalPosition    int     `json:"ordinal_position"`
	IsAutoIncrement    bool    `json:"is_auto_increment"` // Derived field
	IsPrimaryKey       bool    `json:"is_primary_key"`    // Derived field
	IsUnique           bool    `json:"is_unique"`         // Derived field
}

// DBEnum represents a database enum type (PostgreSQL)
type DBEnum struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}

// DBIndex represents a database index
type DBIndex struct {
	Name       string   `json:"name"`
	TableName  string   `json:"table_name"`
	Columns    []string `json:"columns"`
	IsUnique   bool     `json:"is_unique"`
	IsPrimary  bool     `json:"is_primary"`
	Definition string   `json:"definition"` // Full index definition
}

// DBConstraint represents a database constraint
type DBConstraint struct {
	Name          string  `json:"name"`
	TableName     string  `json:"table_name"`
	Type          string  `json:"type"` // PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK
	ColumnName    string  `json:"column_name"`
	ForeignTable  *string `json:"foreign_table"`  // For foreign keys
	ForeignColumn *string `json:"foreign_column"` // For foreign keys
	DeleteRule    *string `json:"delete_rule"`    // CASCADE, RESTRICT, etc.
	UpdateRule    *string `json:"update_rule"`    // CASCADE, RESTRICT, etc.
	CheckClause   *string `json:"check_clause"`   // For CHECK constraints
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
	Dialect string `json:"dialect"` // postgres, mysql, mariadb
	Version string `json:"version"`
	Schema  string `json:"schema"` // public, database name, etc.
	URL     string `json:"url"`    // database connection URL (for reference)
}

// SchemaReader interface for reading database schemas
type SchemaReader interface {
	ReadSchema() (*DBSchema, error)
}

// SchemaWriter interface for writing schemas to databases
type SchemaWriter interface {
	DropAllTables() error
	ExecuteSQL(sql string) error
	BeginTransaction() error
	CommitTransaction() error
	RollbackTransaction() error
	SetDryRun(dryRun bool)
	IsDryRun() bool
}

// DBFunction represents a PostgreSQL custom function read from the database
type DBFunction struct {
	Name       string `json:"name"`        // Function name
	Parameters string `json:"parameters"`  // Function parameters (e.g., "tenant_id_param TEXT")
	Returns    string `json:"returns"`     // Return type (e.g., "VOID", "TEXT")
	Language   string `json:"language"`    // Function language (e.g., "plpgsql", "sql")
	Security   string `json:"security"`    // Security context (e.g., "DEFINER", "INVOKER")
	Volatility string `json:"volatility"`  // Function volatility (e.g., "STABLE", "IMMUTABLE", "VOLATILE")
	Body       string `json:"body"`        // Function body/implementation
	Comment    string `json:"comment"`     // Function comment/description
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
