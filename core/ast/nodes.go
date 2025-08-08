package ast

import (
	"fmt"
)

// Node represents any SQL AST node that can be visited by a Visitor.
//
// All AST nodes implement this interface to participate in the visitor pattern.
// The Accept method allows visitors to traverse the AST and generate
// dialect-specific SQL output.
type Node interface {
	// Accept implements the visitor pattern for rendering
	Accept(visitor Visitor) error
}

// AlterTableNode represents ALTER TABLE statements with one or more operations.
//
// This node can contain multiple operations like adding columns, dropping columns,
// or modifying existing columns. Each operation is represented by a specific
// AlterOperation implementation.
type AlterTableNode struct {
	// Name is the name of the table to alter
	Name string
	// Operations contains the list of operations to perform on the table
	Operations []AlterOperation
}

// Accept implements the Node interface for AlterTableNode.
func (n *AlterTableNode) Accept(visitor Visitor) error {
	return visitor.VisitAlterTable(n)
}

// EnumNode represents a CREATE TYPE ... AS ENUM statement (PostgreSQL-specific).
//
// Enums are primarily supported by PostgreSQL. Other databases may handle
// enum-like functionality differently (e.g., CHECK constraints with IN clauses).
type EnumNode struct {
	// Name is the name of the enum type
	Name string
	// Values contains the list of allowed enum values
	Values []string
}

// NewEnum creates a new enum node with the specified name and values.
//
// Example:
//
//	enum := NewEnum("status", "active", "inactive", "pending")
func NewEnum(name string, values ...string) *EnumNode {
	return &EnumNode{
		Name:   name,
		Values: values,
	}
}

// Accept implements the Node interface for EnumNode.
func (n *EnumNode) Accept(visitor Visitor) error {
	return visitor.VisitEnum(n)
}

// CreateTableNode represents a CREATE TABLE statement with all its components.
//
// This node contains the complete definition of a table including columns,
// constraints, dialect-specific options, and optional comments. It supports
// a fluent API for easy construction.
type CreateTableNode struct {
	// Name is the name of the table to create
	Name string
	// Columns contains all column definitions for the table
	Columns []*ColumnNode
	// Constraints contains table-level constraints (PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK)
	Constraints []*ConstraintNode
	// Options contains dialect-specific table options like ENGINE for MySQL
	Options map[string]string
	// Comment is an optional table comment
	Comment string
}

// NewCreateTable creates a new CREATE TABLE node with the specified table name.
//
// The returned node has empty slices for columns and constraints, and an empty
// options map. Use the fluent API methods to add columns, constraints, and options.
//
// Example:
//
//	table := NewCreateTable("users")
func NewCreateTable(name string) *CreateTableNode {
	return &CreateTableNode{
		Name:        name,
		Columns:     make([]*ColumnNode, 0),
		Constraints: make([]*ConstraintNode, 0),
		Options:     make(map[string]string),
	}
}

// Accept implements the Node interface for CreateTableNode.
func (n *CreateTableNode) Accept(visitor Visitor) error {
	return visitor.VisitCreateTable(n)
}

// AddColumn adds a column to the CREATE TABLE statement and returns the table node for chaining.
//
// Example:
//
//	table.AddColumn(NewColumn("id", "INTEGER").SetPrimary())
func (n *CreateTableNode) AddColumn(column *ColumnNode) *CreateTableNode {
	n.Columns = append(n.Columns, column)
	return n
}

// AddConstraint adds a table-level constraint and returns the table node for chaining.
//
// Example:
//
//	table.AddConstraint(NewUniqueConstraint("uk_email", "email"))
func (n *CreateTableNode) AddConstraint(constraint *ConstraintNode) *CreateTableNode {
	n.Constraints = append(n.Constraints, constraint)
	return n
}

// SetOption sets a dialect-specific table option and returns the table node for chaining.
//
// Common options include:
//   - MySQL/MariaDB: ENGINE, CHARSET, COLLATE
//   - PostgreSQL: TABLESPACE, WITH
//
// Example:
//
//	table.SetOption("ENGINE", "InnoDB")
func (n *CreateTableNode) SetOption(key, value string) *CreateTableNode {
	n.Options[key] = value
	return n
}

// ColumnNode represents a table column definition with all its attributes.
//
// This node contains the complete specification of a table column including
// its data type, constraints, default values, and other properties. It supports
// a fluent API for easy configuration.
type ColumnNode struct {
	// Name is the column name
	Name string
	// Type is the column data type (e.g., "INTEGER", "VARCHAR(255)", "TIMESTAMP")
	Type string
	// Nullable indicates whether the column allows NULL values (default: true)
	Nullable bool
	// Primary indicates whether this column is part of the primary key
	Primary bool
	// Unique indicates whether this column has a unique constraint
	Unique bool
	// AutoInc indicates whether this column is auto-incrementing
	AutoInc bool
	// Default contains the default value specification (literal or function)
	Default *DefaultValue
	// Check contains a check constraint expression for this column
	Check string
	// Comment is an optional column comment
	Comment string
	// ForeignKey contains foreign key reference information if this column references another table
	ForeignKey *ForeignKeyRef
}

// NewColumn creates a new column node with the specified name and data type.
//
// The column is created with nullable=true by default. Use the fluent API
// methods to configure other properties.
//
// Example:
//
//	column := NewColumn("email", "VARCHAR(255)")
func NewColumn(name, dataType string) *ColumnNode {
	return &ColumnNode{
		Name:     name,
		Type:     dataType,
		Nullable: true, // Default to nullable
	}
}

// Accept implements the Node interface for ColumnNode.
func (n *ColumnNode) Accept(visitor Visitor) error {
	return visitor.VisitColumn(n)
}

// SetPrimary marks the column as a primary key and returns the column for chaining.
//
// Setting a column as primary automatically makes it NOT NULL, as primary keys
// cannot contain NULL values in SQL.
//
// Example:
//
//	column.SetPrimary()
func (n *ColumnNode) SetPrimary() *ColumnNode {
	n.Primary = true
	n.Nullable = false // Primary keys are always NOT NULL
	return n
}

// SetNotNull marks the column as NOT NULL and returns the column for chaining.
//
// Example:
//
//	column.SetNotNull()
func (n *ColumnNode) SetNotNull() *ColumnNode {
	n.Nullable = false
	return n
}

// SetUnique marks the column as UNIQUE and returns the column for chaining.
//
// This creates a column-level unique constraint. For multi-column unique
// constraints, use table-level constraints instead.
//
// Example:
//
//	column.SetUnique()
func (n *ColumnNode) SetUnique() *ColumnNode {
	n.Unique = true
	return n
}

// SetAutoIncrement marks the column as auto-incrementing and returns the column for chaining.
//
// Auto-increment behavior varies by database:
//   - MySQL/MariaDB: AUTO_INCREMENT
//   - PostgreSQL: SERIAL or IDENTITY
//   - SQLite: AUTOINCREMENT
//
// Example:
//
//	column.SetAutoIncrement()
func (n *ColumnNode) SetAutoIncrement() *ColumnNode {
	n.AutoInc = true
	return n
}

// SetDefault sets a literal default value and returns the column for chaining.
//
// The value should be properly quoted for string literals (e.g., "'active'").
// For function calls, use SetDefaultExpression instead.
//
// Example:
//
//	column.SetDefault("'active'")
//	column.SetDefault("0")
func (n *ColumnNode) SetDefault(value string) *ColumnNode {
	n.Default = &DefaultValue{Value: value}
	return n
}

// SetDefaultExpression sets a function as the default value and returns the column for chaining.
//
// Common functions include NOW(), CURRENT_TIMESTAMP, UUID(), etc.
//
// Example:
//
//	column.SetDefaultExpression("NOW()")
//	column.SetDefaultExpression("UUID()")
func (n *ColumnNode) SetDefaultExpression(fn string) *ColumnNode {
	n.Default = &DefaultValue{Expression: fn}
	return n
}

// SetCheck sets a check constraint expression and returns the column for chaining.
//
// The expression should be a valid SQL boolean expression that references
// the column.
//
// Example:
//
//	column.SetCheck("status IN ('active', 'inactive')")
//	column.SetCheck("price > 0")
func (n *ColumnNode) SetCheck(expression string) *ColumnNode {
	n.Check = expression
	return n
}

// SetComment sets a column comment and returns the column for chaining.
//
// Example:
//
//	column.SetComment("User's email address")
func (n *ColumnNode) SetComment(comment string) *ColumnNode {
	n.Comment = comment
	return n
}

// SetForeignKey sets a foreign key reference and returns the column for chaining.
//
// This creates a column-level foreign key constraint. The name parameter
// is the constraint name.
//
// Example:
//
//	column.SetForeignKey("users", "id", "fk_orders_user")
func (n *ColumnNode) SetForeignKey(table, column, name string) *ColumnNode {
	n.ForeignKey = &ForeignKeyRef{
		Table:  table,
		Column: column,
		Name:   name,
	}
	return n
}

// ConstraintNode represents table-level constraints (PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK).
//
// Table-level constraints can span multiple columns and are defined separately
// from column definitions. This is different from column-level constraints
// which are defined as part of the column specification.
type ConstraintNode struct {
	// Type specifies the constraint type (PRIMARY KEY, UNIQUE, etc.)
	Type ConstraintType
	// Name is the constraint name (optional for some constraint types)
	Name string
	// Columns contains the list of column names involved in the constraint
	Columns []string
	// Reference contains foreign key reference information (only for FOREIGN KEY constraints)
	Reference *ForeignKeyRef
	// Expression contains the check expression (only for CHECK constraints)
	Expression string
}

// Accept implements the Node interface for ConstraintNode.
func (n *ConstraintNode) Accept(visitor Visitor) error {
	return visitor.VisitConstraint(n)
}

// IndexNode represents a CREATE INDEX statement.
//
// Indexes can be unique or non-unique and may specify an index type
// depending on the database system capabilities. PostgreSQL-specific
// features like partial indexes and operator classes are also supported.
type IndexNode struct {
	// Name is the index name
	Name string
	// Table is the name of the table to index
	Table string
	// Columns contains the list of column names to include in the index
	Columns []string
	// Unique indicates whether this is a unique index
	Unique bool
	// Type specifies the index type (BTREE, HASH, GIN, GIST, etc.) - database-specific
	Type string
	// Comment is an optional index comment
	Comment string

	// PostgreSQL-specific features
	// Condition specifies a WHERE clause for partial indexes
	Condition string
	// Operator specifies the operator class (gin_trgm_ops, etc.)
	Operator string
}

// ExtensionNode represents a CREATE EXTENSION statement for PostgreSQL.
//
// Extensions enable additional functionality in PostgreSQL databases,
// such as trigram similarity search (pg_trgm) or geographic data support (PostGIS).
type ExtensionNode struct {
	// Name is the extension name (pg_trgm, postgis, etc.)
	Name string
	// IfNotExists indicates whether to use IF NOT EXISTS clause
	IfNotExists bool
	// Version specifies a specific version requirement (optional)
	Version string
	// Comment is an optional extension comment
	Comment string
}

// NewExtension creates a new extension node with the specified name.
//
// Example:
//
//	extension := NewExtension("pg_trgm")
//	extension := NewExtension("postgis").SetVersion("3.0").SetIfNotExists()
func NewExtension(name string) *ExtensionNode {
	return &ExtensionNode{
		Name:        name,
		IfNotExists: false,
	}
}

// Accept implements the Node interface for ExtensionNode.
func (n *ExtensionNode) Accept(visitor Visitor) error {
	return visitor.VisitExtension(n)
}

// SetIfNotExists marks the extension to use IF NOT EXISTS clause.
//
// Example:
//
//	extension.SetIfNotExists()
func (n *ExtensionNode) SetIfNotExists() *ExtensionNode {
	n.IfNotExists = true
	return n
}

// SetVersion sets a specific version requirement for the extension.
//
// Example:
//
//	extension.SetVersion("3.0")
func (n *ExtensionNode) SetVersion(version string) *ExtensionNode {
	n.Version = version
	return n
}

// SetComment sets a comment for the extension.
//
// Example:
//
//	extension.SetComment("Enable trigram similarity search")
func (n *ExtensionNode) SetComment(comment string) *ExtensionNode {
	n.Comment = comment
	return n
}

// DropExtensionNode represents a DROP EXTENSION statement for PostgreSQL.
//
// This node is used to remove PostgreSQL extensions from the database.
// Extension removal can be dangerous as it may break existing functionality
// that depends on the extension's features.
type DropExtensionNode struct {
	// Name is the extension name to drop (pg_trgm, postgis, etc.)
	Name string
	// IfExists indicates whether to use IF EXISTS clause
	IfExists bool
	// Cascade indicates whether to use CASCADE option (removes dependent objects)
	Cascade bool
	// Comment is an optional comment for the drop operation
	Comment string
}

// NewDropExtension creates a new drop extension node with the specified name.
//
// Example:
//
//	dropExt := NewDropExtension("pg_trgm")
//	dropExt := NewDropExtension("postgis").SetIfExists().SetCascade()
func NewDropExtension(name string) *DropExtensionNode {
	return &DropExtensionNode{
		Name:     name,
		IfExists: false,
		Cascade:  false,
	}
}

// SetIfExists marks the drop extension to use IF EXISTS clause.
//
// This prevents errors if the extension doesn't exist.
//
// Example:
//
//	dropExt.SetIfExists()
func (n *DropExtensionNode) SetIfExists() *DropExtensionNode {
	n.IfExists = true
	return n
}

// SetCascade marks the drop extension to use CASCADE option.
//
// This removes all objects that depend on the extension.
// Use with extreme caution as it can remove user data.
//
// Example:
//
//	dropExt.SetCascade()
func (n *DropExtensionNode) SetCascade() *DropExtensionNode {
	n.Cascade = true
	return n
}

// SetComment sets a comment for the DROP EXTENSION operation.
//
// This comment can be used for documentation or warnings.
//
// Example:
//
//	dropExt.SetComment("Remove unused extension")
func (n *DropExtensionNode) SetComment(comment string) *DropExtensionNode {
	n.Comment = comment
	return n
}

// Accept implements the Node interface for DropExtensionNode.
func (n *DropExtensionNode) Accept(visitor Visitor) error {
	return visitor.VisitDropExtension(n)
}

// NewIndex creates a new index node with the specified name, table, and columns.
//
// Example:
//
//	index := NewIndex("idx_user_email", "users", "email")
//	index := NewIndex("idx_user_name_status", "users", "name", "status")
func NewIndex(name, table string, columns ...string) *IndexNode {
	return &IndexNode{
		Name:    name,
		Table:   table,
		Columns: columns,
	}
}

// Accept implements the Node interface for IndexNode.
func (n *IndexNode) Accept(visitor Visitor) error {
	return visitor.VisitIndex(n)
}

// SetUnique marks the index as unique and returns the index for chaining.
//
// Unique indexes enforce uniqueness constraints on the indexed columns.
//
// Example:
//
//	index.SetUnique()
func (n *IndexNode) SetUnique() *IndexNode {
	n.Unique = true
	return n
}

// DropIndexNode represents a DROP INDEX statement.
//
// This node supports various DROP INDEX options including IF EXISTS,
// and dialect-specific features. Different databases have different
// syntax for dropping indexes (some require table name, others don't).
type DropIndexNode struct {
	// Name is the name of the index to drop
	Name string
	// Table is the name of the table (required for some databases like MySQL)
	Table string
	// IfExists indicates whether to use IF EXISTS clause
	IfExists bool
	// Comment is an optional comment for the drop operation
	Comment string
}

// NewDropIndex creates a new DROP INDEX node with the specified index name.
//
// The node is created with IfExists=false by default. The table parameter
// is optional and may be required depending on the database dialect.
// Use the fluent API methods to configure options.
//
// Example:
//
//	dropIndex := NewDropIndex("idx_users_email")
//	dropIndex := NewDropIndex("idx_users_email").SetTable("users").SetIfExists()
func NewDropIndex(name string) *DropIndexNode {
	return &DropIndexNode{
		Name:     name,
		IfExists: false,
	}
}

// SetTable sets the table name for the DROP INDEX statement.
//
// Some databases (like MySQL) require the table name in DROP INDEX statements,
// while others (like PostgreSQL) do not.
//
// Example:
//
//	dropIndex.SetTable("users")
func (n *DropIndexNode) SetTable(table string) *DropIndexNode {
	n.Table = table
	return n
}

// SetIfExists sets the IF EXISTS option for the DROP INDEX statement.
//
// This makes the statement safe to execute even if the index doesn't exist.
//
// Example:
//
//	dropIndex.SetIfExists()
func (n *DropIndexNode) SetIfExists() *DropIndexNode {
	n.IfExists = true
	return n
}

// SetComment sets a comment for the DROP INDEX operation.
//
// This comment can be used for documentation or warnings.
//
// Example:
//
//	dropIndex.SetComment("Remove unused index")
func (n *DropIndexNode) SetComment(comment string) *DropIndexNode {
	n.Comment = comment
	return n
}

// Accept implements the Node interface for DropIndexNode.
func (n *DropIndexNode) Accept(visitor Visitor) error {
	return visitor.VisitDropIndex(n)
}

// CommentNode represents SQL comments that can be included in generated scripts.
//
// Comments are useful for documenting generated SQL and providing context
// about the schema structure.
type CommentNode struct {
	// Text is the comment content
	Text string
}

// NewComment creates a new comment node with the specified text.
//
// Example:
//
//	comment := NewComment("User management tables")
func NewComment(text string) *CommentNode {
	return &CommentNode{Text: text}
}

// Accept implements the Node interface for CommentNode.
func (n *CommentNode) Accept(visitor Visitor) error {
	return visitor.VisitComment(n)
}

// DropTableNode represents a DROP TABLE statement.
//
// This node supports various DROP TABLE options including IF EXISTS,
// CASCADE/RESTRICT, and dialect-specific features.
type DropTableNode struct {
	// Name is the name of the table to drop
	Name string
	// IfExists indicates whether to use IF EXISTS clause
	IfExists bool
	// Cascade indicates whether to use CASCADE option (PostgreSQL)
	Cascade bool
	// Comment is an optional comment for the drop operation
	Comment string
}

// NewDropTable creates a new DROP TABLE node with the specified table name.
//
// The node is created with IfExists=false and Cascade=false by default.
// Use the fluent API methods to configure these options.
//
// Example:
//
//	dropTable := NewDropTable("users").SetIfExists().SetCascade()
func NewDropTable(name string) *DropTableNode {
	return &DropTableNode{
		Name:     name,
		IfExists: false,
		Cascade:  false,
	}
}

// SetIfExists sets the IF EXISTS option for the DROP TABLE statement.
//
// This makes the statement safe to execute even if the table doesn't exist.
func (n *DropTableNode) SetIfExists() *DropTableNode {
	n.IfExists = true
	return n
}

// SetCascade sets the CASCADE option for the DROP TABLE statement.
//
// This is primarily used in PostgreSQL to automatically drop dependent objects.
func (n *DropTableNode) SetCascade() *DropTableNode {
	n.Cascade = true
	return n
}

// SetComment sets a comment for the DROP TABLE operation.
//
// This comment can be used for documentation or warnings.
func (n *DropTableNode) SetComment(comment string) *DropTableNode {
	n.Comment = comment
	return n
}

// Accept implements the Node interface for DropTableNode.
func (n *DropTableNode) Accept(visitor Visitor) error {
	return visitor.VisitDropTable(n)
}

// CreateTypeNode represents a CREATE TYPE statement with various type definitions.
//
// This node supports different type definitions including enums, composite types,
// domains, and ranges. The specific type definition is determined by the TypeDef field.
type CreateTypeNode struct {
	// Name is the name of the type to create
	Name string
	// TypeDef contains the type definition (enum, composite, domain, etc.)
	TypeDef TypeDefinition
	// Comment is an optional comment for the type creation
	Comment string
}

// NewCreateType creates a new CREATE TYPE node with the specified name and type definition.
//
// Example:
//
//	createType := NewCreateType("status", NewEnumTypeDef("active", "inactive"))
//	createType := NewCreateType("address", NewCompositeTypeDef(fields...))
func NewCreateType(name string, typeDef TypeDefinition) *CreateTypeNode {
	return &CreateTypeNode{
		Name:    name,
		TypeDef: typeDef,
	}
}

// SetComment sets a comment for the CREATE TYPE operation.
//
// Example:
//
//	createType.SetComment("User status enumeration")
func (n *CreateTypeNode) SetComment(comment string) *CreateTypeNode {
	n.Comment = comment
	return n
}

// Accept implements the Node interface for CreateTypeNode.
func (n *CreateTypeNode) Accept(visitor Visitor) error {
	return visitor.VisitCreateType(n)
}

// AlterTypeNode represents an ALTER TYPE statement with one or more operations.
//
// This node can contain multiple operations like adding enum values, renaming values,
// or modifying type properties. Each operation is represented by a specific
// TypeOperation implementation.
type AlterTypeNode struct {
	// Name is the name of the type to alter
	Name string
	// Operations contains the list of operations to perform on the type
	Operations []TypeOperation
}

// NewAlterType creates a new ALTER TYPE node with the specified type name.
//
// Example:
//
//	alterType := NewAlterType("status")
func NewAlterType(name string) *AlterTypeNode {
	return &AlterTypeNode{
		Name:       name,
		Operations: make([]TypeOperation, 0),
	}
}

// AddOperation adds a type operation and returns the alter type node for chaining.
//
// Example:
//
//	alterType.AddOperation(NewAddEnumValueOperation("pending"))
func (n *AlterTypeNode) AddOperation(operation TypeOperation) *AlterTypeNode {
	n.Operations = append(n.Operations, operation)
	return n
}

// Accept implements the Node interface for AlterTypeNode.
func (n *AlterTypeNode) Accept(visitor Visitor) error {
	return visitor.VisitAlterType(n)
}

// DropTypeNode represents a DROP TYPE statement (PostgreSQL-specific).
//
// This node is used to drop custom types, particularly enum types in PostgreSQL.
// Other databases may not support this operation or handle it differently.
type DropTypeNode struct {
	// Name is the name of the type to drop
	Name string
	// IfExists indicates whether to use IF EXISTS clause
	IfExists bool
	// Cascade indicates whether to use CASCADE option
	Cascade bool
	// Comment is an optional comment for the drop operation
	Comment string
}

// NewDropType creates a new DROP TYPE node with the specified type name.
//
// The node is created with IfExists=false and Cascade=false by default.
// Use the fluent API methods to configure these options.
//
// Example:
//
//	dropType := NewDropType("status_enum").SetIfExists().SetCascade()
func NewDropType(name string) *DropTypeNode {
	return &DropTypeNode{
		Name:     name,
		IfExists: false,
		Cascade:  false,
	}
}

// SetIfExists sets the IF EXISTS option for the DROP TYPE statement.
//
// This makes the statement safe to execute even if the type doesn't exist.
func (n *DropTypeNode) SetIfExists() *DropTypeNode {
	n.IfExists = true
	return n
}

// SetCascade sets the CASCADE option for the DROP TYPE statement.
//
// This automatically drops dependent objects that use this type.
func (n *DropTypeNode) SetCascade() *DropTypeNode {
	n.Cascade = true
	return n
}

// SetComment sets a comment for the DROP TYPE operation.
//
// This comment can be used for documentation or warnings.
func (n *DropTypeNode) SetComment(comment string) *DropTypeNode {
	n.Comment = comment
	return n
}

// Accept implements the Node interface for DropTypeNode.
func (n *DropTypeNode) Accept(visitor Visitor) error {
	return visitor.VisitDropType(n)
}

// CreateFunctionNode represents a CREATE FUNCTION statement for PostgreSQL custom functions.
//
// This node contains the complete definition of a PostgreSQL function including
// parameters, return type, language, security attributes, and function body.
// It supports various PostgreSQL function attributes like SECURITY DEFINER,
// STABLE, IMMUTABLE, etc.
type CreateFunctionNode struct {
	// Name is the name of the function to create
	Name string
	// Parameters contains the function parameter definitions (e.g., "tenant_id_param TEXT")
	Parameters string
	// Returns specifies the return type (e.g., "VOID", "TEXT", "INTEGER")
	Returns string
	// Language specifies the function language (e.g., "plpgsql", "sql")
	Language string
	// Body contains the function implementation code
	Body string
	// Security specifies security attributes (e.g., "DEFINER", "INVOKER")
	Security string
	// Volatility specifies function volatility (e.g., "STABLE", "IMMUTABLE", "VOLATILE")
	Volatility string
	// Comment is an optional comment for the function
	Comment string
}

// NewCreateFunction creates a new CREATE FUNCTION node with the specified name.
//
// Example:
//
//	createFunc := NewCreateFunction("set_tenant_context").
//		SetParameters("tenant_id_param TEXT").
//		SetReturns("VOID").
//		SetLanguage("plpgsql").
//		SetSecurity("DEFINER").
//		SetBody("BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, false); END;")
func NewCreateFunction(name string) *CreateFunctionNode {
	return &CreateFunctionNode{
		Name: name,
	}
}

// SetParameters sets the function parameters.
//
// Example:
//
//	createFunc.SetParameters("tenant_id_param TEXT, user_id INTEGER")
func (n *CreateFunctionNode) SetParameters(parameters string) *CreateFunctionNode {
	n.Parameters = parameters
	return n
}

// SetReturns sets the function return type.
//
// Example:
//
//	createFunc.SetReturns("TEXT")
func (n *CreateFunctionNode) SetReturns(returns string) *CreateFunctionNode {
	n.Returns = returns
	return n
}

// SetLanguage sets the function language.
//
// Example:
//
//	createFunc.SetLanguage("plpgsql")
func (n *CreateFunctionNode) SetLanguage(language string) *CreateFunctionNode {
	n.Language = language
	return n
}

// SetBody sets the function body/implementation.
//
// Example:
//
//	createFunc.SetBody("BEGIN RETURN current_setting('app.current_tenant_id', true); END;")
func (n *CreateFunctionNode) SetBody(body string) *CreateFunctionNode {
	n.Body = body
	return n
}

// SetSecurity sets the function security attribute.
//
// Example:
//
//	createFunc.SetSecurity("DEFINER")
func (n *CreateFunctionNode) SetSecurity(security string) *CreateFunctionNode {
	n.Security = security
	return n
}

// SetVolatility sets the function volatility attribute.
//
// Example:
//
//	createFunc.SetVolatility("STABLE")
func (n *CreateFunctionNode) SetVolatility(volatility string) *CreateFunctionNode {
	n.Volatility = volatility
	return n
}

// SetComment sets a comment for the CREATE FUNCTION operation.
//
// Example:
//
//	createFunc.SetComment("Sets the current tenant context for RLS")
func (n *CreateFunctionNode) SetComment(comment string) *CreateFunctionNode {
	n.Comment = comment
	return n
}

// Accept implements the Node interface for CreateFunctionNode.
func (n *CreateFunctionNode) Accept(visitor Visitor) error {
	return visitor.VisitCreateFunction(n)
}

// CreatePolicyNode represents a CREATE POLICY statement for PostgreSQL Row-Level Security.
//
// This node contains the complete definition of an RLS policy including
// the target table, policy type (FOR clause), target roles (TO clause),
// and the policy expression (USING clause).
type CreatePolicyNode struct {
	// Name is the name of the policy to create
	Name string
	// Table is the name of the table this policy applies to
	Table string
	// PolicyFor specifies what operations the policy applies to (e.g., "ALL", "SELECT", "INSERT", "UPDATE", "DELETE")
	PolicyFor string
	// ToRoles specifies which roles the policy applies to (e.g., "inventario_app", "PUBLIC")
	ToRoles string
	// UsingExpression contains the USING clause expression for the policy
	UsingExpression string
	// WithCheckExpression contains the WITH CHECK clause expression (for INSERT/UPDATE policies)
	WithCheckExpression string
	// Comment is an optional comment for the policy
	Comment string
}

// NewCreatePolicy creates a new CREATE POLICY node with the specified name and table.
//
// Example:
//
//	createPolicy := NewCreatePolicy("user_tenant_isolation", "users").
//		SetPolicyFor("ALL").
//		SetToRoles("inventario_app").
//		SetUsingExpression("tenant_id = get_current_tenant_id()")
func NewCreatePolicy(name, table string) *CreatePolicyNode {
	return &CreatePolicyNode{
		Name:  name,
		Table: table,
	}
}

// SetPolicyFor sets the FOR clause of the policy.
//
// Example:
//
//	createPolicy.SetPolicyFor("SELECT")
func (n *CreatePolicyNode) SetPolicyFor(policyFor string) *CreatePolicyNode {
	n.PolicyFor = policyFor
	return n
}

// SetToRoles sets the TO clause of the policy.
//
// Example:
//
//	createPolicy.SetToRoles("app_user, admin_user")
func (n *CreatePolicyNode) SetToRoles(toRoles string) *CreatePolicyNode {
	n.ToRoles = toRoles
	return n
}

// SetUsingExpression sets the USING clause expression.
//
// Example:
//
//	createPolicy.SetUsingExpression("tenant_id = get_current_tenant_id()")
func (n *CreatePolicyNode) SetUsingExpression(expression string) *CreatePolicyNode {
	n.UsingExpression = expression
	return n
}

// SetWithCheckExpression sets the WITH CHECK clause expression.
//
// Example:
//
//	createPolicy.SetWithCheckExpression("tenant_id = get_current_tenant_id()")
func (n *CreatePolicyNode) SetWithCheckExpression(expression string) *CreatePolicyNode {
	n.WithCheckExpression = expression
	return n
}

// SetComment sets a comment for the CREATE POLICY operation.
//
// Example:
//
//	createPolicy.SetComment("Ensures users can only access their tenant's data")
func (n *CreatePolicyNode) SetComment(comment string) *CreatePolicyNode {
	n.Comment = comment
	return n
}

// Accept implements the Node interface for CreatePolicyNode.
func (n *CreatePolicyNode) Accept(visitor Visitor) error {
	return visitor.VisitCreatePolicy(n)
}

// AlterTableEnableRLSNode represents an ALTER TABLE ... ENABLE ROW LEVEL SECURITY statement.
//
// This node is used to enable Row-Level Security on a specific table.
// RLS must be enabled before policies can be applied to a table.
type AlterTableEnableRLSNode struct {
	// Table is the name of the table to enable RLS on
	Table string
	// Comment is an optional comment for the operation
	Comment string
}

// NewAlterTableEnableRLS creates a new ALTER TABLE ENABLE ROW LEVEL SECURITY node.
//
// Example:
//
//	enableRLS := NewAlterTableEnableRLS("users").
//		SetComment("Enable RLS for multi-tenant isolation")
func NewAlterTableEnableRLS(table string) *AlterTableEnableRLSNode {
	return &AlterTableEnableRLSNode{
		Table: table,
	}
}

// SetComment sets a comment for the ALTER TABLE ENABLE RLS operation.
//
// Example:
//
//	enableRLS.SetComment("Enable RLS for tenant isolation")
func (n *AlterTableEnableRLSNode) SetComment(comment string) *AlterTableEnableRLSNode {
	n.Comment = comment
	return n
}

// Accept implements the Node interface for AlterTableEnableRLSNode.
func (n *AlterTableEnableRLSNode) Accept(visitor Visitor) error {
	return visitor.VisitAlterTableEnableRLS(n)
}

// DropFunctionNode represents a DROP FUNCTION statement for PostgreSQL custom functions.
//
// This node is used to remove PostgreSQL custom functions from the database.
// Function removal should be done carefully as other database objects may depend on the function.
type DropFunctionNode struct {
	// Name is the name of the function to drop
	Name string
	// Parameters contains the function parameter definitions for function signature matching
	// This is needed because PostgreSQL allows function overloading
	Parameters string
	// IfExists indicates whether to use IF EXISTS clause
	IfExists bool
	// Cascade indicates whether to use CASCADE option (removes dependent objects)
	Cascade bool
	// Comment is an optional comment for the drop operation
	Comment string
}

// NewDropFunction creates a new DROP FUNCTION node with the specified name.
//
// Example:
//
//	dropFunc := NewDropFunction("set_tenant_context").
//		SetParameters("tenant_id_param TEXT").
//		SetIfExists().
//		SetComment("Remove tenant context function")
func NewDropFunction(name string) *DropFunctionNode {
	return &DropFunctionNode{
		Name:     name,
		IfExists: false,
		Cascade:  false,
	}
}

// SetParameters sets the function parameters for signature matching.
//
// This is important for PostgreSQL function overloading support.
//
// Example:
//
//	dropFunc.SetParameters("tenant_id_param TEXT, user_id INTEGER")
func (n *DropFunctionNode) SetParameters(parameters string) *DropFunctionNode {
	n.Parameters = parameters
	return n
}

// SetIfExists marks the drop function to use IF EXISTS clause.
//
// This prevents errors if the function doesn't exist.
//
// Example:
//
//	dropFunc.SetIfExists()
func (n *DropFunctionNode) SetIfExists() *DropFunctionNode {
	n.IfExists = true
	return n
}

// SetCascade marks the drop function to use CASCADE option.
//
// This automatically drops dependent objects that use this function.
//
// Example:
//
//	dropFunc.SetCascade()
func (n *DropFunctionNode) SetCascade() *DropFunctionNode {
	n.Cascade = true
	return n
}

// SetComment sets a comment for the DROP FUNCTION operation.
//
// This comment can be used for documentation or warnings.
//
// Example:
//
//	dropFunc.SetComment("WARNING: This function is used by RLS policies")
func (n *DropFunctionNode) SetComment(comment string) *DropFunctionNode {
	n.Comment = comment
	return n
}

// Accept implements the Node interface for DropFunctionNode.
func (n *DropFunctionNode) Accept(visitor Visitor) error {
	return visitor.VisitDropFunction(n)
}

// DropPolicyNode represents a DROP POLICY statement for PostgreSQL Row-Level Security.
//
// This node is used to remove RLS policies from tables. Policy removal should be done
// carefully as it changes the security model of the table.
type DropPolicyNode struct {
	// Name is the name of the policy to drop
	Name string
	// Table is the name of the table this policy applies to
	Table string
	// IfExists indicates whether to use IF EXISTS clause
	IfExists bool
	// Comment is an optional comment for the drop operation
	Comment string
}

// NewDropPolicy creates a new DROP POLICY node with the specified name and table.
//
// Example:
//
//	dropPolicy := NewDropPolicy("user_tenant_isolation", "users").
//		SetIfExists().
//		SetComment("Remove tenant isolation policy")
func NewDropPolicy(name, table string) *DropPolicyNode {
	return &DropPolicyNode{
		Name:     name,
		Table:    table,
		IfExists: false,
	}
}

// SetIfExists marks the drop policy to use IF EXISTS clause.
//
// This prevents errors if the policy doesn't exist.
//
// Example:
//
//	dropPolicy.SetIfExists()
func (n *DropPolicyNode) SetIfExists() *DropPolicyNode {
	n.IfExists = true
	return n
}

// SetComment sets a comment for the DROP POLICY operation.
//
// This comment can be used for documentation or warnings.
//
// Example:
//
//	dropPolicy.SetComment("WARNING: Removing this policy changes table security")
func (n *DropPolicyNode) SetComment(comment string) *DropPolicyNode {
	n.Comment = comment
	return n
}

// Accept implements the Node interface for DropPolicyNode.
func (n *DropPolicyNode) Accept(visitor Visitor) error {
	return visitor.VisitDropPolicy(n)
}

// AlterTableDisableRLSNode represents an ALTER TABLE ... DISABLE ROW LEVEL SECURITY statement.
//
// This node is used to disable Row-Level Security on a specific table.
// RLS should only be disabled after all policies have been removed from the table.
type AlterTableDisableRLSNode struct {
	// Table is the name of the table to disable RLS on
	Table string
	// Comment is an optional comment for the operation
	Comment string
}

// NewAlterTableDisableRLS creates a new ALTER TABLE DISABLE ROW LEVEL SECURITY node.
//
// Example:
//
//	disableRLS := NewAlterTableDisableRLS("users").
//		SetComment("Disable RLS after removing all policies")
func NewAlterTableDisableRLS(table string) *AlterTableDisableRLSNode {
	return &AlterTableDisableRLSNode{
		Table: table,
	}
}

// SetComment sets a comment for the ALTER TABLE DISABLE RLS operation.
//
// Example:
//
//	disableRLS.SetComment("Disable RLS for rollback")
func (n *AlterTableDisableRLSNode) SetComment(comment string) *AlterTableDisableRLSNode {
	n.Comment = comment
	return n
}

// Accept implements the Node interface for AlterTableDisableRLSNode.
func (n *AlterTableDisableRLSNode) Accept(visitor Visitor) error {
	return visitor.VisitAlterTableDisableRLS(n)
}

// StatementList represents a collection of SQL statements that should be executed together.
//
// This is typically used to represent a complete schema or migration script
// that contains multiple DDL statements. The visitor will process each
// statement in order.
type StatementList struct {
	// Statements contains the ordered list of SQL statements
	Statements []Node
}

// Accept implements the Node interface for StatementList.
//
// This method visits each statement in the list in order. If any statement
// fails to be visited, the process stops and returns the error.
func (sl *StatementList) Accept(visitor Visitor) error {
	for _, stmt := range sl.Statements {
		if err := stmt.Accept(visitor); err != nil {
			return fmt.Errorf("error visiting statement: %w", err)
		}
	}
	return nil
}
