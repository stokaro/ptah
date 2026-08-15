package integrationharness

import (
	"fmt"
	"strings"
)

// EntityTemplate provides helper functions for creating Go entity definitions
type EntityTemplate struct{}

// NewEntityTemplate creates a new entity template helper
func NewEntityTemplate() *EntityTemplate {
	return &EntityTemplate{}
}

// BasicUserEntity creates a basic User entity with common fields
func (et *EntityTemplate) BasicUserEntity() string {
	return `package entities

import "time"

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time

	//ptah:schema:field name="updated_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	UpdatedAt time.Time
}
`
}

// BasicPostEntity creates a basic Post entity with foreign key to User
func (et *EntityTemplate) BasicPostEntity() string {
	return `package entities

import "time"

//ptah:schema:table name="posts"
type Post struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="user_id" type="INTEGER" not_null="true" foreign="users(id)"
	UserID int64

	//ptah:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string

	//ptah:schema:field name="content" type="TEXT"
	Content string

	//ptah:schema:field name="published" type="BOOLEAN" not_null="true" default_expr="false"
	Published bool

	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time

	//ptah:schema:field name="updated_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	UpdatedAt time.Time
}

//ptah:schema:index table="posts" name="idx_posts_user_id" columns="user_id"
//ptah:schema:index table="posts" name="idx_posts_published" columns="published"
`
}

// BasicCommentEntity creates a basic Comment entity with foreign keys to Post and User
func (et *EntityTemplate) BasicCommentEntity() string {
	return `package entities

import "time"

//ptah:schema:table name="comments"
type Comment struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="post_id" type="INTEGER" not_null="true" foreign="posts(id)"
	PostID int64

	//ptah:schema:field name="user_id" type="INTEGER" not_null="true" foreign="users(id)"
	UserID int64

	//ptah:schema:field name="content" type="TEXT" not_null="true"
	Content string

	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time

	//ptah:schema:field name="updated_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	UpdatedAt time.Time
}

//ptah:schema:index table="comments" name="idx_comments_post_id" columns="post_id"
//ptah:schema:index table="comments" name="idx_comments_user_id" columns="user_id"
`
}

// ProductEntity creates a Product entity for testing different field types
func (et *EntityTemplate) ProductEntity() string {
	return `package entities

import "time"

//ptah:schema:table name="products"
type Product struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//ptah:schema:field name="description" type="TEXT"
	Description string

	//ptah:schema:field name="price" type="DECIMAL(10,2)" not_null="true"
	Price float64

	//ptah:schema:field name="active" type="BOOLEAN" not_null="true" default_expr="true"
	Active bool

	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time

	//ptah:schema:field name="updated_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	UpdatedAt time.Time
}

//ptah:schema:index table="products" name="idx_products_name" columns="name"
//ptah:schema:index table="products" name="idx_products_active" columns="active"
`
}

// UserWithExtraFields creates a User entity with additional fields for testing field additions
func (et *EntityTemplate) UserWithExtraFields() string {
	return `package entities

import "time"

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//ptah:schema:field name="age" type="INTEGER"
	Age int

	//ptah:schema:field name="bio" type="TEXT"
	Bio string

	//ptah:schema:field name="active" type="BOOLEAN" not_null="true" default_expr="true"
	Active bool

	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time

	//ptah:schema:field name="updated_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	UpdatedAt time.Time
}

//ptah:schema:index table="users" name="idx_users_email" columns="email"
//ptah:schema:index table="users" name="idx_users_active" columns="active"
`
}

// StatusEnumEntity creates an entity with enum field for testing enum support
func (et *EntityTemplate) StatusEnumEntity() string {
	return `package entities

import "time"

//ptah:schema:enum name="status_type" values="draft,published,archived"
type StatusType string

const (
	StatusDraft     StatusType = "draft"
	StatusPublished StatusType = "published"
	StatusArchived  StatusType = "archived"
)

//ptah:schema:table name="articles"
type Article struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string

	//ptah:schema:field name="content" type="TEXT"
	Content string

	//ptah:schema:field name="status" type="status_type" not_null="true" default="draft"
	Status StatusType

	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time

	//ptah:schema:field name="updated_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	UpdatedAt time.Time
}

//ptah:schema:index table="articles" name="idx_articles_status" columns="status"
`
}

// CustomEntity creates a custom entity with the given table name and fields
func (et *EntityTemplate) CustomEntity(tableName, structName string, fields []EntityField) string {
	var sb strings.Builder

	sb.WriteString("package entities\n\n")
	sb.WriteString("import \"time\"\n\n")
	fmt.Fprintf(&sb, "//ptah:schema:table name=\"%s\"\n", tableName)
	fmt.Fprintf(&sb, "type %s struct {\n", structName)

	for _, field := range fields {
		fmt.Fprintf(&sb, "\t//ptah:schema:field name=\"%s\" type=\"%s\"", field.Name, field.Type)

		if field.Primary {
			sb.WriteString(" primary=\"true\"")
		}
		if field.NotNull {
			sb.WriteString(" not_null=\"true\"")
		}
		if field.Unique {
			sb.WriteString(" unique=\"true\"")
		}
		if field.Default != "" {
			fmt.Fprintf(&sb, " default=\"%s\"", field.Default)
		}
		if field.DefaultExpr != "" {
			fmt.Fprintf(&sb, " default_expr=\"%s\"", field.DefaultExpr)
		}
		if field.Foreign != "" {
			fmt.Fprintf(&sb, " foreign=\"%s\"", field.Foreign)
		}

		sb.WriteString("\n")
		fmt.Fprintf(&sb, "\t%s %s\n\n", field.GoName, field.GoType)
	}

	sb.WriteString("}\n")
	return sb.String()
}

// EntityField represents a field in a custom entity
type EntityField struct {
	Name        string // Database column name
	GoName      string // Go field name
	Type        string // Database type
	GoType      string // Go type
	Primary     bool
	NotNull     bool
	Unique      bool
	Default     string
	DefaultExpr string // Default expression / function call (rendered as default_expr=...)
	Foreign     string // Foreign-key target, e.g. "users(id)" (rendered as foreign=...)
}
