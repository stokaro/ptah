package stubs

import "time"

//ptah:schema:table name="simplified_users"
type SimplifiedUser struct {
	//ptah:schema:field name="id" type="SERIAL" primary not_null
	ID int `db:"id"`

	//ptah:schema:field name="email" type="VARCHAR(255)" unique not_null
	Email string `db:"email"`

	//ptah:schema:field name="username" type="VARCHAR(100)" unique not_null
	//ptah:schema:index name="idx_simplified_users_username" fields="username"
	Username string `db:"username"`

	//ptah:schema:field name="password_hash" type="TEXT" not_null
	PasswordHash string `db:"password_hash"`

	//ptah:schema:field name="is_active" type="BOOLEAN" not_null default_expr="true"
	IsActive bool `db:"is_active"`

	//ptah:schema:field name="is_admin" type="BOOLEAN" not_null default_expr="false"
	IsAdmin bool `db:"is_admin"`

	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time `db:"created_at"`

	//ptah:schema:field name="updated_at" type="TIMESTAMP"
	UpdatedAt *time.Time `db:"updated_at"`
}

//ptah:schema:table name="simplified_posts"
type SimplifiedPost struct {
	//ptah:schema:field name="id" type="SERIAL" primary not_null
	ID int `db:"id"`

	//ptah:schema:field name="user_id" type="INTEGER" not_null foreign="simplified_users(id)" foreign_key_name="fk_post_user"
	UserID int `db:"user_id"`

	//ptah:schema:field name="title" type="VARCHAR(255)" not_null
	//ptah:schema:index name="idx_simplified_posts_title" fields="title"
	Title string `db:"title"`

	//ptah:schema:field name="content" type="TEXT"
	Content *string `db:"content"`

	//ptah:schema:field name="is_published" type="BOOLEAN" not_null default_expr="false"
	IsPublished bool `db:"is_published"`

	//ptah:schema:field name="view_count" type="INTEGER" not_null default_expr="0" check="view_count >= 0"
	ViewCount int `db:"view_count"`

	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time `db:"created_at"`
}

//ptah:schema:index name="idx_posts_user_published" fields="user_id,is_published"
var _ = SimplifiedPost{}

// Embedded types with simplified syntax
type SimpleTimestamps struct {
	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time `db:"created_at"`

	//ptah:schema:field name="updated_at" type="TIMESTAMP"
	UpdatedAt *time.Time `db:"updated_at"`
}

type SimpleAudit struct {
	//ptah:schema:field name="created_by" type="VARCHAR(100)"
	CreatedBy *string `db:"created_by"`

	//ptah:schema:field name="updated_by" type="VARCHAR(100)"
	UpdatedBy *string `db:"updated_by"`
}

//ptah:schema:table name="simplified_articles"
type SimplifiedArticle struct {
	//ptah:schema:field name="id" type="SERIAL" primary not_null
	ID int `db:"id"`

	//ptah:schema:field name="title" type="VARCHAR(255)" not_null unique
	Title string `db:"title"`

	//ptah:schema:field name="slug" type="VARCHAR(255)" not_null unique
	//ptah:schema:index name="idx_simplified_articles_slug" fields="slug"
	Slug string `db:"slug"`

	//ptah:embedded mode="inline"
	SimpleTimestamps

	//ptah:embedded mode="inline" prefix="audit_"
	SimpleAudit

	//ptah:embedded mode="json" name="metadata" type="JSONB" platform.mysql.type="JSON" platform.mariadb.type="LONGTEXT"
	Metadata map[string]any `json:"metadata"`

	//ptah:embedded mode="relation" field="author_id" ref="simplified_users(id)" on_delete="CASCADE"
	Author SimplifiedUser
}
