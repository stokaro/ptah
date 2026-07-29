package stubs

// This file demonstrates the difference between verbose syntax and simplified syntax.

// VERBOSE SYNTAX
//
//ptah:schema:table name="verbose_syntax_users"
type VerboseSyntaxUser struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true" not_null="true"
	ID int `db:"id"`

	//ptah:schema:field name="email" type="VARCHAR(255)" unique="true" not_null="true"
	//ptah:schema:index name="idx_verbose_syntax_users_email" fields="email"
	Email string `db:"email"`

	//ptah:schema:field name="is_active" type="BOOLEAN" not_null="true" default_expr="true"
	IsActive bool `db:"is_active"`

	//ptah:schema:field name="description" type="TEXT"
	Description *string `db:"description"`
}

// SIMPLIFIED SYNTAX (recommended)
//
//ptah:schema:table name="new_syntax_users"
type NewSyntaxUser struct {
	//ptah:schema:field name="id" type="SERIAL" primary not_null
	ID int `db:"id"`

	//ptah:schema:field name="email" type="VARCHAR(255)" unique not_null
	//ptah:schema:index name="idx_new_syntax_users_email" fields="email"
	Email string `db:"email"`

	//ptah:schema:field name="is_active" type="BOOLEAN" not_null default_expr="true"
	IsActive bool `db:"is_active"`

	//ptah:schema:field name="description" type="TEXT"
	Description *string `db:"description"`
}

// MIXED SYNTAX (also supported - you can mix both styles)
//
//ptah:schema:table name="mixed_syntax_posts"
type MixedSyntaxPost struct {
	//ptah:schema:field name="id" type="SERIAL" primary not_null
	ID int `db:"id"`

	//ptah:schema:field name="title" type="VARCHAR(255)" not_null unique="true"
	//ptah:schema:index name="idx_mixed_syntax_posts_title" fields="title"
	Title string `db:"title"`

	//ptah:schema:field name="content" type="TEXT"
	Content *string `db:"content"`

	//ptah:schema:field name="view_count" type="INTEGER" not_null default_expr="0" check="view_count >= 0"
	ViewCount int `db:"view_count"`

	//ptah:schema:field name="is_published" type="BOOLEAN" not_null="false" default_expr="false"
	IsPublished bool `db:"is_published"`
}

// EMBEDDED FIELDS WITH SIMPLIFIED SYNTAX
//
//ptah:schema:embed
type ModernTimestamps struct {
	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null default_expr="CURRENT_TIMESTAMP"
	CreatedAt string `db:"created_at"`

	//ptah:schema:field name="updated_at" type="TIMESTAMP"
	UpdatedAt *string `db:"updated_at"`
}

//ptah:schema:table name="modern_articles"
type ModernArticle struct {
	//ptah:schema:field name="id" type="SERIAL" primary not_null
	ID int `db:"id"`

	//ptah:schema:field name="title" type="VARCHAR(255)" not_null unique
	//ptah:schema:index name="idx_modern_articles_title" fields="title"
	Title string `db:"title"`

	//ptah:embedded mode="inline"
	ModernTimestamps

	//ptah:embedded mode="json" name="metadata" type="JSONB" platform.mysql.type="JSON"
	Metadata map[string]any `json:"metadata"`
}
