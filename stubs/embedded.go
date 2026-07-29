package stubs

import "time"

// This file demonstrates that embedded types can be defined separately
// from the tables that use them, but they need to be in the same file
// for the single-file parser to work correctly.

//ptah:schema:table name="embedded_example"
type EmbeddedExample struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true" not_null
	ID int `db:"id"`

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null
	Name string `db:"name"`
}

// Embedded type definitions
//
//ptah:schema:embed
type Timestamps struct {
	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time `db:"created_at"`

	//ptah:schema:field name="updated_at" type="TIMESTAMP" not_null default_expr="CURRENT_TIMESTAMP"
	UpdatedAt time.Time `db:"updated_at"`
}

//ptah:schema:embed
type AuditInfo struct {
	//ptah:schema:field name="by" type="TEXT"
	By string `db:"by"`

	//ptah:schema:field name="reason" type="TEXT"
	Reason string `db:"reason"`
}

//ptah:schema:embed
type Meta struct {
	Author string
	Source string
}

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" auto_increment="true" primary="true" not_null
	ID int `db:"id"`

	//ptah:schema:field name="email" type="VARCHAR(255)" unique="true" not_null
	Email string `db:"email"`
}
