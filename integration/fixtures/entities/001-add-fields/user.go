package entities

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
