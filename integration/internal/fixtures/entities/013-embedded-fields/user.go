package entities

//ptah:schema:table name="users"
type User struct {
	//ptah:embedded mode="inline"
	BaseID

	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//ptah:schema:field name="user_age" type="SMALLINT"
	UserAge int16

	//ptah:schema:field name="description" type="VARCHAR(500)"
	Description string

	//ptah:schema:field name="status" type="ENUM" enum="active,inactive,suspended" not_null="true" default="active"
	Status string

	//ptah:embedded mode="inline"
	Timestamps
}
