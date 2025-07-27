package entities

//migrator:schema:table name="users"
type User struct {
	//migrator:embedded mode="inline"
	BaseID

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//migrator:schema:field name="user_age" type="SMALLINT"
	UserAge int16

	//migrator:schema:field name="description" type="VARCHAR(500)"
	Description string

	//migrator:schema:field name="status" type="ENUM" enum="active,inactive,suspended" not_null="true" default="active"
	Status string

	//migrator:embedded mode="inline"
	Timestamps
}
