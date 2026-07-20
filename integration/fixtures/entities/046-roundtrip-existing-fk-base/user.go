package entities

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="account_id" type="INTEGER"
	AccountID *int64

	//migrator:schema:field name="manager_id" type="INTEGER"
	ManagerID *int64

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string
}
