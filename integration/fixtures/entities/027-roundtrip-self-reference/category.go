package entities

//migrator:schema:table name="categories"
type Category struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//migrator:schema:field name="parent_id" type="INTEGER" foreign="categories(id)" on_delete="SET NULL"
	ParentID *int64
}
