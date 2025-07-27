package entities

//migrator:schema:table name="products"
type Product struct {
	//migrator:embedded mode="inline"
	BaseID

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//migrator:schema:field name="description" type="TEXT"
	Description string

	//migrator:schema:field name="category" type="VARCHAR(100)"
	Category string

	//migrator:schema:field name="price" type="DECIMAL(10,2)" not_null="true"
	Price float64

	//migrator:schema:field name="status" type="ENUM" enum="draft,active,discontinued" not_null="true" default="draft"
	Status string

	//migrator:embedded mode="inline"
	Timestamps
}
