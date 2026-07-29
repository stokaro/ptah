package entities

//ptah:schema:table name="products"
type Product struct {
	//ptah:embedded mode="inline"
	BaseID

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//ptah:schema:field name="description" type="TEXT"
	Description string

	//ptah:schema:field name="category" type="VARCHAR(100)"
	Category string

	//ptah:schema:field name="price" type="DECIMAL(10,2)" not_null="true"
	Price float64

	//ptah:schema:field name="status" type="ENUM" enum="draft,active,discontinued" not_null="true" default="draft"
	Status string

	//ptah:embedded mode="inline"
	Timestamps
}
