package entities

import "time"

//ptah:schema:table name="products"
type Product struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//ptah:schema:field name="description" type="TEXT"
	Description string

	//ptah:schema:field name="category" type="VARCHAR(100)"
	Category string

	//ptah:schema:field name="price" type="DECIMAL(10,2)" not_null="true"
	Price float64

	//ptah:schema:field name="active" type="BOOLEAN" not_null="true" default_expr="true"
	Active bool

	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time

	//ptah:schema:field name="updated_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	UpdatedAt time.Time
}

//ptah:schema:index table="products" name="idx_products_name" columns="name"
//ptah:schema:index table="products" name="idx_products_category" columns="category"
//ptah:schema:index table="products" name="idx_products_active" columns="active"
