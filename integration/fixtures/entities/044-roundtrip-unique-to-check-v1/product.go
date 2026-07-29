package entities

//ptah:schema:table name="products"
type Product struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="sku" type="VARCHAR(64)" not_null="true"
	SKU string

	//ptah:schema:field name="quantity" type="INTEGER" not_null="true"
	Quantity int
}

//ptah:schema:constraint name="products_quantity_guard" type="UNIQUE" table="products" columns="sku"
