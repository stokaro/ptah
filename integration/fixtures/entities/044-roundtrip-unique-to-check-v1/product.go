package entities

//migrator:schema:table name="products"
type Product struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="sku" type="VARCHAR(64)" not_null="true"
	SKU string

	//migrator:schema:field name="quantity" type="INTEGER" not_null="true"
	Quantity int
}

//migrator:schema:constraint name="products_quantity_guard" type="UNIQUE" table="products" columns="sku"
