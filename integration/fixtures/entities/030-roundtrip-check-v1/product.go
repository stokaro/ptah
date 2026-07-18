package entities

//migrator:schema:table name="products"
type Product struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="quantity" type="INTEGER" not_null="true" check="quantity > 0" check_name="products_quantity_check"
	Quantity int
}
