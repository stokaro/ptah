package entities

//migrator:schema:table name="orders"
type Order struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="customer_id" type="INTEGER" not_null="true" foreign="customers(id)"
	CustomerID int64
}
