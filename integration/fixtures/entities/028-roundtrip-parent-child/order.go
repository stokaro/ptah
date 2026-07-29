package entities

//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="customer_id" type="INTEGER" not_null="true" foreign="customers(id)"
	CustomerID int64
}
