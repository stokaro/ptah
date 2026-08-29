package models

//ptah:schema:table name="customers" comment="Customers who can place orders"
type Customer struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true" auto_increment="true" not_null="true"
	ID int64
	//ptah:schema:field name="email" type="TEXT" not_null="true" unique="true"
	Email string
	//ptah:schema:field name="country" type="TEXT" not_null="true"
	//ptah:schema:index name="idx_customers_country" fields="country"
	Country string
}

//ptah:schema:table name="orders" comment="Orders placed by customers"
type Order struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true" auto_increment="true" not_null="true"
	ID int64
	//ptah:schema:field name="customer_id" type="INTEGER" not_null="true" foreign="customers(id)"
	CustomerID int64
	//ptah:schema:field name="status" type="TEXT" not_null="true" default="pending"
	Status string
}
