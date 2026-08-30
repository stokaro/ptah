// Package models declares the baseline schema used by documentation UI fixtures.
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

//ptah:schema:table name="products" comment="Products available for ordering"
type Product struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true" auto_increment="true" not_null="true"
	ID int64
	//ptah:schema:field name="sku" type="TEXT" not_null="true" unique="true"
	SKU string
	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string
}

//ptah:schema:table name="order_items" comment="Products and quantities attached to an order"
type OrderItem struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true" auto_increment="true" not_null="true"
	ID int64
	//ptah:schema:field name="order_id" type="INTEGER" not_null="true" foreign="orders(id)"
	OrderID int64
	//ptah:schema:field name="product_id" type="INTEGER" not_null="true" foreign="products(id)"
	ProductID int64
	//ptah:schema:field name="quantity" type="INTEGER" not_null="true" default="1"
	Quantity int
}

//ptah:schema:role name="reporting" dialects="postgres"
//ptah:schema:grant role="reporting" privilege="SELECT" on_table="orders" dialects="postgres"
//ptah:schema:grant role="reporting" privilege="SELECT" on_table="products" dialects="postgres"
//ptah:schema:grant role="PUBLIC" privilege="SELECT" on_table="orders" dialects="postgres"
//ptah:schema:function name="refresh_order_totals" returns="VOID" language="plpgsql" security="DEFINER" body="BEGIN RETURN; END;" dialects="postgres"
type SecurityDeclarations struct{}
