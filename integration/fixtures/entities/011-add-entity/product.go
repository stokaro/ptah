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

	//ptah:schema:field name="price" type="DECIMAL(10,2)" not_null="true"
	Price float64

	//ptah:schema:field name="stock_quantity" type="INTEGER" not_null="true" default_expr="0"
	StockQuantity int

	//ptah:schema:field name="category_id" type="BIGINT"
	CategoryID *int64

	//ptah:schema:field name="status" type="ENUM" enum="available,discontinued,out_of_stock" not_null="true" default="available"
	Status string

	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time

	//ptah:schema:field name="updated_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	UpdatedAt time.Time
}

//ptah:schema:index table="products" name="idx_products_name" columns="name"
//ptah:schema:index table="products" name="idx_products_status" columns="status"
//ptah:schema:index table="products" name="idx_products_category_id" columns="category_id"
//ptah:schema:check_constraint table="products" name="chk_products_price_positive" condition="price > 0"
//ptah:schema:check_constraint table="products" name="chk_products_stock_non_negative" condition="stock_quantity >= 0"
//ptah:schema:foreign_key table="products" name="fk_products_category_id" columns="category_id" ref_table="categories" ref_columns="id" on_delete="SET NULL"
