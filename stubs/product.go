// Package stubs contains annotated Go entities exercising the Ptah annotation
// surface end to end. The files serve as parser input for goschema tests and
// demos; they are parsed from source rather than imported.
package stubs

//ptah:schema:table name="products" platform.mysql.engine="InnoDB" platform.mysql.comment="Product catalog" platform.mariadb.engine="InnoDB" platform.mariadb.comment="Product catalog"
type Product struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true" platform.mysql.type="INT AUTO_INCREMENT" platform.mariadb.type="INT AUTO_INCREMENT"
	ID int64

	//ptah:schema:field name="sku" type="VARCHAR(50)" not_null="true" unique="true"
	SKU string

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//ptah:schema:field name="description" type="TEXT" not_null="false"
	Description string

	//ptah:schema:field name="price" type="DECIMAL(10,2)" not_null="true" check="price > 0"
	Price float64

	//ptah:schema:field name="status" type="ENUM" enum="active,inactive,discontinued,out_of_stock" not_null="true" default="active"
	Status string

	//ptah:schema:field name="category_id" type="INT" not_null="true" foreign="categories(id)" foreign_key_name="fk_product_category"
	CategoryID int64

	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="NOW()"
	CreatedAt string

	//ptah:schema:field name="updated_at" type="TIMESTAMP" not_null="false"
	UpdatedAt string

	//ptah:schema:field name="in_stock" type="BOOLEAN" not_null="true" default_expr="true"
	InStock bool

	//ptah:schema:index name="idx_products_category" fields="category_id"
	_ int
}
