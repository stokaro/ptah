package entities

//ptah:schema:table name="products" comment="Product catalog table"
type Product struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64 `json:"id" db:"id"`

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string `json:"name" db:"name"`

	//ptah:schema:field name="price" type="DECIMAL(10,2)" not_null="true"
	Price float64 `json:"price" db:"price"`

	//ptah:schema:field name="created_at" type="TIMESTAMP" default_expr="NOW()"
	CreatedAt string `json:"created_at" db:"created_at"`
}
