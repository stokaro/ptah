package entities

//migrator:schema:table name="products" comment="Product catalog table"
type Product struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64 `json:"id" db:"id"`

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string `json:"name" db:"name"`

	//migrator:schema:field name="price" type="DECIMAL(10,2)" not_null="true"
	Price float64 `json:"price" db:"price"`

	//migrator:schema:field name="created_at" type="TIMESTAMP" default_fn="NOW()"
	CreatedAt string `json:"created_at" db:"created_at"`
}
