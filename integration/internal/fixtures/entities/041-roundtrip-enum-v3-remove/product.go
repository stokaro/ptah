package entities

//ptah:schema:table name="products"
type Product struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="status" type="ENUM" enum="draft,active" not_null="true" default="draft"
	Status string
}
