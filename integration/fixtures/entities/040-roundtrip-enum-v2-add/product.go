package entities

//migrator:schema:table name="products"
type Product struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="status" type="ENUM" enum="draft,active,archived" not_null="true" default="draft"
	Status string
}
