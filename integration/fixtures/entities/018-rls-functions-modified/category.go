package entities

//migrator:schema:table name="categories"
type Category struct {
	//migrator:embedded mode="inline"
	BaseID

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true" unique="true"
	Name string

	//migrator:schema:field name="description" type="TEXT"
	Description string

	//migrator:schema:field name="parent_id" type="BIGINT"
	ParentID *int64

	//migrator:embedded mode="inline"
	Timestamps
}

//migrator:schema:index table="categories" name="idx_categories_parent_id" columns="parent_id"
//migrator:schema:foreign_key table="categories" name="fk_categories_parent_id" columns="parent_id" ref_table="categories" ref_columns="id" on_delete="CASCADE"
