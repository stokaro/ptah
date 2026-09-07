package entities

//ptah:schema:table name="categories"
type Category struct {
	//ptah:embedded mode="inline"
	BaseID

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true" unique="true"
	Name string

	//ptah:schema:field name="description" type="TEXT"
	Description string

	//ptah:schema:field name="parent_id" type="BIGINT"
	ParentID *int64

	//ptah:embedded mode="inline"
	Timestamps
}

//ptah:schema:index table="categories" name="idx_categories_parent_id" columns="parent_id"
//ptah:schema:foreign_key table="categories" name="fk_categories_parent_id" columns="parent_id" ref_table="categories" ref_columns="id" on_delete="CASCADE"
