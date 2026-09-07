package entities

import "time"

//ptah:schema:table name="categories"
type Category struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true" unique="true"
	Name string

	//ptah:schema:field name="description" type="TEXT"
	Description string

	//ptah:schema:field name="parent_id" type="BIGINT"
	ParentID *int64

	//ptah:schema:field name="sort_order" type="INTEGER" not_null="true" default_expr="0"
	SortOrder int

	//ptah:schema:field name="is_active" type="BOOLEAN" not_null="true" default_expr="true"
	IsActive bool

	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time

	//ptah:schema:field name="updated_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	UpdatedAt time.Time
}

//ptah:schema:index table="categories" name="idx_categories_name" columns="name"
//ptah:schema:index table="categories" name="idx_categories_parent_id" columns="parent_id"
//ptah:schema:index table="categories" name="idx_categories_sort_order" columns="sort_order"
//ptah:schema:index table="categories" name="idx_categories_active" columns="is_active"
//ptah:schema:foreign_key table="categories" name="fk_categories_parent_id" columns="parent_id" ref_table="categories" ref_columns="id" on_delete="SET NULL"
//ptah:schema:check_constraint table="categories" name="chk_categories_sort_order_non_negative" condition="sort_order >= 0"
