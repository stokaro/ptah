package entities

//ptah:schema:table name="posts"
type Post struct {
	//ptah:embedded mode="inline"
	BaseID

	//ptah:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string

	//ptah:schema:field name="content" type="TEXT" not_null="true"
	Content string

	//ptah:schema:field name="user_id" type="INTEGER" not_null="true"
	UserID int64

	//ptah:schema:field name="status" type="ENUM" enum="draft,published,archived" not_null="true" default="draft"
	Status string

	//ptah:embedded mode="inline"
	Timestamps
}

//ptah:schema:index table="posts" name="idx_posts_user_id" columns="user_id"
//ptah:schema:index table="posts" name="idx_posts_status" columns="status"
//ptah:schema:foreign_key table="posts" name="fk_posts_user_id" columns="user_id" ref_table="users" ref_columns="id" on_delete="CASCADE"
