package entities

//migrator:schema:table name="posts"
type Post struct {
	//migrator:embedded mode="inline"
	BaseID

	//migrator:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string

	//migrator:schema:field name="content" type="TEXT" not_null="true"
	Content string

	//migrator:schema:field name="user_id" type="BIGINT" not_null="true"
	UserID int64

	//migrator:schema:field name="status" type="ENUM" enum="draft,published,archived" not_null="true" default="draft"
	Status string

	//migrator:embedded mode="inline"
	Timestamps
}

//migrator:schema:index table="posts" name="idx_posts_user_id" columns="user_id"
//migrator:schema:index table="posts" name="idx_posts_status" columns="status"
//migrator:schema:foreign_key table="posts" name="fk_posts_user_id" columns="user_id" ref_table="users" ref_columns="id" on_delete="CASCADE"
