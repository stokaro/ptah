package entities

import "time"

//ptah:schema:table name="posts"
type Post struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="user_id" type="INTEGER" not_null="true" foreign="users(id)"
	UserID int64

	//ptah:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string

	//ptah:schema:field name="content" type="TEXT"
	Content string

	//ptah:schema:field name="published" type="BOOLEAN" not_null="true" default_expr="false"
	Published bool

	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time

	//ptah:schema:field name="updated_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	UpdatedAt time.Time
}

//ptah:schema:index table="posts" name="idx_posts_user_id" columns="user_id"
//ptah:schema:index table="posts" name="idx_posts_published" columns="published"
//ptah:schema:index table="posts" name="idx_posts_title" columns="title"
