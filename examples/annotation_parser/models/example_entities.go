package models

//ptah:schema:table name=users engine=InnoDB comment="User accounts"
type User struct {
	//ptah:schema:field name=id type="SERIAL" primary=true
	ID int

	//ptah:schema:field name=email type="TEXT" unique=true not_null=true
	Email string

	//ptah:schema:field name=password_hash type="TEXT" not_null=true
	PasswordHash string

	//ptah:schema:field name=role type="ENUM" enum="admin,user,guest" default="user"
	Role string

	//ptah:schema:field name=created_at type="TIMESTAMP" default_expr="NOW()" not_null=true
	CreatedAt string
}

//ptah:schema:table name=posts engine=InnoDB comment="User posts"
type Post struct {
	//ptah:schema:field name=id type="SERIAL" primary=true
	ID int

	//ptah:schema:field name=user_id type="INT" not_null=true foreign="users(id)" foreign_key_name="fk_posts_user"
	UserID int

	//ptah:schema:field name=title type="TEXT" not_null=true
	Title string

	//ptah:schema:field name=content type="TEXT"
	Content string

	//ptah:schema:field name=created_at type="TIMESTAMP" default_expr="NOW()" not_null=true
	CreatedAt string
}

//ptah:schema:index name=idx_posts_user fields="user_id"
var _ = Post{}
