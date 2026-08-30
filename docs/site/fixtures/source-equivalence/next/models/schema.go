// Package models contains the post-adoption schema fixture.
package models

//ptah:schema:table name="authors"
type Author struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true" not_null="true"
	ID int64

	//ptah:schema:field name="name" type="VARCHAR(120)" not_null="true"
	//ptah:schema:index name="idx_authors_name" fields="name" unique="true"
	Name string
}

//ptah:schema:table name="books"
type Book struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true" not_null="true"
	ID int64

	//ptah:schema:field name="author_id" type="INTEGER" not_null="true" foreign="authors(id)" foreign_key_name="fk_books_author_id"
	AuthorID int64

	//ptah:schema:field name="title" type="VARCHAR(240)" not_null="true"
	Title string

	//ptah:schema:field name="summary" type="TEXT"
	Summary string

	//ptah:schema:field name="published_at" type="TEXT"
	PublishedAt string
}

//ptah:schema:table name="tags"
type Tag struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true" not_null="true"
	ID int64

	//ptah:schema:field name="name" type="VARCHAR(80)" not_null="true"
	Name string
}

//ptah:schema:table name="book_tags"
type BookTag struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true" not_null="true"
	ID int64

	//ptah:schema:field name="book_id" type="INTEGER" not_null="true" foreign="books(id)" foreign_key_name="fk_book_tags_book_id"
	BookID int64

	//ptah:schema:field name="tag_id" type="INTEGER" not_null="true" foreign="tags(id)" foreign_key_name="fk_book_tags_tag_id"
	//ptah:schema:index name="idx_book_tags_pair" fields="book_id,tag_id" unique="true"
	TagID int64
}
