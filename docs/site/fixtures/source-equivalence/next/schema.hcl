table "authors" {
  column "id" {
    type = integer
  }
  column "name" {
    type = varchar(120)
    null = false
  }
  primary_key {
    columns = [column.id]
  }
  index "idx_authors_name" {
    unique  = true
    columns = [column.name]
  }
}

table "books" {
  column "id" {
    type = integer
  }
  column "author_id" {
    type = integer
    null = false
  }
  column "title" {
    type = varchar(240)
    null = false
  }
  column "summary" {
    type = text
    null = true
  }
  column "published_at" {
    type = text
    null = true
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "fk_books_author_id" {
    columns     = [column.author_id]
    ref_columns = [table.authors.column.id]
  }
}

table "tags" {
  column "id" {
    type = integer
  }
  column "name" {
    type = varchar(80)
    null = false
  }
  primary_key {
    columns = [column.id]
  }
}

table "book_tags" {
  column "id" {
    type = integer
  }
  column "book_id" {
    type = integer
    null = false
  }
  column "tag_id" {
    type = integer
    null = false
  }
  primary_key {
    columns = [column.id]
  }
  index "idx_book_tags_pair" {
    unique  = true
    columns = [column.book_id, column.tag_id]
  }
  foreign_key "fk_book_tags_book_id" {
    columns     = [column.book_id]
    ref_columns = [table.books.column.id]
  }
  foreign_key "fk_book_tags_tag_id" {
    columns     = [column.tag_id]
    ref_columns = [table.tags.column.id]
  }
}
