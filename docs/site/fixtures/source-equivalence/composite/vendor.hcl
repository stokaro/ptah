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
