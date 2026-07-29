package stubs

//ptah:schema:table name="articles"
type Article struct {
	//ptah:schema:field name="id" type="INTEGER" primary not_null auto_increment
	ID int `db:"id"`

	//ptah:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string `db:"title"`

	//ptah:embedded mode="inline"
	Timestamps // Injects created_at, updated_at columns

	//ptah:embedded mode="inline" prefix="audit_"
	AuditInfo // Injects audit_by, audit_reason columns

	//ptah:embedded mode="json" name="meta_data" type="JSONB" platform.mysql.type="JSON" platform.mariadb.type="LONGTEXT" platform.mariadb.check="JSON_VALID(meta_data)"
	Meta // Injected as a single JSONB column named meta_data

	//ptah:embedded mode="relation" field="author_id" ref="users(id)" on_delete="CASCADE"
	Author User // Generates author_id INT + FK constraint
}
