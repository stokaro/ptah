package entities

// BlogPost demonstrates all embedding modes using pointer types
// This entity tests that pointer embedded fields (*BaseID, *Timestamps, etc.)
// generate the same database schema as value embedded fields
//
//migrator:schema:table name="blog_posts"
type BlogPost struct {
	// Mode 1: inline with pointer - Should inject individual fields as separate columns
	//migrator:embedded mode="inline"
	*BaseID // Results in: id column (same as value BaseID)

	//migrator:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string

	//migrator:schema:field name="content" type="TEXT" not_null="true"
	Content string

	//migrator:schema:field name="slug" type="VARCHAR(255)" not_null="true" unique="true"
	Slug string

	// Mode 1: inline with pointer - Should inject timestamp fields
	//migrator:embedded mode="inline"
	*Timestamps // Results in: created_at, updated_at columns (same as value Timestamps)

	// Mode 2: inline with prefix and pointer - Should inject fields with prefix
	//migrator:embedded mode="inline" prefix="audit_"
	*AuditInfo // Results in: audit_by, audit_reason columns (same as value AuditInfo)

	// Mode 3: json with pointer - Should serialize struct into one JSON/JSONB column
	//migrator:embedded mode="json" name="meta_data" type="JSONB" platform.mysql.type="JSON" platform.mariadb.type="LONGTEXT" platform.mariadb.check="JSON_VALID(meta_data)"
	*Metadata // Results in: meta_data JSONB column (same as value Metadata)

	// Mode 4: relation with pointer - Should add foreign key field + constraint
	//migrator:embedded mode="relation" field="author_id" ref="users(id)" on_delete="CASCADE"
	*User // Results in: author_id BIGINT + FK constraint (same as value User)

	// Mode 5: skip with pointer - Should ignore this embedded field completely
	//migrator:embedded mode="skip"
	*SkippedInfo // Results in: nothing (ignored, same as value SkippedInfo)

	//migrator:schema:field name="published" type="BOOLEAN" not_null="true" default="false"
	Published bool

	//migrator:schema:field name="view_count" type="INTEGER" not_null="true" default="0"
	ViewCount int
}
