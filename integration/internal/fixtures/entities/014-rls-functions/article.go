package entities

// Article demonstrates all embedding modes in a single table
//
//ptah:schema:table name="articles"
type Article struct {
	//ptah:embedded mode="inline"
	BaseID

	//ptah:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string

	//ptah:schema:field name="content" type="TEXT" not_null="true"
	Content string

	// Mode 1: inline - Injects individual fields as separate columns
	//ptah:embedded mode="inline"
	Timestamps // Results in: created_at, updated_at columns

	// Mode 2: inline with prefix - Injects fields with prefix
	//ptah:embedded mode="inline" prefix="audit_"
	AuditInfo // Results in: audit_by, audit_reason columns

	// Mode 3: json - Serializes struct into one JSON/JSONB column
	//ptah:embedded mode="json" name="meta_data" type="JSONB" platform.mysql.type="JSON" platform.mariadb.type="LONGTEXT" platform.mariadb.check="JSON_VALID(meta_data)"
	Metadata // Results in: meta_data JSONB column

	// Mode 4: relation - Adds foreign key field + constraint
	//ptah:embedded mode="relation" field="author_id" ref="users(id)" on_delete="CASCADE"
	Author User // Results in: author_id BIGINT + FK constraint

	// Mode 5: skip - Ignores this embedded field completely
	//ptah:embedded mode="skip"
	SkippedField SkippedInfo // Results in: nothing (ignored)
}
