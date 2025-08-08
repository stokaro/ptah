package entities

// Article demonstrates all embedding modes in a single table
//
//migrator:schema:table name="articles"
type Article struct {
	//migrator:embedded mode="inline"
	BaseID

	//migrator:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string

	//migrator:schema:field name="content" type="TEXT" not_null="true"
	Content string

	// Mode 1: inline - Injects individual fields as separate columns
	//migrator:embedded mode="inline"
	Timestamps // Results in: created_at, updated_at columns

	// Mode 2: inline with prefix - Injects fields with prefix
	//migrator:embedded mode="inline" prefix="audit_"
	AuditInfo // Results in: audit_by, audit_reason columns

	// Mode 3: json - Serializes struct into one JSON/JSONB column
	//migrator:embedded mode="json" name="meta_data" type="JSONB" platform.mysql.type="JSON" platform.mariadb.type="LONGTEXT" platform.mariadb.check="JSON_VALID(meta_data)"
	Metadata // Results in: meta_data JSONB column

	// Mode 4: relation - Adds foreign key field + constraint
	//migrator:embedded mode="relation" field="author_id" ref="users(id)" on_delete="CASCADE"
	Author User // Results in: author_id BIGINT + FK constraint

	// Mode 5: skip - Ignores this embedded field completely
	//migrator:embedded mode="skip"
	SkippedField SkippedInfo // Results in: nothing (ignored)
}
