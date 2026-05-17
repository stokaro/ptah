package entities

import "time"

// Timestamps represents common timestamp fields that can be embedded in other entities
type Timestamps struct {
	//migrator:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time

	//migrator:schema:field name="updated_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	UpdatedAt time.Time
}

// AuditInfo represents audit information that can be embedded with prefix
type AuditInfo struct {
	//migrator:schema:field name="by" type="VARCHAR(255)"
	By string

	//migrator:schema:field name="reason" type="TEXT"
	Reason string
}

// Metadata represents metadata that can be embedded as JSON
type Metadata struct {
	//migrator:schema:field name="author" type="VARCHAR(255)"
	Author string

	//migrator:schema:field name="source" type="VARCHAR(255)"
	Source string

	//migrator:schema:field name="tags" type="TEXT"
	Tags string
}

// SkippedInfo represents information that should be skipped in embedding
type SkippedInfo struct {
	//migrator:schema:field name="internal_data" type="TEXT"
	InternalData string

	//migrator:schema:field name="temp_field" type="INTEGER"
	TempField int
}
