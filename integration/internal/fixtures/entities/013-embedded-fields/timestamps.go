package entities

import "time"

// Timestamps represents common timestamp fields that can be embedded in other entities
type Timestamps struct {
	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time

	//ptah:schema:field name="updated_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	UpdatedAt time.Time
}

// AuditInfo represents audit information that can be embedded with prefix
type AuditInfo struct {
	//ptah:schema:field name="by" type="VARCHAR(255)"
	By string

	//ptah:schema:field name="reason" type="TEXT"
	Reason string
}

// Metadata represents metadata that can be embedded as JSON
type Metadata struct {
	//ptah:schema:field name="author" type="VARCHAR(255)"
	Author string

	//ptah:schema:field name="source" type="VARCHAR(255)"
	Source string

	//ptah:schema:field name="tags" type="TEXT"
	Tags string
}

// SkippedInfo represents information that should be skipped in embedding
type SkippedInfo struct {
	//ptah:schema:field name="internal_data" type="TEXT"
	InternalData string

	//ptah:schema:field name="temp_field" type="INTEGER"
	TempField int
}
