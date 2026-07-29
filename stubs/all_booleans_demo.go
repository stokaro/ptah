package stubs

// Demonstration of all boolean attributes working with simplified syntax

//ptah:schema:table name="boolean_demo"
type BooleanDemo struct {
	// Primary key with simplified syntax
	//ptah:schema:field name="id" type="SERIAL" primary not_null
	ID int `db:"id"`

	// Unique field with a separate index directive
	//ptah:schema:field name="email" type="VARCHAR(255)" unique not_null
	//ptah:schema:index name="idx_boolean_demo_email" fields="email"
	Email string `db:"email"`

	// Auto increment field (alternative syntax)
	//ptah:schema:field name="sequence_id" type="INTEGER" auto_increment unique
	SequenceID int `db:"sequence_id"`

	// Nullable field (nullable is the default when not_null is omitted)
	//ptah:schema:field name="description" type="TEXT"
	Description *string `db:"description"`

	// Boolean field with default
	//ptah:schema:field name="is_active" type="BOOLEAN" not_null default_expr="true"
	IsActive bool `db:"is_active"`

	// Boolean field following naming pattern (automatically detected as boolean)
	//ptah:schema:field name="has_permission" type="BOOLEAN" not_null default_expr="false"
	HasPermission bool `db:"has_permission"`

	// Field with canonical boolean attributes combined
	//ptah:schema:field name="special_code" type="VARCHAR(50)" unique not_null
	//ptah:schema:index name="idx_boolean_demo_special_code" fields="special_code"
	SpecialCode string `db:"special_code"`

	// Mixed quoted and bare canonical syntax
	//ptah:schema:field name="status" type="VARCHAR(20)" not_null="true" unique default="pending"
	//ptah:schema:index name="idx_boolean_demo_status" fields="status"
	Status string `db:"status"`
}
