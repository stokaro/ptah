package entities

import (
	"context"
	"time"
)

var (
	_ any = (*FileEntity)(nil)
)

// FileEntity represents a file entity in the system
//
// Enable RLS for multi-tenant isolation
//
//migrator:schema:rls:enable table="files" comment="Enable RLS for multi-tenant file isolation"
//migrator:schema:rls:policy name="file_tenant_isolation" table="files" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()" with_check="tenant_id = get_current_tenant_id()" comment="Ensures files can only be accessed and modified by their tenant"
//migrator:schema:table name="files"
type FileEntity struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64 `json:"id" db:"id"`

	//migrator:schema:field name="tenant_id" type="TEXT" not_null="true"
	TenantID string `json:"tenant_id" db:"tenant_id"`

	//migrator:schema:field name="title" type="TEXT"
	Title string `json:"title" db:"title"`

	//migrator:schema:field name="path" type="TEXT" not_null="true"
	Path string `json:"path" db:"path"`

	//migrator:schema:field name="created_at" type="TIMESTAMP" not_null="true"
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

func (*FileEntity) Validate() error {
	return nil
}

func (fe *FileEntity) ValidateWithContext(ctx context.Context) error {
	return nil
}
