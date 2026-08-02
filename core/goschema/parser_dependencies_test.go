package goschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
)

func TestParseFileWithDependencies_RecursivelyIncludesEmbeddedHelpers(t *testing.T) {
	c := qt.New(t)
	directory := c.TempDir()
	mainFile := filepath.Join(directory, "models.go")
	c.Assert(os.WriteFile(mainFile, []byte(`package models

//ptah:schema:table name="tenants"
type Tenant struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}

//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//ptah:embedded mode="inline"
	Ownership
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(directory, "ownership.go"), []byte(`package models

type Ownership struct {
	//ptah:embedded mode="inline"
	TenantKey
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(directory, "tenant_key.go"), []byte(`package models

type TenantKey struct {
	//ptah:schema:field name="tenant_id" type="INTEGER" foreign="tenants(id)"
	TenantID int64
}
`), 0o600), qt.IsNil)

	database, err := goschema.ParseFileWithDependencies(mainFile)

	c.Assert(err, qt.IsNil)
	c.Assert(database.EmbeddedFields, qt.HasLen, 2)
	c.Assert(database.Dependencies["orders"], qt.DeepEquals, []string{"tenants"})
	c.Assert(database.Tables, qt.HasLen, 2)
	c.Assert(database.Tables[0].QualifiedName(), qt.Equals, "tenants")
	c.Assert(database.Tables[1].QualifiedName(), qt.Equals, "orders")
}

func TestParseFileWithDependencies_StopsCyclicInlineHelpers(t *testing.T) {
	c := qt.New(t)
	directory := c.TempDir()
	mainFile := filepath.Join(directory, "models.go")
	c.Assert(os.WriteFile(mainFile, []byte(`package models

//ptah:schema:table name="records"
type Record struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//ptah:embedded mode="inline"
	First
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(directory, "first.go"), []byte(`package models

type First struct {
	//ptah:schema:field name="first_value" type="TEXT"
	Value string

	//ptah:embedded mode="inline"
	*Second
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(directory, "second.go"), []byte(`package models

type Second struct {
	//ptah:schema:field name="second_value" type="TEXT"
	Value string

	//ptah:embedded mode="inline"
	*First
}
`), 0o600), qt.IsNil)

	database, err := goschema.ParseFileWithDependencies(mainFile)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Fields, qt.HasLen, 7)
	c.Assert(database.Fields[0].Name, qt.Equals, "id")
	c.Assert(database.Fields[4].StructName, qt.Equals, "Record")
	c.Assert(database.Fields[4].Name, qt.Equals, "first_value")
	c.Assert(database.Fields[5].Name, qt.Equals, "second_value")
}

func TestParseFileWithDependencies_ExpandsNestedRelationHelper(t *testing.T) {
	c := qt.New(t)
	directory := c.TempDir()
	mainFile := filepath.Join(directory, "models.go")
	c.Assert(os.WriteFile(mainFile, []byte(`package models

//ptah:schema:table name="tenants"
type Tenant struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}

//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//ptah:embedded mode="inline" prefix="owner_"
	Ownership
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(directory, "ownership.go"), []byte(`package models

type Ownership struct {
	//ptah:embedded mode="relation" field="tenant_id" ref="tenants(id)"
	Tenant
}
`), 0o600), qt.IsNil)

	database, err := goschema.ParseFileWithDependencies(mainFile)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Dependencies["orders"], qt.DeepEquals, []string{"tenants"})
	c.Assert(database.Fields, qt.HasLen, 4)
	c.Assert(database.Fields[2].Name, qt.Equals, "owner_tenant_id")
	c.Assert(database.Fields[2].Foreign, qt.Equals, "tenants(id)")
}
