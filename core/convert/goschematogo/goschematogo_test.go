package goschematogo_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/convert/goschematogo"
	"github.com/stokaro/ptah/core/goschema"
)

func TestRenderPerTableFilesRoundTripThroughParser(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{
			StructName: "OrderItem",
			Name:       "order_items",
			PrimaryKey: []string{"tenant_id", "order_id"},
			Comment:    "Imported order items",
		}},
		Fields: []goschema.Field{
			{
				StructName: "OrderItem",
				FieldName:  "TenantID",
				Name:       "tenant_id",
				Type:       "INTEGER",
				Nullable:   false,
				Primary:    true,
			},
			{
				StructName: "OrderItem",
				FieldName:  "Status",
				Name:       "status",
				Type:       "status_type",
				Nullable:   false,
				Default:    "'active'",
			},
			{
				StructName:  "OrderItem",
				FieldName:   "CreatedAt",
				Name:        "created_at",
				Type:        "TIMESTAMPTZ",
				Nullable:    false,
				DefaultExpr: "now()",
			},
		},
		Enums: []goschema.Enum{{
			Name:   "status_type",
			Values: []string{"active", "inactive"},
		}},
		Indexes: []goschema.Index{{
			StructName: "OrderItem",
			Name:       "idx_order_items_status",
			Fields:     []string{"status"},
			Condition:  "status <> 'inactive'",
		}},
		Constraints: []goschema.Constraint{{
			StructName:     "OrderItem",
			Name:           "fk_order_items_orders",
			Type:           "FOREIGN KEY",
			Table:          "order_items",
			Columns:        []string{"tenant_id", "order_id"},
			ForeignTable:   "orders",
			ForeignColumns: []string{"tenant_id", "id"},
			OnDelete:       "CASCADE",
		}},
		Functions: []goschema.Function{{
			Name:       "touch_order_item",
			Returns:    "trigger",
			Language:   "plpgsql",
			Security:   "INVOKER",
			Volatility: "VOLATILE",
			Body:       "BEGIN RAISE NOTICE \"touch\";\nRETURN NEW; END;",
		}},
		Roles: []goschema.Role{{
			Name:    "app_user",
			Login:   true,
			Inherit: true,
		}},
		Grants: []goschema.Grant{{
			Role:       "app_user",
			Privileges: []string{"SELECT", "INSERT"},
			OnTable:    "order_items",
		}},
		RLSEnabledTables: []goschema.RLSEnabledTable{{
			StructName: "OrderItem",
			Table:      "order_items",
		}},
		RLSPolicies: []goschema.RLSPolicy{{
			StructName:      "OrderItem",
			Name:            "tenant_isolation",
			Table:           "order_items",
			PolicyFor:       "ALL",
			ToRoles:         "app_user",
			UsingExpression: "tenant_id = current_setting('app.tenant_id')::int",
		}},
	}

	files, err := goschematogo.Render(db, goschematogo.Options{
		PackageName:     "models",
		AddJSONTags:     true,
		AddDBTags:       true,
		LowercaseFields: true,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(fileNames(files), qt.DeepEquals, []string{"enums.go", "schema_objects.go", "order_items.go"})

	dir := t.TempDir()
	c.Assert(goschematogo.WriteDir(dir, files), qt.IsNil)
	orderItemSource := mustReadFile(c, filepath.Join(dir, "order_items.go"))
	c.Assert(orderItemSource, qt.Contains, "tenantID int")
	c.Assert(orderItemSource, qt.Contains, "`db:\"tenant_id\" json:\"tenant_id\"`")
	c.Assert(orderItemSource, qt.Contains, "createdAt time.Time")
	c.Assert(orderItemSource, qt.Contains, `"time"`)

	parsed, err := goschema.ParseDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Enums, qt.DeepEquals, db.Enums)
	c.Assert(parsed.Tables, qt.HasLen, 1)
	c.Assert(parsed.Tables[0].Name, qt.Equals, "order_items")
	c.Assert(parsed.Tables[0].PrimaryKey, qt.DeepEquals, []string{"tenant_id", "order_id"})
	c.Assert(parsed.Fields, qt.HasLen, 3)
	c.Assert(parsed.Indexes, qt.HasLen, 1)
	c.Assert(parsed.Indexes[0].Condition, qt.Equals, "status <> 'inactive'")
	c.Assert(parsed.Constraints, qt.DeepEquals, db.Constraints)
	c.Assert(parsed.Functions, qt.HasLen, 1)
	c.Assert(parsed.Functions[0].Body, qt.Equals, "BEGIN RAISE NOTICE \"touch\";\nRETURN NEW; END;")
	c.Assert(parsed.Roles, qt.HasLen, 1)
	c.Assert(parsed.Roles[0].Name, qt.Equals, "app_user")
	c.Assert(parsed.Roles[0].Login, qt.IsTrue)
	c.Assert(parsed.Roles[0].Inherit, qt.IsTrue)
	c.Assert(parsed.Grants, qt.HasLen, 1)
	c.Assert(parsed.Grants[0].Role, qt.Equals, "app_user")
	c.Assert(parsed.Grants[0].Privileges, qt.DeepEquals, []string{"SELECT", "INSERT"})
	c.Assert(parsed.Grants[0].OnTable, qt.Equals, "order_items")
	c.Assert(parsed.RLSPolicies, qt.HasLen, 1)
	c.Assert(parsed.RLSEnabledTables, qt.HasLen, 1)
}

func TestRenderSingleFileUsesOneSchemaFile(t *testing.T) {
	c := qt.New(t)

	files, err := goschematogo.Render(&goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{{
			StructName: "User",
			FieldName:  "ID",
			Name:       "id",
			Type:       "SERIAL",
			Nullable:   false,
			Primary:    true,
		}},
	}, goschematogo.Options{PackageName: "models", SingleFile: true})

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(files[0].Name, qt.Equals, "schema.go")
	c.Assert(string(files[0].Data), qt.Contains, "type User struct")
}

func TestRenderPerTableFileNamesIncludeSchema(t *testing.T) {
	c := qt.New(t)

	files, err := goschematogo.Render(&goschema.Database{
		Tables: []goschema.Table{
			{StructName: "BillingUser", Schema: "billing", Name: "users"},
			{StructName: "AuthUser", Schema: "auth", Name: "users"},
		},
	}, goschematogo.Options{PackageName: "models"})

	c.Assert(err, qt.IsNil)
	c.Assert(fileNames(files), qt.DeepEquals, []string{"auth_users.go", "billing_users.go"})
}

func TestRenderRejectsInvalidPackageName(t *testing.T) {
	c := qt.New(t)

	_, err := goschematogo.Render(&goschema.Database{}, goschematogo.Options{PackageName: "type"})

	c.Assert(err, qt.ErrorMatches, `invalid package name "type"`)
}

func TestWriteDirRejectsUnsafeFileNames(t *testing.T) {
	c := qt.New(t)

	err := goschematogo.WriteDir(t.TempDir(), []goschematogo.File{{
		Name: "../escape.go",
		Data: []byte("package models\n"),
	}})

	c.Assert(err, qt.ErrorMatches, `unsafe generated file name "\.\./escape\.go"`)
}

func fileNames(files []goschematogo.File) []string {
	names := make([]string, 0, len(files))
	for _, file := range files {
		names = append(names, file.Name)
	}
	return names
}

func mustReadFile(c *qt.C, path string) string {
	c.Helper()
	data, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	return strings.TrimSpace(string(data))
}
