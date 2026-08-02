package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
)

func TestParseSource_EmbeddedPlatformOverridesReachConcreteFields(t *testing.T) {
	c := qt.New(t)
	source := `
package models

type Audit struct {
	//ptah:schema:field name="created_at" type="TIMESTAMP" platform.mysql.type="DATETIME"
	CreatedAt string
}

type AuditEnvelope struct {
	//ptah:embedded mode="inline" prefix="inner_" platform.mysql.type="DATETIME(3)"
	Audit
}

//ptah:schema:table name="accounts"
type Account struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}

//ptah:schema:table name="users"
type User struct {
	//ptah:embedded mode="inline" prefix="audit_" platform.mysql.type="DATETIME(6)"
	Audit

	//ptah:embedded mode="inline" prefix="outer_" platform.mysql.type="DATETIME(9)"
	AuditEnvelope

	//ptah:embedded mode="json" name="metadata" type="JSONB" nullable="true" platform.mysql.type="JSON"
	Metadata map[string]any

	//ptah:embedded mode="relation" field="manager_id" ref="accounts(id)" type="BIGINT" platform.mysql.type="BIGINT UNSIGNED"
	Manager Account
}
`

	parsed, err := goschema.ParseSource("models.go", source)
	c.Assert(err, qt.IsNil)
	db, err := goschema.Merge(&parsed)

	c.Assert(err, qt.IsNil)
	fields := embeddedFieldsByName(db.Fields)
	c.Assert(fields["User.audit_created_at"].Overrides, qt.DeepEquals, map[string]map[string]string{
		"mysql": {"type": "DATETIME(6)"},
	})
	c.Assert(fields["User.outer_inner_created_at"].Overrides, qt.DeepEquals, map[string]map[string]string{
		"mysql": {"type": "DATETIME(9)"},
	})
	c.Assert(fields["User.metadata"].Nullable, qt.IsTrue)
	c.Assert(fields["User.metadata"].Overrides, qt.DeepEquals, map[string]map[string]string{
		"mysql": {"type": "JSON"},
	})
	c.Assert(fields["User.manager_id"].Foreign, qt.Equals, "accounts(id)")
	c.Assert(fields["User.manager_id"].Type, qt.Equals, "BIGINT")
	c.Assert(fields["User.manager_id"].Overrides, qt.DeepEquals, map[string]map[string]string{
		"mysql": {"type": "BIGINT UNSIGNED"},
	})
}

func embeddedFieldsByName(values []goschema.Field) map[string]goschema.Field {
	result := make(map[string]goschema.Field, len(values))
	for _, value := range values {
		result[value.StructName+"."+value.Name] = value
	}
	return result
}
