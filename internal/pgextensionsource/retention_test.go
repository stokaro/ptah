package pgextensionsource_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/pgextensionsource"
)

func TestAnnotationsRetained_RefusesChangedExtensionData(t *testing.T) {
	rows := []struct{ name, attribute, value string }{
		{"schema", "schema", "app"}, {"version", "version", "1.0"},
		{"comment", "comment", "required extension"}, {"guard", "if_not_exists", "true"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			owners := must.Must(annotation.NewSet(pgextensionsource.Annotations()))
			objects := must.Must(schemaext.NewObjects(must.Must(pgextension.DesiredObject(pgextension.Ref("pg_trgm"), pgextension.DesiredExtension{}))))
			result, err := owners.Retained(context.Background(), annotation.RetentionRequest{Declarations: []annotation.Declaration{{Directive: pgextensionsource.Directive, Attributes: map[string]string{"name": "pg_trgm", row.attribute: row.value}}}, Objects: objects})
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, []bool{false})
		})
	}
}

func TestAnnotationsRetained_PreservesExtensionAndScope(t *testing.T) {
	c := qt.New(t)
	owners := must.Must(annotation.NewSet(pgextensionsource.Annotations()))
	object := must.Must(pgextension.DesiredObject(pgextension.Ref("pg_trgm"), pgextension.DesiredExtension{Schema: "app", Version: "1.0", Comment: "required extension", IfNotExists: true}))
	object.Targets = []string{"postgresql"}
	objects := must.Must(schemaext.NewObjects(object))
	declaration := annotation.Declaration{Directive: pgextensionsource.Directive, Targets: []string{"postgresql"}, Attributes: map[string]string{"name": "pg_trgm", "schema": "app", "version": "1.0", "comment": "required extension", "if_not_exists": "true"}}
	result, err := owners.Retained(context.Background(), annotation.RetentionRequest{Declarations: []annotation.Declaration{declaration}, Objects: objects})
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, []bool{true})
	object.Targets = nil
	result, err = owners.Retained(context.Background(), annotation.RetentionRequest{Declarations: []annotation.Declaration{declaration}, Objects: must.Must(schemaext.NewObjects(object))})
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, []bool{false})
}
