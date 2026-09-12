package goschematogo_test

import (
	"reflect"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

// notExportedAtAll names the types that carry a dialect scope and that this
// exporter emits no annotation for, so the sweep below can tell "the scope is
// dropped" from "the whole object is".
//
// The second is a wider gap with its own issue (stokaro/ptah#3207 covers the
// scope; the missing families are recorded there). This set may shrink and must
// not grow: a family that gains an annotation leaves it, and a new scoped family
// that nothing exports is a decision to take rather than an entry to append.
var notExportedAtAll = map[string]string{
	"Domain":        "no ptah:schema:domain annotation is written",
	"CompositeType": "no ptah:schema:composite annotation is written",
	"Range":         "no ptah:schema:range annotation is written",
	"Sequence":      "no ptah:schema:sequence annotation is written",
}

// scopedDatabase declares one object of every family that carries a dialect
// scope, each scoped to a dialect that is not the default, so a dropped scope
// is visible as an absent attribute rather than as a value that happens to
// match.
func scopedDatabase() *schemamodel.Database {
	scope := []string{"postgres"}
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
		},
		Extensions: []schemamodel.Extension{{Name: "pgcrypto", Dialects: scope}},
		Views:      []schemamodel.View{{StructName: "V", Name: "v", Body: "SELECT 1", Dialects: scope}},
		MaterializedViews: []schemamodel.MaterializedView{
			{StructName: "M", Name: "m", Body: "SELECT 1", Dialects: scope},
		},
		Functions: []schemamodel.Function{
			{StructName: "F", Name: "f", Returns: "integer", Body: "SELECT 1", Dialects: scope},
		},
		Triggers: []schemamodel.Trigger{
			{StructName: "G", Name: "g", Table: "t", Timing: "BEFORE", Event: "INSERT", Dialects: scope},
		},
		Roles:            []schemamodel.Role{{StructName: "R", Name: "r", Dialects: scope}},
		Grants:           []schemamodel.Grant{{StructName: "GR", Role: "r", Privileges: []string{"SELECT"}, OnTable: "t", Dialects: scope}},
		RLSPolicies:      []schemamodel.RLSPolicy{{StructName: "T", Name: "p", Table: "t", Dialects: scope}},
		RLSEnabledTables: []schemamodel.RLSEnabledTable{{StructName: "T", Table: "t", Dialects: scope}},
	}
}

// TestRender_EveryScopedObjectKeepsItsDialectScope drives the export and asserts
// the attribute survives.
//
// The failure it guards against runs the opposite way from a normal dropped
// field: an object whose scope is lost does not disappear, it reappears on
// every dialect, so the first symptom is a statement executed against an engine
// the author excluded.
func TestRender_EveryScopedObjectKeepsItsDialectScope(t *testing.T) {
	c := qt.New(t)

	files, err := goschematogo.Render(scopedDatabase(), goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.Not(qt.HasLen), 0)

	var source strings.Builder
	for _, file := range files {
		source.Write(file.Data)
	}
	rendered := source.String()

	directives := []string{
		"ptah:schema:extension",
		"ptah:schema:view",
		"ptah:schema:matview",
		"ptah:schema:function",
		"ptah:schema:trigger",
		"ptah:schema:role",
		"ptah:schema:grant",
		"ptah:schema:rls:policy",
		"ptah:schema:rls:enable",
	}
	for _, directive := range directives {
		t.Run(directive, func(t *testing.T) {
			c := qt.New(t)
			line := annotationLine(c, rendered, directive)
			c.Assert(line, qt.Contains, `dialects="postgres"`,
				qt.Commentf("the scope is dropped, so this object is exported as belonging to every dialect"))
		})
	}
}

// TestRender_AnUnscopedObjectWritesNoDialectsAttribute is the control.
//
// Without it, an exporter that wrote dialects on every annotation would satisfy
// the test above, and an unscoped object would come back scoped to whatever the
// exporter invented.
func TestRender_AnUnscopedObjectWritesNoDialectsAttribute(t *testing.T) {
	c := qt.New(t)

	db := scopedDatabase()
	db.Extensions[0].Dialects = nil

	files, err := goschematogo.Render(db, goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.IsNil)

	var source strings.Builder
	for _, file := range files {
		source.Write(file.Data)
	}
	line := annotationLine(c, source.String(), "ptah:schema:extension")
	c.Assert(line, qt.Not(qt.Contains), "dialects=")
}

// TestRender_EveryTypeCarryingADialectScopeIsAccountedFor is what keeps the
// test above from going stale.
//
// It reflects over the schema model rather than reading a list, because a list
// of scoped families is exactly the thing that stopped matching the model and
// produced this defect. A family that gains a Dialects field and is exported
// without one fails here.
func TestRender_EveryTypeCarryingADialectScopeIsAccountedFor(t *testing.T) {
	c := qt.New(t)

	unaccounted := scopedFamiliesNobodyAccountsFor()

	c.Assert(unaccounted, qt.HasLen, 0,
		qt.Commentf("these types carry a dialect scope and neither the export sweep nor notExportedAtAll accounts for them: %s",
			strings.Join(unaccounted, ", ")))
}

// exportedWithTheirScope names the families the sweep above drives.
//
// It is a list because the sweep needs one, and the test beside it is what
// keeps the list from being the only statement: a family that gains a dialect
// scope and is missing here fails rather than passing quietly.
var exportedWithTheirScope = map[string]bool{
	"Extension":        true,
	"View":             true,
	"MaterializedView": true,
	"Function":         true,
	"Trigger":          true,
	"Role":             true,
	"Grant":            true,
	"RLSPolicy":        true,
	"RLSEnabledTable":  true,
}

// scopedFamiliesNobodyAccountsFor returns the scoped families that are neither
// driven by the sweep nor recorded as unexported.
func scopedFamiliesNobodyAccountsFor() []string {
	var unaccounted []string
	for _, name := range typesCarryingADialectScope() {
		_, unexported := notExportedAtAll[name]
		if !exportedWithTheirScope[name] && !unexported {
			unaccounted = append(unaccounted, name)
		}
	}
	return unaccounted
}

// typesCarryingADialectScope reads the schema model for the families that have
// a dialect scope at all.
func typesCarryingADialectScope() []string {
	database := reflect.TypeFor[schemamodel.Database]()
	var names []string
	for field := range database.Fields() {
		if field.Type.Kind() != reflect.Slice {
			continue
		}
		element := field.Type.Elem()
		if element.Kind() != reflect.Struct {
			continue
		}
		if _, has := element.FieldByName("Dialects"); has {
			names = append(names, element.Name())
		}
	}
	return names
}

// annotationLine returns the one rendered line carrying the directive.
func annotationLine(c *qt.C, rendered, directive string) string {
	c.Helper()
	var found []string
	for line := range strings.SplitSeq(rendered, "\n") {
		if strings.Contains(line, directive+" ") {
			found = append(found, line)
		}
	}
	c.Assert(found, qt.Not(qt.HasLen), 0, qt.Commentf("no %s annotation was rendered at all", directive))
	return found[0]
}
