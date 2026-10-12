package goschematogo

import (
	"cmp"
	"slices"
	"strconv"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
)

// extensionDeclaration is one extension annotation as the export writes it
// for an extension the owner declares. What a read reported the extension
// supplies is not written: no annotation attribute holds it, and nothing a Go
// source declares reads it.
type extensionDeclaration struct {
	name        string
	schema      string
	version     string
	comment     string
	ifNotExists bool
	dialects    []string
}

func (e extensionDeclaration) annotation() string {
	return annotation("ptah:schema:extension",
		attr{name: "name", value: e.name, set: true},
		attr{name: "schema", value: e.schema, set: e.schema != ""},
		attr{name: "if_not_exists", value: strconv.FormatBool(e.ifNotExists), set: e.ifNotExists},
		attr{name: "version", value: e.version, set: e.version != ""},
		attr{name: "comment", value: e.comment, set: e.comment != ""},
		dialectsAttr(e.dialects),
	)
}

// captureExtension records an extension the owner declares, with the targets
// its declaration is scoped to.
func (ctx *renderContext) captureExtension(object schemaext.Object, extension *pgextension.DesiredExtension) error {
	if err := pgextension.ValidateRef(object.Ref); err != nil {
		return err
	}
	if err := pgextension.ValidateDesired(extension); err != nil {
		return err
	}
	ctx.extensions = append(ctx.extensions, extensionDeclaration{
		name: pgextension.Name(object.Ref), schema: extension.Schema, version: extension.Version,
		comment: extension.Comment, ifNotExists: extension.IfNotExists, dialects: slices.Clone(object.Targets),
	})
	return nil
}

// sortExtensions puts the declared extensions in name order, which is the
// order the export writes them in.
func (ctx *renderContext) sortExtensions() {
	slices.SortStableFunc(ctx.extensions, func(a, b extensionDeclaration) int { return cmp.Compare(a.name, b.name) })
}
