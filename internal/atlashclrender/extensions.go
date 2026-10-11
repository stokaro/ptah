package atlashclrender

import (
	"cmp"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/systemschema"
)

// extensionBlock is one extension block as the document writes it, whether
// the common model declared the extension or the extension owner did. Both
// write the same block, so a document reads back the same whichever declared
// it.
type extensionBlock struct {
	name        string
	schema      string
	version     string
	comment     string
	ifNotExists bool
	provides    []string
}

// captureExtensions validates the extensions the document declares, from the
// common model and from the extension owner, before rendering, and orders
// them by name. A name both declare is refused: the document could write only
// one block for it, and nothing says which declaration is meant.
func (r *renderer) captureExtensions() error {
	declared := make(map[string]bool, len(r.db.Extensions))
	for _, extension := range r.db.Extensions {
		declared[extension.Name] = true
		r.extensions = append(r.extensions, extensionBlock{name: extension.Name, schema: extension.Schema, version: extension.Version,
			comment: extension.Comment, ifNotExists: extension.IfNotExists, provides: slices.Clone(extension.Provides)})
	}
	objects, err := r.db.FeatureObjects.All()
	if err != nil {
		return err
	}
	for _, object := range objects {
		if object.Ref.Kind != objectidentity.Kind(pgextension.ExtensionKind) {
			continue
		}
		value, ok := object.Value.(*pgextension.DesiredExtension)
		if !ok {
			return fmt.Errorf("%w: HCL requires desired extension values", schemaext.ErrInvalidValue)
		}
		if err := pgextension.ValidateRef(object.Ref); err != nil {
			return err
		}
		if err := pgextension.ValidateDesired(value); err != nil {
			return err
		}
		name := pgextension.Name(object.Ref)
		if declared[name] {
			return fmt.Errorf("%w: extension %q is declared twice", ptaherr.ErrInvalidSchemaDiff, name)
		}
		declared[name] = true
		r.extensions = append(r.extensions, extensionBlock{name: name, schema: value.Schema, version: value.Version,
			comment: value.Comment, ifNotExists: value.IfNotExists, provides: slices.Clone(value.Provides)})
	}
	slices.SortFunc(r.extensions, func(a, b extensionBlock) int { return cmp.Compare(a.name, b.name) })
	return nil
}

func (r *renderer) renderExtensions() {
	for _, extension := range r.extensions {
		if r.omitRefusedExtension("extensions."+extension.name, extension) {
			continue
		}
		r.linef(`extension %s {`, quote(extension.name))
		if schema := r.extensionSchemaFor(extension); schema != "" {
			// The common PostgreSQL namespaces are recognized even when dialect is
			// empty, which keeps dialect-neutral artifacts from turning placement
			// into a schema declaration. Target-specific namespaces such as
			// CockroachDB's crdb_internal still require a concrete dialect.
			if systemschema.IsPostgresFamilySystemSchema(r.dialect, schema) {
				r.stringAttr(1, "schema", schema)
			} else {
				r.rawAttr(1, "schema", r.schemaRef(schema))
			}
		}
		if extension.ifNotExists {
			r.trueAttr(1, "if_not_exists")
		}
		r.stringAttr(1, "version", extension.version)
		r.stringAttr(1, "comment", extension.comment)
		r.line("}")
		r.line("")
	}
}
