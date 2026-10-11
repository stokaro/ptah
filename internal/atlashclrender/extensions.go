package atlashclrender

import (
	"fmt"
	"slices"

	"ptah.run/core/hclext"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/systemschema"
)

// extensionBlock is one extension block of the common model as the document
// writes it, and what the Atlas-compatible surface asks of an extension an
// owner writes.
type extensionBlock struct {
	name        string
	schema      string
	version     string
	comment     string
	ifNotExists bool
	provides    []string
}

// refuseRepeatedExtensions refuses an extension the common model declares and
// an owner writes too: the document could write only one block for it, and
// nothing says which declaration is meant.
func (r *renderer) refuseRepeatedExtensions() error {
	declared := make(map[string]bool, len(r.db.Extensions))
	for _, extension := range r.db.Extensions {
		declared[extension.Name] = true
	}
	for _, block := range r.ownerBlocks {
		if block.Type != blockExtension {
			continue
		}
		if declared[block.Name] {
			return fmt.Errorf("%w: extension %q is declared twice", ptaherr.ErrInvalidSchemaDiff, block.Name)
		}
		declared[block.Name] = true
	}
	return nil
}

// renderBeforeTypes writes the blocks the rest of the document may depend
// on: the extensions, the common model's and the ones owners write, ordered
// by name, since an extension's schema is only where it is installed.
func (r *renderer) renderBeforeTypes() {
	entries := make([]slotEntry, 0, len(r.db.Extensions))
	for _, extension := range r.db.Extensions {
		block := extensionBlock{name: extension.Name, schema: extension.Schema, version: extension.Version,
			comment: extension.Comment, ifNotExists: extension.IfNotExists, provides: slices.Clone(extension.Provides)}
		entries = append(entries, slotEntry{blockType: blockExtension, name: extension.Name, write: func() { r.renderExtension(block) }})
	}
	r.renderSlot(hclext.BeforeTypes, nil, entries...)
}

// renderExtension writes an extension the common model declares.
func (r *renderer) renderExtension(extension extensionBlock) {
	if r.omitRefusedExtension("extensions."+extension.name, extension) {
		return
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
