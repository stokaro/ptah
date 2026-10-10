package mysqlsource

import (
	"fmt"
	"strconv"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// BlockSizeAttribute is the attribute of the Go index directive that declares
// an index's KEY_BLOCK_SIZE hint: `//ptah:schema:index name="k" fields="a"
// key_block_size="8"`.
const BlockSizeAttribute = "key_block_size"

// indexDirective is the frontend's own index directive, which the owner adds
// its attribute to.
const indexDirective = "ptah:schema:index"

// Annotations returns the owner's extension of the Go annotation frontend:
// the [BlockSizeAttribute] of the index directive, read as the index's hint,
// and the claim that a Go source describes every index's hint, so an index
// declared without the attribute has none.
func Annotations() annotation.Extension {
	return annotation.Extension{
		Owner: mysqlschema.Owner,
		Kinds: []schemaext.Kind{mysqlschema.IndexBlockSizeKind},
		Attributes: []annotation.DirectiveAttributes{{
			Directive: indexDirective,
			Attributes: []annotation.Attribute{{Name: BlockSizeAttribute, Value: "string",
				Description: "MySQL and MariaDB index block-size hint; zero uses the engine default."}},
			Decode: decodeBlockSizeAttribute,
		}},
		Coverage: annotation.Unlimited(BlockSizeCoverage),
	}
}

// YAML returns the owner's extension of the YAML frontend. A YAML document
// has no spelling for an index's block size, and an index it declares has
// none: applying it to a table that holds one removes it, as applying a
// CREATE INDEX without the option would. The extension makes that claim and
// reads no key.
func YAML() yamlext.Extension {
	return yamlext.Extension{Owner: mysqlschema.Owner, Kinds: []schemaext.Kind{mysqlschema.IndexBlockSizeKind}, Coverage: BlockSizeCoverage}
}

// BlockSizeCoverage is the claim of a source that describes the block size of
// every index it declares: a Go file, a SQL file and a YAML document, which
// declare the hint or none, and an HCL file, which cannot spell it, so
// applying one removes the hint, as its export warns. An index such a source
// declares without a hint requests none.
func BlockSizeCoverage() (schemaext.Coverage, error) {
	return mysqlschema.IndexBlockSizeCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
}

// decodeBlockSizeAttribute reads the attribute a declaration wrote as the
// owner's facet. Zero declares none and adds nothing.
func decodeBlockSizeAttribute(values map[string]string) (schemaext.Facets, error) {
	value := values[BlockSizeAttribute]
	size, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return schemaext.Facets{}, &annotation.DeclarationError{Attribute: BlockSizeAttribute,
			Err: fmt.Errorf("invalid %s %q (must be a non-negative integer)", BlockSizeAttribute, value)}
	}
	facets, err := mysqlschema.WithIndexBlockSize(schemaext.Facets{}, size)
	if err != nil {
		return schemaext.Facets{}, &annotation.DeclarationError{Attribute: BlockSizeAttribute, Err: err}
	}
	return facets, nil
}
