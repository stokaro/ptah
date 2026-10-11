package mysqlsource

import (
	"fmt"
	"strconv"

	"ptah.run/core/annotation"
	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// BlockSizeAttribute is the attribute of the Go index and constraint
// directives that declares the KEY_BLOCK_SIZE hint of an index or of a
// PRIMARY KEY constraint: `//ptah:schema:index name="k" fields="a"
// key_block_size="8"`.
const BlockSizeAttribute = "key_block_size"

// PrimaryKeyBlockSizeAttribute is the attribute of the Go table directive
// that declares the KEY_BLOCK_SIZE hint of the primary key the directive's
// primary_key attribute declares: `//ptah:schema:table name="t"
// primary_key="a,b" primary_key_block_size="8"`.
const PrimaryKeyBlockSizeAttribute = "primary_key_block_size"

// The frontend's own directives the owner adds its attributes to.
const (
	indexDirective      = "ptah:schema:index"
	constraintDirective = "ptah:schema:constraint"
	tableDirective      = "ptah:schema:table"
)

// primaryKeyHint describes a primary key's hint, as the frontend described
// it before the owner read it.
const primaryKeyHint = "MySQL-family primary index block-size hint."

// Annotations returns the owner's extension of the Go annotation frontend:
// the [BlockSizeAttribute] of the index and constraint directives, read as
// the hint of the index or of the PRIMARY KEY constraint, the
// [PrimaryKeyBlockSizeAttribute] of the table directive, read as the hint of
// the table's primary key, and the claim that a Go source describes every
// hint, so an index or a key declared without the attribute has none.
func Annotations() annotation.Extension {
	return annotation.Extension{
		Owner: mysqlschema.Owner,
		Kinds: []schemaext.Kind{mysqlschema.IndexBlockSizeKind},
		Attributes: []annotation.DirectiveAttributes{
			{
				Directive: indexDirective,
				Attributes: []annotation.Attribute{{Name: BlockSizeAttribute, Value: "string",
					Description: "MySQL and MariaDB index block-size hint; zero uses the engine default."}},
				Decode: decodeBlockSize(BlockSizeAttribute),
			},
			{
				Directive:  constraintDirective,
				Attributes: []annotation.Attribute{{Name: BlockSizeAttribute, Value: "string", Description: primaryKeyHint}},
				Decode:     decodeBlockSize(BlockSizeAttribute),
			},
			{
				Directive:  tableDirective,
				Attributes: []annotation.Attribute{{Name: PrimaryKeyBlockSizeAttribute, Value: "string", Description: primaryKeyHint}},
				Decode:     decodeBlockSize(PrimaryKeyBlockSizeAttribute),
				PrimaryKey: true,
			},
		},
		Coverage: annotation.Unlimited(BlockSizeCoverage),
	}
}

// YAML returns the owner's extension of the YAML frontend. A YAML document
// has no spelling for the block size of an index or a primary key, and one it
// declares has none: applying it to a table that holds one removes it, as
// applying a CREATE INDEX without the option would. The extension makes that
// claim and reads no key.
func YAML() yamlext.Extension {
	return yamlext.Extension{Owner: mysqlschema.Owner, Kinds: []schemaext.Kind{mysqlschema.IndexBlockSizeKind}, Coverage: BlockSizeCoverage}
}

// HCL returns the owner's extension of the HCL frontend. An HCL document has
// no spelling for the block size of an index or a primary key, so one it
// declares has none, and applying it removes the hint, as its export warns.
// The extension makes that claim and reads no block.
func HCL() hclext.Extension {
	return hclext.Extension{Owner: mysqlschema.Owner, Kinds: []schemaext.Kind{mysqlschema.IndexBlockSizeKind},
		Coverage: func(hclext.CoverageRequest) (schemaext.Coverage, error) { return BlockSizeCoverage() }}
}

// BlockSizeCoverage is the claim of a source that describes the block size of
// every index and primary key it declares: a Go file, a SQL file and a YAML
// document, which declare the hint or none, and an HCL file, which cannot
// spell it, so applying one removes the hint, as its export warns. An index or
// a key such a source declares without a hint requests none.
func BlockSizeCoverage() (schemaext.Coverage, error) {
	return mysqlschema.IndexBlockSizeCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
}

// decodeBlockSize reads the attribute a declaration wrote as the owner's
// facet. Zero declares none and adds nothing.
func decodeBlockSize(attribute string) func(map[string]string) (schemaext.Facets, error) {
	return func(values map[string]string) (schemaext.Facets, error) {
		value := values[attribute]
		size, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return schemaext.Facets{}, &annotation.DeclarationError{Attribute: attribute,
				Err: fmt.Errorf("invalid %s %q (must be a non-negative integer)", attribute, value)}
		}
		facets, err := mysqlschema.WithIndexBlockSize(schemaext.Facets{}, size)
		if err != nil {
			return schemaext.Facets{}, &annotation.DeclarationError{Attribute: attribute, Err: err}
		}
		return facets, nil
	}
}
