package projectconfig

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// CompositeSchemaMarkerScheme prefixes the value `data "composite_schema"`
// mints for its `url` attribute.
//
// Like the external-schema and remote-schema markers it names no location: it
// is a key into [Config.CompositeSchema], recognized only where a project
// file's own desired-state sources are read. Spelled on a flag it is refused,
// so the composition stays reachable through the project file alone.
const CompositeSchemaMarkerScheme = "ptah-composite-schema"

// CompositeSchema is one evaluated atlas.hcl `data "composite_schema"` block:
// the parts its `schema` blocks name, in the order they are written.
type CompositeSchema struct {
	// Name is the block's second label.
	Name string
	// Parts are the block's `schema` blocks, in declaration order.
	Parts []CompositeSchemaPart
}

// CompositeSchemaPart is one `schema` block of a `data "composite_schema"`.
type CompositeSchemaPart struct {
	// Schema is the block's label, the schema the part's objects belong to. It
	// is empty for an unlabeled block, whose part is read in the scope of the
	// whole database.
	Schema string
	// URL is the evaluated `url` attribute, as the project file wrote it: a
	// path or file:// URL relative to the atlas.hcl directory, or the value
	// another data source minted.
	URL string
	// VarValues and VarsScoped carry the variable scope of the
	// `data "hcl_schema"` block that minted URL, with the meaning
	// [Config.SchemaSourceVars] gives them. VarsScoped is false when no such
	// block selected the URL.
	VarValues  map[string]string
	VarsScoped bool
	// ExternalSchema is the program a `data.external_schema.<name>.url` part
	// runs, and nil for every other part.
	ExternalSchema *ExternalSchemaConfig
	// Filename and Line locate the `schema` block, for diagnostics.
	Filename string
	Line     int
}

// CompositeSchema returns the composition a desired-state source minted by
// `data "composite_schema"` refers to. rawURL is the source as
// [Config.SchemaSources] holds it. The bool is false for every other value.
//
// The returned value is a copy, so a caller cannot change what a later read
// returns.
func (c Config) CompositeSchema(rawURL string) (CompositeSchema, bool) {
	name, ok := compositeSchemaMarkerName(rawURL)
	if !ok {
		return CompositeSchema{}, false
	}
	composite, ok := c.compositeSchemas[name]
	if !ok {
		return CompositeSchema{}, false
	}
	return cloneCompositeSchema(composite), true
}

// HasCompositeSchemaSource reports whether a desired-state source of the
// config is a `data "composite_schema"` value.
func (c Config) HasCompositeSchemaSource() bool {
	return slices.ContainsFunc(c.SchemaSources, func(value string) bool {
		_, ok := compositeSchemaMarkerName(value)
		return ok
	})
}

// compositeSchemaMarkerName reports whether value is a composite-schema marker
// and returns the data source name it carries.
func compositeSchemaMarkerName(value string) (string, bool) {
	return strings.CutPrefix(strings.TrimSpace(value), CompositeSchemaMarkerScheme+"://")
}

// compositeSchemaRefusal is the pinned community binary's refusal of the data
// source, reproduced where the strict policy asks for it. Measured on v1.3.0:
// `migrate diff`, `schema inspect` and `migrate hash` all answer it at exit 1,
// and so does a project that declares the block without referencing it.
func compositeSchemaRefusal() error {
	return fmt.Errorf("missing data source handler for %q", "composite_schema")
}

// validateCompositeSchemaShape checks the block's syntax without evaluating
// it: `schema` blocks only, each with at most one label and exactly a `url`.
func validateCompositeSchemaShape(block *hclsyntax.Block) error {
	if names := sortedAttributeNames(block.Body.Attributes); len(names) > 0 {
		return unsupportedAttr(names[0], block.Body.Attributes[names[0]])
	}
	if len(block.Body.Blocks) == 0 {
		return fmt.Errorf(
			"atlas.hcl data.composite_schema %q requires at least one schema block at %s:%d",
			block.Labels[1], block.TypeRange.Filename, block.TypeRange.Start.Line,
		)
	}
	for _, part := range block.Body.Blocks {
		if part.Type != "schema" || len(part.Labels) > 1 {
			return unsupportedBlock(part)
		}
		if len(part.Body.Blocks) > 0 {
			return unsupportedBlock(part.Body.Blocks[0])
		}
		for _, name := range sortedAttributeNames(part.Body.Attributes) {
			if name != "url" {
				return unsupportedAttr(name, part.Body.Attributes[name])
			}
		}
		if _, ok := part.Body.Attributes["url"]; !ok {
			return fmt.Errorf(
				"atlas.hcl data.composite_schema %q schema block requires url at %s:%d",
				block.Labels[1], part.TypeRange.Filename, part.TypeRange.Start.Line,
			)
		}
	}
	return nil
}

// compositeSchemaDataSource records the block's parts and mints the marker its
// `url` attribute reads as.
//
// The part URLs are evaluated here, after the evaluator resolved every data
// source they reference, so a part naming `data.hcl_schema.<n>.url` carries
// that block's variable scope and one naming `data.external_schema.<n>.url`
// carries its program.
func (p atlasParser) compositeSchemaDataSource(block *hclsyntax.Block) (cty.Value, error) {
	name := block.Labels[1]
	composite := CompositeSchema{Name: name, Parts: make([]CompositeSchemaPart, 0, len(block.Body.Blocks))}
	for _, partBlock := range block.Body.Blocks {
		part, err := p.compositeSchemaPart(name, partBlock)
		if err != nil {
			return cty.NilVal, err
		}
		composite.Parts = append(composite.Parts, part)
	}
	p.compositeSchemas[name] = composite
	return cty.ObjectVal(map[string]cty.Value{
		"url": cty.StringVal(CompositeSchemaMarkerScheme + "://" + name),
	}), nil
}

func (p atlasParser) compositeSchemaPart(name string, block *hclsyntax.Block) (CompositeSchemaPart, error) {
	attr := block.Body.Attributes["url"]
	url, err := p.stringAttr("url", attr)
	if err != nil {
		return CompositeSchemaPart{}, err
	}
	part := CompositeSchemaPart{
		URL:      url,
		Filename: block.TypeRange.Filename,
		Line:     block.TypeRange.Start.Line,
	}
	if len(block.Labels) == 1 {
		part.Schema = block.Labels[0]
	}
	if strings.TrimSpace(url) == "" {
		return CompositeSchemaPart{}, fmt.Errorf(
			"atlas.hcl data.composite_schema %q schema %q url is empty at %s:%d",
			name, part.Schema, attr.NameRange.Filename, attr.NameRange.Start.Line,
		)
	}
	if _, nested := compositeSchemaMarkerName(url); nested {
		return CompositeSchemaPart{}, fmt.Errorf(
			"atlas.hcl data.composite_schema %q schema %q names another data.composite_schema at %s:%d; "+
				"list its parts in this block instead",
			name, part.Schema, attr.NameRange.Filename, attr.NameRange.Start.Line,
		)
	}
	if external, ok := externalSchemaMarkerName(url); ok {
		source, declared := p.externalSchemas[external]
		if !declared {
			return CompositeSchemaPart{}, fmt.Errorf(
				"atlas.hcl data.composite_schema %q references undeclared data.external_schema %q", name, external)
		}
		part.ExternalSchema = &ExternalSchemaConfig{
			Program:    slices.Clone(source.program),
			Format:     source.format,
			WorkingDir: p.resolveExternalSchemaWorkingDir(source.workingDir),
			Env:        slices.Clone(source.env),
			Origin:     AtlasFileName,
		}
		return part, nil
	}
	scopes, err := p.schemaSourceVarScopes(attr, []string{url})
	if err != nil {
		return CompositeSchemaPart{}, err
	}
	if values, scoped := scopes[url]; scoped {
		part.VarValues = values
		part.VarsScoped = true
	}
	return part, nil
}

// rejectCompositeSchemaMarker refuses the marker anywhere but the env's
// desired-state source, which is the only place a composition means anything.
func rejectCompositeSchemaMarker(value, location string) error {
	name, ok := compositeSchemaMarkerName(value)
	if !ok {
		return nil
	}
	return fmt.Errorf(
		"atlas.hcl data.composite_schema.%s.url can only be the env desired-state source (env src or schema.src), not %s",
		name, location,
	)
}

func cloneCompositeSchema(composite CompositeSchema) CompositeSchema {
	parts := make([]CompositeSchemaPart, len(composite.Parts))
	for i, part := range composite.Parts {
		part.VarValues = maps.Clone(part.VarValues)
		if part.ExternalSchema != nil {
			external := *part.ExternalSchema
			external.Program = slices.Clone(external.Program)
			external.Env = slices.Clone(external.Env)
			part.ExternalSchema = &external
		}
		parts[i] = part
	}
	composite.Parts = parts
	return composite
}

func mergeCompositeSchemas(base, override map[string]CompositeSchema) map[string]CompositeSchema {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	merged := make(map[string]CompositeSchema, len(base)+len(override))
	for name, composite := range base {
		merged[name] = cloneCompositeSchema(composite)
	}
	for name, composite := range override {
		merged[name] = cloneCompositeSchema(composite)
	}
	return merged
}

// rejectCompositeSchemaMarkers applies [rejectCompositeSchemaMarker] to every
// env attribute that names a location rather than a desired state.
func rejectCompositeSchemaMarkers(cfg *Config) error {
	locations := []struct {
		value, name string
	}{
		{cfg.DatabaseURL, "env url"},
		{cfg.DevURL, "env dev"},
		{cfg.Migration.Dir, "env migration.dir"},
	}
	for _, location := range locations {
		if err := rejectCompositeSchemaMarker(location.value, location.name); err != nil {
			return err
		}
	}
	for _, value := range cfg.Exclude {
		if err := rejectCompositeSchemaMarker(value, "env exclude"); err != nil {
			return err
		}
	}
	return nil
}
