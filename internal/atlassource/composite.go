package atlassource

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/config/projectconfig"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemasource"
	"ptah.run/dbschema"
	"ptah.run/internal/schemascope"
	"ptah.run/internal/schemaselection"
)

// CompositePart is one part of a [KindCompositeSchema] source.
type CompositePart struct {
	// Schema is the schema the part's objects belong to, from the `schema`
	// block's label. It is empty for an unlabeled block, whose part is read
	// in the scope of the whole database.
	Schema string
	// Source is the part's own classified source: a local schema file or
	// directory, an external schema program, or a registry artifact.
	Source Source
	// Location names the `schema` block, `atlas.hcl:12`, for diagnostics.
	Location string
}

// classifyCompositeSchema classifies every part of a `data "composite_schema"`
// block the env's desired state names.
func classifyCompositeSchema(raw string, composite projectconfig.CompositeSchema, env ProjectEnv) (Source, error) {
	source := Source{Raw: raw, Kind: KindCompositeSchema}
	for _, part := range composite.Parts {
		classified, err := classifyCompositePart(part, env)
		if err != nil {
			return Source{}, fmt.Errorf("data.composite_schema %q %s at %s:%d: %w",
				composite.Name, compositePartName(part.Schema), part.Filename, part.Line, err)
		}
		source.Composite = append(source.Composite, CompositePart{
			Schema:   part.Schema,
			Source:   classified,
			Location: fmt.Sprintf("%s:%d", part.Filename, part.Line),
		})
	}
	return source, nil
}

// compositePartName names a part the way its block is written.
func compositePartName(schema string) string {
	if schema == "" {
		return "unlabeled schema block"
	}
	return fmt.Sprintf("schema %q", schema)
}

// classifyCompositePart classifies one part's URL.
//
// A part is a desired state read without a database: a schema file or
// directory, a program's output, or a registry artifact. A database URL or a
// migration directory is refused rather than read, because reading either
// needs a connection the composition has no way to scope to one part.
func classifyCompositePart(part projectconfig.CompositeSchemaPart, env ProjectEnv) (Source, error) {
	if external := part.ExternalSchema; external != nil {
		// The same gate an env-level external schema meets: the program is
		// repository-controlled code reached through a project file.
		allowed, err := externalSchemaAllowed()
		if err != nil {
			return Source{}, err
		}
		if !allowed {
			return Source{}, errExternalSchemaDisabled()
		}
		return Source{
			Raw:  part.URL,
			Kind: KindExternalSchema,
			Command: schemasource.Command{
				Args:   slices.Clone(external.Program),
				Format: external.Format,
				Dir:    external.WorkingDir,
				Env:    slices.Clone(external.Env),
			},
		}, nil
	}
	source, err := classifyEnvValue(part.URL, env.BaseDir)
	if err != nil {
		return Source{}, err
	}
	switch source.Kind {
	case KindLocalFile:
		source.VarValues = part.VarValues
		source.VarsScoped = part.VarsScoped
		return source, nil
	case KindRemoteSchema:
		return source, nil
	default:
		return Source{}, fmt.Errorf(
			"%q is a %s, which cannot be a composite part; a part is a schema file or directory, "+
				"or a data.hcl_schema, data.external_schema or data.remote_schema value",
			part.URL, source.Kind,
		)
	}
}

// resolveCompositeSchema loads every part, checks a labeled part against its
// label, and merges the parts in declaration order.
//
// The merge is the one native Ptah applies to repeated --schema-file sources,
// so an object two parts both declare conflicts here exactly as it does there.
func (s Set) resolveCompositeSchema(ctx context.Context, opts ResolveOptions) (State, error) {
	source := s.Sources[0]
	defaultSchema := opts.SchemaScope
	if defaultSchema == "" {
		defaultSchema = schemaselection.DialectDefault(opts.Dialect)
	}
	parts := make([]*schemamodel.Database, 0, len(source.Composite))
	for _, part := range source.Composite {
		partSet := Set{Flag: s.Flag, Kind: part.Source.Kind, Sources: []Source{part.Source}}
		var loaded *schemamodel.Database
		err := partSet.resolve(ctx, opts, func(state State, _ *dbschema.DatabaseConnection) error {
			loaded = state.Schema
			return nil
		})
		if err != nil {
			return State{}, fmt.Errorf("%s composite %s at %s: %w",
				s.Flag, compositePartName(part.Schema), part.Location, err)
		}
		placed, err := PlaceCompositePart(loaded, part.Schema, defaultSchema)
		if err != nil {
			return State{}, fmt.Errorf("%s composite %s at %s: %w",
				s.Flag, compositePartName(part.Schema), part.Location, err)
		}
		parts = append(parts, placed)
	}
	merged, err := schemamodel.Merge(parts...)
	if err != nil {
		return State{}, fmt.Errorf("%s %q: merging the composite schema: %w", s.Flag, source.Raw, err)
	}
	return State{Kind: s.Kind, Schema: merged}, nil
}

// PlaceCompositePart returns part as a labeled composite part belongs to
// schema: it declares the schema, and every object it declares lies in it.
//
// An unlabeled part, schema "", is returned unchanged. defaultSchema is the
// schema an unqualified object belongs to in this run: the schema the run's URL
// pins, or the dialect's default when none does.
//
// Nothing is moved. An object the part names in another schema is refused, and
// so is an unqualified object when schema is not the run's default: moving it
// would leave every unqualified name inside a view, routine or trigger body
// resolving through the target's search path, which is a different schema
// from the one the object would then be in. part is not modified.
func PlaceCompositePart(part *schemamodel.Database, schema, defaultSchema string) (*schemamodel.Database, error) {
	if schema == "" || part == nil {
		return part, nil
	}
	if err := checkCompositePlacement(part, schema, defaultSchema); err != nil {
		return nil, err
	}
	placed := *part
	if !slices.ContainsFunc(part.Schemas, func(declared schemamodel.Schema) bool {
		return effectiveSchema(declared.Name, defaultSchema) == schema
	}) {
		placed.Schemas = append(slices.Clone(part.Schemas), schemamodel.Schema{Name: schema})
	}
	return &placed, nil
}

// checkCompositePlacement names the first object outside schema.
//
// The families a schema scope filters are judged by that filter, so what lies
// "in" a schema is decided in one place for both. The type families it keeps
// whole are checked here by their own Schema field.
func checkCompositePlacement(part *schemamodel.Database, schema, defaultSchema string) error {
	inScope := schemascope.FilterGeneratedWithDefaultSchema(part, []string{schema}, defaultSchema)
	for _, table := range part.Tables {
		if !slices.ContainsFunc(inScope.Tables, func(kept schemamodel.Table) bool {
			return kept.QualifiedName() == table.QualifiedName()
		}) {
			return outsidePlacement("table", table.Name, table.Schema, schema)
		}
	}
	for _, declared := range part.Schemas {
		if effectiveSchema(declared.Name, defaultSchema) != schema {
			return fmt.Errorf("the part declares schema %q, outside schema %q", declared.Name, schema)
		}
	}
	for _, view := range part.Views {
		if !slices.ContainsFunc(inScope.Views, func(kept schemamodel.View) bool { return kept.Name == view.Name }) {
			return outsideQualifiedPlacement("view", view.Name, schema)
		}
	}
	for _, view := range part.MaterializedViews {
		if !slices.ContainsFunc(inScope.MaterializedViews, func(kept schemamodel.MaterializedView) bool {
			return kept.Name == view.Name
		}) {
			return outsideQualifiedPlacement("materialized view", view.Name, schema)
		}
	}
	for _, function := range part.Functions {
		if !slices.ContainsFunc(inScope.Functions, func(kept schemamodel.Function) bool {
			return kept.Name == function.Name
		}) {
			return outsideQualifiedPlacement("function", function.Name, schema)
		}
	}
	return checkTypePlacement(part, schema, defaultSchema)
}

// checkTypePlacement checks the declared types and sequences, which a schema
// scope does not filter.
func checkTypePlacement(part *schemamodel.Database, schema, defaultSchema string) error {
	type declared struct{ kind, name, schema string }
	objects := make([]declared, 0, len(part.Enums)+len(part.Domains)+len(part.Sequences))
	for _, enum := range part.Enums {
		objects = append(objects, declared{"enum", enum.Name, enum.Schema})
	}
	for _, domain := range part.Domains {
		objects = append(objects, declared{"domain", domain.Name, domain.Schema})
	}
	for _, composite := range part.CompositeTypes {
		objects = append(objects, declared{"composite type", composite.Name, composite.Schema})
	}
	for _, rangeType := range part.Ranges {
		objects = append(objects, declared{"range type", rangeType.Name, rangeType.Schema})
	}
	for _, sequence := range part.Sequences {
		objects = append(objects, declared{"sequence", sequence.Name, sequence.Schema})
	}
	for _, object := range objects {
		if effectiveSchema(object.schema, defaultSchema) != schema {
			return outsidePlacement(object.kind, object.name, object.schema, schema)
		}
	}
	return nil
}

// outsidePlacement words the refusal for one object. An unqualified object is
// told to qualify itself, because that is the edit that resolves it.
func outsidePlacement(kind, name, objectSchema, schema string) error {
	if objectSchema == "" {
		return fmt.Errorf(
			"%s %q is not in schema %q: an unqualified name belongs to the run's default schema, "+
				"and Ptah does not move it; qualify it as %s.%s",
			kind, name, schema, schema, name)
	}
	return fmt.Errorf("%s %q is in schema %q, outside schema %q", kind, name, objectSchema, schema)
}

// outsideQualifiedPlacement is [outsidePlacement] for a family that carries its
// schema in the name, `auth.v`, rather than in a field of its own.
func outsideQualifiedPlacement(kind, name, schema string) error {
	if qualifier, bare, qualified := strings.Cut(name, "."); qualified {
		return outsidePlacement(kind, bare, qualifier, schema)
	}
	return outsidePlacement(kind, name, "", schema)
}

// effectiveSchema is the schema an object lies in: its own, or the default
// when it names none.
func effectiveSchema(objectSchema, defaultSchema string) string {
	if objectSchema == "" {
		return defaultSchema
	}
	return objectSchema
}
