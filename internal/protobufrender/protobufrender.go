// Package protobufrender renders a Ptah schema as a single Protobuf Edition
// 2023 definition: one message per table, one enum per enum column, and scalar
// fields for foreign keys.
//
// Unlike the OpenAPI and GraphQL targets, this exporter is stateful. Protobuf
// field numbers are persistent wire identifiers, so the previously generated
// file is read back and used as the source of every number it already pins.
// Numbers are preserved across column reordering and additive change, removed
// fields have their number and name reserved, and new numbers are allocated
// monotonically above everything the type has ever used.
//
// Edition 2023 rather than 2024 is deliberate. Edition 2024 enables
// features.enforce_naming_style = STYLE2024, under which protoc rejects
// ordinary database identifiers such as "address_1"; it is also unsupported by
// every tagged release of bufbuild/protocompile, and it switches protoc-gen-go
// to the Opaque API for every downstream consumer. No wire-relevant feature
// default differs between the two editions.
package protobufrender

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemaexport"
)

// RemovalPolicy controls what happens when a whole message or enum disappears
// from the source schema.
type RemovalPolicy string

const (
	// RemovalError refuses the export. It is the default because a table that
	// is dropped and later recreated would otherwise restart numbering at 1 and
	// collide with the numbers old consumers still hold.
	RemovalError RemovalPolicy = "error"
	// RemovalTombstone retains the type with every previous number and name
	// reserved.
	RemovalTombstone RemovalPolicy = "tombstone"
	// RemovalDrop removes the type and abandons its compatibility guarantees.
	RemovalDrop RemovalPolicy = "drop"
)

// ChangePolicy controls what happens when a retained field's translated
// Protobuf type or cardinality changes.
type ChangePolicy string

const (
	// ChangeError refuses the export.
	ChangeError ChangePolicy = "error"
	// ChangeRenumber reserves the old number and name and allocates a new
	// number, the wire-safe equivalent of delete plus add.
	ChangeRenumber ChangePolicy = "renumber"
)

// NameReusePolicy controls what happens when an identifier whose name is
// currently reserved comes back.
type NameReusePolicy string

const (
	// NameReuseError refuses the export.
	NameReuseError NameReusePolicy = "error"
	// NameReuseRelease drops the reserved name entry while keeping the reserved
	// number, abandoning JSON-name compatibility for that identifier.
	NameReuseRelease NameReusePolicy = "release"
)

// Options controls the Protobuf export.
type Options struct {
	IncludeTables []string
	ExcludeTables []string
	// Package is the protobuf package, required.
	Package string
	// GoPackage, when set, is emitted as option go_package.
	GoPackage string
	// OutPath is the destination path. It is used for diagnostics and as the
	// logical file name given to the protobuf compiler.
	OutPath string
	// Previous is the previously generated file's bytes.
	Previous []byte
	// HasPrevious reports that a previous export exists at OutPath. It is
	// explicit rather than inferred from len(Previous), so a zero-byte file is
	// refused by the validation gate instead of silently bootstrapping a fresh,
	// incompatible numbering history.
	HasPrevious bool

	TypeRemoval          RemovalPolicy
	OnIncompatibleChange ChangePolicy
	OnNameReuse          NameReusePolicy
}

// Result is the rendered definition plus any export diagnostics.
type Result struct {
	Data        []byte
	Diagnostics []schemaexport.Diagnostic
	// Bootstrapped reports that no previous export existed, so the numbering
	// starts from 1 and is compatible with no previously published .proto.
	Bootstrapped bool
	// Messages, Fields and Enums count what actually entered the compatibility
	// baseline, after --include-tables/--exclude-tables. On a stateful target
	// the caller has no other signal of what was pinned.
	Messages int
	Fields   int
	Enums    int
}

type builder struct {
	opts        Options
	diagnostics []schemaexport.Diagnostic
	imports     map[string]bool
	// enumByKey deduplicates enums so several columns sharing one named Ptah
	// enum produce exactly one Protobuf enum.
	enumByKey map[string]string
	enums     []enum
	// messageOwners maps a generated message name to the table that produced it,
	// so an enum that would take the same name is rejected with both sources
	// named rather than dying on an opaque protocompile duplicate-symbol error.
	messageOwners map[string]string
	// valueOwners maps an enum value identifier to the sources that produced
	// it. Protobuf enum values are siblings of their type at package scope, so
	// uniqueness is a file-wide property, not a per-enum one.
	valueOwners map[string][]string
}

// Render renders db as a deterministic Protobuf definition, preserving every
// field and enum-value number pinned by opts.Previous.
func Render(ctx context.Context, db *goschema.Database, opts Options) (Result, error) {
	if db == nil {
		return Result{}, fmt.Errorf("schema database is nil")
	}
	if strings.TrimSpace(opts.Package) == "" {
		return Result{}, fmt.Errorf("a protobuf package is required")
	}

	b := &builder{
		opts:          opts,
		imports:       map[string]bool{},
		enumByKey:     map[string]string{},
		valueOwners:   map[string][]string{},
		messageOwners: map[string]string{},
	}

	logicalPath := protoLogicalPath(opts)

	var prev *previousFile
	bootstrapped := true
	if opts.HasPrevious {
		loaded, err := loadPrevious(ctx, logicalPath, opts.Previous, opts.Package)
		if err != nil {
			return Result{}, err
		}
		prev = loaded
		bootstrapped = false
		if prev.Version <= headerLayoutVersion {
			// The rewrite is safe - every pinned number is carried over - but it
			// changes every line's position, so it is announced rather than left
			// for the reader of the diff to work out.
			b.warn(opts.OutPath, fmt.Sprintf(
				"previous export uses format version %d, which carried the generated header at the top of the file; "+
					"rewriting it as format version %d with the header at the bottom, where protoc-gen-go does not copy it into the generated .pb.go",
				prev.Version, exportVersion))
		}
	} else {
		b.warn(opts.OutPath, fmt.Sprintf(
			"no previous export found at %s; field numbering starts from 1 and is not compatible with any previously published .proto",
			opts.OutPath))
	}

	desired, err := b.buildDesired(db)
	if err != nil {
		return Result{}, err
	}

	if err := b.checkValueCollisions(); err != nil {
		return Result{}, err
	}

	out, err := b.reconcile(desired, prev)
	if err != nil {
		return Result{}, err
	}
	out.Package = opts.Package
	out.GoPackage = opts.GoPackage
	for imp := range b.imports {
		out.Imports = append(out.Imports, imp)
	}
	out.sortForOutput()

	rendered := render(out)
	stamped, err := stampDigest([]byte(rendered))
	if err != nil {
		return Result{}, fmt.Errorf("stamp content digest: %w", err)
	}

	// Re-parse what we are about to write. This catches generator bugs at
	// generation time rather than in the caller's build. It is deliberately not
	// an injection backstop: a balanced injection parses and links cleanly,
	// which is why comments and identifiers are escaped in their own right.
	if _, err := compileProto(ctx, logicalPath, string(stamped)); err != nil {
		return Result{}, fmt.Errorf("generated protobuf is invalid: %w", err)
	}

	fieldCount := 0
	for _, msg := range out.Messages {
		fieldCount += len(msg.Fields)
	}
	return Result{
		Data:         stamped,
		Diagnostics:  b.diagnostics,
		Bootstrapped: bootstrapped,
		Messages:     len(out.Messages),
		Fields:       fieldCount,
		Enums:        len(out.Enums),
	}, nil
}

// protoLogicalPath is the path handed to the protobuf compiler. Using the
// package directory keeps compiler diagnostics meaningful even when --out lives
// somewhere else.
func protoLogicalPath(opts Options) string {
	name := "schema.proto"
	if opts.OutPath != "" {
		name = filepath.Base(opts.OutPath)
	}
	dir := strings.ReplaceAll(opts.Package, ".", "/")
	if dir == "" {
		return name
	}
	return dir + "/" + name
}

// desiredShape is the schema as the source describes it, before any previously
// pinned numbering is applied.
type desiredShape struct {
	messages []desiredMessage
	enums    []enum
}

type desiredMessage struct {
	Name    string
	Comment string
	Fields  []desiredField
}

type desiredField struct {
	Name     string
	Type     string
	Repeated bool
	Comment  string
}

func (b *builder) buildDesired(db *goschema.Database) (desiredShape, error) {
	tables := schemaexport.SelectTables(db, schemaexport.Options{
		IncludeTables: b.opts.IncludeTables,
		ExcludeTables: b.opts.ExcludeTables,
	})
	enumIndex := schemaexport.EnumIndex(db)

	names, err := b.assignMessageNames(tables)
	if err != nil {
		return desiredShape{}, err
	}

	shape := desiredShape{}
	for _, table := range tables {
		tableFields := schemaexport.FieldsFor(db, table)
		if len(tableFields) == 0 {
			// An empty message would render as "message X {}" and, more
			// importantly, could never be told apart from a tombstone on the
			// next run. internal/graphqlrender omits such a table too.
			b.warn(table.Name, "table has no exportable columns; message omitted")
			continue
		}
		msg := desiredMessage{
			Name:    names[table.QualifiedName()],
			Comment: table.Comment,
		}
		seen := map[string]string{}
		for _, f := range tableFields {
			df, err := b.buildField(table, f, enumIndex)
			if err != nil {
				return desiredShape{}, err
			}
			if owner, dup := seen[df.Name]; dup {
				return desiredShape{}, fmt.Errorf(
					"columns %q and %q on table %q both map to protobuf field %q; rename one column",
					owner, f.Name, table.Name, df.Name)
			}
			seen[df.Name] = f.Name
			msg.Fields = append(msg.Fields, df)
		}
		shape.messages = append(shape.messages, msg)
	}
	shape.enums = b.enums
	return shape, nil
}

// assignMessageNames resolves table names to message names, disambiguating a
// collision by schema. The rule is total and order-independent: if two tables
// still collide after qualification the export fails naming both, rather than
// aliasing with a numeric suffix whose result would depend on table order.
func (b *builder) assignMessageNames(tables []goschema.Table) (map[string]string, error) {
	bare := map[string][]goschema.Table{}
	for _, table := range tables {
		name, changed := messageName(table.Name)
		if changed {
			b.warn(table.Name, fmt.Sprintf(
				"table %q was sanitized to protobuf message %q; buf lint STANDARD will report MESSAGE_PASCAL_CASE for it",
				table.Name, name))
		}
		bare[name] = append(bare[name], table)
	}

	names := map[string]string{}
	final := map[string][]goschema.Table{}
	for name, group := range bare {
		if len(group) == 1 {
			names[group[0].QualifiedName()] = name
			final[name] = append(final[name], group[0])
			continue
		}
		for _, table := range group {
			qualified, _ := qualifiedMessageName(table.Schema, table.Name)
			names[table.QualifiedName()] = qualified
			final[qualified] = append(final[qualified], table)
		}
	}

	var collisions []string
	for name, group := range final {
		if len(group) < 2 {
			continue
		}
		var sources []string
		for _, table := range group {
			sources = append(sources, fmt.Sprintf("%s (struct %s)", table.QualifiedName(), table.StructName))
		}
		sort.Strings(sources)
		collisions = append(collisions, fmt.Sprintf("%s: %s", name, strings.Join(sources, ", ")))
	}
	if len(collisions) > 0 {
		sort.Strings(collisions)
		return nil, fmt.Errorf(
			"tables map to the same protobuf message name: %s; set a distinct schema= on one of them or exclude it with --exclude-tables",
			strings.Join(collisions, "; "))
	}
	for name, group := range final {
		b.messageOwners[name] = fmt.Sprintf("%s (struct %s)", group[0].QualifiedName(), group[0].StructName)
	}
	return names, nil
}

func (b *builder) buildField(table goschema.Table, f goschema.Field, enumIndex map[string][]string) (desiredField, error) {
	name, changed, lintDirty := fieldName(f.Name)
	path := table.Name + "." + f.Name
	if changed {
		b.warn(path, fmt.Sprintf("column %q was sanitized to protobuf field %q", f.Name, name))
	}
	if lintDirty {
		b.warn(path, fmt.Sprintf(
			"protobuf field %q is not lower_snake_case; buf lint STANDARD reports FIELD_LOWER_SNAKE_CASE for it", name))
	}

	element, isArray := schemaexport.ElementType(f.Type)
	if isArray && f.Nullable {
		b.warn(path, "nullable array column exported as repeated; protobuf cannot distinguish SQL NULL from an empty list")
	}

	// Resolve the enum against the ELEMENT type. An array of an enum still names
	// that enum, and looking it up under the "enum_x[]" spelling would miss,
	// silently pinning `repeated string` as the wire type forever. Mirrors
	// internal/graphqlrender, which resolves the element the same way.
	elementField := f
	elementField.Type = element

	if values, ok := schemaexport.ResolveEnumValues(elementField, enumIndex); ok {
		enumName, err := b.registerEnum(table, elementField, values)
		if err != nil {
			return desiredField{}, err
		}
		return desiredField{Name: name, Type: enumName, Repeated: isArray, Comment: f.Comment}, nil
	}

	mapped := mapProtoType(element)
	if mapped.Import != "" {
		b.imports[mapped.Import] = true
	}
	if !mapped.Known {
		b.warn(path, fmt.Sprintf("column type %q is not recognized and was exported as string", f.Type))
	}
	if mapped.Lossy != "" {
		b.warn(path, mapped.Lossy)
	}
	return desiredField{Name: name, Type: mapped.Name, Repeated: isArray, Comment: f.Comment}, nil
}

// registerEnum creates (or reuses) the Protobuf enum backing an enum column.
// The keying mirrors internal/graphqlrender so several columns sharing one
// named Ptah enum produce exactly one Protobuf enum.
func (b *builder) registerEnum(table goschema.Table, f goschema.Field, values []string) (string, error) {
	// Mirrors internal/graphqlrender.enumSourceKey exactly. Deliberately does
	// NOT also test len(f.Enum) == 0: the parser sets BOTH Field.Type and
	// Field.Enum on the column that DEFINES an enum, so such a test would send
	// the defining column down the inline branch while a second column
	// referencing the same enum by name took the named branch. Both would
	// produce the same identifier from different keys and the export would fail
	// with a spurious "produced by more than one source".
	named := !mapProtoType(f.Type).Known
	key := "col:" + table.Name + "." + f.Name
	if named {
		key = "type:" + f.Type
	}
	if name, ok := b.enumByKey[key]; ok {
		return name, nil
	}

	var raw string
	if named {
		raw = schemaexport.PascalCase(strings.TrimPrefix(f.Type, "enum_"))
	} else {
		raw = schemaexport.TypeName(table.Name) + schemaexport.PascalCase(f.Name)
	}
	name, changed := sanitizeIdent(raw)
	if changed {
		b.warn(table.Name+"."+f.Name, fmt.Sprintf("enum type name %q was sanitized to %q", raw, name))
	}
	for _, existing := range b.enums {
		if existing.Name == name {
			return "", fmt.Errorf(
				"enum type name %q is produced by more than one source; rename the enum or the column that produces it", name)
		}
	}
	if owner, taken := b.messageOwners[name]; taken {
		return "", fmt.Errorf(
			"protobuf name %q is produced by both table %s and the enum for column %s.%s; rename one of them or exclude it with --exclude-tables",
			name, owner, table.Name, f.Name)
	}

	built := enum{Name: name}
	zero := unspecifiedValueName(name)
	built.Values = append(built.Values, enumValue{Name: zero, Number: 0})
	b.valueOwners[zero] = append(b.valueOwners[zero], fmt.Sprintf("synthesized zero value of enum %s", name))

	for i, value := range values {
		valueName, valueChanged, valueLintDirty := enumValueName(name, value)
		if valueChanged {
			b.warn(table.Name+"."+f.Name, fmt.Sprintf(
				"enum label %q was sanitized to protobuf value %q", value, valueName))
		}
		if valueLintDirty {
			b.warn(table.Name+"."+f.Name, fmt.Sprintf(
				"protobuf enum value %q is not UPPER_SNAKE_CASE; buf lint STANDARD reports ENUM_VALUE_UPPER_SNAKE_CASE for it",
				valueName))
		}
		b.valueOwners[valueName] = append(b.valueOwners[valueName],
			fmt.Sprintf("label %q of enum %s", value, name))
		built.Values = append(built.Values, enumValue{Name: valueName, Number: int32(i + 1)})
	}

	b.enumByKey[key] = name
	b.enums = append(b.enums, built)
	return name, nil
}

// checkValueCollisions enforces file-wide uniqueness of enum value identifiers.
// Protobuf uses C++ scoping for enum values, so two values in *different* enums
// collide at package scope, and the synthesized zero value shares that
// namespace with real labels: a source enum containing a label "unspecified"
// produces <ENUM_NAME>_UNSPECIFIED twice.
func (b *builder) checkValueCollisions() error {
	var collisions []string
	for name, owners := range b.valueOwners {
		if len(owners) < 2 {
			continue
		}
		sorted := append([]string(nil), owners...)
		sort.Strings(sorted)
		collisions = append(collisions, fmt.Sprintf("%s: %s", name, strings.Join(sorted, ", ")))
	}
	if len(collisions) == 0 {
		return nil
	}
	sort.Strings(collisions)
	return fmt.Errorf(
		"enum value names collide (protobuf enum values share their package's namespace): %s",
		strings.Join(collisions, "; "))
}

func (b *builder) warn(path, message string) {
	b.diagnostics = append(b.diagnostics, schemaexport.Diagnostic{
		Severity: schemaexport.SeverityWarning,
		Path:     path,
		Message:  message,
	})
}
