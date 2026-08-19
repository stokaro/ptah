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

// FieldRemovalPolicy controls what happens when a field disappears from the
// source schema, which retires its number and reserves its name.
type FieldRemovalPolicy string

const (
	// FieldRemovalError refuses the export. It is the default for the reason
	// every policy here defaults to refusing: retiring a number and reserving
	// a name is a change to the contract a consumer already holds, and it
	// happened silently -- one field number retired, another allocated, exit 0
	// (stokaro/ptah#905).
	//
	// The exporter cannot tell a rename from a removal, because a column
	// carries no identity of its own beyond its name. Both change the wire
	// contract, so both are refused until the caller says which it meant.
	FieldRemovalError FieldRemovalPolicy = "error"
	// FieldRemovalReserve retires the number and the name, which is what keeps
	// the export clean under buf breaking WIRE_JSON.
	FieldRemovalReserve FieldRemovalPolicy = "reserve"
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

// CommentPolicy controls whether prose the source schema carries is copied into
// the published contract.
type CommentPolicy string

const (
	// CommentsAll copies every table and column comment the source schema
	// supplies. It has to be asked for: see CommentsNone, which is the default.
	CommentsAll CommentPolicy = "all"
	// CommentsNone omits them, and is the DEFAULT. It is all-or-nothing by
	// design: a table comment can carry the same internal detail as a column
	// comment, so a per-object switch would advertise a boundary the contract
	// does not have.
	//
	// Omitting by default is the safe direction for an exported interface. A
	// schema comment is written for whoever reads the database — it routinely
	// carries operational notes, sharding hints, or security context — while a
	// .proto is published to consumers and copied onward into their generated
	// code. Publishing that prose has to be a decision, not something that
	// happens because nobody passed a flag.
	//
	// The cost is that an export written before this default flipped loses its
	// source prose on the next run. Field and enum numbers are untouched, so it
	// is a diff rather than a compatibility break, and `--proto-comments=all`
	// restores the previous bytes exactly.
	//
	// Only prose the source schema supplies is suppressed. Text Ptah writes
	// about the generated file itself - the header lines and the tombstone
	// rationale - is not source material, describes the contract rather than the
	// database, and is unaffected.
	CommentsNone CommentPolicy = "none"
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
	// FieldPolicy decides what an undeclared column means. The zero value is
	// the historical behavior: every column of an exported table is exported.
	FieldPolicy schemaexport.FieldPolicy
	// Package is the protobuf package, required.
	Package string
	// GoPackage, when set, is emitted as option go_package.
	GoPackage string
	// OutPath is the destination path of the anchor file. It is used for
	// diagnostics and as the logical file name given to the protobuf compiler.
	OutPath string
	// Previous is the previously generated anchor file's bytes.
	Previous []byte
	// HasPrevious reports that a previous export exists at OutPath. It is
	// explicit rather than inferred from len(Previous), so a zero-byte file is
	// refused by the validation gate instead of silently bootstrapping a fresh,
	// incompatible numbering history.
	HasPrevious bool
	// PreviousSiblings carries the bytes of every other file the previous export
	// wrote, keyed by base name. The caller learns those names from
	// ManifestNames and must supply all of them: a file left out would make its
	// messages look deleted and restart their numbering.
	PreviousSiblings map[string][]byte

	// Split selects how many files the export writes. The zero value is
	// SplitNone.
	Split SplitPolicy

	TypeRemoval RemovalPolicy
	// OnFieldRemoval decides what happens when a field vanishes from a type
	// that itself survives. The zero value refuses, like every other policy
	// here.
	OnFieldRemoval       FieldRemovalPolicy
	OnIncompatibleChange ChangePolicy
	OnNameReuse          NameReusePolicy
	// OnTypeMove decides what happens when an already-pinned type changes files.
	// The zero value refuses, like every other policy here.
	OnTypeMove MovePolicy

	// Comments selects which comments the generated file carries. The zero value
	// is CommentsNone: an embedder that never sets it publishes no source prose,
	// which is the same default the CLI has.
	Comments CommentPolicy
}

// OutputFile is one file of a rendered export set.
type OutputFile struct {
	// Name is the base name inside the anchor's directory.
	Name string
	// Data is the complete file, digest already stamped.
	Data []byte
	// Anchor marks the file --out names.
	Anchor bool
}

// Result is the rendered definition plus any export diagnostics.
type Result struct {
	// Data is the anchor file's bytes, which is the whole export when Split is
	// SplitNone. Files carries the complete set, anchor first.
	Data []byte
	// Files is every file the export writes, anchor first and the rest sorted by
	// name. Writing all of them is what makes the set self-consistent: the
	// anchor's manifest names exactly these files.
	Files []OutputFile
	// Removed lists the base names of files the previous export wrote that this
	// one no longer contains. They are Ptah's own output, already validated, and
	// leaving them behind would redeclare types the set no longer owns.
	Removed     []string
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
	// messageImports records the well-known-type imports each message's fields
	// need, keyed by message name. It is per-message rather than per-file
	// because the file a message lands in is only decided after every field has
	// been mapped.
	messageImports map[string][]string
	// enumByKey deduplicates enums so several columns sharing one named Ptah
	// enum produce exactly one Protobuf enum.
	enumByKey map[string]string
	// enumNames is the set of generated enum type names, used to decide which
	// files have to import the anchor.
	enumNames map[string]bool
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
		opts:           opts,
		messageImports: make(map[string][]string),
		enumByKey:      make(map[string]string),
		enumNames:      make(map[string]bool),
		valueOwners:    make(map[string][]string),
		messageOwners:  make(map[string]string),
	}

	anchor := anchorName(opts.OutPath)
	pkgDir := packageDir(opts.Package)

	var prev *previousSet
	bootstrapped := true
	if opts.HasPrevious {
		loaded, err := loadPrevious(ctx, pkgDir, anchor, opts.Previous, opts.PreviousSiblings, opts.Package)
		if err != nil {
			return Result{}, err
		}
		prev = loaded
		bootstrapped = false
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

	homes, err := b.assignFiles(desired, anchor)
	if err != nil {
		return Result{}, err
	}
	// The move gate runs before any numbering is reconciled, so a refused move
	// cannot leave a partially renumbered model behind.
	if err := b.checkMoves(homes, prev, anchor); err != nil {
		return Result{}, err
	}

	messages, enums, err := b.reconcile(desired, prev)
	if err != nil {
		return Result{}, err
	}

	out := b.group(messages, enums, homes, prev, anchor)

	sources := make(map[string]string)
	pathOf := make(map[string]string)
	names := make([]string, 0, len(out))
	files := make([]OutputFile, 0, len(out))
	for _, current := range out {
		stamped, err := stampDigest([]byte(render(current)))
		if err != nil {
			return Result{}, fmt.Errorf("stamp content digest for %s: %w", current.Name, err)
		}
		path := logicalPath(pkgDir, current.Name)
		sources[path] = string(stamped)
		pathOf[current.Name] = path
		names = append(names, current.Name)
		files = append(files, OutputFile{Name: current.Name, Data: stamped, Anchor: current.Anchor})
	}

	// Re-parse what we are about to write, as one set so cross-file imports are
	// resolved. This catches generator bugs at generation time rather than in
	// the caller's build. It is deliberately not an injection backstop: a
	// balanced injection parses and links cleanly, which is why comments and
	// identifiers are escaped in their own right.
	if _, err := compileProtoSet(ctx, sources, names, pathOf); err != nil {
		return Result{}, fmt.Errorf("generated protobuf is invalid: %w", err)
	}

	messageCount, fieldCount, enumCount := 0, 0, 0
	for _, current := range out {
		messageCount += len(current.Messages)
		enumCount += len(current.Enums)
		for _, msg := range current.Messages {
			fieldCount += len(msg.Fields)
		}
	}
	return Result{
		Data:         files[0].Data,
		Files:        files,
		Removed:      removedFiles(prev, out),
		Diagnostics:  b.diagnostics,
		Bootstrapped: bootstrapped,
		Messages:     messageCount,
		Fields:       fieldCount,
		Enums:        enumCount,
	}, nil
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
	// Import is the well-known-type file the field's type needs, empty for a
	// built-in scalar or a generated enum.
	Import string
}

// exposedFields returns the columns of a table that reach either contract
// shape, with the diagnostics for those the policy withheld.
//
// It asks the shared model rather than filtering here, so this target and the
// two API targets cannot disagree about what is published.
func (b *builder) exposedFields(
	db *goschema.Database,
	table goschema.Table,
) ([]goschema.Field, []schemaexport.Diagnostic, error) {
	policy := b.opts.FieldPolicy
	if policy == "" {
		policy = schemaexport.FieldPolicyAll
	}
	readable, diagnostics, err := schemaexport.ExposedFields(db, table, schemaexport.ShapeRead, policy)
	if err != nil {
		return nil, nil, err
	}
	writable, _, err := schemaexport.ExposedFields(db, table, schemaexport.ShapeWrite, policy)
	if err != nil {
		return nil, nil, err
	}
	published := make(map[string]bool, len(readable))
	fields := append([]goschema.Field(nil), readable...)
	for _, field := range readable {
		published[field.Name] = true
	}
	for _, field := range writable {
		if !published[field.Name] {
			fields = append(fields, field)
			published[field.Name] = true
		}
	}
	kept := make([]schemaexport.Diagnostic, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		column := diagnostic.Path[strings.LastIndex(diagnostic.Path, ".")+1:]
		if published[column] {
			continue
		}
		kept = append(kept, diagnostic)
	}
	return fields, kept, nil
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
		// A Protobuf message carries no direction of its own, so a column
		// reaches it when either contract shape publishes it.
		//
		// Withholding a column that was exported before is, to this target,
		// exactly a column removal -- and removal already retires the number
		// and reserves the name under --proto-on-field-removal. That is what
		// keeps a wire contract safe across a projection change without a
		// second mechanism (stokaro/ptah#904).
		tableFields, exposureDiagnostics, err := b.exposedFields(db, table)
		if err != nil {
			return desiredShape{}, err
		}
		for _, diagnostic := range exposureDiagnostics {
			b.warn(diagnostic.Path, diagnostic.Message)
		}
		if len(tableFields) == 0 {
			// An empty message would render as "message X {}" and, more
			// importantly, could never be told apart from a tombstone on the
			// next run. internal/graphqlrender omits such a table too.
			b.warn(table.Name, "table has no exportable columns; message omitted")
			continue
		}
		msg := desiredMessage{
			Name:    names[table.QualifiedName()],
			Comment: b.sourceComment(table.Comment),
		}
		seen := make(map[string]string)
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
			if df.Import != "" {
				b.messageImports[msg.Name] = append(b.messageImports[msg.Name], df.Import)
			}
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
	bare := make(map[string][]goschema.Table)
	for _, table := range tables {
		name, changed := messageName(schemaexport.TableAPIName(table, schemaexport.TargetProtobuf))
		if changed {
			b.warn(table.Name, fmt.Sprintf(
				"table %q was sanitized to protobuf message %q; buf lint STANDARD will report MESSAGE_PASCAL_CASE for it",
				table.Name, name))
		}
		bare[name] = append(bare[name], table)
	}

	names := make(map[string]string)
	final := make(map[string][]goschema.Table)
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
	// An explicit type override the mapping cannot honor is refused rather than
	// defaulted to string. Here the stake is higher than in the other two
	// exporters: a wire type is persistent, and a silently defaulted one would
	// be pinned by the next reconcile against this file.
	if err := schemaexport.RefuseUnknownAPIType(
		table, f, schemaexport.TargetProtobuf, enumIndex,
		func(t string) bool { return mapProtoType(t).Known },
	); err != nil {
		return desiredField{}, err
	}
	// Substituted once, so the type mapping, the array detection and the enum
	// lookup below all read one answer.
	f = schemaexport.ProjectedField(f)

	// The protobuf field name is derived from the field's API name, and that is
	// what carries the wire compatibility: reconcileMessage keys existing field
	// NUMBERS by this name, so a column renamed while its api_name stays put
	// keeps its number, and changing the api_name goes through the ordinary
	// reservation policy instead (stokaro/ptah#905).
	//
	// The diagnostic path stays on the source names. It is not a coordinate in
	// the generated file; it is where the author has to go to change anything.
	name, changed, lintDirty := fieldName(schemaexport.FieldAPIName(f, schemaexport.TargetProtobuf))
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
		return desiredField{Name: name, Type: enumName, Repeated: isArray, Comment: b.sourceComment(f.Comment)}, nil
	}

	mapped := mapProtoType(element)
	if !mapped.Known {
		b.warn(path, fmt.Sprintf("column type %q is not recognized and was exported as string", f.Type))
	}
	if mapped.Lossy != "" {
		b.warn(path, mapped.Lossy)
	}
	return desiredField{
		Name:     name,
		Type:     mapped.Name,
		Repeated: isArray,
		Comment:  b.sourceComment(f.Comment),
		Import:   mapped.Import,
	}, nil
}

// sourceComment returns the comment a table or column contributes to the
// generated file. Suppression happens here, where source prose enters the
// model, rather than in the writer: what a published contract owes its readers
// is the wire shape plus Ptah's own account of it, and the database's internal
// documentation is neither. Stripping at render time would take the tombstone
// rationale with it and leave a bare `reserved` block no reader can explain.
//
// Comments are not part of the compatibility state - previous.go reads numbers
// and reservations only - so turning this on or off never moves a field number.
func (b *builder) sourceComment(comment string) string {
	if b.opts.Comments != CommentsAll {
		return ""
	}
	return comment
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
		raw = schemaexport.TypeName(schemaexport.TableAPIName(table, schemaexport.TargetProtobuf)) + schemaexport.PascalCase(schemaexport.FieldAPIName(f, schemaexport.TargetProtobuf))
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
	b.enumNames[name] = true
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
