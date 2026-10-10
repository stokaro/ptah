package ydbexternal

import (
	"errors"
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"
	"unicode/utf8"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/internal/ydbpath"
)

// SourceKind identifies one YDB external data source, a scheme object that
// names another system and how YDB reaches it.
const SourceKind schemaext.Kind = "ptah.run/ydb/external-data-source"

// TableKind identifies one YDB external table: columns over files a data
// source of type ObjectStorage holds.
const TableKind schemaext.Kind = "ptah.run/ydb/external-table"

// DesiredSource is a declared external data source.
type DesiredSource struct {
	Spec DataSource `json:"spec"`
	// StructName preserves the Go holder that declared the data source. It
	// has no server counterpart and changes no statement.
	StructName string `json:"struct_name,omitempty"`
}

// ObservedSource is an external data source a database read described, with
// each secret path an option names relative to the database root where it
// lies under it, and without REFERENCES, which the server keeps.
type ObservedSource struct {
	Spec DataSource `json:"spec"`
}

// DesiredTable is a declared external table.
type DesiredTable struct {
	Spec Table `json:"spec"`
	// StructName preserves the Go holder that declared the external table.
	StructName string `json:"struct_name,omitempty"`
}

// ObservedTable is an external table a database read described: its data
// source's path as the server stores it, absolute, and each option as a
// declaration writes it.
type ObservedTable struct {
	Spec Table `json:"spec"`
}

// Kind returns the data source model identity.
func (*DesiredSource) Kind() schemaext.Kind { return SourceKind }

// Kind returns the data source model identity.
func (*ObservedSource) Kind() schemaext.Kind { return SourceKind }

// Kind returns the external table model identity.
func (*DesiredTable) Kind() schemaext.Kind { return TableKind }

// Kind returns the external table model identity.
func (*ObservedTable) Kind() schemaext.Kind { return TableKind }

// Clone returns an independent declaration snapshot.
func (v *DesiredSource) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredSource)(nil)
	}
	return &DesiredSource{Spec: v.Spec.Clone(), StructName: v.StructName}
}

// Clone returns an independent observation snapshot.
func (v *ObservedSource) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedSource)(nil)
	}
	return &ObservedSource{Spec: v.Spec.Clone()}
}

// Clone returns an independent declaration snapshot.
func (v *DesiredTable) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredTable)(nil)
	}
	return &DesiredTable{Spec: v.Spec.Clone(), StructName: v.StructName}
}

// Clone returns an independent observation snapshot.
func (v *ObservedTable) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedTable)(nil)
	}
	return &ObservedTable{Spec: v.Spec.Clone()}
}

// Equal compares captured declarations field by field. [SameDataSource]
// decides whether two descriptions are one data source.
func (v *DesiredSource) Equal(other schemaext.Value) bool {
	right, ok := other.(*DesiredSource)
	if !ok || v == nil || right == nil {
		return ok && v == right
	}
	return v.StructName == right.StructName && v.Spec.equal(right.Spec)
}

// Equal compares raw observations field by field.
func (v *ObservedSource) Equal(other schemaext.Value) bool {
	right, ok := other.(*ObservedSource)
	if !ok || v == nil || right == nil {
		return ok && v == right
	}
	return v.Spec.equal(right.Spec)
}

// Equal compares captured declarations field by field. [SameTable] decides
// whether two descriptions are one external table.
func (v *DesiredTable) Equal(other schemaext.Value) bool {
	right, ok := other.(*DesiredTable)
	if !ok || v == nil || right == nil {
		return ok && v == right
	}
	return v.StructName == right.StructName && v.Spec.equal(right.Spec)
}

// Equal compares raw observations field by field.
func (v *ObservedTable) Equal(other schemaext.Value) bool {
	right, ok := other.(*ObservedTable)
	if !ok || v == nil || right == nil {
		return ok && v == right
	}
	return v.Spec.equal(right.Spec)
}

// Desired converts an observation into a declaration that keeps the data
// source as the database holds it.
func (v *ObservedSource) Desired() *DesiredSource {
	if v == nil {
		return nil
	}
	return &DesiredSource{Spec: v.Spec.Clone()}
}

// Observed projects the declaration as a read would report it once applied.
func (v *DesiredSource) Observed() *ObservedSource {
	if v == nil {
		return nil
	}
	return &ObservedSource{Spec: v.Spec.Clone()}
}

// Desired converts an observation into a declaration that keeps the external
// table as the database holds it.
func (v *ObservedTable) Desired() *DesiredTable {
	if v == nil {
		return nil
	}
	return &DesiredTable{Spec: v.Spec.Clone()}
}

// Observed projects the declaration as a read would report it once applied.
func (v *DesiredTable) Observed() *ObservedTable {
	if v == nil {
		return nil
	}
	return &ObservedTable{Spec: v.Spec.Clone()}
}

// Validate refuses a declaration no statement can carry: a source type or an
// auth method left out, or an option name the declaration may not write.
func (v *DesiredSource) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: a desired external data source is nil", schemaext.ErrInvalidValue)
	}
	if !utf8.ValidString(v.StructName) {
		return fmt.Errorf("%w: a data source's holder must be valid UTF-8", schemaext.ErrInvalidValue)
	}
	return v.Spec.Validate()
}

// Validate refuses a declaration no statement can carry: no data source, no
// location, no column or an invalid one, or an option name the declaration
// may not write.
func (v *DesiredTable) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: a desired external table is nil", schemaext.ErrInvalidValue)
	}
	if !utf8.ValidString(v.StructName) {
		return fmt.Errorf("%w: an external table's holder must be valid UTF-8", schemaext.ErrInvalidValue)
	}
	return v.Spec.Validate()
}

// Clone returns an independent copy of the data source's settings.
func (s DataSource) Clone() DataSource {
	s.Options = maps.Clone(s.Options)
	return s
}

// Clone returns an independent copy of the external table's settings.
func (t Table) Clone() Table {
	t.Columns = slices.Clone(t.Columns)
	t.Options = maps.Clone(t.Options)
	return t
}

func (s DataSource) equal(other DataSource) bool {
	return s.SourceType == other.SourceType && s.Location == other.Location && s.AuthMethod == other.AuthMethod &&
		maps.Equal(s.Options, other.Options)
}

func (t Table) equal(other Table) bool {
	return t.DataSource == other.DataSource && t.Location == other.Location && slices.Equal(t.Columns, other.Columns) &&
		maps.Equal(t.Options, other.Options)
}

// Validate refuses a data source no statement can carry: no source type, no
// auth method, or an option name a declaration may not write (see
// [CheckOptions]). Option names are kept in upper case, as the server keeps
// them.
func (s DataSource) Validate() error {
	switch {
	case strings.TrimSpace(s.SourceType) == "":
		return &DeclarationError{Attribute: AttributeSourceType, Reason: "a data source needs a source_type, such as ObjectStorage or PostgreSQL"}
	case strings.TrimSpace(s.AuthMethod) == "":
		return &DeclarationError{Attribute: AttributeAuthMethod, Reason: "a data source needs an auth_method, such as NONE or BASIC"}
	}
	return checkCanonicalOptions(s.Options, DataSourceReserved)
}

// Validate refuses an external table no statement can carry: no data source,
// no location, columns [CheckColumns] refuses, or an option name a
// declaration may not write.
func (t Table) Validate() error {
	switch {
	case strings.TrimSpace(t.DataSource) == "":
		return &DeclarationError{Attribute: AttributeDataSource, Reason: "an external table needs a data_source, the path of the data source it reads"}
	case strings.TrimSpace(t.Location) == "":
		return &DeclarationError{Attribute: AttributeLocation, Reason: "an external table needs a location"}
	}
	if err := CheckColumns(t.Columns); err != nil {
		return err
	}
	return checkCanonicalOptions(t.Options, TableReserved)
}

// checkCanonicalOptions refuses an option name [CheckOptions] refuses or would
// not return as it is: one that is not upper case, or not one a declaration
// writes. A value is kept as written.
func checkCanonicalOptions(options map[string]string, reserved []string) error {
	checked, err := CheckOptions(options, reserved...)
	if err != nil {
		return err
	}
	for name := range options {
		if _, kept := checked[name]; !kept {
			return &DeclarationError{Attribute: AttributeOptions, Reason: fmt.Sprintf("option name %q is kept in upper case", name)}
		}
	}
	return nil
}

// SourceRef builds the exact identity of the data source name in the
// directory schema, relative to the database root.
func SourceRef(schema, name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).SchemaScopedParts(objectidentity.Kind(SourceKind), schema, name)
}

// TableRef builds the exact identity of the external table name in the
// directory schema, relative to the database root.
func TableRef(schema, name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).SchemaScopedParts(objectidentity.Kind(TableKind), schema, name)
}

// DesiredSourceObject records one declared data source.
func DesiredSourceObject(schema, name, structName string, spec DataSource) schemaext.Object {
	return schemaext.Object{Ref: SourceRef(schema, name), Value: &DesiredSource{Spec: spec.Clone(), StructName: structName}}
}

// ObservedSourceObject records one data source a read described.
func ObservedSourceObject(schema, name string, spec DataSource) schemaext.Object {
	return schemaext.Object{Ref: SourceRef(schema, name), Value: &ObservedSource{Spec: spec.Clone()}}
}

// DesiredTableObject records one declared external table.
func DesiredTableObject(schema, name, structName string, spec Table) schemaext.Object {
	return schemaext.Object{Ref: TableRef(schema, name), Value: &DesiredTable{Spec: spec.Clone(), StructName: structName}}
}

// ObservedTableObject records one external table a read described.
func ObservedTableObject(schema, name string, spec Table) schemaext.Object {
	return schemaext.Object{Ref: TableRef(schema, name), Value: &ObservedTable{Spec: spec.Clone()}}
}

// ValidateIdentity requires the identity of a data source or an external
// table: one of the two kinds, a leaf without a slash, and a directory that is
// a clean path relative to the database root.
func ValidateIdentity(ref objectidentity.ID) error {
	schema, name := ref.Schema.Source, ref.Name.Source
	var want objectidentity.ID
	switch schemaext.Kind(ref.Kind) {
	case SourceKind:
		want = SourceRef(schema, name)
	case TableKind:
		want = TableRef(schema, name)
	default:
		return fmt.Errorf("%w: %s is not an external object", schemaext.ErrInvalidValue, ref.Kind)
	}
	if strings.TrimSpace(name) == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") || strings.ContainsRune(schema, 0) ||
		!utf8.ValidString(schema) || !utf8.ValidString(name) || ref != want ||
		(schema != "" && (path.IsAbs(schema) || path.Clean(schema) != schema || schema == "." || schema == ".." || strings.HasPrefix(schema, "../"))) {
		return fmt.Errorf("%w: an external object requires a schema-scoped YDB identity", schemaext.ErrInvalidValue)
	}
	return nil
}

// DuplicateError is a second declaration of one external object's path. It
// wraps [schemaext.ErrDuplicate].
type DuplicateError struct {
	// Family is "external data source" or "external table".
	Family string
	// Path is the object's path relative to the database root.
	Path string
}

func (e *DuplicateError) Error() string { return e.Family + " " + e.Path + " is declared twice" }

// Unwrap returns [schemaext.ErrDuplicate].
func (e *DuplicateError) Unwrap() error { return schemaext.ErrDuplicate }

// DeclareSource returns objects with the data source name in the directory
// schema added, declared by the Go struct holder (empty for every other source
// format) as spec. Every source format declares a data source through it, so
// they agree on what a declaration may hold and on what a repeated one is
// told. objects is not modified.
//
// The name is one segment of the path: it holds no slash, and a dot is part of
// it. Slashes around the directory are dropped and surrounding space is
// ignored. A name or directory an object cannot take is refused with a
// [DeclarationError] naming the attribute, a spec no statement can carry by
// [DataSource.Validate], and a data source declared twice with a
// [DuplicateError].
func DeclareSource(objects schemaext.Objects, schema, name, holder string, spec DataSource) (schemaext.Objects, error) {
	schema, name, err := declaredPath(schema, name)
	if err != nil {
		return objects, err
	}
	if err := spec.Validate(); err != nil {
		return objects, err
	}
	return declare(objects, DesiredSourceObject(schema, name, holder, spec), "external data source")
}

// DeclareTable returns objects with the external table name in the directory
// schema added, as [DeclareSource] adds a data source.
func DeclareTable(objects schemaext.Objects, schema, name, holder string, spec Table) (schemaext.Objects, error) {
	schema, name, err := declaredPath(schema, name)
	if err != nil {
		return objects, err
	}
	if err := spec.Validate(); err != nil {
		return objects, err
	}
	return declare(objects, DesiredTableObject(schema, name, holder, spec), "external table")
}

// declaredPath reads a declaration's directory and name, and refuses a path an
// object cannot take.
func declaredPath(schema, name string) (directory, leaf string, err error) {
	name = strings.TrimSpace(name)
	schema = strings.Trim(strings.TrimSpace(schema), "/")
	switch {
	case name == "":
		return "", "", &DeclarationError{Attribute: AttributeName, Reason: "an external object needs a name"}
	case strings.Contains(name, "/"):
		return "", "", &DeclarationError{Attribute: AttributeName,
			Reason: fmt.Sprintf("%q holds a slash; name the directory with %s", name, AttributeSchema)}
	case ValidateIdentity(SourceRef("", name)) != nil:
		return "", "", &DeclarationError{Attribute: AttributeName, Reason: fmt.Sprintf("%q is not a path segment", name)}
	case schema != "" && ValidateIdentity(SourceRef(schema, name)) != nil:
		return "", "", &DeclarationError{Attribute: AttributeSchema,
			Reason: fmt.Sprintf("%q is not a directory path relative to the database root", schema)}
	}
	return schema, name, nil
}

func declare(objects schemaext.Objects, object schemaext.Object, family string) (schemaext.Objects, error) {
	declared, err := objects.With(object)
	if errors.Is(err, schemaext.ErrDuplicate) {
		return objects, &DuplicateError{Family: family, Path: Display(object.Ref.Schema.Source, object.Ref.Name.Source)}
	}
	if err != nil {
		return objects, err
	}
	return declared, nil
}

// Display names an external object by the path YDB writes for it, unquoted,
// for messages: `dir/name`, or `name` at the database root.
func Display(schema, name string) string {
	if schema == "" {
		return name
	}
	return schema + "/" + name
}

// ParsePath reads the path of an external object of kind relative to the
// database root, as a limit names one: a slash separates directories and a
// dot is part of a name. A path that starts with a slash is refused with
// [ydbpath.ErrAbsolute], as is one with an empty, `.` or `..` segment.
func ParsePath(kind schemaext.Kind, written string) (objectidentity.ID, error) {
	schema, name, err := ydbpath.Split(written)
	if err != nil {
		return objectidentity.ID{}, fmt.Errorf("%q is not a path (dir/name): %w: %w", written, schemaext.ErrInvalidValue, err)
	}
	var ref objectidentity.ID
	switch kind {
	case SourceKind:
		ref = SourceRef(schema, name)
	case TableKind:
		ref = TableRef(schema, name)
	default:
		return objectidentity.ID{}, fmt.Errorf("%w: %s is not an external object", schemaext.ErrInvalidValue, kind)
	}
	if err := ValidateIdentity(ref); err != nil {
		return objectidentity.ID{}, fmt.Errorf("%q is not a path (dir/name): %w", written, err)
	}
	return ref, nil
}

// ResolveSource reads the path of a data source an external table names,
// relative to root, the absolute path of the database, or written absolute
// under it. It reports false for a path outside root, for an absolute path
// when root is empty, and for one that cannot name a data source.
func ResolveSource(root, written string) (objectidentity.ID, bool) {
	relative, err := ydbpath.Relative(root, strings.TrimSpace(written))
	if err != nil {
		return objectidentity.ID{}, false
	}
	ref, err := ParsePath(SourceKind, relative)
	return ref, err == nil
}

// The reasons a statement on an external object needs review, which the
// change values and the safety report give. Neither object holds data in YDB,
// so none of them loses data YDB stores.
const (
	// DropSourceReason is why dropping a data source needs review.
	DropSourceReason = "DROP EXTERNAL DATA SOURCE removes what YDB reads another system through; no data YDB stores is lost"
	// DropTableReason is why dropping an external table needs review.
	DropTableReason = "DROP EXTERNAL TABLE removes the columns YDB reads files through; the files stay"
	// ReplaceReason is why replacing an external object needs review.
	ReplaceReason = "CREATE OR REPLACE changes what queries reading the external object read"
)
