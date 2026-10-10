package ydbtopic

import (
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"unicode/utf8"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/internal/ydbpath"
)

// Kind identifies one standalone YDB topic: a message queue at a path of the
// scheme tree, with its settings and consumers. A changefeed's topic belongs
// to its table and is not one of these.
const Kind schemaext.Kind = "ptah.run/ydb/topic"

// Desired is a declared topic. A setting it leaves at its zero value declares
// nothing, and [Resolve] reads it as the value YDB gives a new topic.
type Desired struct {
	Spec Spec `json:"spec"`
	// StructName preserves the Go holder that declared the topic. It has no
	// server counterpart and changes no statement.
	StructName string `json:"struct_name,omitempty"`
}

// Observed is a topic a database read described: every setting the server
// holds, the auto-partitioning ones only while auto-partitioning is enabled,
// and every consumer. Coverage records whether a read looked.
type Observed struct {
	Spec Spec `json:"spec"`
}

// Kind returns the topic model identity.
func (*Desired) Kind() schemaext.Kind { return Kind }

// Kind returns the topic model identity.
func (*Observed) Kind() schemaext.Kind { return Kind }

// Clone returns an independent declaration snapshot.
func (v *Desired) Clone() schemaext.Value {
	if v == nil {
		return (*Desired)(nil)
	}
	return &Desired{Spec: v.Spec.Clone(), StructName: v.StructName}
}

// Clone returns an independent observation snapshot.
func (v *Observed) Clone() schemaext.Value {
	if v == nil {
		return (*Observed)(nil)
	}
	return &Observed{Spec: v.Spec.Clone()}
}

// Equal compares captured declarations field by field, without resolving
// defaults: two declarations that resolve to one topic may still differ here.
func (v *Desired) Equal(other schemaext.Value) bool {
	right, ok := other.(*Desired)
	if !ok {
		return false
	}
	if v == nil || right == nil {
		return v == right
	}
	return v.StructName == right.StructName && specEqual(v.Spec, right.Spec)
}

// Equal compares raw observations field by field.
func (v *Observed) Equal(other schemaext.Value) bool {
	right, ok := other.(*Observed)
	if !ok {
		return false
	}
	if v == nil || right == nil {
		return v == right
	}
	return specEqual(v.Spec, right.Spec)
}

// Desired converts an observation into a declaration that keeps the topic as
// the database holds it: every setting the read reported, and every consumer.
func (v *Observed) Desired() *Desired {
	if v == nil {
		return nil
	}
	return &Desired{Spec: v.Spec.Clone()}
}

// Observed projects the declaration as a read would report it once applied.
// It establishes no inspection evidence; coverage is recorded separately.
func (v *Desired) Observed() *Observed {
	if v == nil {
		return nil
	}
	return &Observed{Spec: v.Spec.Clone()}
}

// Validate refuses a declaration YDB would not keep as written, by the rules
// [ParseTopic] and [ParseConsumer] read one with, a consumer named twice, and
// a holder name that is not valid UTF-8.
func (v *Desired) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: a desired topic is nil", schemaext.ErrInvalidValue)
	}
	if !utf8.ValidString(v.StructName) {
		return fmt.Errorf("%w: a topic's holder must be valid UTF-8", schemaext.ErrInvalidValue)
	}
	if err := Validate(v.Spec); err != nil {
		return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
	}
	return nil
}

// Validate holds spec to the rules a declaration is read with: its settings
// as [ParseTopic] takes them, and each consumer once, as [ParseConsumer]
// takes it.
func Validate(spec Spec) error {
	if err := checkSettings(spec); err != nil {
		return err
	}
	names := make(map[string]bool, len(spec.Consumers))
	for _, consumer := range spec.Consumers {
		if names[consumer.Name] {
			return fmt.Errorf("two of its consumers are named %q, and YDB names a consumer once per topic "+
				"(`Consumer %s defined more than once`)", consumer.Name, consumer.Name)
		}
		names[consumer.Name] = true
		if _, err := ParseConsumer(consumerValues(consumer)); err != nil {
			return fmt.Errorf("consumer %q: %w", consumer.Name, err)
		}
	}
	return nil
}

// Ref builds the exact identity of the topic name in the directory schema,
// relative to the database root. A literal dot stays part of its component.
func Ref(schema, name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).SchemaScopedParts(objectidentity.Kind(Kind), schema, name)
}

// DesiredObject records one declared topic. Collections clone the value on
// entry.
func DesiredObject(schema, name, structName string, spec Spec) schemaext.Object {
	return schemaext.Object{Ref: Ref(schema, name), Value: &Desired{Spec: spec.Clone(), StructName: structName}}
}

// ObservedObject records one topic a read described.
func ObservedObject(schema, name string, spec Spec) schemaext.Object {
	return schemaext.Object{Ref: Ref(schema, name), Value: &Observed{Spec: spec.Clone()}}
}

// ValidateIdentity requires separate exact YDB directory and leaf names: a
// leaf without a slash, and a directory that is a clean path relative to the
// database root.
func ValidateIdentity(ref objectidentity.ID) error {
	schema, name := ref.Schema.Source, ref.Name.Source
	if strings.TrimSpace(name) == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") || strings.ContainsRune(schema, 0) ||
		!utf8.ValidString(schema) || !utf8.ValidString(name) || ref != Ref(schema, name) ||
		(schema != "" && (path.IsAbs(schema) || path.Clean(schema) != schema || schema == "." || schema == ".." || strings.HasPrefix(schema, "../"))) {
		return fmt.Errorf("%w: a topic requires a schema-scoped YDB identity", schemaext.ErrInvalidValue)
	}
	return nil
}

// DuplicateError is a second declaration of one topic path. It wraps
// [schemaext.ErrDuplicate].
type DuplicateError struct {
	// Path is the topic's path relative to the database root.
	Path string
}

func (e *DuplicateError) Error() string { return "topic " + e.Path + " is declared twice" }

// Unwrap returns [schemaext.ErrDuplicate].
func (e *DuplicateError) Unwrap() error { return schemaext.ErrDuplicate }

// ErrAbsolutePath is what [ParsePath] wraps for a path that starts with a
// slash, which names the database it lies in; it is the error a secret's path
// is refused with too.
var ErrAbsolutePath = ydbpath.ErrAbsolute

// ErrOutsideDatabase is what [ResolvePath] wraps for an absolute path outside
// the database it is read against.
var ErrOutsideDatabase = ydbpath.ErrOutsideDatabase

// ParsePath reads the path of a topic relative to the database root, as YDB
// writes it: a slash separates directories, the segment after the last slash
// is the name, and a dot is part of the segment that holds it. Surrounding
// space is ignored. A path that starts with a slash is refused with
// [ErrAbsolutePath], and one with an empty, `.` or `..` segment, a trailing
// slash included, is refused. [ResolvePath] reads an absolute path where the
// database root is known.
func ParsePath(written string) (objectidentity.ID, error) {
	schema, name, err := ydbpath.Split(written)
	if err != nil {
		return objectidentity.ID{}, fmt.Errorf("%q is not a topic path (dir/name): %w: %w", written, schemaext.ErrInvalidValue, err)
	}
	ref := Ref(schema, name)
	if err := ValidateIdentity(ref); err != nil {
		return objectidentity.ID{}, fmt.Errorf("%q is not a topic path (dir/name): %w", written, err)
	}
	return ref, nil
}

// ResolvePath reads path as [ParsePath] does, except that a path starting
// with a slash is read against root, the absolute path of the database, such
// as /local: `/local/ext/events` is the topic ext/events there. An absolute
// path outside root is refused with [ErrOutsideDatabase]; with an empty root,
// every absolute path is refused with [ErrAbsolutePath].
func ResolvePath(root, written string) (objectidentity.ID, error) {
	relative, err := ydbpath.Relative(root, written)
	if errors.Is(err, ydbpath.ErrOutsideDatabase) {
		return objectidentity.ID{}, fmt.Errorf("%q is outside the database /%s: %w: %w", written, strings.Trim(root, "/"), schemaext.ErrInvalidValue, err)
	}
	if err != nil {
		return ParsePath(written)
	}
	return ParsePath(relative)
}

// CheckDirectory refuses a directory a topic cannot lie in: one written from
// the server root, which names the database the topic lies in, and one with a
// trailing slash, an empty segment, or a `.` or `..` segment. The directory of
// a topic, and the one a consumer names its topic in, are relative to the
// database root, so a topic and its consumers are refused alike. Surrounding
// space is ignored. The refusal is a [DeclarationError] naming the schema
// attribute.
func CheckDirectory(schema string) error {
	schema = strings.TrimSpace(schema)
	switch {
	case strings.HasPrefix(schema, "/"):
		return &DeclarationError{Attribute: AttributeSchema, Value: schema,
			Reason: "starts with a slash; name the directory relative to the database root, without the database's own path"}
	case schema != "" && ValidateIdentity(Ref(schema, "topic")) != nil:
		return &DeclarationError{Attribute: AttributeSchema, Value: schema,
			Reason: "is not a directory path relative to the database root; write it without a trailing slash or an empty, . or .. segment"}
	}
	return nil
}

// Declare returns objects with the topic name in the directory schema added,
// declared by the Go struct holder (empty for every other source format) as
// spec. Every source format declares a topic through it, so they agree on
// what a declaration may hold and on what a repeated one is told. objects is
// not modified.
//
// The name is one segment of the topic's path: it holds no slash, and a dot
// is part of it. The directory is relative to the database root and takes no
// leading or trailing slash; surrounding space is ignored. A name or directory
// a topic cannot take is refused with a [DeclarationError] naming the
// attribute, a spec YDB would not keep as written by [Validate], and a topic
// declared twice with a [DuplicateError] naming its path. Two declarations
// that meet only when sources are merged are refused by the merge, with
// [schemaext.ErrDuplicate] and the topic's identity.
func Declare(objects schemaext.Objects, schema, name, holder string, spec Spec) (schemaext.Objects, error) {
	name = strings.TrimSpace(name)
	switch {
	case name == "":
		return objects, &DeclarationError{Attribute: AttributeName, Reason: "a topic needs a name"}
	case strings.Contains(name, "/"):
		return objects, &DeclarationError{Attribute: AttributeName, Value: name,
			Reason: "holds a slash; name the directory with " + AttributeSchema}
	case ValidateIdentity(Ref("", name)) != nil:
		return objects, &DeclarationError{Attribute: AttributeName, Value: name, Reason: "is not a path segment"}
	}
	if err := CheckDirectory(schema); err != nil {
		return objects, err
	}
	object := DesiredObject(strings.TrimSpace(schema), name, holder, spec)
	if err := Validate(spec); err != nil {
		return objects, err
	}
	declared, err := objects.With(object)
	if errors.Is(err, schemaext.ErrDuplicate) {
		return objects, &DuplicateError{Path: Display(object.Ref.Schema.Source, name)}
	}
	if err != nil {
		return objects, err
	}
	return declared, nil
}

func specEqual(a, b Spec) bool {
	return a.MinActivePartitions == b.MinActivePartitions &&
		a.MaxActivePartitions == b.MaxActivePartitions &&
		a.AutoPartitioningStrategy == b.AutoPartitioningStrategy &&
		a.AutoPartitioningUpUtilizationPercent == b.AutoPartitioningUpUtilizationPercent &&
		a.AutoPartitioningDownUtilizationPercent == b.AutoPartitioningDownUtilizationPercent &&
		a.AutoPartitioningStabilizationWindow == b.AutoPartitioningStabilizationWindow &&
		a.RetentionPeriod == b.RetentionPeriod &&
		a.PartitionWriteSpeedBytesPerSecond == b.PartitionWriteSpeedBytesPerSecond &&
		a.PartitionWriteBurstBytes == b.PartitionWriteBurstBytes &&
		slices.Equal(a.SupportedCodecs, b.SupportedCodecs) &&
		slices.EqualFunc(a.Consumers, b.Consumers, consumerFieldsEqual)
}

func consumerFieldsEqual(a, b ConsumerSpec) bool {
	return a.Name == b.Name && a.Important == b.Important && a.ReadFrom == b.ReadFrom &&
		slices.Equal(a.SupportedCodecs, b.SupportedCodecs) && a.AvailabilityPeriod == b.AvailabilityPeriod
}
