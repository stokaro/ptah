package ydbtopic_test

import (
	"encoding/json"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine"
)

// fullSpec names every setting and two consumers.
func fullSpec() ydbtopic.Spec {
	return ydbtopic.Spec{
		MinActivePartitions: 2, MaxActivePartitions: 6, AutoPartitioningStrategy: "scale_up",
		AutoPartitioningUpUtilizationPercent: 70, AutoPartitioningDownUtilizationPercent: 10,
		AutoPartitioningStabilizationWindow: "PT2M", RetentionPeriod: "PT36H",
		PartitionWriteSpeedBytesPerSecond: 2097152, PartitionWriteBurstBytes: 3145728,
		SupportedCodecs: []string{"raw", "gzip"},
		Consumers: []ydbtopic.ConsumerSpec{
			{Name: "billing", Important: true},
			{Name: "audit", ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"zstd"}, AvailabilityPeriod: "PT2H"},
		},
	}
}

// TestModelTransport_KeepsSettingsAndConsumers round-trips a declaration and
// an observation through the registered codecs: every setting, every consumer
// and the holder survive, and the decoded value is independent of the
// original.
func TestModelTransport_KeepsSettingsAndConsumers(t *testing.T) {
	tests := []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
	}{
		{name: "a declaration", representation: schemaext.Desired, value: &ydbtopic.Desired{Spec: fullSpec(), StructName: "Events"}},
		{name: "an observation", representation: schemaext.Observed, value: &ydbtopic.Observed{Spec: fullSpec()}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(engine.New(engine.Provider{ID: "ptah.run/ydb", Codecs: ydbtopic.Codecs()}))
			objects := must.Must(schemaext.NewObjects(schemaext.Object{Ref: ydbtopic.Ref("app", "events.v1"), Value: test.value}))

			data, err := runtime.Codecs().EncodeObjects(t.Context(), test.representation, objects)
			c.Assert(err, qt.IsNil)
			decoded, err := runtime.Codecs().DecodeObjects(t.Context(), test.representation, data)

			c.Assert(err, qt.IsNil)
			values, err := decoded.All()
			c.Assert(err, qt.IsNil)
			c.Assert(values, qt.DeepEquals, []schemaext.Object{{Ref: ydbtopic.Ref("app", "events.v1"), Value: test.value}})
		})
	}
}

// TestModelClone_IsIndependent changes a clone's lists and leaves the
// original as it was.
func TestModelClone_IsIndependent(t *testing.T) {
	c := qt.New(t)
	original := &ydbtopic.Desired{Spec: fullSpec(), StructName: "Events"}

	clone := original.Clone().(*ydbtopic.Desired)
	clone.Spec.SupportedCodecs[0] = "lzop"
	clone.Spec.Consumers[1].SupportedCodecs[0] = "raw"
	clone.Spec.Consumers[0].Name = "other"

	c.Assert(original, qt.DeepEquals, &ydbtopic.Desired{Spec: fullSpec(), StructName: "Events"})
}

// TestConversion_KeepsTheTopic turns an observation into a declaration that
// keeps the topic as the database holds it, and a declaration into the
// observation a read would report once it is applied.
func TestConversion_KeepsTheTopic(t *testing.T) {
	c := qt.New(t)
	c.Assert((&ydbtopic.Observed{Spec: fullSpec()}).Desired(), qt.DeepEquals, &ydbtopic.Desired{Spec: fullSpec()})
	c.Assert((&ydbtopic.Desired{Spec: fullSpec(), StructName: "Events"}).Observed(), qt.DeepEquals, &ydbtopic.Observed{Spec: fullSpec()})
}

// TestModelCodecs_RefuseALossyWire refuses a document that would decode to a
// topic other than the one written: an unknown field at any depth, a null, a
// missing spec, a holder on an observation, and settings or consumers YDB
// would not keep as written.
func TestModelCodecs_RefuseALossyWire(t *testing.T) {
	tests := []struct {
		name           string
		representation int
		input          string
	}{
		{name: "an unknown field", representation: 0, input: `{"spec":{},"retention_storage_mb":100}`},
		{name: "an unknown setting", representation: 0, input: `{"spec":{"retention_storage_mb":100}}`},
		{name: "an unknown consumer field", representation: 1, input: `{"spec":{"consumers":[{"name":"c","type":"shared"}]}}`},
		{name: "a null setting", representation: 0, input: `{"spec":{"retention_period":null}}`},
		{name: "a null document", representation: 1, input: `null`},
		{name: "a null in a list", representation: 0, input: `{"spec":{"supported_codecs":["raw",null]}}`},
		{name: "no spec", representation: 0, input: `{"struct_name":"Events"}`},
		{name: "a holder on an observation", representation: 1, input: `{"spec":{},"struct_name":"Events"}`},
		{name: "a maximum without a strategy", representation: 0, input: `{"spec":{"max_active_partitions":4}}`},
		{name: "a consumer named twice", representation: 1, input: `{"spec":{"consumers":[{"name":"c"},{"name":"c"}]}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value, err := ydbtopic.Codecs()[test.representation].Decode(json.RawMessage(test.input))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

// TestValidateIdentity_FailurePath refuses an identity that is not a
// directory and a leaf relative to the database root.
func TestValidateIdentity_FailurePath(t *testing.T) {
	tests := []struct {
		name         string
		schema, leaf string
	}{
		{name: "a slash in the leaf", leaf: "app/events"},
		{name: "an absolute directory", schema: "/app", leaf: "events"},
		{name: "a parent directory", schema: "../app", leaf: "events"},
		{name: "an unclean directory", schema: "app//queues", leaf: "events"},
		{name: "an empty leaf", schema: "app"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbtopic.ValidateIdentity(ydbtopic.Ref(test.schema, test.leaf)), qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}

// TestParsePath_HappyPath reads a topic's path: a slash separates directories
// and a dot stays in its segment.
func TestParsePath_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		schema, leaf string
	}{
		{name: "a dotted root name", path: "events.v1", leaf: "events.v1"},
		{name: "a directory", path: "app/events", schema: "app", leaf: "events"},
		{name: "a changefeed's topic", path: "app/orders/updates", schema: "app/orders", leaf: "updates"},
		{name: "surrounding space", path: " app/events ", schema: "app", leaf: "events"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref, err := ydbtopic.ParsePath(test.path)
			c.Assert(err, qt.IsNil)
			c.Assert(ref, qt.DeepEquals, ydbtopic.Ref(test.schema, test.leaf))
		})
	}
}

// TestParsePath_FailurePath refuses a path with no name or with an empty,
// current or parent segment, a trailing slash included, and a path written
// from the server root.
func TestParsePath_FailurePath(t *testing.T) {
	tests := []struct {
		path   string
		wantIs error
	}{
		{path: "", wantIs: schemaext.ErrInvalidValue},
		{path: "app/..", wantIs: schemaext.ErrInvalidValue},
		{path: "app//events", wantIs: schemaext.ErrInvalidValue},
		{path: "app/events/", wantIs: schemaext.ErrInvalidValue},
		{path: "/local/app/events", wantIs: ydbtopic.ErrAbsolutePath},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			c := qt.New(t)
			ref, err := ydbtopic.ParsePath(test.path)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, `".*" is not a topic path \(dir/name\): .*`)
			c.Assert(ref, qt.DeepEquals, objectidentity.ID{})
		})
	}
}

// TestResolvePath_HappyPath reads an absolute path against the database root
// it lies under, and a relative one as [ydbtopic.ParsePath] does.
func TestResolvePath_HappyPath(t *testing.T) {
	tests := []struct {
		name, root, path string
		want             objectidentity.ID
	}{
		{name: "relative", root: "/local", path: "app/events", want: ydbtopic.Ref("app", "events")},
		{name: "absolute under the root", root: "/local", path: "/local/app/events", want: ydbtopic.Ref("app", "events")},
		{name: "absolute at the root", root: "/local", path: "/local/events.v1", want: ydbtopic.Ref("", "events.v1")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref, err := ydbtopic.ResolvePath(test.root, test.path)
			c.Assert(err, qt.IsNil)
			c.Assert(ref, qt.DeepEquals, test.want)
		})
	}
}

// TestResolvePath_FailurePath refuses an absolute path outside the root, and
// any absolute path when the root is not known.
func TestResolvePath_FailurePath(t *testing.T) {
	tests := []struct {
		name, root, path string
		wantIs           error
		wantErr          string
	}{
		{name: "another database", root: "/local", path: "/other/app/events", wantIs: ydbtopic.ErrOutsideDatabase,
			wantErr: `"/other/app/events" is outside the database /local: .*`},
		{name: "no root", root: "", path: "/local/app/events", wantIs: ydbtopic.ErrAbsolutePath,
			wantErr: `"/local/app/events" is not a topic path \(dir/name\): .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref, err := ydbtopic.ResolvePath(test.root, test.path)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(ref, qt.DeepEquals, objectidentity.ID{})
		})
	}
}

// TestDeclare_HappyPath adds each declaration under its path, with a dot kept
// in the name and the directory read relative to the database root, and leaves
// the input collection as it was.
func TestDeclare_HappyPath(t *testing.T) {
	c := qt.New(t)
	empty := schemaext.Objects{}
	objects, err := ydbtopic.Declare(empty, " app/queues ", " events.v1 ", "Events", fullSpec())
	c.Assert(err, qt.IsNil)
	objects, err = ydbtopic.Declare(objects, "", "plain", "", ydbtopic.Spec{})
	c.Assert(err, qt.IsNil)

	values, err := objects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.ContentEquals, []schemaext.Object{
		ydbtopic.DesiredObject("app/queues", "events.v1", "Events", fullSpec()),
		ydbtopic.DesiredObject("", "plain", "", ydbtopic.Spec{}),
	})
	c.Assert(empty.Len(), qt.Equals, 0)
}

// TestDeclare_FailurePath refuses a declaration a topic cannot take, naming
// the attribute, and a spec YDB would not keep as written.
func TestDeclare_FailurePath(t *testing.T) {
	declared := must.Must(ydbtopic.Declare(schemaext.Objects{}, "app", "events", "", ydbtopic.Spec{}))
	tests := []struct {
		name         string
		schema, leaf string
		wantErr      string
		attribute    string
	}{
		{name: "no name", leaf: " ", wantErr: "invalid name: a topic needs a name", attribute: ydbtopic.AttributeName},
		{name: "a path as the name", leaf: "app/events", wantErr: `invalid name "app/events": holds a slash; name the directory with schema`,
			attribute: ydbtopic.AttributeName},
		{name: "a parent segment as the name", leaf: "..", wantErr: `invalid name "..": is not a path segment`, attribute: ydbtopic.AttributeName},
		{name: "an unclean directory", schema: "app//queues", leaf: "events",
			wantErr: `invalid schema "app//queues": is not a directory path relative to the database root; write it without a trailing slash or an empty, \. or \.\. segment`, attribute: ydbtopic.AttributeSchema},
		{name: "an absolute directory", schema: " /local/app ", leaf: "events",
			wantErr:   `invalid schema "/local/app": starts with a slash; name the directory relative to the database root, without the database's own path`,
			attribute: ydbtopic.AttributeSchema},
		{name: "a trailing slash", schema: "app/", leaf: "events",
			wantErr: `invalid schema "app/": is not a directory path relative to the database root; write it without a trailing slash or an empty, \. or \.\. segment`, attribute: ydbtopic.AttributeSchema},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			objects, err := ydbtopic.Declare(declared, test.schema, test.leaf, "", ydbtopic.Spec{})
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			declarationError, ok := errors.AsType[*ydbtopic.DeclarationError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(declarationError.Attribute, qt.Equals, test.attribute)
			c.Assert(objects.Refs(), qt.DeepEquals, declared.Refs())
		})
	}
}

// TestDeclare_RefusesASpecYDBWouldNotKeep refuses a declaration whose settings
// or consumers YDB would refuse or keep differently.
func TestDeclare_RefusesASpecYDBWouldNotKeep(t *testing.T) {
	c := qt.New(t)

	objects, err := ydbtopic.Declare(schemaext.Objects{}, "app", "events", "", ydbtopic.Spec{
		Consumers: []ydbtopic.ConsumerSpec{{Name: "c"}, {Name: "c"}}})

	c.Assert(err, qt.ErrorMatches, `two of its consumers are named "c", .*`)
	c.Assert(objects.Len(), qt.Equals, 0)
}

// TestDeclare_RefusesASecondDeclaration refuses a second declaration of one
// path, even one with the same settings, naming the path: a topic has one
// declaration.
func TestDeclare_RefusesASecondDeclaration(t *testing.T) {
	c := qt.New(t)
	declared := must.Must(ydbtopic.Declare(schemaext.Objects{}, "app", "events", "", ydbtopic.Spec{}))

	objects, err := ydbtopic.Declare(declared, " app ", "events", "Other", ydbtopic.Spec{})

	c.Assert(err, qt.ErrorMatches, "topic app/events is declared twice")
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(err, qt.ErrorAs, new(*ydbtopic.DuplicateError))
	c.Assert(objects.Refs(), qt.DeepEquals, declared.Refs())
}

// TestCheckDirectory_HappyPath takes a directory relative to the database
// root, and none.
func TestCheckDirectory_HappyPath(t *testing.T) {
	for _, schema := range []string{"", "app", " app/queues "} {
		t.Run(schema, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbtopic.CheckDirectory(schema), qt.IsNil)
		})
	}
}

// TestCheckDirectory_FailurePath refuses a directory written from the server
// root and one that is not a clean path, naming the schema attribute.
func TestCheckDirectory_FailurePath(t *testing.T) {
	tests := []struct{ name, schema, wantErr string }{
		{name: "from the server root", schema: " /local/app", wantErr: `invalid schema "/local/app": starts with a slash; .*`},
		{name: "a trailing slash", schema: "app/", wantErr: `invalid schema "app/": is not a directory path relative to the database root; .*`},
		{name: "an empty segment", schema: "app//queues", wantErr: `invalid schema "app//queues": is not a directory path relative to the database root; .*`},
		{name: "a parent segment", schema: "../app", wantErr: `invalid schema "\.\./app": is not a directory path relative to the database root; .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := ydbtopic.CheckDirectory(test.schema)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			declarationError, ok := errors.AsType[*ydbtopic.DeclarationError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(declarationError.Attribute, qt.Equals, ydbtopic.AttributeSchema)
		})
	}
}

// TestCoverage_EnrollsTheTopicModel records a claim about the topic namespace
// in either representation, and nothing about another kind.
func TestCoverage_EnrollsTheTopicModel(t *testing.T) {
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		t.Run(string(representation), func(t *testing.T) {
			c := qt.New(t)
			coverage, err := ydbtopic.Coverage(representation, schemaext.Knowledge{State: schemaext.Complete}, nil)
			c.Assert(err, qt.IsNil)
			c.Assert(coverage.Lookup(ydbtopic.Kind, ydbtopic.Ref("app", "events")).State, qt.Equals, schemaext.Complete)
			c.Assert(coverage.KindRecords(), qt.HasLen, 1)
		})
	}
}
