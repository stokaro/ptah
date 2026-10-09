package ydbworkload_test

import (
	"encoding/json"
	"math"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine"
)

func TestPoolTransportPreservesEveryLimitAndSourceHolder(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(engine.Provider{ID: "ptah.run/ydb", Codecs: ydbworkload.Codecs()}))
	spec := ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0)), QueueSize: new(int32(1)),
		DatabaseLoadCPUThreshold: new(12.5), QueryMemoryLimitPercentPerNode: new(25.5),
		QueryCPULimitPercentPerNode: new(37.5), TotalCPULimitPercentPerNode: new(50.5), ResourceWeight: new(75.5)}
	original := &ydbworkload.DesiredPool{Spec: spec, StructName: "ReportingPool"}
	objects := must.Must(schemaext.NewObjects(schemaext.Object{Ref: ydbworkload.PoolRef("Reporting.v1"), Value: original}))
	data := must.Must(runtime.Codecs().EncodeObjects(t.Context(), schemaext.Desired, objects))
	decoded := must.Must(runtime.Codecs().DecodeObjects(t.Context(), schemaext.Desired, data))
	values := must.Must(decoded.All())
	c.Assert(values, qt.HasLen, 1)
	c.Assert(values[0].Ref, qt.DeepEquals, ydbworkload.PoolRef("Reporting.v1"))
	c.Assert(values[0].Value, qt.DeepEquals, original)
	copySpec := values[0].Value.(*ydbworkload.DesiredPool).Spec
	*copySpec.ConcurrentQueryLimit, *copySpec.QueueSize = 10, 20
	*copySpec.DatabaseLoadCPUThreshold, *copySpec.QueryMemoryLimitPercentPerNode = 90, 90
	*copySpec.QueryCPULimitPercentPerNode, *copySpec.TotalCPULimitPercentPerNode, *copySpec.ResourceWeight = 90, 90, 90
	c.Assert(original.Spec, qt.DeepEquals, ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0)), QueueSize: new(int32(1)),
		DatabaseLoadCPUThreshold: new(12.5), QueryMemoryLimitPercentPerNode: new(25.5),
		QueryCPULimitPercentPerNode: new(37.5), TotalCPULimitPercentPerNode: new(50.5), ResourceWeight: new(75.5)})
	c.Assert(original.Observed().Desired().StructName, qt.Equals, "")
	c.Assert(original.Observed().Spec, qt.DeepEquals, original.Spec)
	*original.Observed().Spec.ConcurrentQueryLimit = 40
	c.Assert(*original.Spec.ConcurrentQueryLimit, qt.Equals, int32(0))
}

func TestWorkloadCodecsPreserveEmptyPoolAndExplicitClassifierRank(t *testing.T) {
	for _, test := range []struct {
		name  string
		codec schemaext.Codec
		value schemaext.Value
		wire  string
	}{
		{"empty desired pool", ydbworkload.PoolCodecs()[0], &ydbworkload.DesiredPool{}, `{"spec":{}}`},
		{"empty observed pool", ydbworkload.PoolCodecs()[1], &ydbworkload.ObservedPool{}, `{"spec":{}}`},
		{"zero limit", ydbworkload.PoolCodecs()[0], &ydbworkload.DesiredPool{Spec: ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0))}}, `{"spec":{"concurrent_query_limit":0}}`},
		{"zero rank", ydbworkload.ClassifierCodecs()[0], &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "Reporting", Rank: 0}}, `{"spec":{"resource_pool":"Reporting","rank":0}}`},
		{"maximum rank", ydbworkload.ClassifierCodecs()[1], &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "missing", MemberName: "team", Rank: math.MaxInt64}}, `{"spec":{"resource_pool":"missing","member_name":"team","rank":9223372036854775807}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			wire, err := test.codec.Encode(test.value)
			c.Assert(err, qt.IsNil)
			c.Assert(string(wire), qt.Equals, test.wire)
			value, err := test.codec.Decode(wire)
			c.Assert(err, qt.IsNil)
			c.Assert(value, qt.DeepEquals, test.value)
		})
	}
}

func TestModelCodecsRefuseAmbiguousOrInvalidWire(t *testing.T) {
	for _, codec := range ydbworkload.Codecs() {
		for _, input := range []string{
			`null`, `{}`, `{"spec":null}`, `{"spec":[]}`, `{"Spec":{}}`, `{"spec":{},"extra":true}`,
			`{"spec":{},"spec":{}}`, `{"spec":{},"struct_name":null}`, `{"spec":{"UNKNOWN":0}}`,
			`{"spec":{"resource_pool":"p","rank":0,"Rank":1}}`, `{"spec":{"resource_pool":"p","rank":0,"rank":1}}`,
		} {
			t.Run(string(codec.Prototype.Kind())+"/"+string(codec.Representation)+"/"+input, func(t *testing.T) {
				c := qt.New(t)
				value, err := codec.Decode(json.RawMessage(input))
				c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
				c.Assert(value, qt.IsNil)
			})
		}
	}
}

func TestPoolCodecsRefuseInvalidLimits(t *testing.T) {
	for _, codec := range ydbworkload.PoolCodecs() {
		for _, input := range []string{
			`{"spec":{"concurrent_query_limit":null}}`, `{"spec":{"concurrent_query_limit":-1}}`,
			`{"spec":{"concurrent_query_limit":2147483648}}`, `{"spec":{"concurrent_query_limit":0.5}}`,
			`{"spec":{"queue_size":0}}`, `{"spec":{"resource_weight":101}}`, `{"spec":{"resource_weight":1e400}}`,
			`{"spec":{"ResourceWeight":12}}`, `{"spec":{"resource_weight":"12"}}`,
		} {
			t.Run(string(codec.Representation)+"/"+input, func(t *testing.T) {
				c := qt.New(t)
				value, err := codec.Decode(json.RawMessage(input))
				c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
				c.Assert(value, qt.IsNil)
			})
		}
	}
}

func TestClassifierCodecsRequireExplicitCompleteRouting(t *testing.T) {
	for _, codec := range ydbworkload.ClassifierCodecs() {
		for _, input := range []string{
			`{"spec":{}}`, `{"spec":{"resource_pool":"p"}}`, `{"spec":{"rank":0}}`,
			`{"spec":{"resource_pool":"","rank":0}}`, `{"spec":{"resource_pool":"p","rank":null}}`,
			`{"spec":{"resource_pool":"p","rank":-1}}`, `{"spec":{"resource_pool":"p","rank":9223372036854775808}}`,
			`{"spec":{"resource_pool":"p","rank":1.5}}`, `{"spec":{"resource_pool":"p","rank":0,"member_name":null}}`,
		} {
			t.Run(string(codec.Representation)+"/"+input, func(t *testing.T) {
				c := qt.New(t)
				value, err := codec.Decode(json.RawMessage(input))
				c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
				c.Assert(value, qt.IsNil)
			})
		}
	}
}

func TestRawEqualityAndProjectionPreserveOwnership(t *testing.T) {
	c := qt.New(t)
	unset := &ydbworkload.DesiredPool{}
	zero := &ydbworkload.DesiredPool{Spec: ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0))}}
	c.Assert(unset.Equal(zero), qt.IsFalse)
	c.Assert(unset.Equal(unset.Observed()), qt.IsFalse)
	c.Assert(unset.Equal(&ydbworkload.DesiredPool{StructName: "Holder"}), qt.IsFalse)
	c.Assert(zero.Clone(), qt.DeepEquals, zero)
	classifier := &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "P", MemberName: "Team", Rank: 0}, StructName: "Holder"}
	c.Assert(classifier.Clone(), qt.DeepEquals, classifier)
	c.Assert(classifier.Observed().Desired(), qt.DeepEquals, &ydbworkload.DesiredClassifier{Spec: classifier.Spec})
	c.Assert(classifier.Equal(classifier.Observed()), qt.IsFalse)
	c.Assert(classifier.Equal(classifier.Observed().Desired()), qt.IsFalse)
}

func TestWorkloadNamesAreDatabaseScopedAndCaseSensitive(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbworkload.ValidateIdentity(ydbworkload.PoolRef("Pool.v1"), ydbworkload.PoolKind), qt.IsNil)
	c.Assert(ydbworkload.ValidateIdentity(ydbworkload.ClassifierRef("Team`'"), ydbworkload.ClassifierKind), qt.IsNil)
	c.Assert(ydbworkload.PoolRef("Pool").Equal(ydbworkload.PoolRef("pool")), qt.IsFalse)
	c.Assert(ydbworkload.PoolRef("same").Equal(ydbworkload.ClassifierRef("same")), qt.IsFalse)
}

func TestWorkloadIdentityRefusesPathsAndForeignKinds(t *testing.T) {
	builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))
	for _, ref := range []objectidentity.ID{
		ydbworkload.PoolRef(""), ydbworkload.PoolRef("dir/pool"), ydbworkload.PoolRef("two pools"), ydbworkload.PoolRef("пул"),
		ydbworkload.ClassifierRef("pool"), builder.SchemaScopedParts(objectidentity.Kind(ydbworkload.PoolKind), "dir", "pool"),
	} {
		t.Run(ref.String(), func(t *testing.T) {
			qt.New(t).Assert(ydbworkload.ValidateIdentity(ref, ydbworkload.PoolKind), qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}

func TestCoverageKeepsFamiliesAndUnreadableSubjectsSeparate(t *testing.T) {
	c := qt.New(t)
	unknown := schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "metadata access denied"}
	coverage, err := ydbworkload.Coverage(ydbworkload.PoolKind, schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
		{Kind: ydbworkload.PoolKind, Subject: ydbworkload.PoolRef("hidden"), Knowledge: unknown},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(coverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("hidden")), qt.DeepEquals, unknown)
	c.Assert(coverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("missing")).State, qt.Equals, schemaext.Complete)
	c.Assert(coverage.Lookup(ydbworkload.ClassifierKind, ydbworkload.ClassifierRef("missing")).State, qt.Equals, schemaext.Uninspected)
}
