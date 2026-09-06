package dbschema

// White-box testing required: recordUnmodeledObjectKinds is unexported, and
// reaching it through ReadSchemaWithSchemas needs a live server of the one
// dialect the rule is about.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
)

// TestRecordUnmodeledObjectKinds_SpannerChangeStreams pins that a description
// says what it does not cover.
//
// A change stream is a Spanner object with its own lifecycle that Ptah does not
// model. Measured against the Cloud Spanner emulator behind PGAdapter 0.55.2,
// `CREATE CHANGE STREAM ttl_a_stream FOR ttl_a` is accepted and listed in
// information_schema.change_streams, and before this the reader met the
// database holding it and said nothing (stokaro/ptah#2236).
func TestRecordUnmodeledObjectKinds_SpannerChangeStreams(t *testing.T) {
	tests := []struct {
		name string
		// dialect is what the connection reports.
		dialect string
		// declined is whether the description stops claiming to cover change
		// streams. Read through Describes, which is the question every consumer
		// of the record asks: is the absence of this kind authoritative?
		declined bool
	}{
		{name: "spanner has change streams", dialect: platform.Spanner, declined: true},
		{
			// A spelling a caller may pass, so the answer does not depend on
			// the caller having normalized first.
			name:     "a spanner alias",
			dialect:  "cloudspanner",
			declined: true,
		},
		{
			// The controls. Every other engine on this wire has no such object,
			// so recording the kind for them would claim a gap that is not
			// there -- and a record naming a kind the target cannot hold makes
			// every description look less complete than it is.
			name:     "postgres has none",
			dialect:  platform.Postgres,
			declined: false,
		},
		{name: "cockroachdb has none", dialect: platform.CockroachDB, declined: false},
		{name: "yugabytedb has none", dialect: platform.YugabyteDB, declined: false},
		{name: "no dialect at all", dialect: "", declined: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			schema := recordUnmodeledObjectKinds(&catalog.Database{}, test.dialect)

			c.Assert(schema.NotDescribed.Describes(coverage.ChangeStream), qt.Equals, !test.declined)
		})
	}
}

// TestRecordUnmodeledObjectKinds_NilSchemaStaysNil keeps the helper from
// turning a failed read into a schema.
func TestRecordUnmodeledObjectKinds_NilSchemaStaysNil(t *testing.T) {
	c := qt.New(t)

	c.Assert(recordUnmodeledObjectKinds(nil, platform.Spanner), qt.IsNil)
}
