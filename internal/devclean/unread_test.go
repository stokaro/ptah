package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/devclean"
)

const (
	// readKind is a kind the dev database reads.
	readKind schemaext.Kind = "example.org/unread/topic"
	// poolKind is a kind the dev database reads nothing of, as a YDB dev
	// realm reads no resource pool.
	poolKind schemaext.Kind = "example.org/unread/pool"
)

// noPools is what a YDB dev realm knows of its database's pools.
var noPools = schemaext.Knowledge{State: schemaext.Uninspected, Reason: "A dev realm holds no resource pools of its own"}

type unreadValue struct{ ID schemaext.Kind }

func (v *unreadValue) Kind() schemaext.Kind   { return v.ID }
func (v *unreadValue) Clone() schemaext.Value { result := *v; return &result }
func (v *unreadValue) Equal(other schemaext.Value) bool {
	right, ok := other.(*unreadValue)
	return ok && right != nil && *v == *right
}

func unreadRef(kind schemaext.Kind, schema, name string) objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.Kind(kind), Schema: objectidentity.Part{Source: schema, Normalized: schema}, Name: objectidentity.Part{Source: name, Normalized: name}}
}

func unreadModel(kind schemaext.Kind, representation schemaext.Representation) schemaext.CodecIdentity {
	return schemaext.CodecIdentity{Owner: "example.org/unread", Kind: kind, Representation: representation, Version: 1, Definition: "definition"}
}

// unreadTarget is the target's current schema as a rehearsal recreates it: a
// topic in a directory and the database's pool, both read completely.
func unreadTarget() *schemamodel.Database {
	topic, pool := unreadRef(readKind, "app", "events"), unreadRef(poolKind, "", "default")
	complete := schemaext.Knowledge{State: schemaext.Complete}
	return &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: topic, Value: &unreadValue{ID: readKind}}, schemaext.Object{Ref: pool, Value: &unreadValue{ID: poolKind}})),
		FeatureCoverage: must.Must(schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{
			{Model: unreadModel(readKind, schemaext.Desired), Knowledge: schemaext.GlobalKnowledge(complete)},
			{Model: unreadModel(poolKind, schemaext.Desired), Knowledge: schemaext.GlobalKnowledge(complete)},
		}, []schemaext.SubjectCoverage{{Kind: readKind, Subject: topic, Knowledge: complete}, {Kind: poolKind, Subject: pool, Knowledge: complete}})),
	}
}

// devRead is what a dev database read: the topic kind completely, and the
// pool kind with pools.
func devRead(pools schemaext.Knowledge) *catalog.Database {
	return &catalog.Database{FeatureCoverage: must.Must(schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{
		{Model: unreadModel(readKind, schemaext.Observed), Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete})},
		{Model: unreadModel(poolKind, schemaext.Observed), Knowledge: schemaext.GlobalKnowledge(pools)},
	}, nil))}
}

// The objects a dev database read nothing of leave the recreated schema with
// their coverage records, and the rest stay; the target is not changed.
func TestWithoutUnreadObjects_HappyPath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		pools schemaext.Knowledge
		want  []objectidentity.ID
	}{
		{name: "a dev database that reads no pool", pools: noPools, want: []objectidentity.ID{unreadRef(readKind, "app", "events")}},
		{name: "a dev database that reads pools", pools: schemaext.Knowledge{State: schemaext.Complete},
			want: []objectidentity.ID{unreadRef(poolKind, "", "default"), unreadRef(readKind, "app", "events")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			target := unreadTarget()

			result := devclean.WithoutUnreadObjects(target, devRead(test.pools))

			c.Assert(result.FeatureObjects.Refs(), qt.ContentEquals, test.want)
			var subjects []objectidentity.ID
			for _, record := range result.FeatureCoverage.SubjectRecords() {
				subjects = append(subjects, record.Subject)
			}
			c.Assert(subjects, qt.ContentEquals, test.want)
			c.Assert(result.FeatureCoverage.KindRecords(), qt.HasLen, 2)
			c.Assert(target.FeatureObjects.Len(), qt.Equals, 2)
			c.Assert(target.FeatureCoverage.SubjectRecords(), qt.HasLen, 2)
		})
	}
}

// Without a read there is nothing to hold the target to, and it comes back as
// it is.
func TestWithoutUnreadObjectsWithoutARead_HappyPath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	target := unreadTarget()

	result := devclean.WithoutUnreadObjects(target, nil)

	c.Assert(result, qt.Equals, target)
	c.Assert(devclean.WithoutUnreadObjects(nil, devRead(noPools)), qt.IsNil)
}
