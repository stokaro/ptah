package atlasschema_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
)

const (
	workerKind schemaext.Kind = "example.org/workload/worker"
	poolKind   schemaext.Kind = "example.org/workload/pool"
)

// worker is an object named in a schema that runs on a pool, as a YDB
// classifier routes to a resource pool.
type worker struct {
	Pool string `json:"pool,omitempty"`
}

func (*worker) Kind() schemaext.Kind { return workerKind }

func (w *worker) Clone() schemaext.Value { return new(*w) }

func (w *worker) Equal(other schemaext.Value) bool {
	o, ok := other.(*worker)
	return ok && *o == *w
}

// pool is a database-wide object.
type pool struct{}

func (*pool) Kind() schemaext.Kind { return poolKind }

func (*pool) Clone() schemaext.Value { return &pool{} }

func (*pool) Equal(other schemaext.Value) bool {
	_, ok := other.(*pool)
	return ok
}

func workloadCodec(prototype schemaext.Value, representation schemaext.Representation) schemaext.Codec {
	encode := func(value schemaext.Payload) (json.RawMessage, error) { return json.Marshal(value) }
	return schemaext.Codec{Prototype: prototype, Representation: representation, Version: 1,
		Definition: json.RawMessage(`{"type":"object"}`),
		Clone:      func(value schemaext.Payload) (schemaext.Payload, error) { return value.(schemaext.Value).Clone(), nil },
		Encode:     encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			value := prototype.Clone()
			return value, json.Unmarshal(data, value)
		},
	}
}

func workloadRef(kind schemaext.Kind, schema, name string) objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.Kind(kind), Schema: objectidentity.Part{Source: schema, Normalized: schema},
		Name: objectidentity.Part{Source: name, Normalized: name}}
}

// workloadService requires the pool each kept worker runs on.
type workloadService struct{}

func (workloadService) SelectObjects(_ context.Context, request selectext.Request) (selectext.Result, error) {
	kept, err := request.Kept.All()
	if err != nil {
		return selectext.Result{}, err
	}
	var result selectext.Result
	for _, object := range kept {
		if value, ok := object.Value.(*worker); ok && value.Pool != "" {
			result.Requires = append(result.Requires, workloadRef(poolKind, "", value.Pool))
		}
	}
	return result, nil
}

// workloadRuntime serves the built-in engines and an owner whose workers are
// selected in their schema and whose pools are selected by a database-wide
// name.
func workloadRuntime() *engine.Runtime {
	var codecs []schemaext.Codec
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		codecs = append(codecs, workloadCodec(&worker{}, representation), workloadCodec(&pool{}, representation))
	}
	return must.Must(builtin.New(engine.Provider{
		ID:     "example.org/workload",
		Codecs: codecs,
		Selections: []selectext.Extension{{Owner: "example.org/workload", Service: workloadService{}, Selections: []selectext.Selection{
			{Kind: workerKind, Type: "worker", Naming: selectext.InSchema},
			{Kind: poolKind, Type: "pool", Naming: selectext.DatabaseWide},
		}}},
	}))
}

// heldWorker and heldPool are an object of a side: a worker in schema app,
// and a pool.
func heldWorker(name, onPool string) schemaext.Object {
	return schemaext.Object{Ref: workloadRef(workerKind, "app", name), Value: &worker{Pool: onPool}}
}

func heldPool(name string) schemaext.Object {
	return schemaext.Object{Ref: workloadRef(poolKind, "", name), Value: &pool{}}
}

// workloadNames lists the objects of a side as kind and name.
func workloadNames(objects schemaext.Objects) []string {
	names := make([]string, 0, objects.Len())
	for _, ref := range objects.Refs() {
		names = append(names, string(ref.Kind)[len("example.org/workload/"):]+" "+ref.Name.Source)
	}
	slices.Sort(names)
	return names
}

// TestLiveSelection_ApplyKeepsARequiredObjectOnBothSides selects one worker
// and keeps the pool the declared worker runs on on both sides, where each
// holds it, so the comparison plans the worker alone: kept on the desired side
// only, a held pool beside a declared worker would be planned as a creation
// of a pool the database holds. A pool only the held worker runs on is left
// out of both sides, so moving the worker plans no removal of its old pool.
func TestLiveSelection_ApplyKeepsARequiredObjectOnBothSides(t *testing.T) {
	tests := []struct {
		name        string
		current     []schemaext.Object
		desired     []schemaext.Object
		wantCurrent []string
		wantDesired []string
	}{
		{name: "a declared worker beside a held pool", current: []schemaext.Object{heldPool("fast")},
			desired:     []schemaext.Object{heldWorker("jobs", "fast"), heldPool("fast")},
			wantCurrent: []string{"pool fast"}, wantDesired: []string{"pool fast", "worker jobs"}},
		{name: "a held worker moved to another held pool",
			current:     []schemaext.Object{heldWorker("jobs", "fast"), heldPool("fast"), heldPool("slow")},
			desired:     []schemaext.Object{heldWorker("jobs", "slow"), heldPool("fast"), heldPool("slow")},
			wantCurrent: []string{"pool slow", "worker jobs"}, wantDesired: []string{"pool slow", "worker jobs"}},
		{name: "a declared worker and its pool, neither held", current: []schemaext.Object{heldPool("slow")},
			desired:     []schemaext.Object{heldWorker("jobs", "fast"), heldPool("fast")},
			wantCurrent: make([]string, 0), wantDesired: []string{"pool fast", "worker jobs"}},
		{name: "a held worker the declaration leaves out", current: []schemaext.Object{heldWorker("jobs", "fast"), heldPool("fast")},
			desired:     []schemaext.Object{heldPool("fast")},
			wantCurrent: []string{"worker jobs"}, wantDesired: make([]string, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current := &catalog.Database{Schemas: []catalog.Schema{{Name: "app"}}, FeatureObjects: must.Must(schemaext.NewObjects(test.current...))}
			desired := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(test.desired...))}
			selection := atlasschema.LiveSelection{Include: []string{"app.jobs[type=worker]"}}

			gotCurrent, gotDesired, err := selection.Apply(c.Context(), workloadRuntime(), realmInfo, current, desired)

			c.Assert(err, qt.IsNil)
			c.Assert(workloadNames(gotCurrent.FeatureObjects), qt.DeepEquals, test.wantCurrent)
			c.Assert(workloadNames(gotDesired.FeatureObjects), qt.DeepEquals, test.wantDesired)
		})
	}
}
