package ydbsource

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
)

// Selections is how a command's scope names the YDB owner's standalone
// objects. A coordination node, streaming query, secret, topic, external
// data source or table, async replication and transfer is named by its own
// type, in the directory that holds it; a topic's consumers belong to it and
// are never named on their own. A resource pool and its classifier have
// database-wide names, in which a dot is a literal character. A scope keeps
// the resource pool a kept classifier or streaming query routes to, even
// when no selector names the pool, when the side holds the pool's
// definition; an exclusion of the pool stays authoritative.
func Selections() selectext.Extension {
	named := func(kind schemaext.Kind, selectorType string) selectext.Selection {
		return selectext.Selection{Kind: kind, Type: selectorType, Naming: selectext.InSchema}
	}
	return selectext.Extension{
		Owner: ydbschema.Owner,
		Selections: []selectext.Selection{
			named(ydbcoordination.Kind, "coordination_node"), named(ydbstreaming.Kind, "streaming_query"),
			named(ydbsecret.Kind, "secret"), named(ydbtopic.Kind, "topic"),
			named(ydbexternal.SourceKind, "external_data_source"), named(ydbexternal.TableKind, "external_table"),
			named(ydbreplication.ReplicationKind, "async_replication"), named(ydbreplication.TransferKind, "transfer"),
			{Kind: ydbworkload.PoolKind, Type: "resource_pool", Naming: selectext.DatabaseWide},
			{Kind: ydbworkload.ClassifierKind, Type: "resource_pool_classifier", Naming: selectext.DatabaseWide},
		},
		Service: selectionService{},
	}
}

// selectionService names the resource pools kept objects route to.
type selectionService struct{}

// SelectObjects requires the pool each kept classifier and streaming query
// routes to. A value of either kind that is not the owner's model refuses
// the scope rather than lose the dependency. It clears nothing: no YDB
// selection has fields.
func (selectionService) SelectObjects(_ context.Context, request selectext.Request) (selectext.Result, error) {
	kept, err := request.Kept.All()
	if err != nil {
		return selectext.Result{}, err
	}
	var result selectext.Result
	for _, object := range kept {
		pool, routes, err := routedPool(object)
		if err != nil {
			return selectext.Result{}, err
		}
		if routes && pool != "" {
			result.Requires = append(result.Requires, ydbworkload.PoolRef(pool))
		}
	}
	return result, nil
}

// routedPool is the pool a classifier or a streaming query routes to, and
// false for an object of another kind.
func routedPool(object schemaext.Object) (string, bool, error) {
	switch object.Ref.Kind {
	case objectidentity.Kind(ydbstreaming.Kind):
		switch value := object.Value.(type) {
		case *ydbstreaming.Desired:
			return ydbstreaming.Pool(value.Spec), true, nil
		case *ydbstreaming.Observed:
			return ydbstreaming.Pool(value.Spec), true, nil
		default:
			return "", false, fmt.Errorf("%w: unexpected streaming query %T", schemaext.ErrInvalidValue, object.Value)
		}
	case objectidentity.Kind(ydbworkload.ClassifierKind):
		switch value := object.Value.(type) {
		case *ydbworkload.DesiredClassifier:
			return value.Spec.ResourcePool, true, nil
		case *ydbworkload.ObservedClassifier:
			return value.Spec.ResourcePool, true, nil
		default:
			return "", false, fmt.Errorf("%w: unexpected resource pool classifier %T", schemaext.ErrInvalidValue, object.Value)
		}
	default:
		return "", false, nil
	}
}
