package ydb

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/migration/schemadiff/difftypes"
)

// poolPlan is the resource pool and classifier statements of a plan, in the
// order they run: drops before every creation, so a name or a rank a drop
// frees is free by the time a creation takes it.
type poolPlan struct {
	nodes []ast.Node
}

// planResourcePools refuses a pool or classifier change the target cannot
// make, and orders the rest:
//
//  1. DROP RESOURCE POOL CLASSIFIER for every classifier the plan removes, and
//     for every classifier it moves onto a rank another classifier of the
//     plan holds now;
//  2. DROP RESOURCE POOL for every pool the plan removes, once no classifier
//     the plan drops still names it;
//  3. CREATE RESOURCE POOL and ALTER RESOURCE POOL, before the classifiers
//     that name the pools;
//  4. ALTER RESOURCE POOL CLASSIFIER in place;
//  5. CREATE RESOURCE POOL CLASSIFIER for every added classifier and every
//     one step 1 dropped to move.
//
// YDB keeps one classifier per rank at every step: measured on 25.1.4.7 and
// 26.2.1.14, a CREATE or an ALTER naming a rank another classifier holds
// answers `Classifier with rank 20 already exists, its name c5`. A classifier
// moving onto a rank another one of this plan gives up is therefore dropped
// and created again after the drops and the moves; one moving onto a free
// rank is altered in place. A rank a classifier the plan does not touch holds
// is the server's to refuse, naming both.
//
// The plan runs these after the users and groups it creates and before the
// ones it drops, since a classifier names a user or a group as its member.
func (p *Planner) planResourcePools(diff *difftypes.SchemaDiff) (poolPlan, error) {
	if err := p.refuseResourcePools(diff); err != nil {
		return poolPlan{}, err
	}
	ranks := make(map[int64]string, len(diff.ResourcePoolClassifiersModified))
	for _, change := range diff.ResourcePoolClassifiersModified {
		ranks[change.Current.Rank] = change.Name
	}
	var dropped, moved, created []ast.Node
	for _, classifier := range diff.ResourcePoolClassifiersRemoved {
		dropped = append(dropped, &ast.ExtensionStatement{Payload: &ydbast.ResourcePoolClassifier{Operation: ydbast.PoolDrop, Name: classifier.Name}})
	}
	for _, change := range diff.ResourcePoolClassifiersModified {
		holder, held := ranks[change.Desired.Rank]
		if change.RankChanged && held && holder != change.Name {
			dropped = append(dropped, &ast.ExtensionStatement{Payload: &ydbast.ResourcePoolClassifier{Operation: ydbast.PoolDrop, Name: change.Name}})
			created = append(created, &ast.ExtensionStatement{Payload: &ydbast.ResourcePoolClassifier{Operation: ydbast.PoolCreate, Name: change.Name, Spec: new(change.Desired)}})
			continue
		}
		moved = append(moved, &ast.ExtensionStatement{Payload: &ydbast.ResourcePoolClassifier{Operation: ydbast.PoolAlter, Name: change.Name, Spec: new(change.Desired), Previous: new(change.Current)}})
	}
	for _, classifier := range diff.ResourcePoolClassifiersAdded {
		created = append(created, &ast.ExtensionStatement{Payload: &ydbast.ResourcePoolClassifier{Operation: ydbast.PoolCreate, Name: classifier.Name, Spec: new(classifier.Spec)}})
	}

	nodes := dropped
	for _, pool := range diff.ResourcePoolsRemoved {
		nodes = append(nodes, &ast.ExtensionStatement{Payload: &ydbast.ResourcePool{Operation: ydbast.PoolDrop, Name: pool.Name}})
	}
	for _, pool := range diff.ResourcePoolsAdded {
		nodes = append(nodes, &ast.ExtensionStatement{Payload: &ydbast.ResourcePool{Operation: ydbast.PoolCreate, Name: pool.Name, Spec: new(pool.Spec.Clone())}})
	}
	for _, change := range diff.ResourcePoolsModified {
		nodes = append(nodes, &ast.ExtensionStatement{Payload: &ydbast.ResourcePool{Operation: ydbast.PoolAlter, Name: change.Name, Spec: new(change.Desired.Clone()), Previous: new(change.Current.Clone())}})
	}
	nodes = append(nodes, moved...)
	nodes = append(nodes, created...)
	return poolPlan{nodes: nodes}, nil
}

// refuseResourcePools refuses, before any node is returned, a pool or a
// classifier the target cannot hold -- on a target without the
// resource_pools key, every one -- a drop of the pool `default`, and a set of
// classifiers that would share a rank once the plan ran.
func (p *Planner) refuseResourcePools(diff *difftypes.SchemaDiff) error {
	for _, pool := range diff.ResourcePoolsAdded {
		if err := poolPlanRefusal(ydbworkload.CheckPool(pool.Name, pool.Spec, p.caps)); err != nil {
			return err
		}
	}
	for _, change := range diff.ResourcePoolsModified {
		if err := poolPlanRefusal(ydbworkload.CheckPool(change.Name, change.Desired, p.caps)); err != nil {
			return err
		}
	}
	for _, pool := range diff.ResourcePoolsRemoved {
		if err := poolPlanRefusal(ydbworkload.CheckPoolDrop(pool.Name, p.caps)); err != nil {
			return err
		}
	}
	var after []ydbworkload.Classifier
	for _, classifier := range diff.ResourcePoolClassifiersAdded {
		if err := poolPlanRefusal(ydbworkload.CheckClassifier(classifier.Name, classifier.Spec, p.caps)); err != nil {
			return err
		}
		after = append(after, ydbworkload.Classifier{Name: classifier.Name, Spec: classifier.Spec})
	}
	for _, change := range diff.ResourcePoolClassifiersModified {
		if err := poolPlanRefusal(ydbworkload.CheckClassifier(change.Name, change.Desired, p.caps)); err != nil {
			return err
		}
		after = append(after, ydbworkload.Classifier{Name: change.Name, Spec: change.Desired})
	}
	for _, classifier := range diff.ResourcePoolClassifiersRemoved {
		refusal := ydbworkload.CheckClassifier(classifier.Name, classifier.Spec, p.caps)
		if err := poolPlanRefusal(refusal); err != nil {
			return err
		}
	}
	ranks := make(map[int64]string, len(after))
	for _, classifier := range after {
		if other, taken := ranks[classifier.Spec.Rank]; taken {
			return refuseFact(fmt.Sprintf("resource pool classifier %q", classifier.Name), fmt.Sprintf(
				"its rank %d is the rank classifier %q takes in the same plan, and YDB keeps one classifier per rank",
				classifier.Spec.Rank, other))
		}
		ranks[classifier.Spec.Rank] = classifier.Name
	}
	return nil
}

// poolPlanRefusal turns a pool or classifier refusal into the planner's
// error. A refusal through the key says how the cluster turns it on.
func poolPlanRefusal(refusal *ydbworkload.Refusal) error {
	switch {
	case refusal == nil:
		return nil
	case refusal.Key != "":
		return &ptaherr.CapabilityError{
			Dialect: platform.YDB,
			Feature: string(refusal.Key),
			Err:     ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("%s, which requires target capability %s, unavailable on this %s target; %s",
				refusal.Subject, refusal.Key, platform.YDB, refusal.Reason),
		}
	default:
		return refuseFact(refusal.Subject, refusal.Reason)
	}
}
