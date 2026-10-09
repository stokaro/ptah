package ydb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbworkload"
)

// ResourcePools is what a YDB database reports of its resource pools and the
// classifiers that send queries to them.
type ResourcePools struct {
	// Database is the absolute path of the database that holds them, such as
	// /local. A pool and a classifier belong to a database, not to a
	// directory in it.
	Database string
	// Objects carries individual pool and classifier observations. Empty views
	// do not establish absence when the cluster disables workload management.
	Objects []schemaext.Object
}

// ErrResourcePoolsRefused reports a database that would not let the read see
// its resource pools, which YDB reports only through .sys/resource_pools and
// .sys/resource_pool_classifiers. A system view is refused the way
// [ErrPrincipalsRefused] says, to a user who may list the database and not
// read it.
var ErrResourcePoolsRefused = errors.New("the server refused to report its resource pools")

// readResourcePools reads the two views through run, which executes one
// read-only query and returns its first result set.
//
// Measured on every line from 25.1.4.7 to 26.2.1.14: .sys/resource_pools
// reports each pool's name and seven settings, two as Int32 and five as
// Double, and an unset one as -1; .sys/resource_pool_classifiers reports each
// classifier's name, rank as Int64, member and pool. Both are empty on a
// database whose EnableResourcePools flag is off, and list the pool `default`
// once it is on.
func readResourcePools(
	ctx context.Context,
	database string,
	run func(context.Context, string) (*Ydb.ResultSet, error),
) (ResourcePools, error) {
	read := ResourcePools{Database: "/" + strings.Trim(database, "/")}
	pools, err := run(ctx, "SELECT Name, ConcurrentQueryLimit, QueueSize, DatabaseLoadCpuThreshold, "+
		"QueryMemoryLimitPercentPerNode, QueryCpuLimitPercentPerNode, TotalCpuLimitPercentPerNode, "+
		"ResourceWeight FROM "+systemView(database, "resource_pools"))
	if err != nil {
		return ResourcePools{}, err
	}
	for _, row := range pools.GetRows() {
		read.Objects = append(read.Objects, ydbworkload.ObservedPoolObject(textOf(row, pools, "Name"), ydbworkload.PoolSpec{
			ConcurrentQueryLimit:           int32Setting(cell(row, pools, "ConcurrentQueryLimit")),
			QueueSize:                      int32Setting(cell(row, pools, "QueueSize")),
			DatabaseLoadCPUThreshold:       doubleSetting(cell(row, pools, "DatabaseLoadCpuThreshold")),
			QueryMemoryLimitPercentPerNode: doubleSetting(cell(row, pools, "QueryMemoryLimitPercentPerNode")),
			QueryCPULimitPercentPerNode:    doubleSetting(cell(row, pools, "QueryCpuLimitPercentPerNode")),
			TotalCPULimitPercentPerNode:    doubleSetting(cell(row, pools, "TotalCpuLimitPercentPerNode")),
			ResourceWeight:                 doubleSetting(cell(row, pools, "ResourceWeight")),
		}))
	}
	classifiers, err := run(ctx, "SELECT Name, Rank, MemberName, ResourcePool FROM "+
		systemView(database, "resource_pool_classifiers"))
	if err != nil {
		return ResourcePools{}, err
	}
	for _, row := range classifiers.GetRows() {
		read.Objects = append(read.Objects, ydbworkload.ObservedClassifierObject(textOf(row, classifiers, "Name"), ydbworkload.ClassifierSpec{
			ResourcePool: textOf(row, classifiers, "ResourcePool"),
			MemberName:   textOf(row, classifiers, "MemberName"),
			Rank:         cell(row, classifiers, "Rank").GetInt64Value(),
		}))
	}
	return read, nil
}

// unsetSetting is how .sys/resource_pools reports a setting nobody set.
const unsetSetting = -1

// int32Setting is a whole-number setting of a pool, nil when unset.
func int32Setting(value *Ydb.Value) *int32 {
	if isNull(value) || value.GetInt32Value() == unsetSetting {
		return nil
	}
	return new(value.GetInt32Value())
}

// doubleSetting is a percentage setting of a pool, nil when unset.
func doubleSetting(value *Ydb.Value) *float64 {
	if isNull(value) || value.GetDoubleValue() == unsetSetting {
		return nil
	}
	return new(value.GetDoubleValue())
}

// isNull reports a cell that holds no value, or no cell at all.
func isNull(value *Ydb.Value) bool {
	if value == nil {
		return true
	}
	_, null := value.GetValue().(*Ydb.Value_NullFlagValue)
	return null
}

// ResourcePools reads the database's resource pools and classifiers from
// .sys, one snapshot read-only query each on the read's own session, through
// the raw table service for the reason [grpcSource.Principals] gives.
func (s *grpcSource) ResourcePools(ctx context.Context) (ResourcePools, error) {
	read, err := readResourcePools(ctx, s.database, s.query)
	if errors.Is(err, ErrPrincipalsRefused) {
		return ResourcePools{}, fmt.Errorf("%w: %w", ErrResourcePoolsRefused, err)
	}
	return read, err
}

// resourcePools records individual observations independently of target support.
// A dev realm cannot claim its database's global pools. A disabled cluster can
// return empty views even when configuration exists, so only returned objects
// are known; missing names remain uninspected. A refused read has no known names.
func (r *Reader) resourcePools(ctx context.Context, source Source, db *catalog.Database) error {
	read, err := source.ResourcePools(ctx)
	if errors.Is(err, ErrResourcePoolsRefused) {
		return recordWorkloadCoverage(db, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the server refused to report resource pools and classifiers"}, nil)
	}
	if err != nil {
		return fmt.Errorf("read the YDB resource pools: %w", err)
	}
	if read.Database != r.database {
		return recordWorkloadCoverage(db, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "database-wide workload objects are outside this directory's scope"}, nil)
	}
	namespace := schemaext.Knowledge{State: schemaext.Complete}
	if !r.caps.Has(capability.ResourcePools) {
		namespace = schemaext.Knowledge{State: schemaext.Uninspected, Reason: "resource pool support is unavailable; empty system views do not establish absence"}
	}
	var subjects []schemaext.SubjectCoverage
	for _, object := range read.Objects {
		if err := schemaext.ValidatePayload(object.Value); err != nil {
			return err
		}
		if err := ydbworkload.ValidateIdentity(object.Ref, object.Value.Kind()); err != nil {
			return err
		}
		knowledge := schemaext.Knowledge{State: schemaext.Complete}
		if err := validateWorkloadObservation(object); err != nil {
			knowledge = schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: err.Error()}
		} else {
			db.FeatureObjects, err = db.FeatureObjects.With(object)
			if err != nil {
				return err
			}
		}
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: object.Value.Kind(), Subject: object.Ref, Knowledge: knowledge})
	}
	return recordWorkloadCoverage(db, namespace, subjects)
}

func validateWorkloadObservation(object schemaext.Object) error {
	switch value := object.Value.(type) {
	case *ydbworkload.ObservedPool:
		if value != nil {
			return ydbworkload.ValidatePoolRef(object.Ref, value.Spec)
		}
	case *ydbworkload.ObservedClassifier:
		if value != nil {
			return ydbworkload.ValidateClassifier(value.Spec)
		}
	}
	return fmt.Errorf("%w: expected a captured pool or classifier, got %T", schemaext.ErrInvalidValue, object.Value)
}

func recordWorkloadCoverage(db *catalog.Database, namespace schemaext.Knowledge, subjects []schemaext.SubjectCoverage) error {
	for _, kind := range []schemaext.Kind{ydbworkload.PoolKind, ydbworkload.ClassifierKind} {
		var selected []schemaext.SubjectCoverage
		for _, subject := range subjects {
			if subject.Kind == kind {
				selected = append(selected, subject)
			}
		}
		coverage, err := ydbworkload.Coverage(kind, schemaext.Observed, namespace, selected)
		if err != nil {
			return err
		}
		db.FeatureCoverage, err = db.FeatureCoverage.Combine(coverage)
		if err != nil {
			return err
		}
	}
	return nil
}
