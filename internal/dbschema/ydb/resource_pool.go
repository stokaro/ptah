package ydb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
)

// ResourcePools is what a YDB database reports of its resource pools and the
// classifiers that send queries to them.
type ResourcePools struct {
	// Database is the absolute path of the database that holds them, such as
	// /local. A pool and a classifier belong to a database, not to a
	// directory in it.
	Database string
	// Pools are the pools, the pool `default` among them, each with every
	// setting the database holds and an unset one nil.
	Pools []catalog.ResourcePool
	// Classifiers are the classifiers.
	Classifiers []catalog.ResourcePoolClassifier
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
		read.Pools = append(read.Pools, catalog.ResourcePool{
			Name: textOf(row, pools, "Name"),
			Spec: ast.ResourcePoolSpec{
				ConcurrentQueryLimit:           int32Setting(cell(row, pools, "ConcurrentQueryLimit")),
				QueueSize:                      int32Setting(cell(row, pools, "QueueSize")),
				DatabaseLoadCPUThreshold:       doubleSetting(cell(row, pools, "DatabaseLoadCpuThreshold")),
				QueryMemoryLimitPercentPerNode: doubleSetting(cell(row, pools, "QueryMemoryLimitPercentPerNode")),
				QueryCPULimitPercentPerNode:    doubleSetting(cell(row, pools, "QueryCpuLimitPercentPerNode")),
				TotalCPULimitPercentPerNode:    doubleSetting(cell(row, pools, "TotalCpuLimitPercentPerNode")),
				ResourceWeight:                 doubleSetting(cell(row, pools, "ResourceWeight")),
			},
		})
	}
	classifiers, err := run(ctx, "SELECT Name, Rank, MemberName, ResourcePool FROM "+
		systemView(database, "resource_pool_classifiers"))
	if err != nil {
		return ResourcePools{}, err
	}
	for _, row := range classifiers.GetRows() {
		read.Classifiers = append(read.Classifiers, catalog.ResourcePoolClassifier{
			Name: textOf(row, classifiers, "Name"),
			Spec: ast.ResourcePoolClassifierSpec{
				ResourcePool: textOf(row, classifiers, "ResourcePool"),
				MemberName:   textOf(row, classifiers, "MemberName"),
				Rank:         cell(row, classifiers, "Rank").GetInt64Value(),
			},
		})
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

// resourcePools reads the resource pools and classifiers into db.
//
// They belong to the whole database, so the read describes them wherever it
// walks the database itself, and records each kind whole as outside the read's
// scope where it walks a dev realm: a realm is a directory standing in for a
// database, and the pools are those of the database that holds it. On a server
// without [capability.ResourcePools], which every YDB line is until its
// cluster turns EnableResourcePools on, each pool and classifier the views
// report is recorded rather than described, so a plan meets none it would
// refuse. A database that refuses the read is recorded as such rather than
// failing it, as [Reader.principals] records one; any other error fails it.
func (r *Reader) resourcePools(ctx context.Context, source Source, db *catalog.Database) error {
	read, err := source.ResourcePools(ctx)
	if errors.Is(err, ErrResourcePoolsRefused) {
		db.NotDescribed = db.NotDescribed.With(
			coverage.Refused(coverage.ResourcePool), coverage.Refused(coverage.ResourcePoolClassifier))
		return nil
	}
	if err != nil {
		return fmt.Errorf("read the YDB resource pools: %w", err)
	}
	if read.Database != r.database {
		db.NotDescribed = db.NotDescribed.With(
			coverage.Object{Kind: coverage.ResourcePool, Reason: coverage.OutsideScope, Provenance: coverage.Observed},
			coverage.Object{
				Kind: coverage.ResourcePoolClassifier, Reason: coverage.OutsideScope, Provenance: coverage.Observed,
			},
		)
		return nil
	}
	if !r.caps.Has(capability.ResourcePools) {
		for _, pool := range read.Pools {
			db.NotDescribed = db.NotDescribed.With(unmodeled(coverage.ResourcePool, "", pool.Name))
		}
		for _, classifier := range read.Classifiers {
			db.NotDescribed = db.NotDescribed.With(unmodeled(coverage.ResourcePoolClassifier, "", classifier.Name))
		}
		return nil
	}
	db.ResourcePools = read.Pools
	db.ResourcePoolClassifiers = read.Classifiers
	return nil
}
