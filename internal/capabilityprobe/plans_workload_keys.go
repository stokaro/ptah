package capabilityprobe

import (
	"context"
	"fmt"
	"hash/fnv"
	"maps"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbworkload"
)

// withWorkloadKeys answers the keys about YDB's resource pools, backup
// collections and streaming queries.
//
// On YDB, pools and stopped streaming queries are created and read back
// through Ptah's reader. Backup collections remain unmodeled; see
// [ydbWorkloadUndecided]. On every other engine all three are declared: each
// names an object only YDB has, and SQL Server's Resource Governor has a
// `CREATE RESOURCE POOL` of its own, which creates a server-wide object a
// probe has no business making.
func withWorkloadKeys(p plan, dialect string) plan {
	if p.undecided == nil {
		p.undecided = make(map[capability.Capability]string)
	}
	if platform.NormalizeDialect(dialect) == platform.YDB {
		p.experiments = append(p.experiments, ydbResourcePools(), ydbStreamingQueries())
		maps.Copy(p.undecided, ydbWorkloadUndecided())
		return p
	}
	p.undecided[capability.ResourcePools] = "the key names whether Ptah plans YDB's resource pools and " +
		"classifiers, which only the YDB planner does; this engine's own workload objects, such as SQL " +
		"Server's Resource Governor pools, are another thing and outlive any namespace a probe makes"
	p.undecided[capability.BackupCollections] = "the key names a YDB backup collection, which Ptah models on " +
		"no engine"
	p.undecided[capability.StreamingQueries] = "the key names a YDB streaming query; only the YDB planner renders and reads this object family"
	return p
}

// ydbWorkloadUndecided are the YDB keys a probe declares rather than asks,
// each with the measurement that keeps it from asking.
func ydbWorkloadUndecided() map[capability.Capability]string {
	return map[capability.Capability]string{
		capability.BackupCollections: "the key names whether Ptah declares, reads and plans a backup " +
			"collection, which it does not: no public API reads one back. A probe does not create one to " +
			"measure the server either, because its teardown would be DROP BACKUP COLLECTION, which stops a " +
			"25.1.4.7 server, measured",
	}
}

// ydbResourcePools decides ResourcePools in YDB's own spelling: a pool with
// a whole-number and a fractional setting, and a classifier that sends the
// queries of a member the database does not have to it, each read back
// through Ptah's reader, which is the question a plan asks.
//
// Both belong to the database rather than to the namespace directory, and the
// namespace pragma does not place them (measured: `CREATE RESOURCE POOL` after
// `PRAGMA TablePathPrefix` creates a pool of the database), so they are named
// from the namespace and recorded for the teardown. The classifier's rank is
// derived from the namespace too, high enough to stay clear of the ranks a
// database's own classifiers take, because YDB keeps one classifier per rank.
// The member is a name no user holds, so the classifier routes no query.
func ydbResourcePools() experiment {
	return experiment{
		decides: []capability.Capability{capability.ResourcePools},
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			pool := ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(3)), QueryMemoryLimitPercentPerNode: new(12.5)}
			classifier := ydbworkload.ClassifierSpec{
				ResourcePool: s.namespace + "_rpk", MemberName: s.namespace + "_nobody", Rank: probeRank(s.namespace),
			}
			poolStatement := ydbworkload.CreatePoolStatement(s.namespace+"_rpk", pool)
			created := s.exec(ctx, poolStatement)
			attempts := []Attempt{created}
			if !created.Accepted {
				return verdicts{capability.ResourcePools: readBack{statement: poolStatement}.observation()}, attempts
			}
			s.resourcePools = append(s.resourcePools, s.namespace+"_rpk")
			classifierStatement := ydbworkload.CreateClassifierStatement(s.namespace+"_rpc", classifier)
			createdClassifier := s.exec(ctx, classifierStatement)
			attempts = append(attempts, createdClassifier)
			if createdClassifier.Accepted {
				s.resourcePoolClassifiers = append(s.resourcePoolClassifiers, s.namespace+"_rpc")
			}
			read := Attempt{Statement: "read the resource pools and classifiers of " + s.database +
				" through Ptah's YDB reader"}
			db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{s.namespace})
			if err != nil {
				read.ServerErr = err.Error()
				return verdicts{capability.ResourcePools: cannotDecide("the read-back was refused (%s)",
					collapse(err.Error()))}, append(attempts, read)
			}
			read.Accepted = true
			found, err := observedWorkloadMatches(db.FeatureObjects, s.namespace+"_rpk", pool, s.namespace+"_rpc", classifier)
			if err != nil {
				return verdicts{capability.ResourcePools: cannotDecide("the read-back could not capture workload objects (%s)", collapse(err.Error()))}, append(attempts, read)
			}
			return verdicts{capability.ResourcePools: readBack{
				accepted: createdClassifier.Accepted, statement: classifierStatement,
				what: "the resource pool with its two settings and the classifier that names it", found: found,
			}.observation()}, append(attempts, read)
		},
	}
}

// probeRank is the rank of the probe's classifier: four trillion plus a hash
// of the namespace, so two runs on one database take different ranks.
func probeRank(namespace string) int64 {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(namespace))
	return 4_000_000_000_000 + int64(hash.Sum32())
}

// observedWorkloadMatches checks the actual captured values. A missing or
// malformed observation cannot qualify a successful statement alone.
func observedWorkloadMatches(objects schemaext.Objects, poolName string, pool ydbworkload.PoolSpec, classifierName string, classifier ydbworkload.ClassifierSpec) (bool, error) {
	poolObject, hasPool, err := objects.Get(ydbworkload.PoolRef(poolName))
	if err != nil {
		return false, err
	}
	classifierObject, hasClassifier, err := objects.Get(ydbworkload.ClassifierRef(classifierName))
	if err != nil {
		return false, err
	}
	heldPool, poolOK := poolObject.Value.(*ydbworkload.ObservedPool)
	heldClassifier, classifierOK := classifierObject.Value.(*ydbworkload.ObservedClassifier)
	if (hasPool && !poolOK) || (hasClassifier && !classifierOK) {
		return false, fmt.Errorf("%w: workload read-back requires observed pool and classifier values", schemaext.ErrInvalidValue)
	}
	if !hasPool || !hasClassifier {
		return false, nil
	}
	return ydbworkload.PoolsEqual(heldPool.Spec, pool) && heldClassifier.Spec == classifier, nil
}
