package capabilityprobe

import (
	"context"
	"fmt"
	"path"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbcoordination"
)

// withCoordinationNodes adds the question whether Ptah manages YDB's
// coordination nodes.
//
// On YDB it is asked in Ptah's own statements, which Ptah's connection runs
// through the coordination service because YQL has none: a node is created
// with one setting, changed in another, and read back through Ptah's reader,
// and the namespace's teardown drops it with the third statement. Every other
// engine declares the key, because no statement can ask an engine about an
// object it does not have, and sending Ptah's statement to one would measure
// only that the engine does not parse it.
func withCoordinationNodes(p plan, dialect string) plan {
	if platform.NormalizeDialect(dialect) == platform.YDB {
		p.experiments = append(p.experiments, proven(capability.CoordinationNodes, schemaChange{
			change: []string{
				"CREATE COORDINATION NODE cnp WITH (self_check_period = Interval('PT2S'))",
				"ALTER COORDINATION NODE cnp SET (read_consistency_mode = 'strict')",
			},
			after: []check{ydbDescribedCoordinationNode("cnp", ydbcoordination.Spec{
				SelfCheckPeriodMillis: 2000, ReadConsistencyMode: "strict",
			})},
		}))
		return p
	}
	if p.undecided == nil {
		p.undecided = make(map[capability.Capability]string)
	}
	p.undecided[capability.CoordinationNodes] = "the key names whether Ptah manages YDB coordination nodes, which " +
		"only YDB has and which Ptah creates through a statement of its own; no statement asks this engine " +
		"about an object it does not have"
	return p
}

// ydbDescribedCoordinationNode reads the coordination node name in the
// namespace back through Ptah's YDB reader and holds when its configuration
// is want.
func ydbDescribedCoordinationNode(name string, want ydbcoordination.Spec) check {
	return check{
		describes: fmt.Sprintf("coordination node %s with the configuration %+v", name, want),
		inspect: func(ctx context.Context, s *session) (Attempt, bool, string) {
			attempt := Attempt{Statement: fmt.Sprintf("read coordination node %s through Ptah's YDB reader",
				path.Join(s.database, s.namespace, name))}
			db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{s.namespace})
			if err != nil {
				attempt.ServerErr = err.Error()
				return attempt, false, "was refused"
			}
			attempt.Accepted = true
			object, found, err := db.FeatureObjects.Get(ydbcoordination.Ref(s.namespace, name))
			if err != nil {
				return attempt, false, err.Error()
			}
			if !found {
				return attempt, false, "found no such node"
			}
			node, ok := object.Value.(*ydbcoordination.Observed)
			if !ok {
				return attempt, false, "read an unexpected coordination value"
			}
			if node.Spec != want {
				return attempt, false, fmt.Sprintf("read %+v", node.Spec)
			}
			return attempt, true, "found it"
		},
	}
}
