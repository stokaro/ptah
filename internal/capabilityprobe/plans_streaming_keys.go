package capabilityprobe

import (
	"context"
	"fmt"
	"path"

	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbstreaming"
)

// The stopped query validates its declaration without consuming messages or
// writing output. Its topics and checkpoint stay inside the probe namespace.
func ydbStreamingQueries() experiment {
	return experiment{
		decides: []capability.Capability{capability.StreamingQueries},
		setup:   []string{"CREATE TOPIC stream_in", "CREATE TOPIC stream_out"},
		creates: []string{"stream_key"},
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			spec := ydbstreaming.Spec{Run: new(false), Text: fmt.Sprintf("INSERT INTO `%s` SELECT * FROM `%s`;", path.Join(s.namespace, "stream_out"), path.Join(s.namespace, "stream_in"))}
			statement := ydbstreaming.Create(path.Join(s.namespace, "stream_key"), spec, ydbstreaming.CreateOptions{})
			created := s.execAtRoot(ctx, statement)
			if !created.Accepted {
				return verdicts{capability.StreamingQueries: decided(false)}, []Attempt{created}
			}
			read := Attempt{Statement: "read the stopped streaming query through Ptah's YDB reader"}
			db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{s.namespace})
			if err != nil {
				read.ServerErr = err.Error()
				return verdicts{capability.StreamingQueries: cannotDecide("streaming query readback failed: %v", err)}, []Attempt{created, read}
			}
			read.Accepted = true
			object, found, err := db.FeatureObjects.Get(ydbstreaming.Ref(s.namespace, "stream_key"))
			if err != nil {
				return verdicts{capability.StreamingQueries: cannotDecide("streaming query capture failed: %v", err)}, []Attempt{created, read}
			}
			query, typed := object.Value.(*ydbstreaming.Observed)
			found = found && typed && ydbstreaming.Equal(query.Spec, spec)
			return verdicts{capability.StreamingQueries: readBack{accepted: true, statement: statement, what: "the stopped query with its declared text and default pool", found: found}.observation()}, []Attempt{created, read}
		},
	}
}
