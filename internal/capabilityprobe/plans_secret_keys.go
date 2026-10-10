package capabilityprobe

import (
	"context"
	"fmt"
	"path"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbsecret"
)

// secretStatement creates the probe's secret. The value is a literal: the
// probe sends its statement as written, and a secret in its own namespace
// protects nothing.
const secretStatement = "CREATE SECRET sk_secret WITH (value = 'probe')"

// withSecretKeys adds the question the YDB renderer, reader and planner decide
// a secret with: whether the line creates one that a read of the scheme tree
// then finds.
//
// On YDB the statement is read back through Ptah's reader, which lists the
// secret by its path, since nothing returns a secret's value or its options.
// A secret Ptah models is YDB's, so every other engine is asked the same
// statement and its refusal is the measurement, as with a changefeed.
func withSecretKeys(p plan, dialect string) plan {
	if platform.NormalizeDialect(dialect) != platform.YDB {
		p.experiments = append(p.experiments, proven(capability.Secrets, schemaChange{
			change: []string{secretStatement},
		}))
		return p
	}
	p.experiments = append(p.experiments, proven(capability.Secrets, schemaChange{
		change: []string{secretStatement},
		after:  []check{ydbDescribedSecret("sk_secret")},
	}))
	return p
}

// ydbDescribedSecret reads the probe namespace through Ptah's YDB reader and
// holds when it lists the secret name.
func ydbDescribedSecret(name string) check {
	return check{
		describes: "the secret listed by its path",
		inspect: func(ctx context.Context, s *session) (Attempt, bool, string) {
			attempt := Attempt{Statement: fmt.Sprintf("read secret %s through Ptah's YDB reader",
				path.Join(s.database, s.namespace, name))}
			db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{s.namespace})
			if err != nil {
				attempt.ServerErr = err.Error()
				return attempt, false, "was refused"
			}
			attempt.Accepted = true
			object, found, err := db.FeatureObjects.Get(ydbsecret.Ref(s.namespace, name))
			if err != nil {
				attempt.ServerErr = err.Error()
				return attempt, false, "could not be captured"
			}
			if _, observed := object.Value.(*ydbsecret.Observed); found && observed {
				return attempt, true, "listed it"
			}
			return attempt, false, "found no such secret"
		},
	}
}
