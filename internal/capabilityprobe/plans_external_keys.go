package capabilityprobe

import (
	"context"
	"fmt"
	"path"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
)

// The statements the external object experiments send. The location names a
// host under the reserved .invalid domain: YDB contacts a data source only
// when a query reads through it, and the probe reads nothing through one.
const (
	externalSourceStatement = "CREATE EXTERNAL DATA SOURCE sk_eds WITH (SOURCE_TYPE = 'ObjectStorage', " +
		"LOCATION = 'https://probe.invalid/bucket/', AUTH_METHOD = 'NONE')"
	externalTableStatement = "CREATE EXTERNAL TABLE sk_ext (c Utf8) WITH (DATA_SOURCE = 'sk_eds', " +
		"LOCATION = 'files/', FORMAT = 'json_each_row')"
	externalReplaceStatement = "CREATE OR REPLACE EXTERNAL DATA SOURCE sk_eds_replaced WITH (SOURCE_TYPE = " +
		"'ObjectStorage', LOCATION = 'https://probe.invalid/other/', AUTH_METHOD = 'NONE')"
)

// withExternalKeys adds the questions the YDB renderer, reader and planner
// decide an external data source and an external table with: whether the line
// creates both, read back through Ptah's reader, and whether it replaces a data
// source with CREATE OR REPLACE.
//
// Every other engine is asked the same statements, and its refusal is the
// measurement, as with a secret. The key that says whether a data source names
// a secret by its path is declared undecided on YDB: its statement needs
// EnableExternalDataSources, which is off by default on every line the probe
// runs, so the server refuses it for that flag before it reads the path. The
// YDB live tests turn the flag on and measure it there.
func withExternalKeys(p plan, dialect string) plan {
	if platform.NormalizeDialect(dialect) != platform.YDB {
		p.experiments = append(p.experiments,
			proven(capability.ExternalDataSources, schemaChange{change: []string{externalSourceStatement}}),
			proven(capability.ExternalObjectReplace, schemaChange{change: []string{externalReplaceStatement}}),
			proven(capability.ExternalDataSourceSecretPaths, schemaChange{change: []string{
				"CREATE EXTERNAL DATA SOURCE sk_eds_pg WITH (SOURCE_TYPE = 'PostgreSQL', LOCATION = " +
					"'probe.invalid:5432', DATABASE_NAME = 'd', AUTH_METHOD = 'BASIC', LOGIN = 'u', " +
					"PASSWORD_SECRET_PATH = 'sk_secret')",
			}}),
		)
		return p
	}
	p.experiments = append(p.experiments,
		proven(capability.ExternalDataSources, schemaChange{
			change: []string{externalSourceStatement, externalTableStatement},
			after:  []check{ydbDescribedExternalObjects("sk_eds", "sk_ext")},
		}),
		proven(capability.ExternalObjectReplace, schemaChange{
			change: []string{externalReplaceStatement},
			after:  []check{ydbDescribedExternalObjects("sk_eds_replaced", "")},
		}),
	)
	if p.undecided == nil {
		p.undecided = make(map[capability.Capability]string)
	}
	p.undecided[capability.ExternalDataSourceSecretPaths] = "the statement that would decide it creates an " +
		"external data source, which needs EnableExternalDataSources, off by default on every YDB line; the " +
		"server refuses it for the flag before it reads the secret's path"
	return p
}

// ydbDescribedExternalObjects reads the probe namespace through Ptah's YDB
// reader and holds when it lists the data source source and, when table is
// not empty, the external table table over it.
func ydbDescribedExternalObjects(source, table string) check {
	return check{
		describes: "the external objects listed by their paths",
		inspect: func(ctx context.Context, s *session) (Attempt, bool, string) {
			attempt := Attempt{Statement: fmt.Sprintf("read external data source %s through Ptah's YDB reader",
				path.Join(s.database, s.namespace, source))}
			db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{s.namespace})
			if err != nil {
				attempt.ServerErr = err.Error()
				return attempt, false, "was refused"
			}
			attempt.Accepted = true
			sourceFound := false
			for _, described := range db.ExternalDataSources {
				sourceFound = sourceFound || described.Name == source
			}
			if !sourceFound {
				return attempt, false, "found no such data source"
			}
			if table == "" {
				return attempt, true, "listed it"
			}
			for _, described := range db.ExternalTables {
				if described.Name == table && described.DataSource == path.Join(s.namespace, source) {
					return attempt, true, "listed both"
				}
			}
			return attempt, false, "found no external table over it"
		},
	}
}
