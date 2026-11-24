package atlas

import (
	"os"
	"strings"

	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/envbinding"
)

// migrationsEngineEnvVar names the storage engine the revision table is created
// with, on the compatibility surface.
//
// WHY AN ENVIRONMENT VARIABLE AND NOT A FLAG. The pinned community binary
// registers no such flag on any migrate verb, so registering one here would put
// a non-Atlas flag on the compatibility surface and break the conformance
// `cli-surface` tier, which asserts flag parity against that binary. An
// environment variable is invisible to the help surface, which is why it is the
// sanctioned spelling for a capability the community binary has no spelling for
// at all (precedent: PTAH_STRICT_DIR_QUERY).
//
// WHY THE CAPABILITY IS EXPOSED AT ALL. Ptah names the engine the revision
// table is created with, and on ClickHouse that decides whether the statement
// is even legal -- `default_table_engine` answers `Table engine is not
// specified in CREATE query` at its own default of `None`. A replicated
// deployment needs `ReplicatedMergeTree` with its keeper path and replica name;
// with the default the migration history exists on whichever node ran the
// migration and every replica reports itself consistent. Reachable only through
// native `ptah`, that would be no migration path for someone porting an Atlas
// pipeline, which the compatibility policy forbids (stokaro/ptah#2234).
//
// The same string is the native flag's environment twin, so one exported value
// governs both binaries in a pipeline that uses each for part of its work.
const migrationsEngineEnvVar = "PTAH_MIGRATIONS_ENGINE"

// migrationsEngineFromEnv is the engine the compatibility surface was told to
// use, empty when the variable is unset, which means the dialect's own default.
//
// A value that is set but empty or blank is refused, as the native flag's
// binding refuses it. Read as unset, it creates the revision table with the
// default engine, and the table keeps the engine it is created with: on a
// replicated ClickHouse cluster the history then exists on one node. Each verb
// that reads the variable resolves it before its first early return, so a
// blank value is refused on every run of the verb rather than on the run that
// first creates the table.
//
// Beyond that there is no invalid value to refuse here: which engines a
// revision table may have is the server's judgment, it answers with its own
// message, and the refusal creates nothing. The two engines Ptah itself turns
// down are refused by the migrator before any statement runs, on both
// surfaces.
func migrationsEngineFromEnv() (string, error) {
	value, set := os.LookupEnv(migrationsEngineEnvVar)
	if !set {
		return "", nil
	}
	rule := envbinding.EmptyRule{Omitted: dbcli.OmittedMigrationsEngine}
	if err := rule.CheckVariable(migrationsEngineEnvVar, value); err != nil {
		return "", err
	}
	return strings.TrimSpace(value), nil
}
