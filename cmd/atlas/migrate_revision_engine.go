package atlas

import (
	"os"
	"strings"
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
// at all (precedent: PTAH_ALLOW_EXTERNAL_SCHEMA, PTAH_STRICT_DIR_QUERY).
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
// use, empty when it was told nothing.
//
// An empty or blank value means the dialect's own default, which is what every
// deployment that never heard of this variable gets. There is no invalid value
// to refuse here: which engines a revision table may have is the server's
// judgment, it answers with its own message, and the refusal creates nothing.
// The two engines Ptah itself turns down are refused by the migrator before any
// statement runs, on both surfaces.
func migrationsEngineFromEnv() string {
	return strings.TrimSpace(os.Getenv(migrationsEngineEnvVar))
}
