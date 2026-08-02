package atlas

import (
	"fmt"
	"io/fs"
	"slices"
	"strings"

	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
)

// legacyFlywayRevisionsTable is the table `migrate apply` records revisions in
// when --revisions-schema is not given. It is only ever used to render recovery
// SQL, never to query.
const legacyFlywayRevisionsTable = "atlas_schema_revisions"

// checkLegacyFlywayRevisions refuses an apply that would re-run migrations a
// pre-#982 Ptah build already executed.
//
// #982 converged the Flyway importer on Atlas CE's selection, and converging it
// forced a different projection onto the int64 Atlas version — CE keys a
// migration on an opaque version string, and reproducing its ORDER (a surviving
// baseline runs first whatever its own version) needs bands that the previous
// encoding had no room for. That version is what `atlas_schema_revisions`
// stores, so a database migrated by Ptah v0.1.0 through v0.1.2 through
// `?format=flyway` carries rows this build matches to no file: every migration
// reads as pending.
//
// The failure is not reliably loud. Re-running a CREATE TABLE fails, leaving a
// dirty revision that blocks every subsequent apply; re-running a backfill or a
// seed succeeds, exits 0, and duplicates the rows. This turns both into a
// refusal before anything executes.
//
// It fires only when a legacy version is recorded AND the version that file
// converts to today is not, so a database already migrated by this build, and
// an ordinary baseline squash that retires an applied migration, both pass.
// The FormatFlyway guard is deliberately not mutation-tested. Producing a
// legacy pairing at all needs V or B prefixed files, and no other loader
// accepts those — a goose or liquibase directory holding them reports "no
// importable migration files found" long before this runs — so no input
// separates the guard from its absence. It is defensive against a future format
// whose files this reconstruction would recognize, and saying so is better than
// implying a test holds it.
func checkLegacyFlywayRevisions(
	captured fs.FS,
	format atlasmigrateimport.Format,
	plan atlasmigrate.ApplyPlan,
	revisionsSchema string,
) error {
	if format != atlasmigrateimport.FormatFlyway || plan.Status == nil {
		return nil
	}
	applied := plan.Status.AppliedMigrations
	if len(applied) == 0 {
		return nil
	}
	pairs, err := atlasmigrateimport.LegacyFlywayAtlasVersions(captured)
	if err != nil {
		// The conversion error this would report is the one the caller already
		// surfaces on its own path; nothing to add here.
		return nil //nolint:nilerr // conversion failures are reported by the apply path itself.
	}

	var stale []atlasmigrateimport.LegacyFlywayVersion
	for _, pair := range pairs {
		if slices.Contains(applied, pair.Legacy) && !slices.Contains(applied, pair.Current) {
			stale = append(stale, pair)
		}
	}
	if len(stale) == 0 {
		return nil
	}
	return fmt.Errorf("%s", legacyFlywayRefusal(stale, revisionsSchema))
}

// legacyFlywayRefusal renders the refusal, including the exact statements that
// migrate the recorded versions forward.
//
// The SQL is spelled out because there is no verb on the compat surface that
// can do it: `migrate set` and `migrate status` both reject `?format=` and
// `--dir-format flyway` (stokaro/ptah#1002), and `--baseline` refuses outright
// once the revisions table is non-empty. Rewriting the version column is enough
// on its own — the recorded hash covers the converted SQL body, which this
// change does not touch.
func legacyFlywayRefusal(stale []atlasmigrateimport.LegacyFlywayVersion, revisionsSchema string) string {
	table := legacyFlywayRevisionsTable
	if schema := strings.TrimSpace(revisionsSchema); schema != "" {
		table = schema + "." + table
	}

	var b strings.Builder
	fmt.Fprintf(&b, "this database was migrated by a Ptah build older than the one that fixed stokaro/ptah#982, "+
		"which recorded Flyway migrations under a different Atlas version; %d already-applied migration(s) "+
		"would run a second time and nothing has been applied", len(stale))
	b.WriteString("\n\nrecorded version -> version this build uses:\n")
	for _, pair := range stale {
		fmt.Fprintf(&b, "  %-20d -> %-20d %s\n", pair.Legacy, pair.Current, pair.Source)
	}
	b.WriteString("\nto adopt the new encoding, migrate the recorded versions forward and re-run:\n")
	for _, pair := range stale {
		fmt.Fprintf(&b, "  UPDATE %s SET version = '%d' WHERE version = '%d';\n", table, pair.Current, pair.Legacy)
	}
	return b.String()
}
