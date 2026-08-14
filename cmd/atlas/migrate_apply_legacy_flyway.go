package atlas

import (
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/revisiontable"
)

type legacyFlywayRevision struct {
	source   string
	recorded string
	target   string
	delete   bool
}

type ambiguousLegacyFlywayRevision struct {
	source                         string
	target                         string
	recorded                       string
	collisionSource                string
	collisionTarget                string
	collisionIsExactToken          bool
	recordedHasNonLegacyProvenance bool
}

type legacyFlywayRevisionAnalysis struct {
	stale     []legacyFlywayRevision
	ambiguous []ambiguousLegacyFlywayRevision
}

type legacyFlywayCandidateOwner struct {
	source string
	target string
}

type recordedFlywayRevision struct {
	identity        string
	operatorVersion string
}

// legacyFlywayRevisionsTable is the table `migrate apply` records revisions in
// when --revisions-schema is not given. It is only ever used to render recovery
// SQL, never to query.
const legacyFlywayRevisionsTable = "atlas_schema_revisions"

// checkLegacyFlywayRevisions refuses an apply that would re-run migrations an
// older Ptah build already executed under an internal ordering key.
//
// #1206 separates Flyway's exact source-token identity from the numeric key
// that preserves execution order. A pre-#1206 revision table therefore carries
// the current ordering key, while a pre-#982 table carries the earlier numeric
// encoding. Neither is the exact identity this build assigns the file, so an
// unchanged database would otherwise read the migration as pending.
//
// The failure is not reliably loud. Re-running a CREATE TABLE fails, leaving a
// dirty revision that blocks every subsequent apply; re-running a backfill or a
// seed succeeds, exits 0, and duplicates the rows. This turns both into a
// refusal before anything executes.
//
// It fires only when an obsolete key is recorded and the exact source token is
// absent. If both exist, the recovery deletes the duplicate obsolete row
// rather than trying to update it onto an occupied primary key. Exact matching
// is byte-for-byte: an empty repeatable token is a present identity.
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
	dialect string,
) error {
	if format != atlasmigrateimport.FormatFlyway || plan.Status == nil {
		return nil
	}
	recorded := make([]recordedFlywayRevision, len(plan.Revisions))
	for index := range plan.Revisions {
		recorded[index] = recordedFlywayRevision{
			identity:        plan.Revisions[index].RevisionVersion(),
			operatorVersion: plan.Revisions[index].OperatorVersion,
		}
	}
	if len(recorded) == 0 {
		return nil
	}
	covered, err := atlasmigrateimport.FlywayCoveredSourceVersions(captured, format)
	if err != nil {
		// The conversion error this would report is the one the caller already
		// surfaces on its own path; nothing to add here.
		return nil //nolint:nilerr // conversion failures are reported by the apply path itself.
	}

	legacy, err := atlasmigrateimport.LegacyFlywayAtlasVersions(captured)
	if err != nil {
		return nil //nolint:nilerr // conversion failures are reported by the apply path itself.
	}
	analysis := analyzeLegacyFlywayRevisions(covered, legacy, recorded)
	if len(analysis.ambiguous) > 0 {
		return fmt.Errorf("%s", ambiguousLegacyFlywayRefusal(analysis.ambiguous))
	}
	if len(analysis.stale) == 0 {
		return nil
	}
	return fmt.Errorf("%s", legacyFlywayRefusal(analysis.stale, plan.RevisionsTableIdentifier, dialect))
}

func analyzeLegacyFlywayRevisions(
	covered []atlasmigrateimport.FlywayCoveredSourceVersion,
	legacy []atlasmigrateimport.LegacyFlywayVersion,
	recorded []recordedFlywayRevision,
) legacyFlywayRevisionAnalysis {
	recordedSet := make(map[string]recordedFlywayRevision, len(recorded))
	for _, revision := range recorded {
		recordedSet[revision.identity] = revision
	}
	legacyByCurrent := make(map[int64]int64, len(legacy))
	for _, version := range legacy {
		legacyByCurrent[version.Current] = version.Legacy
	}
	targetTokens := make(map[string]string, len(covered))
	for _, migration := range covered {
		targetTokens[migration.Token] = migration.Source
	}
	ambiguousCandidates := ambiguousRetiredFlywayCandidates(covered, legacyByCurrent, recordedSet)

	var analysis legacyFlywayRevisionAnalysis
	for _, recorded := range slices.Sorted(maps.Keys(ambiguousCandidates)) {
		owners := ambiguousCandidates[recorded]
		analysis.ambiguous = append(analysis.ambiguous, ambiguousLegacyFlywayRevision{
			source:          owners[0].source,
			target:          owners[0].target,
			recorded:        recorded,
			collisionSource: owners[1].source,
			collisionTarget: owners[1].target,
		})
	}
	for _, migration := range covered {
		target := migration.Token
		_, targetExists := recordedSet[target]
		candidates := []string{strconv.FormatInt(migration.Version, 10)}
		if old, ok := legacyByCurrent[migration.Version]; ok {
			candidates = append(candidates, strconv.FormatInt(old, 10))
		}
		seenCandidates := make(map[string]struct{}, len(candidates))
		for _, recorded := range candidates {
			if recorded == target {
				continue
			}
			if _, seen := seenCandidates[recorded]; seen {
				continue
			}
			seenCandidates[recorded] = struct{}{}
			recordedRevision, ok := recordedSet[recorded]
			if !ok {
				continue
			}
			if _, ambiguous := ambiguousCandidates[recorded]; ambiguous {
				continue
			}
			// An obsolete numeric key may also be the exact source token of a
			// different covered migration. When this migration's own exact token
			// is absent, the one row cannot prove which file produced it. Never
			// guess: either UPDATE direction can make an applied migration pending.
			if collisionSource, ambiguous := targetTokens[recorded]; ambiguous {
				if !targetExists {
					analysis.ambiguous = append(analysis.ambiguous, ambiguousLegacyFlywayRevision{
						source:                migration.Source,
						target:                target,
						recorded:              recorded,
						collisionSource:       collisionSource,
						collisionTarget:       recorded,
						collisionIsExactToken: true,
					})
				}
				continue
			}
			// Only Ptah's pre-#1206 rows prove that a numeric value is an
			// internal ordering key. Current Ptah marks exact mapped source
			// identities, while Atlas and other writers carry their own marker.
			// Without that provenance the same numeric text can be a retired
			// exact Flyway token, so printing an UPDATE would corrupt history.
			if recordedRevision.operatorVersion != revisiontable.PtahOperatorVersion {
				analysis.ambiguous = append(analysis.ambiguous, ambiguousLegacyFlywayRevision{
					source:                         migration.Source,
					target:                         target,
					recorded:                       recorded,
					recordedHasNonLegacyProvenance: true,
				})
				continue
			}
			analysis.stale = append(analysis.stale, legacyFlywayRevision{
				source:   migration.Source,
				recorded: recorded,
				target:   target,
				delete:   targetExists,
			})
			targetExists = true
		}
	}
	return analysis
}

func ambiguousRetiredFlywayCandidates(
	covered []atlasmigrateimport.FlywayCoveredSourceVersion,
	legacyByCurrent map[int64]int64,
	recorded map[string]recordedFlywayRevision,
) map[string][]legacyFlywayCandidateOwner {
	ownersByCandidate := make(map[string][]legacyFlywayCandidateOwner)
	for _, migration := range covered {
		owner := legacyFlywayCandidateOwner{source: migration.Source, target: migration.Token}
		candidates := []string{strconv.FormatInt(migration.Version, 10)}
		if old, ok := legacyByCurrent[migration.Version]; ok {
			candidates = append(candidates, strconv.FormatInt(old, 10))
		}
		for _, candidate := range candidates {
			if candidate == migration.Token || hasLegacyFlywayCandidateOwner(ownersByCandidate[candidate], owner) {
				continue
			}
			ownersByCandidate[candidate] = append(ownersByCandidate[candidate], owner)
		}
	}
	ambiguous := make(map[string][]legacyFlywayCandidateOwner)
	for candidate, owners := range ownersByCandidate {
		if _, exists := recorded[candidate]; !exists {
			continue
		}
		unresolved := slices.DeleteFunc(slices.Clone(owners), func(owner legacyFlywayCandidateOwner) bool {
			_, targetExists := recorded[owner.target]
			return targetExists
		})
		if len(unresolved) > 1 {
			ambiguous[candidate] = unresolved
		}
	}
	return ambiguous
}

func hasLegacyFlywayCandidateOwner(owners []legacyFlywayCandidateOwner, target legacyFlywayCandidateOwner) bool {
	return slices.ContainsFunc(owners, func(owner legacyFlywayCandidateOwner) bool {
		return owner.source == target.source && owner.target == target.target
	})
}

func ambiguousLegacyFlywayRefusal(ambiguous []ambiguousLegacyFlywayRevision) string {
	var b strings.Builder
	b.WriteString("this database records a Flyway revision identity that can belong to more than one migration after replacing older Ptah ordering keys; nothing has been applied\n")
	for _, collision := range ambiguous {
		if collision.recordedHasNonLegacyProvenance {
			fmt.Fprintf(&b, "\n  recorded %q is an exact source identity or belongs to another writer, but it is also an older Ptah ordering-key candidate for %s (exact token %q)\n",
				collision.recorded, collision.source, collision.target)
			continue
		}
		if collision.collisionIsExactToken {
			fmt.Fprintf(&b, "\n  recorded %q is ambiguous between an exact source token and an older Ptah ordering key: it is the exact token of %s and the retired key for %s (exact token %q)\n",
				collision.recorded, collision.collisionSource, collision.source, collision.target)
			continue
		}
		fmt.Fprintf(&b, "\n  recorded %q is an older Ptah ordering-key candidate for both %s (exact token %q) and %s (exact token %q)\n",
			collision.recorded,
			collision.source,
			collision.target,
			collision.collisionSource,
			collision.collisionTarget)
	}
	b.WriteString("\nautomatic recovery cannot determine which migration owns the recorded row. Review the revision checksum and migration history before changing it; no repair SQL has been generated.\n")
	return b.String()
}

// legacyFlywayRefusal renders the refusal, including the exact one-way
// statements that adopt source-token identities. Ptah is pre-v1, so this does
// not preserve the retired ordering-key representation as a readable alias.
//
// The SQL is spelled out because no compat verb performs an identity-preserving
// rewrite of every revision row. `migrate set` targets one revision, may discard
// later rows, and cannot preserve each row's checksum and metadata. Direct SQL
// also lets the operator address a non-default revisions schema. Rewriting the
// version column is enough on its own — the recorded hash covers the converted
// SQL body, which this change does not touch.
func legacyFlywayRefusal(stale []legacyFlywayRevision, revisionsTableIdentifier, dialect string) string {
	table := revisionsTableIdentifier
	if table == "" {
		table = legacyFlywayRevisionsTable
	}

	var b strings.Builder
	fmt.Fprintf(&b, "this database was migrated by a Ptah build that recorded converted Flyway migrations under an internal ordering key; "+
		"%d obsolete revision row(s) "+
		"would run a second time and nothing has been applied", len(stale))
	b.WriteString("\n\nrecorded version -> exact Flyway source token:\n")
	for _, revision := range stale {
		fmt.Fprintf(&b, "  %q -> %q %s\n", revision.recorded, revision.target, revision.source)
	}
	b.WriteString("\nto adopt the exact source-token identity, run the following one-way repair and re-run:\n")
	for _, revision := range stale {
		if revision.delete {
			fmt.Fprintf(&b, "  DELETE FROM %s WHERE version = %s;\n", table, flywayRevisionSQLLiteral(dialect, revision.recorded))
			continue
		}
		fmt.Fprintf(&b, "  UPDATE %s SET version = %s WHERE version = %s;\n",
			table,
			flywayRevisionSQLLiteral(dialect, revision.target),
			flywayRevisionSQLLiteral(dialect, revision.recorded))
	}
	return b.String()
}

func flywayRevisionSQLLiteral(dialect, version string) string {
	return revisiontable.VersionLiteral(dialect, version)
}
