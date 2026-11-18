package migrator

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// ExecOrder controls how the migrator handles unapplied migrations whose
// version is below the current high-water mark.
type ExecOrder string

const (
	// ExecOrderLinear rejects out-of-order pending migrations.
	ExecOrderLinear ExecOrder = "linear"
	// ExecOrderLinearSkip leaves out-of-order pending migrations unapplied.
	ExecOrderLinearSkip ExecOrder = "linear-skip"
	// ExecOrderNonLinear applies every pending migration in version order.
	ExecOrderNonLinear ExecOrder = "non-linear"
)

func normalizeExecOrder(value ExecOrder) ExecOrder {
	switch value {
	case "", ExecOrderLinear:
		return ExecOrderLinear
	case ExecOrderLinearSkip, ExecOrderNonLinear:
		return value
	default:
		return ExecOrderLinear
	}
}

// outOfOrderExempt drops versions whose position below the high-water mark is
// known not to mean "authored earlier".
//
// The linear guard reads "version below the current one" as evidence that a
// migration was authored before what is already applied. That inference holds
// only while the version is a chronology. It is not one for a Flyway directory
// read through ?format=: there the int64 is a projection of Atlas CE's sum
// order, in which a surviving baseline is emitted FIRST regardless of its own
// version, so the baseline is deliberately placed below every survivor. Reading
// that placement as out-of-order refuses a directory Atlas CE applies.
//
// The exemption is a list of specific versions supplied by the caller that
// computed the projection, never a rule this package infers. Everything else
// stays guarded, which is what keeps an ordinary migration inserted below the
// high-water mark refused — Atlas CE refuses that too.
func outOfOrderExempt(versions, exempt []int64) []int64 {
	if len(exempt) == 0 {
		return versions
	}
	out := make([]int64, 0, len(versions))
	for _, version := range versions {
		if !slices.Contains(exempt, version) {
			out = append(out, version)
		}
	}
	return out
}

// outOfOrderSourceVersions reports the pending versions a converted directory's
// SOURCE tool considers out of order, which is a different question from the
// numeric one above and has to be asked separately.
//
// A directory read through a foreign layout carries two orders, and they are
// not the same order. Its int64 version is a projection of the source tool's
// ORDER, which for Flyway is numeric on the version components — V2 executes
// before V10. Whether a file "was added after everything already applied" is
// decided by that tool on the version TOKEN as a string, and `"10" < "2"`. So a
// V10 added beside an applied V2 is numerically above the mark and textually
// below it, and only the second answer is the one the source tool gives
// (stokaro/ptah#1098).
//
// The comparison is against the highest applied token, not against the token of
// the highest applied version — the two differ exactly when the tokens disagree
// with the ordering, which is the whole case this exists for.
//
// Recorded exact keys are included even when their source files have rotated
// out of the current directory. Their conversion-time numeric tie slot is no
// longer reconstructable, but the source tool's linearity decision is defined
// on these tokens, so dropping them would execute an older migration the source
// tool refuses.
func outOfOrderSourceVersions(
	pending, applied []int64,
	appliedKeys []string,
	sourceVersions map[int64]string,
) []int64 {
	if len(sourceVersions) == 0 {
		return nil
	}
	highest, ok := highestAppliedSourceVersion(applied, appliedKeys, sourceVersions)
	if !ok {
		return nil
	}
	out := make([]int64, 0, len(pending))
	for _, version := range pending {
		// A token that does not sort strictly above every applied token is one
		// the source tool never executes: it either refuses it as out of order
		// or drops it as already covered. Refusing is the safe half of that.
		if source, found := sourceVersions[version]; found && source <= highest {
			out = append(out, version)
		}
	}
	return out
}

func highestAppliedSourceVersion(
	applied []int64,
	appliedKeys []string,
	sourceVersions map[int64]string,
) (string, bool) {
	var highest string
	found := false
	for _, version := range applied {
		source, ok := sourceVersions[version]
		if !ok {
			continue
		}
		if !found || source > highest {
			highest, found = source, true
		}
	}
	for _, source := range appliedKeys {
		if !found || source > highest {
			highest, found = source, true
		}
	}
	return highest, found
}

// mergeOutOfOrderVersions unions the two comparisons' verdicts, keeping the
// ascending order the callers' pending list already has.
//
// It is a union rather than a replacement, and both halves are load-bearing.
// The numeric comparison catches an ordinary migration inserted below the mark
// whose token still sorts above it (V3 added to an applied V2 and V10); the
// source comparison catches a migration above the mark whose token sorts below
// it (V10 added to an applied V2). Dropping either one reopens a shape the
// pinned community binary refuses.
func mergeOutOfOrderVersions(numeric, source []int64) []int64 {
	if len(source) == 0 {
		return numeric
	}
	merged := make([]int64, 0, len(numeric)+len(source))
	merged = append(merged, numeric...)
	merged = append(merged, source...)
	slices.Sort(merged)
	return slices.Compact(merged)
}

// sourceVersionLabel is how a log line names version: by the source tool's
// own token for a directory read through a foreign layout, and by the number
// otherwise. The int64 of a converted directory appears in no file name and in
// no Atlas output, so a line naming it names nothing anyone can look up.
func sourceVersionLabel(version int64, sourceVersions map[int64]string) any {
	if source, ok := sourceVersions[version]; ok {
		return source
	}
	return version
}

// currentSourceVersionLabel names the mark the out-of-order verdict compared
// against, as [sourceVersionLabel] names a version.
func (m *Migrator) currentSourceVersionLabel(
	currentVersion int64,
	applied []int64,
	appliedIdentities migrationIdentitySet,
) any {
	if len(m.sourceVersions) == 0 {
		return currentVersion
	}
	if source, ok := m.retiredCurrentSourceVersion(currentVersion, applied, appliedIdentities); ok {
		return source
	}
	return sourceVersionLabel(currentVersion, m.sourceVersions)
}

// retiredCurrentSourceVersion is the token naming a current version the
// directory no longer maps, such as one a squash retired: the highest token
// applied, which is the mark the source comparison used. The refusal and the
// skip warning both name the mark this way, so they name the same one.
func (m *Migrator) retiredCurrentSourceVersion(
	currentVersion int64,
	applied []int64,
	appliedIdentities migrationIdentitySet,
) (string, bool) {
	if _, mapped := m.sourceVersions[currentVersion]; mapped {
		return "", false
	}
	return highestAppliedSourceVersion(applied, appliedIdentities.revisionKeys(), m.sourceVersions)
}

// OutOfOrderError reports pending migrations that linear execution refuses.
type OutOfOrderError struct {
	CurrentVersion int64
	Versions       []int64
	// SourceVersions carries the source tool's own version token for the
	// current version and for every version listed, for a directory read
	// through a foreign layout. It is empty for a native Atlas directory.
	//
	// It replaces the numeric projection in user-facing diagnostics because the
	// int64 is not a name anyone can look up: it appears in no file name and in
	// no Atlas output.
	SourceVersions          map[int64]string
	currentSourceVersion    string
	currentSourceVersionSet bool
}

func (e *OutOfOrderError) Error() string {
	const escape = "(use --exec-order=non-linear to apply or --exec-order=linear-skip to ignore)"
	if len(e.SourceVersions) == 0 {
		return fmt.Sprintf(
			"out-of-order pending migrations below current version %d: %v %s",
			e.CurrentVersion,
			e.Versions,
			escape,
		)
	}
	// "below" is dropped for a converted directory on purpose. A version can be
	// refused here while sorting ABOVE the current one, because the source
	// tool's own comparison put it below; claiming otherwise would describe the
	// int64 order, which is not the order that decided this.
	listed := make([]string, 0, len(e.Versions))
	for _, version := range e.Versions {
		listed = append(listed, e.describe(version))
	}
	current := e.describe(e.CurrentVersion)
	if e.currentSourceVersionSet {
		current = fmt.Sprintf("%q", e.currentSourceVersion)
	}
	return fmt.Sprintf(
		"out-of-order pending migrations for current version %s: %s %s",
		current,
		strings.Join(listed, ", "),
		escape,
	)
}

func (e *OutOfOrderError) describe(version int64) string {
	source, ok := e.SourceVersions[version]
	if !ok {
		return strconv.FormatInt(version, 10)
	}
	return fmt.Sprintf("%q", source)
}

// newOutOfOrderError builds the typed error returned for linear execution when
// pending migrations the execution order refuses are present.
func newOutOfOrderError(currentVersion int64, versions []int64) *OutOfOrderError {
	return &OutOfOrderError{
		CurrentVersion: currentVersion,
		Versions:       slices.Clone(versions),
	}
}

// newOutOfOrderSourceError builds the same error for a directory read through a
// foreign layout, carrying the source version tokens needed to name the files
// the refusal is about.
func newOutOfOrderSourceError(currentVersion int64, versions []int64, sourceVersions map[int64]string) *OutOfOrderError {
	err := newOutOfOrderError(currentVersion, versions)
	if len(sourceVersions) == 0 {
		return err
	}
	err.SourceVersions = make(map[int64]string, len(versions)+1)
	for _, version := range append(slices.Clone(versions), currentVersion) {
		if source, ok := sourceVersions[version]; ok {
			err.SourceVersions[version] = source
		}
	}
	if len(err.SourceVersions) == 0 {
		err.SourceVersions = nil
	}
	return err
}

// readsAtlasRevisionOrder reports whether linearity over migrations is decided
// on the Atlas revision strings rather than on numbers: the caller selected
// [Migrator.WithAtlasStringLinearity], the revision table is an Atlas one over
// a directory read in the Atlas format, and no source tool's own tokens decide
// instead (see [Migrator.WithSourceVersions]).
func (m *Migrator) readsAtlasRevisionOrder(migrations []*Migration) bool {
	if !m.atlasStringLinearity || !m.revisionTableFormat.isAtlas() || len(m.sourceVersions) != 0 || len(migrations) == 0 {
		return false
	}
	for _, migration := range migrations {
		if migration.atlasOrderKey == "" {
			return false
		}
	}
	return true
}

// atlasRevisionOutOfOrder returns the pending migrations an Atlas directory
// runs out of order. A file is out of order when its version string sorts
// below the highest applied one, or when its file name sorts below the name of
// an applied file, because an Atlas directory runs by file name.
//
// Measured on the pinned community binary v1.3.0 over SQLite. 3__x.sql added
// after 20260101000000_ts.sql applies, since "3" sorts above
// "20260101000000"; a number would call it out of order. 10_b.sql added after
// 1_a.sql is refused, since 10_b.sql sorts before 1_a.sql; a number would
// apply it. The binary refuses 1_a.sql pending after 1R__rep.sql, since "1"
// sorts below "1R"; here 1_a.sql shares the runtime version of 1R and reads as
// applied, so it never reaches this function (stokaro/ptah#6229). The binary
// has a third answer this function does not copy:
// 10_b.sql added after 9_a.sql is neither pending nor refused there, so it
// never runs and the run exits 0. Both halves of the test hold for that file,
// so it is refused here.
func atlasRevisionOutOfOrder(migrations, pending []*Migration, applied migrationIdentitySet) []*Migration {
	highestVersion, haveVersion := "", false
	for _, key := range applied.revisionKeys() {
		if !haveVersion || key > highestVersion {
			highestVersion, haveVersion = key, true
		}
	}
	highestName := ""
	for _, migration := range migrations {
		if applied.containsMigration(migration) && migration.atlasOrderKey > highestName {
			highestName = migration.atlasOrderKey
		}
	}
	late := make([]*Migration, 0, len(pending))
	for _, migration := range pending {
		belowVersion := haveVersion && migration.RevisionVersion() < highestVersion
		if belowVersion || migration.atlasOrderKey < highestName {
			late = append(late, migration)
		}
	}
	return late
}
