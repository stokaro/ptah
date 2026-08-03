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

// ParseExecOrder parses a CLI/API exec-order value.
func ParseExecOrder(value string) (ExecOrder, error) {
	switch ExecOrder(strings.ToLower(strings.TrimSpace(value))) {
	case "", ExecOrderLinear:
		return ExecOrderLinear, nil
	case ExecOrderLinearSkip:
		return ExecOrderLinearSkip, nil
	case ExecOrderNonLinear:
		return ExecOrderNonLinear, nil
	default:
		return "", fmt.Errorf("invalid exec-order %q: expected linear, linear-skip, or non-linear", value)
	}
}

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
// A version with no token is skipped rather than assumed. Tokens come from the
// directory as it stands now, so an applied migration whose file a baseline has
// since squashed away contributes none, and the numeric comparison is left to
// answer for it alone. That is the conservative direction: it can only fail to
// flag, never flag something the source tool would execute.
func outOfOrderSourceVersions(pending, applied []int64, sourceVersions map[int64]string) []int64 {
	if len(sourceVersions) == 0 {
		return nil
	}
	highest, ok := highestAppliedSourceVersion(applied, sourceVersions)
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

func highestAppliedSourceVersion(applied []int64, sourceVersions map[int64]string) (string, bool) {
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

// OutOfOrderError reports pending migrations that linear execution refuses.
type OutOfOrderError struct {
	CurrentVersion int64
	Versions       []int64
	// SourceVersions carries the source tool's own version token for the
	// current version and for every version listed, for a directory read
	// through a foreign layout. It is empty for a native Atlas directory.
	//
	// It is reported because the int64 is not a name anyone can look up: it
	// appears in no file name and in no Atlas output, so a refusal that only
	// prints 4611686018427836747 does not tell an operator which file to move.
	SourceVersions map[int64]string
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
	return fmt.Sprintf(
		"out-of-order pending migrations for current version %s: %s %s",
		e.describe(e.CurrentVersion),
		strings.Join(listed, ", "),
		escape,
	)
}

func (e *OutOfOrderError) describe(version int64) string {
	source, ok := e.SourceVersions[version]
	if !ok {
		return strconv.FormatInt(version, 10)
	}
	return fmt.Sprintf("%d (source version %q)", version, source)
}

// NewOutOfOrderError builds the typed error returned for linear execution when
// pending migrations the execution order refuses are present.
func NewOutOfOrderError(currentVersion int64, versions []int64) *OutOfOrderError {
	return &OutOfOrderError{
		CurrentVersion: currentVersion,
		Versions:       slices.Clone(versions),
	}
}

// NewOutOfOrderSourceError builds the same error for a directory read through a
// foreign layout, carrying the source version tokens needed to name the files
// the refusal is about.
func NewOutOfOrderSourceError(currentVersion int64, versions []int64, sourceVersions map[int64]string) *OutOfOrderError {
	err := NewOutOfOrderError(currentVersion, versions)
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
