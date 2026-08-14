package migrator

import "fmt"

// AtlasRevisionType identifies how an Atlas-format revision row was recorded.
type AtlasRevisionType uint

// AtlasRevisionOrderIdentity carries the source-order facts available for one
// exact Atlas revision identity. RevisionVersion is the opaque persisted key;
// AtlasType and OperatorVersion preserve source-role markers when the writer
// recorded them. Repeatable is true only for a migration the current provider
// owns, because Atlas revision rows do not persist that file role.
//
// Compatibility adapters use this value only to compare retired metadata with
// a selected migration. A comparator must return unavailable when these facts
// cannot prove the source order; opaque identity bytes are not an ordering
// fallback.
type AtlasRevisionOrderIdentity struct {
	RevisionVersion string
	AtlasType       AtlasRevisionType
	OperatorVersion string
	Repeatable      bool
}

// AtlasRevisionVersionComparator compares a retired exact revision identity on
// the left with the selected provider migration on the right. The integer is
// negative when the retired identity belongs before the target, positive when
// it belongs after the target, and zero when they are identical. The bool
// result is false when persisted metadata lacks the role or tie-breaker needed
// to establish that relationship.
type AtlasRevisionVersionComparator func(
	left, right AtlasRevisionOrderIdentity,
) (order int, ok bool)

// AtlasRevisionChange identifies a migration added to or removed from Atlas
// revision history. Version is its numeric order key; RevisionVersion is the
// exact persisted identity, including an empty identity or one no longer owned
// by a file.
type AtlasRevisionChange struct {
	Version         int64
	RevisionVersion string
	Description     string
}

// AtlasRevisionSetResult describes the migrations changed by SetAtlasRevision.
// Set and Removed are ordered by ascending version.
type AtlasRevisionSetResult struct {
	CurrentVersion int64
	Set            []AtlasRevisionChange
	Removed        []AtlasRevisionChange
}

const (
	// AtlasRevisionTypeUnknown is the zero value used when no Atlas revision
	// type is available.
	AtlasRevisionTypeUnknown AtlasRevisionType = iota
	// AtlasRevisionTypeBaseline marks a revision recorded by --baseline.
	AtlasRevisionTypeBaseline
	// AtlasRevisionTypeApplied marks a normally executed revision.
	AtlasRevisionTypeApplied
	_
	// AtlasRevisionTypeManuallySet marks a revision recorded by migrate set.
	AtlasRevisionTypeManuallySet
)

// String returns the label exposed by Atlas migrate status templates.
func (t AtlasRevisionType) String() string {
	switch t {
	case AtlasRevisionTypeBaseline:
		return "baseline"
	case AtlasRevisionTypeApplied:
		return "applied"
	case AtlasRevisionTypeBaseline | AtlasRevisionTypeApplied:
		return "applied"
	case AtlasRevisionTypeManuallySet:
		return "manually set"
	case AtlasRevisionTypeApplied | AtlasRevisionTypeManuallySet:
		return "applied + manually set"
	case AtlasRevisionTypeBaseline | AtlasRevisionTypeApplied | AtlasRevisionTypeManuallySet:
		return "manually set"
	default:
		return fmt.Sprintf("unknown (%04b)", uint(t))
	}
}

// sqlArg keeps revision types numeric at driver boundaries. Some drivers
// prefer Stringer text over the underlying integer when given the named type.
func (t AtlasRevisionType) sqlArg() uint64 {
	return uint64(t)
}
