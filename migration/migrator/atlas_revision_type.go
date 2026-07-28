package migrator

import "fmt"

// AtlasRevisionType identifies how an Atlas-format revision row was recorded.
type AtlasRevisionType uint

// AtlasRevisionChange identifies a migration added to or removed from Atlas
// revision history.
type AtlasRevisionChange struct {
	Version     int64
	Description string
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
	case AtlasRevisionTypeManuallySet:
		return "manually set"
	case AtlasRevisionTypeApplied | AtlasRevisionTypeManuallySet:
		return "applied + manually set"
	default:
		return fmt.Sprintf("unknown (%04b)", uint(t))
	}
}
