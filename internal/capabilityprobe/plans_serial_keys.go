package capabilityprobe

import (
	"ptah.run/core/platform/capability"
)

// withSerialKeys declares the keys about a Serial column's sequence: whether
// Ptah changes its start and increment, and whether a 16-bit or 32-bit
// Serial's sequence keeps ending where its column does through that change.
// The first names whether Ptah's planner compares and plans a serial's
// sequence, which no built-in planner does, and the second presupposes it, so
// both are declared rather than asked: what an engine's ALTER SEQUENCE does
// says nothing about what that dialect's planner plans. The YDB provider
// states both for its own lines.
func withSerialKeys(p plan) plan {
	if p.undecided == nil {
		p.undecided = make(map[capability.Capability]string)
	}
	p.undecided[capability.SerialSequenceOptions] = "the key names whether Ptah's planner compares a serial " +
		"column's sequence start and increment and plans ALTER SEQUENCE for a change, which no built-in planner " +
		"does; this server altering a sequence or not says nothing about what this dialect's planner plans"
	p.undecided[capability.SerialSequenceKeepsRange] = "the key says whether Ptah may change a 16-bit or " +
		"32-bit Serial's sequence, and presupposes serial_sequence_options, which no built-in planner has"
	return p
}
