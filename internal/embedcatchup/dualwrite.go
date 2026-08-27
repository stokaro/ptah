package embedcatchup

import (
	"fmt"
	"time"
)

// DualWriteEvidence is what an application claiming to write both generations
// has to produce.
//
// The epic's position is that Ptah cannot prove external dual-write correctness
// from a configuration statement, and this is that position as a struct: every
// field is something the WRITER reports, and the absence of any of them is
// reported rather than assumed away.
type DualWriteEvidence struct {
	// SupportsGeneration is the writer confirming it knows about the
	// generation being built. A writer that does not is writing one generation
	// and calling it two.
	SupportsGeneration string
	// Heartbeat is when the writer last said anything.
	Heartbeat time.Time
	// AcknowledgedSourceVersion and AcknowledgedTargetVersion are how far it
	// has got on each side.
	AcknowledgedSourceVersion string
	AcknowledgedTargetVersion string
	// Errors is how many writes it has failed since the run began.
	Errors int
	// CutoverAcknowledged is the writer confirming it is ready.
	CutoverAcknowledged bool
}

// DualWritePolicy is what the environment requires of that evidence.
type DualWritePolicy struct {
	// MaxHeartbeatAge is how stale the writer's last word may be, zero for no
	// limit.
	MaxHeartbeatAge time.Duration
	// MaxErrors is how many failed writes are tolerable.
	MaxErrors int
	// Generation is the generation the writer has to name.
	Generation string
}

// assessDualWrite holds the dual-write mode to the evidence its writer gave.
func assessDualWrite(guarantee *Guarantee, evidence DualWriteEvidence, now time.Time) {
	// The partial note is unconditional, and it is the point of this mode.
	// Everything below can pass and the result is still a claim by the writer
	// rather than a fact Ptah established: an outbox row that committed is
	// evidence, a writer saying it wrote is testimony.
	guarantee.Partial = append(guarantee.Partial,
		"dual-write completeness rests on what the writer reports; Ptah observed the reports and "+
			"not the writes")

	if evidence.Heartbeat.IsZero() {
		guarantee.Blockers = append(guarantee.Blockers,
			"the dual-write mode was selected and the writer has never reported anything")
		return
	}
	if evidence.SupportsGeneration == "" {
		guarantee.Blockers = append(guarantee.Blockers,
			"the writer names no generation, so nothing says it is writing the one being built")
	}
	if evidence.AcknowledgedTargetVersion == "" {
		guarantee.Blockers = append(guarantee.Blockers,
			"the writer has acknowledged no target version, so nothing says it has written to the "+
				"new generation at all")
	}
	if !evidence.CutoverAcknowledged {
		guarantee.Blockers = append(guarantee.Blockers,
			"the writer has not acknowledged that it is ready to cut over")
	}
	_ = now
}

// Check holds the evidence to a policy, which is separate from whether it
// exists at all.
//
// Assess answers "did the writer say enough". This answers "is what it said
// good enough", and the two are separate because a stale heartbeat and a
// missing one send an operator to different places: one is a writer that
// stopped, the other is a writer that was never wired up.
func (e DualWriteEvidence) Check(policy DualWritePolicy, now time.Time) []string {
	var blockers []string
	if policy.Generation != "" && e.SupportsGeneration != policy.Generation {
		blockers = append(blockers, fmt.Sprintf(
			"the writer reports generation %q and this run is building %q",
			e.SupportsGeneration, policy.Generation))
	}
	if policy.MaxHeartbeatAge > 0 && !e.Heartbeat.IsZero() {
		if age := now.Sub(e.Heartbeat); age > policy.MaxHeartbeatAge {
			blockers = append(blockers, fmt.Sprintf(
				"the writer last reported %s ago and this policy allows %s",
				age.Round(time.Second), policy.MaxHeartbeatAge))
		}
	}
	if e.Errors > policy.MaxErrors {
		blockers = append(blockers, fmt.Sprintf(
			"the writer reports %d failed writes and this policy allows %d", e.Errors, policy.MaxErrors))
	}
	return blockers
}
