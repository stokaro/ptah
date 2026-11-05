package capabilityprobe

import (
	"ptah.run/core/platform/capability"
)

// withCoordinationNodes declares the key that names whether Ptah manages
// YDB's coordination nodes. No statement can ask an engine about an object it
// does not have, and YQL has no statement for a node either, so the key is
// declared on every engine here; the YDB provider states it for its own
// lines.
func withCoordinationNodes(p plan) plan {
	if p.undecided == nil {
		p.undecided = make(map[capability.Capability]string)
	}
	p.undecided[capability.CoordinationNodes] = "the key names whether Ptah manages YDB coordination nodes, which " +
		"only YDB has and which Ptah creates through a statement of its own; no statement asks this engine " +
		"about an object it does not have"
	return p
}
