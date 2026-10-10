package capabilityprobe

import "ptah.run/core/platform/capability"

// withRoutineReplacement declares the key that says how Ptah's plan replaces a
// modified routine. It is the planner's choice, DROP and CREATE or CREATE OR
// REPLACE, and not something a server answers, so every engine declares it.
func withRoutineReplacement(p plan) plan {
	if p.undecided == nil {
		p.undecided = make(map[capability.Capability]string)
	}
	p.undecided[capability.RoutineReplacementResetsDefiner] = "the key states how Ptah's plan replaces a modified " +
		"routine, which the planner chooses; no statement asks the server about it"
	return p
}
