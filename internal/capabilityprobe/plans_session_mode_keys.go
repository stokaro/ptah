package capabilityprobe

import "ptah.run/core/platform/capability"

// withSessionModeKeys declares the key a MySQL-family connector reads off the
// session's sql_mode. No preset holds it, and the probe's servers run the
// default sql_mode, so a statement the probe sends would only confirm what
// every preset already says; every engine declares it.
func withSessionModeKeys(p plan) plan {
	if p.undecided == nil {
		p.undecided = make(map[capability.Capability]string)
	}
	p.undecided[capability.NoBackslashEscapes] = "the connector sets the key from the session's sql_mode, " +
		"which the probe's servers leave at the default; no preset holds it"
	return p
}
