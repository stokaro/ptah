package capabilityprobe

import "ptah.run/core/platform/capability"

// withSessionModeKeys declares the keys a connector reads off the server when
// it connects: the MySQL family's sql_mode and Oracle's MAX_STRING_SIZE. No
// preset holds either, and the probe's servers run the defaults, so a
// statement the probe sends would only confirm what every preset already
// says; every engine declares them.
func withSessionModeKeys(p plan) plan {
	if p.undecided == nil {
		p.undecided = make(map[capability.Capability]string)
	}
	p.undecided[capability.NoBackslashEscapes] = "the connector sets the key from the session's sql_mode, " +
		"which the probe's servers leave at the default; no preset holds it"
	p.undecided[capability.StandardStringSize] = "the Oracle connector sets the key from the database's " +
		"MAX_STRING_SIZE, which the probe's servers leave at its default; no preset holds it"
	return p
}
