package descriptor

// Capabilities states how core/platform/capability chooses a capability preset
// for a dialect: the one a dialect name selects offline, the product names a
// server's version string carries, and the ladder a server version climbs.
//
// A preset is named, not spelled out here: every name is one of the presets
// core/platform/capability ships, as capability.NamedPresets lists them. The
// enumerated values are spelled as that package spells its VersionScheme and
// VersionAttribution values. That package checks every value when it reads
// a descriptor; see capability.NewResolver.
//
// The zero value states nothing: the dialect has no preset, and a capability
// question about it answers with the conservative empty set.
type Capabilities struct {
	// DefaultPreset names the preset a dialect name selects without a server
	// to ask: the current supported release line, such as "Postgres17".
	DefaultPreset string `json:"default_preset,omitempty"`
	// BannerTokens are lowercase texts whose presence in a server version
	// string names this product, such as "cockroachdb" or "-yb-". A version
	// string is compared with them ignoring case. A product whose version
	// string never names it leaves them empty; its ladder is then reached
	// only through the dialect the caller declared.
	//
	// A token may also appear in the banner of a product in the same family:
	// YugabyteDB reports "PostgreSQL 15.12-YB-...". Where two products match,
	// a family member wins over the head of its family.
	BannerTokens []string `json:"banner_tokens,omitempty"`
	// Versions is the ladder a server version climbs. The zero value is no
	// ladder: every version receives DefaultPreset.
	Versions Versions `json:"versions"`
}

// Versions is a version ladder: how a version is read out of the string a
// server reports, which preset each version receives, and which versions
// were measured.
type Versions struct {
	// Scheme is how the version is read out of the string, such as "dotted"
	// for the first dotted number in it. Empty means the dialect has no
	// ladder, and then every other field is empty too.
	Scheme string `json:"scheme,omitempty"`
	// Floor names the preset a version below every step receives.
	Floor string `json:"floor,omitempty"`
	// Steps are the versions at which the preset changes, oldest first. A
	// version receives the preset of the newest step at or below it.
	Steps []VersionStep `json:"steps,omitempty"`
	// Attribution is how a version is credited to a measured release line,
	// such as "measured-lines".
	Attribution string `json:"attribution,omitempty"`
	// Measured are the release lines a server was measured on, as
	// "major.minor". Only the "measured-lines" attribution reads them.
	Measured []string `json:"measured,omitempty"`
	// Newest is the newest measured release line. A version past it is
	// reported as past the top of the ladder: the preset it receives is a
	// stand-in, not a measurement. Empty means no line is the newest, and no
	// version is past it.
	//
	// Raising it claims that a newer server line behaves like the preset it
	// lands on. Do it in the change that measures that line, never as a side
	// effect of bumping a container tag.
	Newest string `json:"newest,omitempty"`
	// Unreadable is the answer for a string the scheme cannot read a version
	// out of.
	Unreadable UnreadableVersion `json:"unreadable"`
}

// VersionStep is one step of a version ladder.
type VersionStep struct {
	// Min is the oldest version the step covers, as "major", "major.minor"
	// or "major.minor.patch".
	Min string `json:"min"`
	// Preset names the preset the step selects.
	Preset string `json:"preset"`
}

// UnreadableVersion is a ladder's answer for a string that names no version
// it can read.
type UnreadableVersion struct {
	// Preset names the preset such a string receives. Empty means
	// Capabilities.DefaultPreset.
	Preset string `json:"preset,omitempty"`
	// Recognized reports such a string as one that named the server even so,
	// as a product banner with no number in it does.
	Recognized bool `json:"recognized,omitempty"`
	// ReportNewest reports the ladder's newest measured line even so.
	ReportNewest bool `json:"report_newest,omitempty"`
}

// IdentifierLimit is the longest identifier an engine accepts, and the unit
// the length counts. The zero value states no limit. Unit is spelled as
// core/platform/capability spells its IdentifierUnit values, "bytes" or
// "characters", and that package checks it.
type IdentifierLimit struct {
	// Max is the limit, or zero when no limit is stated.
	Max int `json:"max,omitempty"`
	// Unit is what Max counts, empty exactly when Max is zero.
	Unit string `json:"unit,omitempty"`
}
